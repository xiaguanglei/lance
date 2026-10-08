// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset-level compatibility over an owned, unauthenticated Hadoop MiniDFS.
//! Enable `hdfs-test` explicitly; ordinary HDFS builds do not start Java.

#![cfg(feature = "hdfs-test")]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Int32Type};
use arrow_array::{Float32Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use futures::TryStreamExt;
use hdfs_native::minidfs::MiniDfs;
use lance::Dataset;
use lance::dataset::scanner::ColumnOrdering;
use lance::dataset::{UpdateBuilder, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_datagen::{ByteCount, Dimension, RowCount, array, gen_batch};
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_io::object_store::providers::uses_hdfs_native_backend;
use lance_linalg::distance::MetricType;
use uuid::Uuid;

fn batch(start: i32, rows: usize, payload_bytes: u64) -> RecordBatch {
    gen_batch()
        .col("id", array::step_custom::<Int32Type>(start, 1))
        .col("value", array::step_custom::<Int32Type>(0, 0))
        .col(
            "payload",
            array::rand_fixedbin(ByteCount::from(payload_bytes), false),
        )
        .col(
            "vector",
            array::rand_vec::<Float32Type>(Dimension::from(16)),
        )
        .into_batch_rows(RowCount::from(rows as u64))
        .unwrap()
}

fn reader(batch: RecordBatch) -> impl RecordBatchReader + Send + 'static {
    let schema = batch.schema();
    RecordBatchIterator::new(vec![Ok(batch)], schema)
}

async fn assert_original_snapshot(dataset: &Dataset, original: &RecordBatch) {
    let scanned = dataset
        .scan()
        .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
            "id".to_string(),
        )]))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(&scanned, original);
}

async fn assert_vector_recall(dataset: &Dataset, query: &Float32Array, original_rows: usize) {
    let mut results = Vec::with_capacity(2);
    for use_index in [false, true] {
        let mut scanner = dataset.scan();
        scanner
            .nearest("vector", query, 10)
            .unwrap()
            .nprobes(2)
            .use_index(use_index)
            .prefilter(true)
            .filter(&format!("id >= 0 AND id < {original_rows}"))
            .unwrap()
            .project(&["id"])
            .unwrap();
        let plan = scanner.explain_plan(false).await.unwrap();
        if use_index {
            assert!(plan.contains("ANNSubIndex"), "expected IVF plan: {plan}");
        } else {
            assert!(
                !plan.contains("ANNSubIndex"),
                "exact oracle used ANN: {plan}"
            );
        }
        let hits = scanner.try_into_batch().await.unwrap();
        assert_eq!(hits.num_rows(), 10);
        let ids = hits["id"].as_primitive::<Int32Type>();
        let distances = hits["_distance"].as_primitive::<Float32Type>();
        let by_id = ids
            .values()
            .iter()
            .copied()
            .zip(distances.values().iter().copied())
            .collect::<BTreeMap<_, _>>();
        assert_eq!(by_id.len(), 10, "duplicate logical ids in vector results");
        results.push(by_id);
    }
    assert_eq!(
        results[0].keys().collect::<Vec<_>>(),
        results[1].keys().collect::<Vec<_>>(),
        "all-partition IVF_FLAT must have Recall@10 = 1.0"
    );
    for (id, exact_distance) in &results[0] {
        let indexed_distance = results[1][id];
        assert!(
            (exact_distance - indexed_distance).abs() <= 1e-5,
            "distance mismatch for {id}: {exact_distance} vs {indexed_distance}"
        );
    }
}

async fn assert_value(dataset: &Dataset, expected: i32) {
    let values = dataset
        .scan()
        .filter("id = 9")
        .unwrap()
        .project(&["value"])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        values.num_rows(),
        1,
        "update duplicated or lost logical id 9"
    );
    assert_eq!(
        values["value"]
            .as_primitive::<Int32Type>()
            .values()
            .as_ref(),
        &[expected]
    );
}

#[tokio::test]
#[serial_test::serial]
async fn native_hdfs_dataset_mutations_indices_and_snapshot_versions() {
    assert!(std::env::var_os("LANCE_DISABLE_SAFE_WRITE").is_none());
    assert!(std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none());
    // The helper owns its local XML/server lifecycle. No security/HA feature,
    // keytab command, or real cluster endpoint is selected.
    let dfs = MiniDfs::default();
    assert_eq!(dfs.url, "hdfs://127.0.0.1:9000");
    assert!(uses_hdfs_native_backend(None));
    let owned_root = format!("lance-dataset-backport-{}", Uuid::new_v4().simple());

    for (format, v2_manifests, rows, payload_bytes) in [
        ("v1", false, 512usize, 32u64),
        ("v2", true, 8192usize, 2048u64),
    ] {
        let uri = format!("{}/{owned_root}/scope%25tenant/{format}%25.lance", dfs.url);
        let original = batch(0, rows, payload_bytes);
        let query = original["vector"]
            .as_fixed_size_list()
            .value(77)
            .as_primitive::<Float32Type>()
            .clone();
        let params = WriteParams {
            max_rows_per_file: rows / 2,
            enable_v2_manifest_paths: v2_manifests,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader(original.clone()), uri.as_str(), Some(params))
            .await
            .unwrap();
        assert_eq!(dataset.version().version, 1);
        assert_eq!(dataset.get_fragments().len(), 2);
        assert_eq!(dataset.count_rows(None).await.unwrap(), rows);
        let store = dataset.object_store(None).await.unwrap();
        assert_eq!(store.scheme(), "hdfs");
        assert!(store.needs_safe_write());
        if v2_manifests {
            // Random binary bodies are incompressible. Assert actual stored
            // size, not just input size, to exercise native large-file reads.
            let files = store
                .inner
                .list(Some(&dataset.data_dir()))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(files.len(), 2);
            assert!(files.iter().all(|file| file.size > 5 * 1024 * 1024));
        }
        let original_snapshot = Dataset::open(&uri).await.unwrap();
        assert_original_snapshot(&original_snapshot, &original).await;

        dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_btree".to_string()),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                false,
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_ivf_flat".to_string()),
                &VectorIndexParams::ivf_flat(2, MetricType::L2),
                false,
            )
            .await
            .unwrap();
        assert_eq!(dataset.load_indices().await.unwrap().len(), 2);
        let indexed_snapshot = Dataset::open(&uri).await.unwrap();
        assert_eq!(indexed_snapshot.version().version, 3);
        let mut scalar_scan = indexed_snapshot.scan();
        scalar_scan
            .filter("id = 77")
            .unwrap()
            .project(&["id"])
            .unwrap();
        let scalar_plan = scalar_scan.explain_plan(false).await.unwrap();
        assert!(
            scalar_plan.contains("ScalarIndexQuery"),
            "BTree not used: {scalar_plan}"
        );
        assert_eq!(
            scalar_scan.try_into_batch().await.unwrap()["id"]
                .as_primitive::<Int32Type>()
                .values()
                .as_ref(),
            &[77]
        );
        assert_vector_recall(&indexed_snapshot, &query, rows).await;

        let appended = batch(rows as i32, 128, payload_bytes);
        let appended_query = appended["vector"]
            .as_fixed_size_list()
            .value(17)
            .as_primitive::<Float32Type>()
            .clone();
        dataset.append(reader(appended), None).await.unwrap();
        assert_eq!(dataset.version().version, 4);
        assert_eq!(dataset.count_rows(None).await.unwrap(), rows + 128);
        assert_eq!(
            dataset
                .count_rows(Some(format!("id >= {rows}")))
                .await
                .unwrap(),
            128
        );
        // Original index fragments do not cover appended rows. Public scalar
        // and vector queries must still include the uncovered fragments.
        let appended_hit = dataset
            .scan()
            .nearest("vector", &appended_query, 1)
            .unwrap()
            .prefilter(true)
            .filter(&format!("id >= {rows}"))
            .unwrap()
            .project(&["id"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(appended_hit.num_rows(), 1);
        assert_eq!(
            appended_hit["id"].as_primitive::<Int32Type>().value(0),
            rows as i32 + 17
        );
        assert_eq!(
            appended_hit["_distance"]
                .as_primitive::<Float32Type>()
                .value(0),
            0.0
        );
        assert_vector_recall(&dataset, &query, rows).await;
        let updated = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id = 9")
            .unwrap()
            .set("value", "value + 1")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(updated.rows_updated, 1);
        dataset = updated.new_dataset.as_ref().clone();
        assert_eq!(dataset.version().version, 5);
        assert_value(&dataset, 1).await;
        let deleted = dataset.delete("id < 2").await.unwrap();
        assert_eq!(deleted.num_deleted_rows, 2);
        assert_eq!(dataset.version().version, 6);
        assert_eq!(dataset.count_rows(None).await.unwrap(), rows + 126);
        assert_eq!(
            dataset
                .count_rows(Some("id < 2".to_string()))
                .await
                .unwrap(),
            0
        );
        assert_vector_recall(&dataset, &query, rows).await;

        // Both independent handles start from exactly the same head. Append
        // commits can rebase: both disjoint rows must survive, once each.
        let mut left = Dataset::open(&uri).await.unwrap();
        let mut right = Dataset::open(&uri).await.unwrap();
        assert_eq!(left.version().version, right.version().version);
        let (left_append, right_append) = tokio::join!(
            left.append(reader(batch(rows as i32 + 128, 1, payload_bytes)), None),
            right.append(reader(batch(rows as i32 + 129, 1, payload_bytes)), None),
        );
        left_append.unwrap();
        right_append.unwrap();
        dataset = Dataset::open(&uri).await.unwrap();
        assert_eq!(dataset.version().version, 8);
        assert_eq!(dataset.count_rows(None).await.unwrap(), rows + 128);
        assert_eq!(
            dataset
                .count_rows(Some(format!("id >= {}", rows + 128)))
                .await
                .unwrap(),
            2
        );

        // Two stale jobs increment the same logical row. If optimistic commit
        // retries silently lose a write, the value will be 2 instead of 3.
        let before_updates = Arc::new(Dataset::open(&uri).await.unwrap());
        let left_update = UpdateBuilder::new(before_updates.clone())
            .update_where("id = 9")
            .unwrap()
            .set("value", "value + 1")
            .unwrap()
            .build()
            .unwrap();
        let right_update = UpdateBuilder::new(Arc::new(Dataset::open(&uri).await.unwrap()))
            .update_where("id = 9")
            .unwrap()
            .set("value", "value + 1")
            .unwrap()
            .build()
            .unwrap();
        let (left_result, right_result) =
            tokio::join!(left_update.execute(), right_update.execute());
        assert_eq!(left_result.unwrap().rows_updated, 1);
        assert_eq!(right_result.unwrap().rows_updated, 1);
        dataset = Dataset::open(&uri).await.unwrap();
        assert_eq!(dataset.version().version, 10);
        assert_value(&dataset, 3).await;

        let failure = UpdateBuilder::new(before_updates)
            .update_where("id = 9")
            .unwrap()
            .set("value", "value + 1000")
            .unwrap()
            .conflict_retries(0)
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&failure, lance::Error::TooMuchWriteContention { .. }),
            "unexpected zero-retry error: {failure}"
        );
        assert!(failure.to_string().contains("Attempted 0 retries"));
        let latest = Dataset::open(&uri).await.unwrap();
        assert_eq!(latest.version().version, 10);
        assert_value(&latest, 3).await;
        assert_eq!(latest.count_rows(None).await.unwrap(), rows + 128);
        assert_vector_recall(&latest, &query, rows).await;

        // Existing readers retain their immutable snapshot; a fresh handle and
        // explicit historical checkout must agree on the requested version.
        assert_eq!(original_snapshot.version().version, 1);
        assert_original_snapshot(&original_snapshot, &original).await;
        assert_eq!(indexed_snapshot.version().version, 3);
        assert_eq!(indexed_snapshot.count_rows(None).await.unwrap(), rows);
        assert_value(&indexed_snapshot, 0).await;
        let historical = latest.checkout_version(1).await.unwrap();
        assert_original_snapshot(&historical, &original).await;
        assert_eq!(
            latest
                .versions()
                .await
                .unwrap()
                .into_iter()
                .map(|v| v.version)
                .collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
    }
}
