// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//! Public manifest commit contracts, without Hadoop, JNI, or external storage.
//!
//! The canonical manifest writer and reader exercise actual serialized files.
//! InMemory supplies atomic Create semantics at the backend trait boundary; a
//! live HDFS/JNI rename gate is an additional, separate integration requirement.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance_core::datatypes::Schema;
use lance_file::version::LanceFileVersion;
use lance_io::object_store::providers::uses_hdfs_native_backend;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, StorageOptionsAccessor,
};
use lance_table::format::{DataStorageFormat, Manifest};
use lance_table::io::commit::{
    CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestNamingScheme,
    RenameCommitHandler, commit_handler_from_url, write_manifest_file_to_path,
};
use lance_table::io::manifest::read_manifest;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use rstest::rstest;
use url::Url;

fn manifest(version: u64, tag: &str) -> Manifest {
    let schema = ArrowSchema::new(vec![ArrowField::new("i", DataType::Int32, false)]);
    let mut manifest = Manifest::new(
        Schema::try_from(&schema).unwrap(),
        Arc::new(vec![]),
        DataStorageFormat::new(LanceFileVersion::Stable.resolve()),
        HashMap::new(),
    );
    manifest.version = version;
    manifest.tag = Some(tag.into());
    manifest
}

fn tagged_memory_store(
    scheme: &str,
    backend: Option<&str>,
) -> (LanceObjectStore, ObjectStoreParams) {
    let options = backend
        .map(|value| HashMap::from([("hdfs_backend".into(), value.into())]))
        .unwrap_or_default();
    let params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            options.clone(),
        ))),
        ..Default::default()
    };
    let store = LanceObjectStore::new(
        Arc::new(InMemory::new()),
        Url::parse(&format!("{scheme}://contract/warehouse/table.lance")).unwrap(),
        Some(4096),
        None,
        false,
        true,
        4,
        0,
        Some(&options),
    );
    (store, params)
}

#[rstest]
#[case::native_hdfs("hdfs", None)]
#[case::explicit_native_hdfs("hdfs", Some("native"))]
#[case::jni_hdfs("hdfs", Some("jni"))]
#[case::case_insensitive_jni_hdfs("hdfs", Some("JNI"))]
#[case::explicit_jni_scheme("hdfs-jni", None)]
#[case::qbfs("qbfs", None)]
#[case::memory("memory", None)]
#[tokio::test]
async fn selected_commit_handler_preserves_canonical_manifest_and_conflict(
    #[case] scheme: &str,
    #[case] backend: Option<&str>,
    #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)] naming: ManifestNamingScheme,
) {
    let (store, params) = tagged_memory_store(scheme, backend);
    let use_rename = scheme == "hdfs" && uses_hdfs_native_backend(Some(&params));
    let handler = commit_handler_from_url(
        &format!("{scheme}://contract/warehouse/table.lance"),
        &Some(params),
    )
    .await
    .unwrap();
    assert_eq!(
        format!("{handler:?}"),
        if use_rename {
            "RenameCommitHandler"
        } else {
            "ConditionalPutCommitHandler"
        },
    );
    let base = Path::from("warehouse/table.lance");
    let unrelated = base.clone().join("_versions/unrelated.manifest-private");
    store
        .inner
        .put(&unrelated, b"keep".as_slice().into())
        .await
        .unwrap();
    let mut first = manifest(7, "winner");
    let location = handler
        .commit(
            &mut first,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            naming,
            None,
        )
        .await
        .unwrap();
    assert_eq!(location.version, 7);
    assert_eq!(location.path, naming.manifest_path(&base, 7));
    assert_eq!(location.naming_scheme, naming);
    assert_eq!(
        naming.parse_version(location.path.filename().unwrap()),
        Some(7),
    );
    let bytes_before = store
        .inner
        .get(&location.path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(location.size, Some(bytes_before.len() as u64));
    let decoded = read_manifest(&store, &location.path, location.size)
        .await
        .unwrap();
    assert_eq!(decoded.version, 7);
    assert_eq!(decoded.tag.as_deref(), Some("winner"));
    assert_eq!(decoded.schema, first.schema);
    let mut conflicting = manifest(7, "loser");
    let error = handler
        .commit(
            &mut conflicting,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            naming,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CommitError::CommitConflict));
    assert_eq!(format!("{error:?}"), "CommitConflict");
    assert_eq!(
        store
            .inner
            .get(&location.path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        bytes_before
    );
    assert_eq!(
        store
            .inner
            .get(&unrelated)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        b"keep".as_slice()
    );
    let mut newer = manifest(10, "next");
    let next_location = handler
        .commit(
            &mut newer,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            naming,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        handler
            .resolve_latest_location(&base, &store)
            .await
            .unwrap()
            .version,
        10
    );
    assert_eq!(
        read_manifest(&store, &next_location.path, None)
            .await
            .unwrap()
            .tag
            .as_deref(),
        Some("next")
    );
    let mut paths = store
        .inner
        .list(Some(&base))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    paths.sort();
    let mut expected = vec![location.path, next_location.path, unrelated];
    expected.sort();
    assert_eq!(
        paths, expected,
        "failed commit must remove only its own private staging object"
    );
}

#[rstest]
#[case::rename(true)]
#[case::conditional_put(false)]
#[tokio::test]
async fn concurrent_canonical_manifest_writers_have_exactly_one_winner(
    #[case] use_rename: bool,
    #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)] naming: ManifestNamingScheme,
) {
    let store = LanceObjectStore::memory();
    let base = Path::from("table.lance");
    let handler: Arc<dyn CommitHandler> = if use_rename {
        Arc::new(RenameCommitHandler)
    } else {
        Arc::new(ConditionalPutCommitHandler)
    };
    let mut first = manifest(1, "first");
    let mut second = manifest(1, "second");
    let (first_result, second_result) = tokio::join!(
        handler.commit(
            &mut first,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            naming,
            None
        ),
        handler.commit(
            &mut second,
            None,
            &base,
            &store,
            write_manifest_file_to_path,
            naming,
            None
        ),
    );
    let winner_tag = match (&first_result, &second_result) {
        (Ok(_), Err(CommitError::CommitConflict)) => "first",
        (Err(CommitError::CommitConflict), Ok(_)) => "second",
        _ => panic!(
            "expected one successful commit and one conflict: {first_result:?}, {second_result:?}"
        ),
    };
    let path = naming.manifest_path(&base, 1);
    let decoded = read_manifest(&store, &path, None).await.unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.tag.as_deref(), Some(winner_tag));
    let locations = handler
        .list_manifest_locations(&base, &store, false)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(locations.len(), 1);
    assert_eq!(locations[0].version, 1);
    assert_eq!(locations[0].path, path);
    assert!(
        store
            .inner
            .list(Some(&base))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .all(|meta| !meta.location.as_ref().contains(".manifest-"))
    );
}
