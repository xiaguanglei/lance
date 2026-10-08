// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//! Real local native/JNI provider contracts. QBFS here verifies provider
//! dispatch to the ordinary Hadoop MiniDFS, not a deployed QBFS plugin.

#![cfg(all(feature = "hdfs-test", feature = "hdfs-jni", feature = "qbfs"))]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use hdfs_native::minidfs::MiniDfs;
use lance_io::object_store::providers::uses_hdfs_native_backend;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use lance_io::traits::Writer;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn public_hdfs_factories_publish_safe_writes_and_protect_concurrent_create() {
    // MiniDfs::default enables no security or HA features. It owns the local
    // server and its XML configuration; credentials/CLASSPATH come only from
    // the external, explicit test command. Missing JNI classes must fail here.
    assert!(std::env::var_os("LANCE_DISABLE_SAFE_WRITE").is_none());
    assert!(std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none());
    let dfs = MiniDfs::default();
    assert_eq!(dfs.url, "hdfs://127.0.0.1:9000");
    let registry = Arc::new(ObjectStoreRegistry::default());
    let owned_root = format!("lance-backport-contract-{}", Uuid::new_v4().simple());

    for (mode, scheme, backend, expected_native) in [
        ("native", "hdfs", None, true),
        ("jni", "hdfs", Some("jni"), false),
        ("qbfs-dispatch", "qbfs", None, false),
    ] {
        let mut options = HashMap::from([("name_node".to_string(), dfs.url.clone())]);
        if let Some(backend) = backend {
            options.insert("hdfs_backend".into(), backend.into());
        }
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        };
        // QBFS has its own JNI registry route; the helper is specifically the
        // native/JNI decision for the hdfs scheme.
        if scheme == "hdfs" {
            assert_eq!(uses_hdfs_native_backend(Some(&params)), expected_native);
        }
        let prefix = Path::parse(format!("{owned_root}/{mode}/scope%25tenant")).unwrap();
        let ordinary = Path::parse(format!("{prefix}/memory%25.lance")).unwrap();
        let uri = format!("{scheme}://127.0.0.1:9000/{ordinary}");
        let (store, extracted) = ObjectStore::from_uri_and_params(registry.clone(), &uri, &params)
            .await
            .unwrap_or_else(|error| panic!("{mode} real provider initialization failed: {error}"));
        assert_eq!(store.scheme(), scheme);
        assert_eq!(extracted, ordinary);
        assert!(store.needs_safe_write());

        // Cross the real multipart boundary rather than proving visibility
        // solely while a tiny payload is still in ObjectWriter's memory buffer.
        let payload = vec![0x6d; 5 * 1024 * 1024 + 37];
        let mut writer = store.create(&ordinary).await.unwrap();
        writer.write_all(&payload).await.unwrap();
        writer.flush().await.unwrap();
        assert!(matches!(
            store.inner.head(&ordinary).await,
            Err(object_store::Error::NotFound { .. })
        ));
        let result = Writer::shutdown(writer.as_mut()).await.unwrap();
        assert_eq!(result.size, payload.len());
        AsyncWriteExt::shutdown(writer.as_mut()).await.unwrap();
        let reader = store.open(&ordinary).await.unwrap();
        assert_eq!(reader.get_all().await.unwrap().as_ref(), payload.as_slice());
        drop(reader);

        let transaction = Path::parse(format!("{prefix}/transaction%25.txn")).unwrap();
        let unrelated =
            Path::parse(format!("{prefix}/transaction%25.txn.tmp.someone-else")).unwrap();
        store
            .inner
            .put(&unrelated, Bytes::from_static(b"unrelated").into())
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            store.put_if_absent(&transaction, Bytes::from_static(b"first").into()),
            store.put_if_absent(&transaction, Bytes::from_static(b"second").into()),
        );
        let winner = match (first, second) {
            (Ok(()), Err(object_store::Error::AlreadyExists { path, .. })) => {
                assert_eq!(path, transaction.as_ref());
                b"first".as_slice()
            }
            (Err(object_store::Error::AlreadyExists { path, .. }), Ok(())) => {
                assert_eq!(path, transaction.as_ref());
                b"second".as_slice()
            }
            outcomes => panic!("{mode} expected one winner and one conflict: {outcomes:?}"),
        };
        let failure = store
            .put_if_absent(&transaction, Bytes::from_static(b"overwrite loser").into())
            .await
            .unwrap_err();
        assert!(matches!(
            &failure,
            object_store::Error::AlreadyExists { path, .. } if path == transaction.as_ref()
        ));
        assert!(failure.to_string().contains(transaction.as_ref()));
        assert_eq!(
            store
                .open(&transaction)
                .await
                .unwrap()
                .get_all()
                .await
                .unwrap()
                .as_ref(),
            winner
        );
        assert_eq!(
            store
                .open(&unrelated)
                .await
                .unwrap()
                .get_all()
                .await
                .unwrap()
                .as_ref(),
            b"unrelated".as_slice()
        );

        let mut actual = store
            .inner
            .list(Some(&prefix))
            .map_ok(|metadata| metadata.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut expected = vec![ordinary, transaction, unrelated];
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{mode} left an owned staging object");
    }
}
