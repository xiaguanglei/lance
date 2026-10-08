// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//! Public IO contracts for the HDFS backport, using actual in-memory uploads.
//!
//! No provider is connected to a NameNode or JVM. URI tags select Lance's
//! production writer policy; the storage and multipart uploads are InMemory.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
#[cfg(any(feature = "hdfs", feature = "hdfs-jni"))]
use lance_io::object_store::providers::ObjectStoreProvider;
#[cfg(feature = "hdfs")]
use lance_io::object_store::providers::hdfs::HdfsStoreProvider;
#[cfg(feature = "hdfs-jni")]
use lance_io::object_store::providers::hdfs_jni::HdfsJniStoreProvider;
use lance_io::object_store::providers::uses_hdfs_native_backend;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, StorageOptionsAccessor, WrappingObjectStore,
};
use lance_io::object_writer::ObjectWriter;
use lance_io::traits::Writer;
use object_store::list::PaginatedListStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    RenameTargetMode, UploadPart,
};
use rstest::rstest;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use url::Url;

/// The wrapper delegates every successful request to the real memory store.
/// Faults are injected at the backend boundary, not by simulating a writer.
#[derive(Debug, Default)]
struct ObservedStore {
    memory: InMemory,
    multipart: AtomicUsize,
    rename: AtomicUsize,
    rename_paths: Mutex<Vec<(Path, Path)>>,
    create_rename: AtomicUsize,
    create_put: AtomicUsize,
    get: AtomicUsize,
    upload_fault: Mutex<Option<UploadFault>>,
    complete: Arc<AtomicUsize>,
    abort: Arc<AtomicUsize>,
    fail_rename: AtomicBool,
    block_rename: AtomicBool,
    rename_entered: Notify,
    rename_release: Notify,
}

impl fmt::Display for ObservedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObservedInMemory")
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if matches!(options.mode, PutMode::Create) {
            self.create_put.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(*self.upload_fault.lock().unwrap(), Some(UploadFault::Put)) {
            return Err(upload_error("single PUT denied"));
        }
        self.memory.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.multipart.fetch_add(1, Ordering::SeqCst);
        let fault = *self.upload_fault.lock().unwrap();
        if matches!(fault, Some(UploadFault::Create)) {
            return Err(upload_error("multipart creation denied"));
        }
        let upload = self.memory.put_multipart_opts(location, options).await?;
        Ok(Box::new(ObservedUpload {
            upload,
            fault,
            complete: self.complete.clone(),
            abort: self.abort.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get.fetch_add(1, Ordering::SeqCst);
        self.memory.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.memory.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.memory.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.memory.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.memory.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.rename.fetch_add(1, Ordering::SeqCst);
        self.rename_paths
            .lock()
            .unwrap()
            .push((from.clone(), to.clone()));
        if options.target_mode == RenameTargetMode::Create {
            self.create_rename.fetch_add(1, Ordering::SeqCst);
        }
        self.rename_entered.notify_one();
        if self.block_rename.load(Ordering::SeqCst) {
            self.rename_release.notified().await;
        }
        if self.fail_rename.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "hdfs-contract",
                source: io::Error::new(io::ErrorKind::PermissionDenied, "publication denied")
                    .into(),
            });
        }
        self.memory.rename_opts(from, to, options).await
    }
}

#[derive(Debug, Clone, Copy)]
enum UploadFault {
    Put,
    Create,
    Part,
    Complete,
}

fn upload_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "hdfs-contract",
        source: io::Error::new(io::ErrorKind::PermissionDenied, message).into(),
    }
}

#[derive(Debug)]
struct ObservedUpload {
    upload: Box<dyn MultipartUpload>,
    fault: Option<UploadFault>,
    complete: Arc<AtomicUsize>,
    abort: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for ObservedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let actual = self.upload.put_part(data);
        let fail = matches!(self.fault, Some(UploadFault::Part));
        Box::pin(async move {
            actual.await?;
            if fail {
                Err(upload_error("multipart part denied"))
            } else {
                Ok(())
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.complete.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, Some(UploadFault::Complete)) {
            return Err(upload_error("multipart completion denied"));
        }
        self.upload.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.abort.fetch_add(1, Ordering::SeqCst);
        self.upload.abort().await
    }
}

fn tagged_store(
    store: Arc<ObservedStore>,
    scheme: &str,
    backend: Option<&str>,
) -> LanceObjectStore {
    let options = backend.map(|backend| HashMap::from([("hdfs_backend".into(), backend.into())]));
    LanceObjectStore::new(
        store,
        Url::parse(&format!("{scheme}://contract/warehouse/table.lance")).unwrap(),
        Some(4096),
        None,
        false,
        true,
        4,
        0,
        options.as_ref(),
    )
}

async fn stored_bytes(store: &dyn ObjectStore, path: &Path) -> Bytes {
    store.get(path).await.unwrap().bytes().await.unwrap()
}

async fn assert_missing(store: &dyn ObjectStore, path: &Path) {
    assert!(matches!(
        store.head(path).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

async fn assert_only_paths(store: &dyn ObjectStore, expected: &[Path]) {
    let mut paths = store
        .list(None)
        .map_ok(|metadata| metadata.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut expected = expected.to_vec();
    paths.sort();
    expected.sort();
    assert_eq!(paths, expected);
}

#[rstest]
#[case::single(false)]
#[case::multipart(true)]
#[tokio::test]
async fn safe_writer_publishes_only_at_either_shutdown(
    #[case] is_multipart: bool,
    #[values("hdfs", "hdfs-jni", "qbfs")] scheme: &str,
    #[values(false, true)] use_lance_shutdown: bool,
) {
    // Run this binary with LANCE_INITIAL_UPLOAD_SIZE unset. Keeping the real
    // multipart threshold avoids changing process-global settings in a test.
    assert!(std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none());
    let observed = Arc::new(ObservedStore::default());
    let store = tagged_store(observed.clone(), scheme, None);
    assert!(store.needs_safe_write());
    let final_path = Path::from("data/final.lance");
    let payload = vec![
        0x5a;
        if is_multipart {
            5 * 1024 * 1024 + 37
        } else {
            37
        }
    ];
    let mut writer = store.create(&final_path).await.unwrap();
    assert_eq!(writer.write(&[]).await.unwrap(), 0);
    writer.write_all(&payload).await.unwrap();
    writer.flush().await.unwrap();
    assert_missing(observed.as_ref(), &final_path).await;
    assert_eq!(Writer::tell(&mut writer).await.unwrap(), payload.len());
    if use_lance_shutdown {
        let result = Writer::shutdown(&mut writer).await.unwrap();
        assert_eq!(result.size, payload.len());
    } else {
        AsyncWriteExt::shutdown(&mut writer).await.unwrap();
    }
    let result = Writer::shutdown(&mut writer).await.unwrap();
    assert_eq!(result.size, payload.len());
    AsyncWriteExt::shutdown(&mut writer).await.unwrap();
    assert_eq!(
        stored_bytes(observed.as_ref(), &final_path).await.as_ref(),
        payload
    );
    assert_eq!(observed.rename.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.multipart.load(Ordering::SeqCst),
        usize::from(is_multipart)
    );
    assert_only_paths(observed.as_ref(), &[final_path]).await;
    let error = writer.write(b"after-close").await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(error.to_string().contains("closed"));
}

#[tokio::test]
async fn ordinary_memory_writer_does_not_gain_hdfs_publication() {
    let observed = Arc::new(ObservedStore::default());
    let store = tagged_store(observed.clone(), "memory", None);
    assert!(!store.needs_safe_write());
    let path = Path::from("data/plain");
    let mut writer = store.create(&path).await.unwrap();
    writer.write_all(b"ordinary").await.unwrap();
    Writer::shutdown(&mut writer).await.unwrap();
    assert_eq!(
        stored_bytes(observed.as_ref(), &path).await,
        b"ordinary".as_slice()
    );
    assert_eq!(observed.rename.load(Ordering::SeqCst), 0);
}

#[rstest]
#[case::same_parent("warehouse/table.lance/data/file.lance", false)]
#[case::no_parent("file.lance", false)]
#[case::space_in_parent("warehouse/with space/file.lance", false)]
#[case::percent_name("warehouse/100%25-file.lance", false)]
#[case::percent_parent("warehouse/100%25-scope/file.lance", false)]
#[case::parsed_percent_name("warehouse/100%25-file.lance", true)]
#[case::parsed_percent_parent("warehouse/scope%252Ftenant/file%25.lance", true)]
fn legacy_staging_helper_keeps_same_parent_and_unique_owned_name(
    #[case] final_name: &str,
    #[case] parse_existing: bool,
) {
    let store = tagged_store(Arc::new(ObservedStore::default()), "hdfs", None);
    let final_path = if parse_existing {
        Path::parse(final_name).unwrap()
    } else {
        Path::from(final_name)
    };
    let first = store.make_temp_write_path(&final_path).unwrap();
    let second = store.make_temp_write_path(&final_path).unwrap();
    assert_ne!(first, second);
    assert_ne!(first, final_path);
    let final_parent = final_path
        .as_ref()
        .rsplit_once('/')
        .map(|(parent, _)| parent);
    assert_eq!(
        first.as_ref().rsplit_once('/').map(|(parent, _)| parent),
        final_parent
    );
    assert_eq!(
        second.as_ref().rsplit_once('/').map(|(parent, _)| parent),
        final_parent
    );
    let prefix = format!(".tmp_{}_", final_path.filename().unwrap());
    for path in [first, second] {
        let name = path.filename().unwrap();
        let suffix = name.strip_prefix(&prefix).expect("legacy staging prefix");
        assert_eq!(suffix.len(), 16);
        assert!(suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}

#[rstest]
#[case::encoded_from_literal(false)]
#[case::parsed_existing_path(true)]
#[tokio::test]
async fn safe_writer_preserves_percent_encoded_target_and_staging_parent(
    #[case] parse_existing: bool,
) {
    let observed = Arc::new(ObservedStore::default());
    let store = tagged_store(observed.clone(), "hdfs", None);
    let name = "warehouse/scope%25tenant/file%25.lance";
    let target = if parse_existing {
        Path::parse(name).unwrap()
    } else {
        Path::from(name)
    };
    let mut writer = store.create(&target).await.unwrap();
    writer.write_all(b"exact target").await.unwrap();
    assert_missing(observed.as_ref(), &target).await;
    Writer::shutdown(writer.as_mut()).await.unwrap();
    assert_eq!(
        stored_bytes(observed.as_ref(), &target).await,
        b"exact target".as_slice()
    );
    assert_eq!(observed.rename.load(Ordering::SeqCst), 1);
    assert_only_paths(observed.as_ref(), &[target]).await;
}

#[test]
fn safe_write_escape_hatch_is_presence_based_and_does_not_disable_atomic_create() {
    const CHILD: &str = "LANCE_HDFS_SAFE_WRITE_CONTRACT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        assert!(std::env::var_os("LANCE_DISABLE_SAFE_WRITE").is_some());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            for scheme in ["hdfs", "hdfs-jni", "qbfs"] {
                let observed = Arc::new(ObservedStore::default());
                let store = tagged_store(observed.clone(), scheme, None);
                assert!(!store.needs_safe_write());
                let path = Path::from("transaction.txn");
                store
                    .put_if_absent(&path, Bytes::from_static(b"winner").into())
                    .await
                    .unwrap();
                let error = store
                    .put_if_absent(&path, Bytes::from_static(b"loser").into())
                    .await
                    .unwrap_err();
                assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
                assert_eq!(
                    stored_bytes(observed.as_ref(), &path).await,
                    b"winner".as_slice()
                );
                assert_only_paths(observed.as_ref(), &[path]).await;
            }
        });
        return;
    }
    assert!(std::env::var_os("LANCE_DISABLE_SAFE_WRITE").is_none());
    for value in ["", "0", "1"] {
        // Command changes only the child's environment. No unsafe mutation of
        // this parallel test process or authentication state is performed.
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "safe_write_escape_hatch_is_presence_based_and_does_not_disable_atomic_create",
            ])
            .env(CHILD, "1")
            .env("LANCE_DISABLE_SAFE_WRITE", value)
            .env_remove("LANCE_INITIAL_UPLOAD_SIZE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated value {value:?} failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn rejected_staging_alias_cannot_overwrite_destination() {
    let observed = Arc::new(ObservedStore::default());
    let store = tagged_store(observed.clone(), "hdfs", None);
    let path = Path::from("data/final");
    observed
        .put(&path, Bytes::from_static(b"existing").into())
        .await
        .unwrap();
    let error = match ObjectWriter::new_with_options(&store, &path, Some(&path)).await {
        Ok(_) => panic!("staging and final path must differ"),
        Err(error) => error,
    };
    assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("staging and final"));
    assert_eq!(
        stored_bytes(observed.as_ref(), &path).await,
        b"existing".as_slice()
    );
}

#[tokio::test]
async fn failed_publication_is_terminal_and_drop_cleans_only_staging() {
    let observed = Arc::new(ObservedStore::default());
    observed.fail_rename.store(true, Ordering::SeqCst);
    let store = tagged_store(observed.clone(), "hdfs", None);
    let staging = Path::from("data/final.tmp.owned");
    let final_path = Path::from("data/final");
    let unrelated = Path::from("data/final.tmp.someone-else");
    for path in [&final_path, &unrelated] {
        observed
            .put(path, Bytes::from_static(b"keep").into())
            .await
            .unwrap();
    }
    let mut writer = ObjectWriter::new_with_options(&store, &staging, Some(&final_path))
        .await
        .unwrap();
    writer.write_all(b"replacement").await.unwrap();
    let first = AsyncWriteExt::shutdown(&mut writer).await.unwrap_err();
    let second = AsyncWriteExt::shutdown(&mut writer).await.unwrap_err();
    assert_eq!(first.kind(), second.kind());
    assert_eq!(first.to_string(), second.to_string());
    assert!(first.to_string().contains("publication denied"));
    assert!(
        Writer::shutdown(&mut writer)
            .await
            .unwrap_err()
            .to_string()
            .contains("publication denied")
    );
    assert!(
        writer
            .write(b"late")
            .await
            .unwrap_err()
            .to_string()
            .contains("publication denied")
    );
    assert_eq!(observed.rename.load(Ordering::SeqCst), 1);
    assert_eq!(
        stored_bytes(observed.as_ref(), &final_path).await,
        b"keep".as_slice()
    );
    assert_eq!(
        stored_bytes(observed.as_ref(), &staging).await,
        b"replacement".as_slice()
    );
    drop(writer);
    tokio::time::timeout(Duration::from_millis(500), async {
        while observed.head(&staging).await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_only_paths(observed.as_ref(), &[final_path, unrelated]).await;
}

#[rstest]
#[case::single_put(UploadFault::Put, "single PUT denied")]
#[case::multipart_creation(UploadFault::Create, "multipart creation denied")]
#[case::multipart_part(UploadFault::Part, "multipart part denied")]
#[case::multipart_completion(UploadFault::Complete, "multipart completion denied")]
#[tokio::test]
async fn failed_upload_cannot_be_forgotten_by_later_shutdown(
    #[case] fault: UploadFault,
    #[case] expected_message: &str,
) {
    assert!(std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none());
    let observed = Arc::new(ObservedStore::default());
    *observed.upload_fault.lock().unwrap() = Some(fault);
    let store = tagged_store(observed.clone(), "hdfs", None);
    let staging = Path::from("data/final.tmp.owned");
    let final_path = Path::from("data/final");
    let mut writer = ObjectWriter::new_with_options(&store, &staging, Some(&final_path))
        .await
        .unwrap();
    let payload = vec![
        0x7c;
        if matches!(fault, UploadFault::Put) {
            37
        } else {
            5 * 1024 * 1024 + 37
        }
    ];
    // A part failure can be observed either by write_all's task reap or by
    // shutdown. It must become terminal in either case, not be reaped and lost.
    let first = match writer.write_all(&payload).await {
        Ok(()) => AsyncWriteExt::shutdown(&mut writer).await.unwrap_err(),
        Err(error) => error,
    };
    assert!(first.to_string().contains(expected_message));
    for _ in 0..2 {
        let repeated = AsyncWriteExt::shutdown(&mut writer).await.unwrap_err();
        assert_eq!(first.kind(), repeated.kind());
        assert_eq!(first.to_string(), repeated.to_string());
    }
    assert!(
        Writer::shutdown(&mut writer)
            .await
            .unwrap_err()
            .to_string()
            .contains(expected_message)
    );
    assert!(
        writer
            .write(b"late")
            .await
            .unwrap_err()
            .to_string()
            .contains(expected_message)
    );
    assert_eq!(observed.rename.load(Ordering::SeqCst), 0);
    if matches!(fault, UploadFault::Part) {
        assert_eq!(observed.complete.load(Ordering::SeqCst), 0);
    }
    writer.abort().await;
    if matches!(fault, UploadFault::Part) {
        assert_eq!(observed.abort.load(Ordering::SeqCst), 1);
    }
    assert_missing(observed.as_ref(), &final_path).await;
    assert_only_paths(observed.as_ref(), &[]).await;
}

#[rstest]
#[case::abort(false)]
#[case::cancel_and_drop(true)]
#[tokio::test]
async fn cancelled_publication_preserves_target_and_unrelated_objects(#[case] drop_writer: bool) {
    let observed = Arc::new(ObservedStore::default());
    observed.block_rename.store(true, Ordering::SeqCst);
    let store = tagged_store(observed.clone(), "hdfs", None);
    let staging = Path::from("data/final.tmp.owned");
    let final_path = Path::from("data/final");
    let unrelated = Path::from("data/final.tmp.other");
    for path in [&final_path, &unrelated] {
        observed
            .put(path, Bytes::from_static(b"keep").into())
            .await
            .unwrap();
    }
    let mut writer = ObjectWriter::new_with_options(&store, &staging, Some(&final_path))
        .await
        .unwrap();
    writer.write_all(b"cancelled").await.unwrap();
    // Drive the actual upload to completion, but cancel shutdown while the
    // publication request is pending, before it can mutate the destination.
    tokio::time::timeout(Duration::from_millis(500), async {
        tokio::select! {
            result = AsyncWriteExt::shutdown(&mut writer) => panic!("rename gate unexpectedly completed: {result:?}"),
            _ = observed.rename_entered.notified() => {},
        }
    }).await.unwrap();
    assert_eq!(
        stored_bytes(observed.as_ref(), &staging).await,
        b"cancelled".as_slice()
    );
    if drop_writer {
        drop(writer);
    } else {
        writer.abort().await;
        writer.abort().await;
        let error = AsyncWriteExt::shutdown(&mut writer).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("aborted"));
        let error = writer.write(b"after-abort").await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(error.to_string().contains("aborted"));
        assert!(
            Writer::shutdown(&mut writer)
                .await
                .unwrap_err()
                .to_string()
                .contains("aborted")
        );
    }
    tokio::time::timeout(Duration::from_millis(500), async {
        while observed.head(&staging).await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    observed.rename_release.notify_one();
    assert_eq!(observed.rename.load(Ordering::SeqCst), 1);
    assert_eq!(
        stored_bytes(observed.as_ref(), &final_path).await,
        b"keep".as_slice()
    );
    assert_only_paths(observed.as_ref(), &[final_path, unrelated]).await;
}

#[rstest]
#[case::default(None)]
#[case::native(Some("native"))]
#[case::jni(Some("jni"))]
#[case::case_insensitive_jni(Some("JNI"))]
fn backend_selection_matches_enabled_features(#[case] backend: Option<&str>) {
    let options = backend.map(|value| HashMap::from([("hdfs_backend".into(), value.into())]));
    let params = options.map(|options| ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            options,
        ))),
        ..Default::default()
    });
    let is_native = cfg!(feature = "hdfs")
        && !(cfg!(feature = "hdfs-jni")
            && backend.is_some_and(|value| value.eq_ignore_ascii_case("jni")));
    assert_eq!(uses_hdfs_native_backend(params.as_ref()), is_native);
}

#[rstest]
#[case::native(None)]
#[case::explicit_jni(Some("jni"))]
#[tokio::test]
async fn conditional_create_preserves_winner_and_cleans_its_own_staging(
    #[case] backend: Option<&str>,
    #[values("data/transaction.txn", "data/scope%25tenant/transaction%25.txn")] raw_path: &str,
) {
    let observed = Arc::new(ObservedStore::default());
    let store = tagged_store(observed.clone(), "hdfs", backend);
    let path = Path::parse(raw_path).unwrap();
    let unrelated = Path::parse(format!("{path}.tmp.other")).unwrap();
    observed
        .put(&unrelated, Bytes::from_static(b"keep").into())
        .await
        .unwrap();
    store
        .put_if_absent(&path, Bytes::from_static(b"winner").into())
        .await
        .unwrap();
    let error = store
        .put_if_absent(&path, Bytes::from_static(b"loser").into())
        .await
        .unwrap_err();
    assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
    assert!(error.to_string().contains(path.as_ref()));
    assert_eq!(
        stored_bytes(observed.as_ref(), &path).await,
        b"winner".as_slice()
    );
    let is_native =
        cfg!(feature = "hdfs") && !(cfg!(feature = "hdfs-jni") && backend == Some("jni"));
    assert_eq!(
        observed.create_rename.load(Ordering::SeqCst),
        if is_native { 2 } else { 0 }
    );
    assert_eq!(
        observed.create_put.load(Ordering::SeqCst),
        if is_native { 0 } else { 2 }
    );
    let renamed = observed.rename_paths.lock().unwrap().clone();
    assert_eq!(renamed.len(), if is_native { 2 } else { 0 });
    if is_native {
        assert_ne!(renamed[0].0, renamed[1].0);
        let parent = path.as_ref().rsplit_once('/').map(|(parent, _)| parent);
        let prefix = format!("{}.tmp.", path.filename().unwrap());
        for (staged, destination) in &renamed {
            assert_eq!(destination, &path);
            assert_ne!(staged, &path);
            assert_ne!(staged, &unrelated);
            assert_eq!(
                staged.as_ref().rsplit_once('/').map(|(parent, _)| parent),
                parent
            );
            let suffix = staged
                .filename()
                .unwrap()
                .strip_prefix(&prefix)
                .expect("conditional staging preserves the encoded filename");
            assert_eq!(suffix.len(), 32);
            assert!(suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_missing(observed.as_ref(), staged).await;
        }
    }
    assert_eq!(
        stored_bytes(observed.as_ref(), &unrelated).await,
        b"keep".as_slice()
    );
    assert_only_paths(observed.as_ref(), &[path, unrelated]).await;
}

#[derive(Debug)]
struct ReplacingWrapper(Arc<ObservedStore>);

impl WrappingObjectStore for ReplacingWrapper {
    fn wrap(&self, _prefix: &str, _original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.0.clone()
    }

    fn wrap_paginated(
        &self,
        _prefix: &str,
        _original: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        None
    }
}

#[tokio::test]
async fn installed_wrapper_observes_public_sized_and_unsized_reads_and_writes() {
    let original = Arc::new(ObservedStore::default());
    let wrapped = Arc::new(ObservedStore::default());
    let path = Path::from("data/object");
    original
        .put(&path, Bytes::from_static(b"original").into())
        .await
        .unwrap();
    wrapped
        .put(&path, Bytes::from_static(b"wrapped!").into())
        .await
        .unwrap();
    let mut store = tagged_store(original.clone(), "hdfs", None);
    store.apply_wrapper(&ReplacingWrapper(wrapped.clone()));
    let reader = store.open(&path).await.unwrap();
    assert_eq!(
        reader.get_range(0..8).await.unwrap(),
        b"wrapped!".as_slice()
    );
    let reader = store.open_with_size(&path, 8).await.unwrap();
    assert_eq!(reader.get_all().await.unwrap(), b"wrapped!".as_slice());
    assert_eq!(original.get.load(Ordering::SeqCst), 0);
    assert!(wrapped.get.load(Ordering::SeqCst) >= 2);
    store.put(&path, b"updated!").await.unwrap();
    assert_eq!(wrapped.rename.load(Ordering::SeqCst), 1);
    assert_eq!(
        stored_bytes(original.as_ref(), &path).await,
        b"original".as_slice()
    );
    assert_eq!(
        stored_bytes(wrapped.as_ref(), &path).await,
        b"updated!".as_slice()
    );
    // The native factory is crate-private. Its disabling on apply_wrapper and
    // provider construction must additionally be audited/tested within lance-io.
}

#[cfg(feature = "hdfs")]
#[rstest]
#[case::logical_ha("hdfs://logical-ha/warehouse/table.lance", "warehouse/table.lance")]
#[case::explicit_rpc("hdfs://namenode:8020/warehouse/table.lance", "warehouse/table.lance")]
#[case::root("hdfs://logical-ha/", "")]
fn native_provider_path_preserves_authority_independent_object_path(
    #[case] uri: &str,
    #[case] expected: &str,
) {
    assert_eq!(
        HdfsStoreProvider
            .extract_path(&Url::parse(uri).unwrap())
            .unwrap(),
        Path::from(expected)
    );
}

#[cfg(feature = "hdfs-jni")]
#[rstest]
#[case::hdfs_ha("hdfs://logical-ha/warehouse/table.lance", "warehouse/table.lance")]
#[case::qbfs_authority("qbfs://logical-ha/warehouse/table.lance", "warehouse/table.lance")]
fn jni_provider_path_preserves_hdfs_and_qbfs_paths(#[case] uri: &str, #[case] expected: &str) {
    assert_eq!(
        HdfsJniStoreProvider
            .extract_path(&Url::parse(uri).unwrap())
            .unwrap(),
        Path::from(expected)
    );
}
