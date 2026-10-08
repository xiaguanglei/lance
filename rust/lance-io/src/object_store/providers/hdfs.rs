// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::env;
use std::ops::Range;
use std::sync::Arc;

use futures::future::BoxFuture;
use hdfs_native::{Client, ClientBuilder, HdfsError};
use lance_core::Error;
use object_store::{Result as ObjectStoreResult, path::Path};
use object_store_opendal::OpendalStore;
use opendal::{
    Operator,
    raw::{build_rooted_abs_path, normalize_root, percent_decode_path},
    services::HdfsNative,
};

use crate::object_reader::CloudRangeReader;
use crate::object_store::{
    CloudRangeReaderFactory, DEFAULT_CLOUD_IO_PARALLELISM, ObjectStore, ObjectStoreParams,
    ObjectStoreProvider, StorageOptions,
};
use crate::utils::tracking_store::IOTracker;

const HDFS_SCHEME_PREFIX: &str = "hdfs://";
const HDFS_DEFAULT_AUTHORITY: &str = "nameservice";
const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";
const BLOCK_LOCATIONS_CACHE_CAPACITY: &str = "block_locations_cache_capacity";
const BLOCK_LOCATIONS_CACHE_TTL_MS: &str = "block_locations_cache_ttl_ms";
const DATANODE_SOCKET_CACHE_CAPACITY: &str = "datanode_socket_cache_capacity";
const DATANODE_SOCKET_CACHE_TTL_MS: &str = "datanode_socket_cache_ttl_ms";
const DATANODE_SOCKET_CACHE_MAX_TOTAL: &str = "datanode_socket_cache_max_total";
const DATANODE_REPLICA_ROUND_ROBIN: &str = "datanode_replica_round_robin";

fn to_object_store_error(error: HdfsError, path: &Path) -> object_store::Error {
    if matches!(
        &error,
        HdfsError::FileNotFound(_) | HdfsError::BlocksNotFound(_)
    ) || matches!(&error, HdfsError::RPCError(class, _) if class.contains("java.io.FileNotFoundException"))
    {
        object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(error),
        }
    } else {
        object_store::Error::Generic {
            store: "hdfs",
            source: Box::new(error),
        }
    }
}

fn init_hdfs_config(name_node_uri: &str) -> HashMap<String, String> {
    let namenodes = name_node_uri
        .split(',')
        .filter_map(|entry| {
            let entry = entry
                .trim()
                .trim_start_matches(HDFS_SCHEME_PREFIX)
                .trim_end_matches('/');
            (!entry.is_empty()).then_some(entry)
        })
        .collect::<Vec<_>>();

    // A bare authority is a logical nameservice and must be resolved from
    // hdfs-site.xml. Port-based entries use the same synthetic HA layout as
    // OpenDAL's HDFS native service.
    if !namenodes.iter().any(|namenode| namenode.contains(':')) {
        return HashMap::new();
    }

    let mut config = HashMap::new();
    let aliases = namenodes
        .iter()
        .enumerate()
        .map(|(index, namenode)| {
            let alias = format!("nn{index}");
            config.insert(
                format!("{HA_NAMENODE_RPC_ADDRESS_PREFIX}.{HDFS_DEFAULT_AUTHORITY}.{alias}"),
                (*namenode).to_string(),
            );
            alias
        })
        .collect::<Vec<_>>();
    config.insert(
        format!("{HA_NAMENODES_PREFIX}.{HDFS_DEFAULT_AUTHORITY}"),
        aliases.join(","),
    );
    config
}

fn build_reader_client(
    storage_options: &HashMap<String, String>,
) -> Result<(Client, String), Error> {
    let name_node = storage_options.get("name_node").ok_or_else(|| {
        Error::invalid_input("Cannot configure HDFS native reader without name_node")
    })?;
    let mut config = init_hdfs_config(name_node);

    // Forward native Hadoop keys and translate the OpenDAL-facing HDFS tuning
    // options. Unknown OpenDAL options are deliberately not passed through.
    config.extend(
        storage_options
            .iter()
            .filter(|(key, _)| {
                key.starts_with("dfs.")
                    || key.starts_with("fs.")
                    || key.starts_with("hadoop.")
                    || key.starts_with("ipc.")
            })
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    if let Some(capacity) = storage_options.get(BLOCK_LOCATIONS_CACHE_CAPACITY) {
        config.insert(
            "dfs.client.read.block-locations-cache.capacity".to_string(),
            capacity.clone(),
        );
    }
    if let Some(ttl_ms) = storage_options.get(BLOCK_LOCATIONS_CACHE_TTL_MS) {
        config.insert(
            "dfs.client.read.block-locations-cache.ttl-ms".to_string(),
            ttl_ms.clone(),
        );
    }
    for (option, native_key) in [
        (
            DATANODE_SOCKET_CACHE_CAPACITY,
            "dfs.client.socketcache.capacity",
        ),
        (
            DATANODE_SOCKET_CACHE_TTL_MS,
            "dfs.client.socketcache.expiryMsec",
        ),
        (
            DATANODE_SOCKET_CACHE_MAX_TOTAL,
            "dfs.client.socketcache.maxTotal",
        ),
        (
            DATANODE_REPLICA_ROUND_ROBIN,
            "dfs.client.read.replica-round-robin",
        ),
    ] {
        if let Some(value) = storage_options.get(option) {
            config.insert(native_key.to_string(), value.clone());
        }
    }

    let client_url =
        if config.contains_key(&format!("{HA_NAMENODES_PREFIX}.{HDFS_DEFAULT_AUTHORITY}")) {
            format!("{HDFS_SCHEME_PREFIX}{HDFS_DEFAULT_AUTHORITY}")
        } else {
            name_node.clone()
        };
    let mut builder = ClientBuilder::new()
        .with_url(client_url)
        .with_config(config);
    if let Ok(config_dir) = env::var("HADOOP_CONF_DIR") {
        builder = builder.with_config_dir(config_dir);
    }
    if let Some(user) = storage_options.get("user") {
        builder = builder.with_user(user.clone());
    }

    let client = builder.build().map_err(|error| {
        Error::invalid_input(format!("Failed to create HDFS native reader: {error}"))
    })?;
    let root = normalize_root(
        storage_options
            .get("root")
            .map(String::as_str)
            .unwrap_or_default(),
    );
    Ok((client, root))
}

struct HdfsRangeReader {
    inner: Arc<hdfs_native::file::FileReader>,
    path: Path,
    io_tracker: IOTracker,
}

impl std::fmt::Debug for HdfsRangeReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HdfsRangeReader")
            .field("path", &self.path)
            .field("file_length", &self.inner.file_length())
            .finish_non_exhaustive()
    }
}

impl CloudRangeReader for HdfsRangeReader {
    fn size(&self) -> usize {
        self.inner.file_length()
    }

    fn get_range(
        &self,
        range: Range<usize>,
    ) -> BoxFuture<'static, ObjectStoreResult<bytes::Bytes>> {
        let reader = Arc::clone(&self.inner);
        let path = self.path.clone();
        let io_tracker = self.io_tracker.clone();
        Box::pin(async move {
            let metrics = io_tracker.begin_io("get");
            if range.start > range.end || range.end > reader.file_length() {
                return Err(object_store::Error::Generic {
                    store: "hdfs",
                    source: format!(
                        "range {}..{} is outside file length {}",
                        range.start,
                        range.end,
                        reader.file_length()
                    )
                    .into(),
                });
            }
            let result = reader
                .read_range(range.start, range.end - range.start)
                .await
                .map_err(|error| to_object_store_error(error, &path));
            let byte_count = result.as_ref().map_or(0, |bytes| bytes.len() as u64);
            metrics.record(&result, byte_count);
            let bytes = result?;
            io_tracker.record_read(
                "get_range",
                path,
                bytes.len() as u64,
                Some(range.start as u64..range.end as u64),
            );
            Ok(bytes)
        })
    }
}

#[derive(Debug)]
struct HdfsRangeReaderFactory {
    client: Arc<Client>,
    root: String,
}

#[async_trait::async_trait]
impl CloudRangeReaderFactory for HdfsRangeReaderFactory {
    async fn open(
        &self,
        path: Path,
        io_tracker: IOTracker,
    ) -> ObjectStoreResult<Arc<dyn CloudRangeReader>> {
        let decoded_path = percent_decode_path(path.as_ref());
        let rooted_path = build_rooted_abs_path(&self.root, &decoded_path);
        let reader = self
            .client
            .read(&rooted_path)
            .await
            .map_err(|error| to_object_store_error(error, &path))?;
        Ok(Arc::new(HdfsRangeReader {
            inner: Arc::new(reader),
            path,
            io_tracker,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct HdfsStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for HdfsStoreProvider {
    async fn new_store(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> Result<ObjectStore, lance_core::Error> {
        let mut storage_options =
            StorageOptions(params.storage_options().cloned().unwrap_or_default());

        let download_retry_count = storage_options.download_retry_count();
        if !storage_options.0.contains_key("name_node") {
            storage_options.0.insert(
                "name_node".to_string(),
                format!("{}://{}", base_path.scheme(), base_path.authority()),
            );
        }

        // Default root to "/" so OpenDAL always knows where to start.
        // StorageOptions users can override this if needed.
        if !storage_options.0.contains_key("root") {
            storage_options
                .0
                .insert("root".to_string(), "/".to_string());
        }

        let reader = params
            .object_store_wrapper
            .is_none()
            .then(|| build_reader_client(&storage_options.0))
            .transpose()?;
        let operator =
            Operator::from_iter::<HdfsNative>(storage_options.0.into_iter()).map_err(|e| {
                Error::invalid_input(format!("Failed to create HDFS native operator: {e:?}"))
            })?;
        let opendal_store = Arc::new(OpendalStore::new(operator));
        let store = ObjectStore::new(
            opendal_store,
            base_path,
            params.resolved_block_size()?,
            None,
            params.use_constant_size_upload_parts,
            true,
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        );

        // Backend-specific reads bypass the object-store trait, so retain the
        // generic path when a caller needs a wrapper to observe every read.
        match reader {
            Some((client, root)) => Ok(store.with_range_reader_factory(Arc::new(
                HdfsRangeReaderFactory {
                    client: Arc::new(client),
                    root,
                },
            ))),
            None => Ok(store),
        }
    }

    fn extract_path(&self, url: &url::Url) -> Result<Path, Error> {
        if let Ok(file_path) = url.to_file_path()
            && let Ok(path) = Path::from_absolute_path(&file_path)
        {
            return Ok(path);
        }

        Path::parse(url.path()).map_err(|e| {
            Error::invalid_input(format!("Failed to parse path '{}': {}", url.path(), e))
        })
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;
    use url::Url;

    use super::*;

    #[test]
    fn test_hdfs_store_path() {
        let provider = HdfsStoreProvider;

        let url = Url::parse("hdfs://hdfs-server/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = Path::from("/path/to/file");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_init_reader_config_for_port_based_namenodes() {
        let config = init_hdfs_config("hdfs://namenode1:9000,namenode2:9000/");
        assert_eq!(
            config.get("dfs.ha.namenodes.nameservice"),
            Some(&"nn0,nn1".to_string())
        );
        assert_eq!(
            config.get("dfs.namenode.rpc-address.nameservice.nn0"),
            Some(&"namenode1:9000".to_string())
        );
        assert_eq!(
            config.get("dfs.namenode.rpc-address.nameservice.nn1"),
            Some(&"namenode2:9000".to_string())
        );
    }

    #[test]
    fn test_init_reader_config_defers_bare_nameservice_resolution() {
        assert!(init_hdfs_config("hdfs://my-cluster-ns").is_empty());
    }

    #[cfg(feature = "hdfs-test")]
    #[tokio::test]
    #[serial_test::serial]
    async fn test_rename_if_not_exists_is_atomic() {
        use hdfs_native::minidfs::MiniDfs;
        use object_store::Error as ObjectStoreError;

        let _dfs = MiniDfs::default();
        let operator = Operator::new(
            HdfsNative::default()
                .name_node("hdfs://127.0.0.1:9000")
                .root("/"),
        )
        .unwrap();
        let store = OpendalStore::new(operator);
        let source_a = Path::from("source-a");
        let source_b = Path::from("source-b");
        let target = Path::from("target");
        store
            .put(&source_a, bytes::Bytes::from_static(b"first").into())
            .await
            .unwrap();
        store
            .put(&source_b, bytes::Bytes::from_static(b"second").into())
            .await
            .unwrap();

        let (first, second) = tokio::join!(
            store.rename_if_not_exists(&source_a, &target),
            store.rename_if_not_exists(&source_b, &target),
        );

        let (winning_source, losing_source, expected_target) = match (first, second) {
            (Ok(()), Err(ObjectStoreError::AlreadyExists { .. })) => {
                (source_a, source_b, b"first".as_slice())
            }
            (Err(ObjectStoreError::AlreadyExists { .. }), Ok(())) => {
                (source_b, source_a, b"second".as_slice())
            }
            results => panic!("expected one winner and one conflict, got {results:?}"),
        };
        assert!(store.head(&losing_source).await.is_ok());
        assert!(matches!(
            store.head(&winning_source).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
        assert_eq!(
            store.get(&target).await.unwrap().bytes().await.unwrap(),
            expected_target
        );
    }

    #[cfg(feature = "hdfs-test")]
    #[tokio::test]
    #[serial_test::serial]
    async fn test_repeated_lance_opens_reuse_hdfs_block_locations() {
        use std::collections::HashMap;

        use futures::future::try_join_all;
        use hdfs_native::test::{read_path_metrics, reset_read_path_metrics};
        use hdfs_native::{Client, WriteOptions, minidfs::MiniDfs};

        use crate::object_store::StorageOptionsAccessor;

        let dfs = MiniDfs::default();
        let client = Client::default();
        let mut file = client
            .create("/repeated-range", WriteOptions::default().replication(1))
            .await
            .unwrap();
        Box::pin(file.write_bytes(bytes::Bytes::from_static(b"abcdefgh")))
            .await
            .unwrap();
        Box::pin(file.close()).await.unwrap();

        let uri = format!("{}/repeated-range", dfs.url);
        let params = ObjectStoreParams {
            block_size: Some(4),
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    (
                        "block_locations_cache_capacity".to_string(),
                        "8".to_string(),
                    ),
                    (
                        "block_locations_cache_ttl_ms".to_string(),
                        "60000".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };
        let registry = Arc::new(crate::object_store::ObjectStoreRegistry::default());
        let (store, path) = ObjectStore::from_uri_and_params(registry, &uri, &params)
            .await
            .unwrap();

        reset_read_path_metrics();
        let readers = try_join_all((0..8).map(|_| store.open_with_size(&path, 8)))
            .await
            .unwrap();
        assert_eq!(readers[0].size().await.unwrap(), 8);
        assert_eq!(readers[0].get_range(0..2).await.unwrap().as_ref(), b"ab");
        assert_eq!(readers[1].get_range(4..8).await.unwrap().as_ref(), b"efgh");

        let metrics = read_path_metrics();
        assert_eq!(metrics.file_reader_opens, 8);
        assert_eq!(metrics.block_locations_cache_misses, 1);
        assert_eq!(metrics.block_locations_cache_hits, 7);
        let io_stats = store.io_stats_snapshot();
        assert_eq!(io_stats.read_iops, 2);
        assert_eq!(io_stats.read_bytes, 6);
    }
}
