// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! HDFS object store provider using OpenDAL's Java-based HDFS service.
//!
//! This provider uses OpenDAL's `Hdfs` service which relies on the Java HDFS
//! library (libhdfs) via JNI. It requires `JAVA_HOME` and `HADOOP_HOME`
//! environment variables to be set.
//!
//! Compared to the `HdfsNative` provider (`hdfs` feature), this supports
//! additional features such as Kerberos authentication, High Availability (HA),
//! and append operations.
//!
//! # Configuration
//!
//! Storage options (passed via `ObjectStoreParams::storage_options`):
//!
//! - `name_node`: Override the HDFS name node (auto-derived from URL if not set)
//! - `root`: Set the root working directory
//! - `user`: HDFS user name
//! - `kerberos_ticket_cache_path`: Path to Kerberos ticket cache (from `klist`)
//! - `enable_append`: Enable append capability (default: false)
//!
//! # Environment Variables
//!
//! - `JAVA_HOME`: Path to Java installation
//! - `HADOOP_HOME`: Path to Hadoop installation
//! - `LD_LIBRARY_PATH`: May need to include `${JAVA_HOME}/lib/server`
//! - `CLASSPATH`: May need Hadoop JARs if not auto-discovered
//! - `HADOOP_CONF_DIR`: Path to Hadoop config folder (required for HA setups)

use std::sync::Arc;

use lance_core::Error;
use object_store::path::Path;
use object_store_opendal::OpendalStore;
use opendal::{services::Hdfs, Operator};

use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions,
    DEFAULT_CLOUD_IO_PARALLELISM,
};

#[derive(Debug, Clone)]
pub struct HdfsJniStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for HdfsJniStoreProvider {
    async fn new_store(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> Result<ObjectStore, lance_core::Error> {
        let mut storage_options = StorageOptions(
                            params
                                .storage_options()
                                .cloned()
                                .unwrap_or_default(),
                        );

        let download_retry_count = storage_options.download_retry_count();

        if !storage_options.0.contains_key("name_node") {
            storage_options.0.insert(
                "name_node".to_string(),
                format!("{}://{}", base_path.scheme(), base_path.authority()),
            );
        }

        let root = base_path.path();
        if !root.is_empty() && root != "/" && !storage_options.0.contains_key("root") {
            storage_options.0.insert("root".to_string(), "/".to_string());
        }

        let operator = Operator::from_iter::<Hdfs>(storage_options.0.into_iter())
            .map_err(|e| {
                Error::invalid_input(
                    format!("Failed to create HDFS operator: {:?}", e)
                )
            })?
            .finish();

        let opendal_store = Arc::new(OpendalStore::new(operator));
        Ok(ObjectStore::new(
            opendal_store,
            base_path,
            params.block_size,
            None,
            params.use_constant_size_upload_parts,
            true,
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        ))
    }

    fn extract_path(&self, url: &url::Url) -> Result<Path, Error> {
        if let Ok(file_path) = url.to_file_path() {
            if let Ok(path) = Path::from_absolute_path(&file_path) {
                return Ok(path);
            }
        }

        Path::parse(url.path()).map_err(|e| {
            Error::invalid_input(
                format!("Failed to parse path '{}': {}", url.path(), e)
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    #[test]
    fn test_hdfs_provided_store_path() {
        let provider = HdfsJniStoreProvider;

        let url = Url::parse("hdfs://hdfs-server/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = Path::from("/path/to/file");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_hdfs_provided_store_path_with_port() {
        let provider = HdfsJniStoreProvider;

        let url = Url::parse("hdfs://namenode:8020/data/lance_test").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = Path::from("/data/lance_test");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_hdfs_jni_store_path_root() {
        let provider = HdfsJniStoreProvider;

        let url = Url::parse("hdfs://namenode:9000/").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path, Path::from("/"));
    }

    #[test]
    fn test_hdfs_jni_store_path_nested() {
        let provider = HdfsJniStoreProvider;

        let url = Url::parse("hdfs://ns1/warehouse/db/table.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path, Path::from("/warehouse/db/table.lance"));
    }

    #[test]
    fn test_hdfs_jni_operator_creation() {
        let options: Vec<(String, String)> = vec![
            ("name_node".to_string(), "hdfs://localhost:9000".to_string()),
            ("root".to_string(), "/".to_string()),
        ];

        let result = Operator::from_iter::<Hdfs>(options.into_iter());
        assert!(result.is_ok(), "Hdfs operator should be created from valid config");
    }

    #[test]
    fn test_hdfs_jni_new_store_auto_derives_name_node() {
        let provider = HdfsJniStoreProvider;
        let url = Url::parse("hdfs://my-namenode:8020/data/test").unwrap();

        let params = ObjectStoreParams::default();
        let mut storage_options =
            StorageOptions(params.storage_options().cloned().unwrap_or_default());

        assert!(!storage_options.0.contains_key("name_node"));

        storage_options.0.insert(
            "name_node".to_string(),
            format!("{}://{}", url.scheme(), url.authority()),
        );
        assert_eq!(
            storage_options.0.get("name_node").unwrap(),
            "hdfs://my-namenode:8020"
        );

        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path, Path::from("/data/test"));
    }
}
