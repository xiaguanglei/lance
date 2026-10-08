// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hdfs_native::HdfsError;
use hdfs_native::WriteOptions;
use hdfs_native::file::FileWriter;
use log::warn;

use opendal_core::raw::*;
use opendal_core::*;

static NEXT_COPY_ID: AtomicU64 = AtomicU64::new(0);

fn parse_publish_error(error: HdfsError, conditional: bool) -> Error {
    if conditional && matches!(error, HdfsError::AlreadyExists(_)) {
        return Error::new(
            ErrorKind::ConditionNotMatch,
            "HDFS destination already exists",
        )
        .set_source(error);
    }
    parse_hdfs_error(error)
}

async fn cleanup_copy_staging(
    client: Arc<hdfs_native::Client>,
    path: String,
    mut writer: Option<FileWriter>,
) {
    if let Some(writer) = writer.as_mut()
        && let Err(error) = writer.close().await
    {
        warn!("failed to close HDFS copy staging file {path}: {error}");
    }
    if let Err(error) = client.delete(&path, true).await {
        warn!("failed to delete HDFS copy staging file {path}: {error}");
    }
}

struct CopyStagingGuard {
    client: Arc<hdfs_native::Client>,
    path: String,
    writer: Option<FileWriter>,
    is_armed: bool,
}

impl CopyStagingGuard {
    fn new(client: Arc<hdfs_native::Client>, path: String, writer: FileWriter) -> Self {
        Self {
            client,
            path,
            writer: Some(writer),
            is_armed: true,
        }
    }

    async fn cleanup(&mut self) {
        cleanup_copy_staging(self.client.clone(), self.path.clone(), self.writer.take()).await;
        self.is_armed = false;
    }

    fn disarm(&mut self) {
        self.is_armed = false;
        self.writer = None;
    }
}

impl Drop for CopyStagingGuard {
    fn drop(&mut self) {
        if !self.is_armed {
            return;
        }
        let client = self.client.clone();
        let path = self.path.clone();
        let writer = self.writer.take();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(cleanup_copy_staging(client, path, writer));
            }
            Err(error) => {
                warn!("cannot schedule cleanup for HDFS copy staging file {path}: {error}")
            }
        }
    }
}

/// HdfsNativeCore contains code that directly interacts with HDFS Native client.
#[derive(Clone)]
pub struct HdfsNativeCore {
    pub info: ServiceInfo,
    pub capability: Capability,
    pub root: String,
    pub client: Arc<hdfs_native::Client>,
}

impl Debug for HdfsNativeCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeCore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl HdfsNativeCore {
    const COPY_CHUNK_SIZE: usize = 8 * 1024 * 1024;
    pub async fn hdfs_create_dir(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .mkdirs(&p, 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_stat(&self, path: &str) -> Result<Metadata> {
        let p = build_rooted_abs_path(&self.root, path);

        let status: hdfs_native::client::FileStatus = self
            .client
            .get_file_info(&p)
            .await
            .map_err(parse_hdfs_error)?;

        let mode = if status.isdir {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        };

        let mut metadata = if mode == EntryMode::FILE {
            MetadataBuilder::file(status.length as u64)
        } else {
            MetadataBuilder::dir()
        };
        metadata.last_modified(Timestamp::from_millisecond(
            status.modification_time as i64,
        )?);

        Ok(metadata.build())
    }

    pub async fn hdfs_open(&self, path: &str) -> Result<hdfs_native::file::FileReader> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client.read(&p).await.map_err(parse_hdfs_error)
    }

    pub async fn hdfs_write(
        &self,
        path: &str,
        args: &OpWrite,
    ) -> Result<(hdfs_native::file::FileWriter, u64)> {
        // Conditional writes are not advertised: accepting one here would
        // silently weaken the object's atomic publication contract.
        if args.if_not_exists()
            || args.if_match().is_some()
            || args.if_none_match().is_some()
            || args.if_version_match().is_some()
            || args.if_version_not_match().is_some()
        {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "HDFS native conditional write is not supported; use conditional copy",
            ));
        }
        let target_path = build_rooted_abs_path(&self.root, path);
        let mut initial_size = 0;

        let target_exists = match self.client.get_file_info(&target_path).await {
            Ok(status) => {
                initial_size = status.length as u64;
                true
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => false,
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        let f = if target_exists {
            if args.append() {
                self.client
                    .append(&target_path)
                    .await
                    .map_err(parse_hdfs_error)?
            } else {
                initial_size = 0;
                self.client
                    .create(&target_path, WriteOptions::default().overwrite(true))
                    .await
                    .map_err(parse_hdfs_error)?
            }
        } else {
            initial_size = 0;
            self.client
                .create(&target_path, WriteOptions::default())
                .await
                .map_err(parse_hdfs_error)?
        };

        Ok((f, initial_size))
    }

    pub async fn hdfs_delete(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        self.client
            .delete(&p, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }

    pub async fn hdfs_list(&self, path: &str) -> Result<Option<(String, Option<String>)>> {
        let p: String = build_rooted_abs_path(&self.root, path);

        let isdir = match self.client.get_file_info(&p).await {
            Ok(status) => status.isdir,
            Err(err) => {
                return match &err {
                    HdfsError::FileNotFound(_) => Ok(None),
                    _ => Err(parse_hdfs_error(err)),
                };
            }
        };

        let current_path = if isdir {
            if !path.ends_with("/") {
                Some(path.to_string() + "/")
            } else {
                Some(path.to_string())
            }
        } else {
            None
        };

        Ok(Some((p, current_path)))
    }

    pub async fn hdfs_rename(&self, from: &str, to: &str, overwrite: bool) -> Result<()> {
        let from_path = build_rooted_abs_path(&self.root, from);
        let to_path = build_rooted_abs_path(&self.root, to);

        match self.client.get_file_info(&to_path).await {
            Ok(status) => {
                if status.isdir {
                    return Err(Error::new(ErrorKind::IsADirectory, "path should be a file")
                        .with_context("input", &to_path));
                }
            }
            Err(err) => match &err {
                HdfsError::FileNotFound(_) => {}
                _ => return Err(parse_hdfs_error(err)),
            },
        };

        // Creating only the parent keeps the final path invisible until the
        // NameNode atomically publishes the file. Never delete the destination.
        self.client
            .mkdirs(get_parent(&to_path), 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;
        self.client
            .rename(&from_path, &to_path, overwrite)
            .await
            .map_err(|error| parse_publish_error(error, !overwrite))?;

        Ok(())
    }

    /// Publish a private sibling copy with one atomic NameNode rename. The
    /// destination-existence check must happen inside rename, never via stat.
    pub async fn hdfs_copy(&self, from: &str, to: &str, if_not_exists: bool) -> Result<Metadata> {
        let from_path = build_rooted_abs_path(&self.root, from);
        let to_path = build_rooted_abs_path(&self.root, to);
        let staging_path = format!(
            "{to_path}.opendal-copy-{}-{}-{}",
            std::process::id(),
            NEXT_COPY_ID.fetch_add(1, Ordering::Relaxed),
            from_path.rsplit('/').next().unwrap_or("source")
        );
        self.client
            .mkdirs(get_parent(&to_path), 0o777, true)
            .await
            .map_err(parse_hdfs_error)?;
        let source = self
            .client
            .read(&from_path)
            .await
            .map_err(parse_hdfs_error)?;
        let file_length = source.file_length();
        let writer = self
            .client
            .create(&staging_path, WriteOptions::default().create_parent(true))
            .await
            .map_err(parse_hdfs_error)?;
        let mut staging = CopyStagingGuard::new(self.client.clone(), staging_path.clone(), writer);
        let result = async {
            let mut offset = 0;
            while offset < file_length {
                let size = (file_length - offset).min(Self::COPY_CHUNK_SIZE);
                let chunk = source
                    .read_range(offset, size)
                    .await
                    .map_err(parse_hdfs_error)?;
                let writer = staging.writer.as_mut().ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "HDFS copy staging writer is closed")
                })?;
                writer.write_bytes(chunk).await.map_err(parse_hdfs_error)?;
                offset += size;
            }
            let writer = staging.writer.as_mut().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "HDFS copy staging writer is closed")
            })?;
            writer.close().await.map_err(parse_hdfs_error)?;
            staging.writer = None;
            self.client
                .rename(&staging_path, &to_path, !if_not_exists)
                .await
                .map_err(|error| parse_publish_error(error, if_not_exists))?;
            Ok(MetadataBuilder::file(file_length as u64).build())
        }
        .await;
        if result.is_ok() {
            staging.disarm();
        } else {
            staging.cleanup().await;
        }
        result
    }
}

/// Parse hdfs-native error into opendal::Error.
pub fn parse_hdfs_error(hdfs_error: HdfsError) -> Error {
    let (kind, retryable, msg) = match &hdfs_error {
        HdfsError::IOError(err) => (ErrorKind::Unexpected, false, err.to_string()),
        HdfsError::DataTransferError(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::ChecksumError => (
            ErrorKind::Unexpected,
            false,
            "checksums didn't match".to_string(),
        ),
        HdfsError::UrlParseError(err) => (ErrorKind::Unexpected, false, err.to_string()),
        HdfsError::AlreadyExists(msg) => (ErrorKind::AlreadyExists, false, msg.clone()),
        HdfsError::OperationFailed(msg) => (ErrorKind::Unexpected, false, msg.clone()),
        HdfsError::RPCError(msg0, msg1) => {
            if msg0.contains("java.io.FileNotFoundException") {
                (ErrorKind::NotFound, false, msg1.clone())
            } else {
                (ErrorKind::Unexpected, false, msg1.clone())
            }
        }
        HdfsError::FileNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::BlocksNotFound(msg) => (ErrorKind::NotFound, false, msg.clone()),
        HdfsError::IsADirectoryError(msg) => (ErrorKind::IsADirectory, false, msg.clone()),
        _ => (
            ErrorKind::Unexpected,
            false,
            "unexpected error from hdfs".to_string(),
        ),
    };

    let mut err = Error::new(kind, msg).set_source(hdfs_error);

    if retryable {
        err = err.set_temporary();
    }

    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::HdfsNativeLazyWriter;
    use hdfs_native::{ClientBuilder, minidfs::MiniDfs};
    use opendal_core::raw::oio::Write;
    use std::collections::HashSet;

    fn test_core() -> HdfsNativeCore {
        HdfsNativeCore {
            info: ServiceInfo::new(crate::HDFS_NATIVE_SCHEME, "/", ""),
            capability: Capability::default(),
            root: "/".to_string(),
            client: Arc::new(
                ClientBuilder::new()
                    .with_url("hdfs://127.0.0.1:9000")
                    .build()
                    .unwrap(),
            ),
        }
    }

    async fn write_file(core: &HdfsNativeCore, path: &str, content: &[u8]) {
        let mut writer = core
            .client
            .create(path, WriteOptions::default().create_parent(true))
            .await
            .unwrap();
        writer.write_bytes(content.to_vec().into()).await.unwrap();
        writer.close().await.unwrap();
    }

    async fn read_file(core: &HdfsNativeCore, path: &str) -> Vec<u8> {
        let reader = core.client.read(path).await.unwrap();
        reader
            .read_range(0, reader.file_length())
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn rename_creates_parent_and_atomically_overwrites() {
        let _lock = crate::MINIDFS_TEST_LOCK.lock().await;
        let _dfs = MiniDfs::with_features(&HashSet::new());
        let core = test_core();
        write_file(&core, "/source", b"first").await;
        core.hdfs_rename("source", "missing/parent/target", true)
            .await
            .unwrap();
        assert!(matches!(
            core.client.get_file_info("/source").await,
            Err(HdfsError::FileNotFound(_))
        ));
        write_file(&core, "/replacement", b"second").await;
        let error = core
            .hdfs_rename("replacement", "missing/parent/target", false)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
        assert!(error.to_string().contains("destination already exists"));
        assert_eq!(read_file(&core, "/replacement").await, b"second");
        assert_eq!(read_file(&core, "/missing/parent/target").await, b"first");
        core.hdfs_rename("replacement", "missing/parent/target", true)
            .await
            .unwrap();
        assert_eq!(read_file(&core, "/missing/parent/target").await, b"second");
    }

    #[tokio::test]
    async fn conditional_copy_preserves_concurrent_winner_and_sources() {
        let _lock = crate::MINIDFS_TEST_LOCK.lock().await;
        let _dfs = MiniDfs::with_features(&HashSet::new());
        let core = test_core();
        write_file(&core, "/source-a", b"first").await;
        write_file(&core, "/source-b", b"second").await;
        let (first, second) = tokio::join!(
            core.hdfs_copy("source-a", "target", true),
            core.hdfs_copy("source-b", "target", true)
        );
        assert!(first.is_ok() ^ second.is_ok());
        let error = if first.is_err() {
            first.unwrap_err()
        } else {
            second.unwrap_err()
        };
        assert_eq!(error.kind(), ErrorKind::ConditionNotMatch);
        assert!(error.to_string().contains("destination already exists"));
        let target = read_file(&core, "/target").await;
        assert!(target == b"first" || target == b"second");
        assert_eq!(read_file(&core, "/source-a").await, b"first");
        assert_eq!(read_file(&core, "/source-b").await, b"second");
        let entries = core.client.list_status("/", false).await.unwrap();
        assert!(
            entries
                .iter()
                .all(|entry| !entry.path.contains(".opendal-copy-"))
        );
    }

    #[tokio::test]
    async fn dropped_copy_guard_finalizes_and_removes_staging() {
        let _lock = crate::MINIDFS_TEST_LOCK.lock().await;
        let _dfs = MiniDfs::with_features(&HashSet::new());
        let core = test_core();
        let path = "/cancelled-copy-staging";
        let mut writer = core
            .client
            .create(path, WriteOptions::default())
            .await
            .unwrap();
        writer
            .write_bytes(b"partial".to_vec().into())
            .await
            .unwrap();
        drop(CopyStagingGuard::new(
            core.client.clone(),
            path.to_string(),
            writer,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match core.client.get_file_info(path).await {
                    Err(HdfsError::FileNotFound(_)) => break,
                    Err(error) => panic!("unexpected staging stat failure: {error}"),
                    Ok(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn writer_abort_cleans_only_an_initialized_incomplete_object() {
        let _lock = crate::MINIDFS_TEST_LOCK.lock().await;
        let _dfs = MiniDfs::with_features(&HashSet::new());
        let core = Arc::new(test_core());
        write_file(&core, "/existing", b"keep").await;
        let mut unopened = HdfsNativeLazyWriter::new(core.clone(), "existing", OpWrite::default());
        unopened.abort().await.unwrap();
        assert_eq!(read_file(&core, "/existing").await, b"keep");
        let mut partial = HdfsNativeLazyWriter::new(core.clone(), "partial", OpWrite::default());
        partial.write(Buffer::from("incomplete")).await.unwrap();
        partial.abort().await.unwrap();
        let error = core.hdfs_stat("partial").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);
        let mut complete = HdfsNativeLazyWriter::new(core.clone(), "complete", OpWrite::default());
        complete.write(Buffer::from("done")).await.unwrap();
        complete.close().await.unwrap();
        complete.abort().await.unwrap();
        assert_eq!(read_file(&core, "/complete").await, b"done");
    }

    #[tokio::test]
    async fn raw_conditional_write_is_explicitly_unsupported() {
        let _lock = crate::MINIDFS_TEST_LOCK.lock().await;
        let _dfs = MiniDfs::with_features(&HashSet::new());
        let core = test_core();
        write_file(&core, "/existing", b"keep").await;
        let (args, _) = OpWrite::from_options(
            &Capability::default(),
            opendal_core::options::WriteOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
        .unwrap();
        let error = core.hdfs_write("existing", &args).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert!(
            error
                .to_string()
                .contains("conditional write is not supported")
        );
        assert_eq!(read_file(&core, "/existing").await, b"keep");
    }
}
