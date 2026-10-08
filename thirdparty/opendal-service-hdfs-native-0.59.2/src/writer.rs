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

use hdfs_native::file::FileWriter;
use log::warn;
use std::sync::Arc;

use crate::core::{HdfsNativeCore, parse_hdfs_error};
use opendal_core::raw::*;
use opendal_core::*;
pub struct HdfsNativeWriter {
    f: Option<FileWriter>,
    size: u64,
    core: Arc<HdfsNativeCore>,
    path: String,
}

impl HdfsNativeWriter {
    pub fn new(f: FileWriter, initial_size: u64, core: Arc<HdfsNativeCore>, path: String) -> Self {
        HdfsNativeWriter {
            f: Some(f),
            size: initial_size,
            core,
            path,
        }
    }
}

impl oio::Write for HdfsNativeWriter {
    async fn write(&mut self, mut buf: Buffer) -> Result<()> {
        let len = buf.len() as u64;
        let f = self.f.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer is already closed or aborted")
        })?;

        for bs in buf.by_ref() {
            f.write_bytes(bs).await.map_err(parse_hdfs_error)?;
        }

        self.size += len;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let f = self.f.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer is already closed or aborted")
        })?;
        f.close().await.map_err(parse_hdfs_error)?;
        self.f = None;

        Ok({
            let mut metadata = MetadataBuilder::unknown();
            metadata.set_file(self.size);
            metadata.build()
        })
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(mut f) = self.f.take() {
            // Finalize the open block lease before deleting the incomplete
            // object. An already completed writer must not delete its result.
            if let Err(error) = f.close().await {
                warn!("failed to close hdfs-native writer during abort: {error}");
            }
            self.core.hdfs_delete(&self.path).await?;
        }
        Ok(())
    }
}
