use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, stream};
use log::warn;
use rand::RngExt;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::runtime::Handle;

use crate::block_locations::LocatedBlocksState;
use crate::common::config::Configuration;
use crate::ec::{EcSchema, resolve_ec_policy};
use crate::hdfs::block_reader::get_block_stream;
use crate::hdfs::block_writer::BlockWriter;
use crate::hdfs::connection::DatanodeConnectionCache;
use crate::hdfs::crypto::FileCryptoCodec;
use crate::hdfs::protocol::{LeaseTracker, NamenodeProtocol};
use crate::proto::hdfs;
use crate::{HdfsError, Result};

const COMPLETE_RETRY_DELAY_MS: u64 = 500;
const COMPLETE_RETRIES: u32 = 5;
fn io_error(error: HdfsError) -> io::Error {
    io::Error::other(error)
}

struct PendingRead {
    stream: BoxStream<'static, Result<Bytes>>,
    end_position: usize,
}

pub struct FileReader {
    protocol: Arc<NamenodeProtocol>,
    path: String,
    located_blocks: Arc<LocatedBlocksState>,
    ec_schema: Option<EcSchema>,
    position: usize,
    config: Arc<Configuration>,
    handle: Handle,
    datanode_connection_cache: Arc<DatanodeConnectionCache>,
    pending_read: Option<std::sync::Mutex<PendingRead>>,
    crypto: Option<Arc<FileCryptoCodec>>,
}

impl FileReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        protocol: Arc<NamenodeProtocol>,
        path: String,
        located_blocks: Arc<LocatedBlocksState>,
        ec_schema: Option<EcSchema>,
        config: Arc<Configuration>,
        handle: Handle,
        datanode_connection_cache: Arc<DatanodeConnectionCache>,
        crypto: Option<Arc<FileCryptoCodec>>,
    ) -> Self {
        Self {
            protocol,
            path,
            located_blocks,
            ec_schema,
            position: 0,
            config,
            handle,
            datanode_connection_cache,
            pending_read: None,
            crypto,
        }
    }

    /// Returns the total size of the file
    pub fn file_length(&self) -> usize {
        self.located_blocks.current().file_length as usize
    }

    /// Returns the remaining bytes left based on the current cursor position.
    pub fn remaining(&self) -> usize {
        if self.position > self.file_length() {
            0
        } else {
            self.file_length() - self.position
        }
    }

    /// Sets the cursor to the position. Panics if the position is beyond the end of the file
    pub fn set_position(&mut self, pos: usize) {
        if pos > self.file_length() {
            panic!("Cannot seek beyond the end of a file");
        }
        self.pending_read = None;
        self.position = pos;
    }

    /// Returns the current cursor position in the file
    pub fn tell(&self) -> usize {
        self.position
    }

    /// Read up to `len` bytes into a new [Bytes] object, advancing the internal position in the file.
    /// An empty [Bytes] object will be returned if the end of the file has been reached.
    pub async fn read_bytes(&mut self, len: usize) -> Result<Bytes> {
        self.pending_read = None;
        if self.position >= self.file_length() {
            Ok(Bytes::new())
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + len, self.file_length());
            self.read_range(offset, self.position - offset).await
        }
    }

    /// Read up to `buf.len()` bytes into the provided slice, advancing the internal position in the file.
    /// Returns the number of bytes that were read, or 0 if the end of the file has been reached.
    pub async fn read_into(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.pending_read = None;
        if self.position >= self.file_length() {
            Ok(0)
        } else {
            let offset = self.position;
            self.position = usize::min(self.position + buf.len(), self.file_length());
            let read_bytes = self.position - offset;
            self.read_range_buf(&mut buf[..read_bytes], offset).await?;
            Ok(read_bytes)
        }
    }

    /// Read up to `len` bytes starting at `offset` into a new [Bytes] object. The returned buffer
    /// could be smaller than `len` if `offset + len` extends beyond the end of the file.
    ///
    /// Panics if the requested range is outside of the file
    pub async fn read_range(&self, offset: usize, len: usize) -> Result<Bytes> {
        let stream = self.read_range_stream(offset, len);
        futures::pin_mut!(stream);
        let mut buf = BytesMut::with_capacity(len);
        while let Some(bytes) = stream.next().await.transpose()? {
            buf.put(bytes);
        }
        Ok(buf.freeze())
    }

    /// Read file data into an existing buffer
    ///
    /// Panics if the requested range is outside of the file
    pub async fn read_range_buf(&self, mut buf: &mut [u8], offset: usize) -> Result<()> {
        let stream = self.read_range_stream(offset, buf.len());
        futures::pin_mut!(stream);
        while let Some(bytes) = stream.next().await.transpose()? {
            buf.put(bytes);
        }

        Ok(())
    }

    /// Return a stream of `Bytes` objects containing the content of the file
    ///
    /// Panics if the requested range is outside of the file
    pub fn read_range_stream(
        &self,
        offset: usize,
        len: usize,
    ) -> BoxStream<'static, Result<Bytes>> {
        let located_blocks = self.located_blocks.current();
        let file_length = located_blocks.file_length as usize;
        assert!(
            offset <= file_length && len <= file_length - offset,
            "Cannot read past end of the file"
        );
        let max_block_acquire_failures = self.config.max_block_acquire_failures();
        let retry_window_base_ms = self.config.retry_window_base_millis();
        let state = RangeReadState {
            stream: Self::range_stream(
                &located_blocks,
                offset,
                len,
                Arc::clone(&self.protocol),
                self.ec_schema.clone(),
                Arc::clone(&self.config),
                self.handle.clone(),
                Arc::clone(&self.datanode_connection_cache),
                self.crypto.clone(),
            ),
            protocol: Arc::clone(&self.protocol),
            path: self.path.clone(),
            shared_located_blocks: Arc::clone(&self.located_blocks),
            located_blocks,
            config: Arc::clone(&self.config),
            handle: self.handle.clone(),
            datanode_connection_cache: Arc::clone(&self.datanode_connection_cache),
            crypto: self.crypto.clone(),
            next_offset: offset,
            remaining: len,
            refreshes: 0,
            max_block_acquire_failures,
            retry_window_base_ms,
        };

        Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if state.remaining == 0 {
                    return None;
                }

                match state.stream.next().await {
                    Some(Ok(bytes)) => {
                        if bytes.len() > state.remaining {
                            state.remaining = 0;
                            return Some((
                                Err(HdfsError::DataTransferError(
                                    "DataNode returned more data than requested".to_string(),
                                )),
                                state,
                            ));
                        }
                        state.next_offset += bytes.len();
                        state.remaining -= bytes.len();
                        return Some((Ok(bytes), state));
                    }
                    Some(Err(error)) => {
                        if state.refreshes >= state.max_block_acquire_failures {
                            state.remaining = 0;
                            return Some((Err(error), state));
                        }
                    }
                    None => {
                        if state.refreshes >= state.max_block_acquire_failures {
                            state.remaining = 0;
                            return Some((
                                Err(HdfsError::DataTransferError(
                                    "Not enough data returned from DataNode".to_string(),
                                )),
                                state,
                            ));
                        }
                    }
                }

                let failures = u64::try_from(state.refreshes).unwrap_or(u64::MAX);
                state.refreshes += 1;
                let retry_window = state
                    .retry_window_base_ms
                    .saturating_mul(failures.saturating_add(1));
                let retry_delay = state
                    .retry_window_base_ms
                    .saturating_mul(failures)
                    .saturating_add(if retry_window == 0 {
                        0
                    } else {
                        rand::rng().random_range(0..retry_window)
                    });
                if retry_delay > 0 {
                    tokio::time::sleep(Duration::from_millis(retry_delay)).await;
                }
                let refreshed = match state
                    .shared_located_blocks
                    .refresh_if_current(&state.protocol, &state.path, &state.located_blocks)
                    .await
                {
                    Ok(refreshed) => refreshed,
                    Err(error) => {
                        state.remaining = 0;
                        return Some((Err(error), state));
                    }
                };
                let ec_schema = match refreshed
                    .ec_policy
                    .as_ref()
                    .map(resolve_ec_policy)
                    .transpose()
                {
                    Ok(ec_schema) => ec_schema,
                    Err(error) => {
                        state.remaining = 0;
                        return Some((Err(error), state));
                    }
                };
                state.stream = Self::range_stream(
                    &refreshed,
                    state.next_offset,
                    state.remaining,
                    Arc::clone(&state.protocol),
                    ec_schema,
                    Arc::clone(&state.config),
                    state.handle.clone(),
                    Arc::clone(&state.datanode_connection_cache),
                    state.crypto.clone(),
                );
                state.located_blocks = refreshed;
            }
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn range_stream(
        located_blocks: &hdfs::LocatedBlocksProto,
        offset: usize,
        len: usize,
        protocol: Arc<NamenodeProtocol>,
        ec_schema: Option<EcSchema>,
        config: Arc<Configuration>,
        handle: Handle,
        datanode_connection_cache: Arc<DatanodeConnectionCache>,
        crypto: Option<Arc<FileCryptoCodec>>,
    ) -> BoxStream<'static, Result<Bytes>> {
        if len == 0 {
            return stream::empty().boxed();
        }

        let range_end = offset + len;
        let block_streams: Vec<BoxStream<Result<Bytes>>> = located_blocks
            .blocks
            .iter()
            .flat_map(move |block| {
                let block_file_start = block.offset as usize;
                let block_file_end = block_file_start + block.b.num_bytes() as usize;

                if block_file_start < range_end && block_file_end > offset {
                    // We need to read this block
                    let block_start = offset - usize::min(offset, block_file_start);
                    let block_end = usize::min(range_end, block_file_end) - block_file_start;
                    Some(get_block_stream(
                        Arc::clone(&protocol),
                        block.clone(),
                        block_start,
                        block_end - block_start,
                        ec_schema.clone(),
                        Arc::clone(&config),
                        handle.clone(),
                        Arc::clone(&datanode_connection_cache),
                        crypto.clone(),
                    ))
                } else {
                    // No data is needed from this block
                    None
                }
            })
            .collect();

        stream::iter(block_streams).flatten().boxed()
    }
}

struct RangeReadState {
    stream: BoxStream<'static, Result<Bytes>>,
    protocol: Arc<NamenodeProtocol>,
    path: String,
    shared_located_blocks: Arc<LocatedBlocksState>,
    located_blocks: Arc<hdfs::LocatedBlocksProto>,
    config: Arc<Configuration>,
    handle: Handle,
    datanode_connection_cache: Arc<DatanodeConnectionCache>,
    crypto: Option<Arc<FileCryptoCodec>>,
    next_offset: usize,
    remaining: usize,
    refreshes: usize,
    max_block_acquire_failures: usize,
    retry_window_base_ms: u64,
}

impl AsyncRead for FileReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let starting_len = buf.filled().len();

        loop {
            if self.pending_read.is_none() {
                if self.position >= self.file_length() {
                    return Poll::Ready(Ok(()));
                }

                let offset = self.position;
                let len = usize::min(buf.remaining(), self.file_length() - self.position);
                let stream = self.read_range_stream(offset, len).boxed();
                self.pending_read = Some(std::sync::Mutex::new(PendingRead {
                    stream,
                    end_position: offset + len,
                }));
            }

            let poll_result = {
                let mut pending = self.pending_read.as_ref().unwrap().lock().unwrap();
                Pin::new(&mut pending.stream).poll_next(cx)
            };

            match poll_result {
                Poll::Ready(Some(Ok(bytes))) => {
                    self.position += bytes.len();
                    buf.put_slice(&bytes);

                    if self.pending_read.as_ref().is_some_and(|pending| {
                        self.position >= pending.lock().unwrap().end_position
                    }) {
                        self.pending_read = None;
                    }

                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.pending_read = None;
                    return Poll::Ready(Err(io_error(error)));
                }
                Poll::Ready(None) => {
                    self.pending_read = None;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    if buf.filled().len() > starting_len {
                        return Poll::Ready(Ok(()));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

impl AsyncSeek for FileReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let file_length = self.file_length() as i128;
        let current = self.tell() as i128;
        let new_pos = match position {
            SeekFrom::Start(pos) => i128::from(pos),
            SeekFrom::End(offset) => file_length + i128::from(offset),
            SeekFrom::Current(offset) => current + i128::from(offset),
        };

        if new_pos < 0 || new_pos > file_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek outside of file bounds",
            ));
        }

        self.set_position(new_pos as usize);
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.tell() as u64))
    }
}

pub struct FileWriter {
    src: String,
    protocol: Arc<NamenodeProtocol>,
    status: hdfs::HdfsFileStatusProto,
    config: Arc<Configuration>,
    handle: Handle,
    block_writer: Option<BlockWriter>,
    last_block: Option<hdfs::LocatedBlockProto>,
    closed: bool,
    bytes_written: usize,
    crypto: Option<Arc<FileCryptoCodec>>,
}

impl FileWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        protocol: Arc<NamenodeProtocol>,
        src: String,
        status: hdfs::HdfsFileStatusProto,
        config: Arc<Configuration>,
        handle: Handle,
        // Some for append, None for create
        last_block: Option<hdfs::LocatedBlockProto>,
        crypto: Option<Arc<FileCryptoCodec>>,
    ) -> Self {
        protocol.add_file_lease(status.file_id(), status.namespace.clone());
        Self {
            protocol,
            src,
            status,
            config,
            handle,
            block_writer: None,
            last_block,
            closed: false,
            bytes_written: 0,
            crypto,
        }
    }

    async fn create_block_writer(&mut self) -> Result<()> {
        let new_block = if let Some(last_block) = self.last_block.take() {
            // Append operation on first write. Erasure code appends always just create a new block.
            if last_block.b.num_bytes() < self.status.blocksize() && self.status.ec_policy.is_none()
            {
                // The last block isn't full, just write data to it
                last_block
            } else {
                // The last block is full, so create a new block to write to
                self.protocol
                    .add_block(&self.src, Some(last_block.b), self.status.file_id)
                    .await?
                    .block
            }
        } else {
            // Not appending to an existing block, just create a new one
            // If there's an existing block writer, close it first
            let extended_block = if let Some(block_writer) = self.block_writer.take() {
                Some(block_writer.close().await?)
            } else {
                None
            };

            self.protocol
                .add_block(&self.src, extended_block, self.status.file_id)
                .await?
                .block
        };

        let block_writer = BlockWriter::new(
            Arc::clone(&self.protocol),
            new_block,
            self.protocol.get_cached_server_defaults().await?,
            Arc::clone(&self.config),
            self.handle.clone(),
            self.status
                .ec_policy
                .as_ref()
                .map(resolve_ec_policy)
                .transpose()?
                .as_ref(),
            &self.src,
            &self.status,
            self.crypto.clone(),
        )
        .await?;

        self.block_writer = Some(block_writer);
        Ok(())
    }

    async fn get_block_writer(&mut self) -> Result<&mut BlockWriter> {
        // If the current writer is full, or hasn't been created, create one
        if self.block_writer.as_ref().is_some_and(|b| b.is_full()) || self.block_writer.is_none() {
            self.create_block_writer().await?;
        }

        Ok(self.block_writer.as_mut().unwrap())
    }

    pub async fn write_bytes(&mut self, mut buf: Bytes) -> Result<usize> {
        let bytes_to_write = buf.len();
        while !buf.is_empty() {
            let block_writer = self.get_block_writer().await?;

            block_writer.write(&mut buf).await?;
        }

        self.bytes_written += bytes_to_write;

        Ok(bytes_to_write)
    }

    pub async fn close(&mut self) -> Result<()> {
        if !self.closed {
            let extended_block = if let Some(block_writer) = self.block_writer.take() {
                Some(block_writer.close().await?)
            } else {
                None
            };

            let mut retry_delay = COMPLETE_RETRY_DELAY_MS;
            let mut retries = 0;
            while retries < COMPLETE_RETRIES {
                let successful = self
                    .protocol
                    .complete(&self.src, extended_block.clone(), self.status.file_id)
                    .await?
                    .result;

                if successful {
                    self.closed = true;
                    return Ok(());
                }

                // Sleep in the provided runtime in case we are not called from a tokio runtime
                let sleep = {
                    let _guard = self.handle.enter();
                    tokio::time::sleep(Duration::from_millis(retry_delay))
                };
                sleep.await;

                retry_delay *= 2;
                retries += 1;
            }
            Err(HdfsError::OperationFailed(
                "Failed to complete file in time".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        if !self.closed {
            warn!(
                "FileWriter dropped without being closed. File content may not have saved or may not be complete"
            );
        }

        self.protocol
            .remove_file_lease(self.status.file_id(), self.status.namespace.clone());
    }
}
