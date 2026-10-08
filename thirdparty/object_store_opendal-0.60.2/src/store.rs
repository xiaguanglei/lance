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

use std::fmt::{self, Debug, Display, Formatter};
use std::future::IntoFuture;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use crate::datetime_to_timestamp;
use crate::utils::*;
use async_trait::async_trait;
use asyncband::mutex::Mutex;
use asyncband::oneshot;
use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::Attributes;
use object_store::CopyMode as ObjectStoreCopyMode;
use object_store::CopyOptions as ObjectStoreCopyOptions;
use object_store::Extensions;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::RenameOptions;
use object_store::RenameTargetMode;
use object_store::path::Path;
use object_store::{GetOptions, UploadPart};
use object_store::{GetRange, GetResultPayload};
use object_store::{GetResult, PutMode};
use opendal::Buffer;
use opendal::BytesRange;
use opendal::Writer;
use opendal::options::CopyOptions;
use opendal::options::ReaderOptions;
use opendal::options::StatOptions;
use opendal::raw::percent_decode_path;
use opendal::{Operator, OperatorInfo};
use std::collections::HashMap;

const DEFAULT_CONCURRENT: usize = 8;
const DEFAULT_GET_RANGES_GAP: usize = object_store::OBJECT_STORE_COALESCE_DEFAULT as usize;

fn format_object_attributes(meta: &opendal::Metadata) -> Attributes {
    let mut attributes = Attributes::new();
    if let Some(user_meta) = meta.user_metadata() {
        for (key, value) in user_meta {
            attributes.insert(
                object_store::Attribute::Metadata(key.to_string().into()),
                value.to_string().into(),
            );
        }
    }

    attributes
}

fn format_reader_options(options: &GetOptions, content_length_hint: Option<u64>) -> ReaderOptions {
    ReaderOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        content_length_hint,
        ..Default::default()
    }
}

fn format_stat_options(options: &GetOptions) -> StatOptions {
    StatOptions {
        version: options.version.clone(),
        if_match: options.if_match.clone(),
        if_none_match: options.if_none_match.clone(),
        if_modified_since: options.if_modified_since.and_then(datetime_to_timestamp),
        if_unmodified_since: options.if_unmodified_since.and_then(datetime_to_timestamp),
        ..Default::default()
    }
}

fn format_read_range(range: Option<&GetRange>, size: u64) -> Range<u64> {
    match range {
        Some(GetRange::Bounded(r)) => {
            if r.start >= r.end || r.start >= size {
                0..0
            } else {
                r.start..r.end.min(size)
            }
        }
        Some(GetRange::Offset(offset)) => {
            if *offset < size {
                *offset..size
            } else {
                0..0
            }
        }
        Some(GetRange::Suffix(suffix)) if *suffix < size => (size - *suffix)..size,
        _ => 0..size,
    }
}

fn format_without_stat_error(err: opendal::Error, path: &str) -> object_store::Error {
    match err.kind() {
        // Ask get_opts to fall back to the stat path when read-open can't provide
        // enough metadata to build a valid GetResult, such as ranges beyond EOF.
        opendal::ErrorKind::Unsupported | opendal::ErrorKind::RangeNotSatisfied => {
            object_store::Error::NotSupported {
                source: Box::new(err),
            }
        }
        _ => format_object_store_error(err, path),
    }
}

/// OpendalStore implements ObjectStore trait by using opendal.
///
/// This allows users to use opendal as an object store without extra cost.
///
/// Visit [`opendal::services`] for more information about supported services.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use object_store::path::Path;
/// use object_store::ObjectStore;
/// use object_store::ObjectStoreExt;
/// use object_store_opendal::OpendalStore;
/// use opendal::services::S3;
/// use opendal::{Builder, Operator};
///
/// #[tokio::main]
/// async fn main() {
///    let builder = S3::default()
///     .access_key_id("my_access_key")
///     .secret_access_key("my_secret_key")
///     .endpoint("my_endpoint")
///     .region("my_region");
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap();
///
///     // Create a new object store
///     let object_store = Arc::new(OpendalStore::new(operator));
///
///     let path = Path::from("data/nested/test.txt");
///     let bytes = Bytes::from_static(b"hello, world! I am nested.");
///
///     object_store.put(&path, bytes.clone().into()).await.unwrap();
///
///     let content = object_store
///         .get(&path)
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///
///     assert_eq!(content, bytes);
/// }
/// ```
#[derive(Clone)]
pub struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
    get_ranges_gap: usize,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self {
            info: op.info().into(),
            inner: op,
            get_ranges_gap: DEFAULT_GET_RANGES_GAP,
        }
    }

    /// Set the maximum gap that [`ObjectStore::get_ranges`] coalesces.
    ///
    /// The default is [`object_store::OBJECT_STORE_COALESCE_DEFAULT`] (1 MiB).
    /// Set this value to `0` to avoid merging ranges separated by any bytes.
    /// Overlapping or adjacent ranges are still merged without over-fetching.
    pub fn with_get_ranges_gap(mut self, gap: usize) -> Self {
        self.get_ranges_gap = gap;
        self
    }

    /// Get the Operator info.
    pub fn info(&self) -> &OperatorInfo {
        self.info.as_ref()
    }

    async fn get_opts_without_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let reader = self
            .inner
            .reader_options(raw_location, format_reader_options(options, None))
            .into_send()
            .await
            .map_err(|err| format_without_stat_error(err, location.as_ref()))?;

        let mut stream = match options.range.as_ref() {
            Some(GetRange::Bounded(range)) => {
                reader
                    .into_bytes_stream(range.start..range.end)
                    .into_send()
                    .await
            }
            Some(GetRange::Offset(offset)) => reader.into_bytes_stream(*offset..).into_send().await,
            Some(GetRange::Suffix(suffix)) => {
                reader
                    .into_bytes_stream(BytesRange::suffix(*suffix))
                    .into_send()
                    .await
            }
            None => reader.into_bytes_stream(..).into_send().await,
        }
        .map_err(|err| format_without_stat_error(err, location.as_ref()))?;

        let metadata = stream
            .metadata()
            .into_send()
            .await
            .map_err(|err| format_without_stat_error(err, location.as_ref()))?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);
        let read_range = format_read_range(options.range.as_ref(), meta.size);

        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        if matches!(
            options.range.as_ref(),
            Some(GetRange::Bounded(range)) if range.end > meta.size
        ) {
            let reader = self
                .inner
                .reader_options(
                    raw_location,
                    format_reader_options(options, Some(meta.size)),
                )
                .into_send()
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?;
            stream = reader
                .into_bytes_stream(read_range.start..read_range.end)
                .into_send()
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        }

        let stream = stream
            .into_send()
            .map_err(|err: io::Error| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range,
            meta,
            attributes,
            extensions: Extensions::new(),
        })
    }

    async fn get_opts_with_stat(
        &self,
        location: &Path,
        raw_location: &str,
        options: &GetOptions,
    ) -> object_store::Result<GetResult> {
        let metadata = self
            .inner
            .stat_options(raw_location, format_stat_options(options))
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let attributes = format_object_attributes(&metadata);
        let meta = format_object_meta(location.as_ref(), &metadata);

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        let read_range = format_read_range(options.range.as_ref(), meta.size);
        if read_range.start >= read_range.end {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: read_range,
                meta,
                attributes,
                extensions: Extensions::new(),
            });
        }

        let reader = self
            .inner
            .reader_options(
                raw_location,
                format_reader_options(options, Some(meta.size)),
            )
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let stream = reader
            .into_bytes_stream(read_range.start..read_range.end)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?
            .into_send()
            .map_err(|err: io::Error| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range,
            meta,
            attributes,
            extensions: Extensions::new(),
        })
    }

    /// Copy a file from one location to another
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store::Result<()> {
        let mut copy_options = CopyOptions::default();
        if if_not_exists {
            copy_options.if_not_exists = true;
        }

        // Perform the copy operation
        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                copy_options,
            )
            .into_send()
            .await
            .map_err(|err| {
                if if_not_exists
                    && matches!(
                        err.kind(),
                        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
                    )
                {
                    object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })?;

        Ok(())
    }
}

impl Debug for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.capability())
            .field("get_ranges_gap", &self.get_ranges_gap)
            .finish()
    }
}

impl Display for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let info = self.inner.info();
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            info.scheme(),
            info.name(),
            info.root()
        )
    }
}

impl From<Operator> for OpendalStore {
    fn from(value: Operator) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(bytes));
        let opts_mode = opts.mode.clone();
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    Err(object_store::Error::NotSupported {
                        source: Box::new(opendal::Error::new(
                            opendal::ErrorKind::Unsupported,
                            "etag is required for conditional put",
                        )),
                    })?
                };
                future_write = future_write.if_match(etag.as_str());
            }
        }
        let rp = future_write.into_send().await.map_err(|err| {
            match format_object_store_error(err, location.as_ref()) {
                object_store::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    object_store::Error::AlreadyExists { path, source }
                }
                e => e,
            }
        })?;

        let e_tag = rp.etag().map(|s| s.to_string());
        let version = rp.version().map(|s| s.to_string());

        Ok(PutResult {
            e_tag,
            version,
            extensions: Extensions::new(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let mut options = opendal::options::WriteOptions {
            concurrent: DEFAULT_CONCURRENT,
            ..Default::default()
        };

        // Collect user metadata separately to handle multiple entries
        let mut user_metadata = HashMap::new();

        // Handle attributes if provided
        for (key, value) in opts.attributes.iter() {
            match key {
                object_store::Attribute::CacheControl => {
                    options.cache_control = Some(value.to_string());
                }
                object_store::Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                object_store::Attribute::ContentEncoding => {
                    options.content_encoding = Some(value.to_string());
                }
                object_store::Attribute::ContentLanguage => {
                    // no support
                    continue;
                }
                object_store::Attribute::ContentType => {
                    options.content_type = Some(value.to_string());
                }
                object_store::Attribute::Metadata(k) => {
                    user_metadata.insert(k.to_string(), value.to_string());
                }
                _ => {}
            }
        }

        // Apply user metadata if any entries were collected
        if !user_metadata.is_empty() {
            options.user_metadata = Some(user_metadata);
        }

        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_options(&decoded_location, options)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());

        if options.head {
            return self
                .get_opts_with_stat(location, &raw_location, &options)
                .await;
        }

        match self
            .get_opts_without_stat(location, &raw_location, &options)
            .await
        {
            Ok(result) => return Ok(result),
            Err(object_store::Error::NotSupported { .. }) => {}
            Err(err) => return Err(err),
        }

        self.get_opts_with_stat(location, &raw_location, &options)
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let raw_location = percent_decode_path(location.as_ref());
        let reader = self
            .inner
            .reader_with(&raw_location)
            .concurrent(DEFAULT_CONCURRENT)
            .gap(self.get_ranges_gap)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let buffers = reader
            .fetch(ranges.to_vec())
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(buffers.into_iter().map(|buf| buf.to_bytes()).collect())
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let this = self.clone();
        locations
            .and_then(move |location| {
                let this = this.clone();
                async move {
                    let decoded = percent_decode_path(location.as_ref());
                    this.inner
                        .delete(&decoded)
                        .into_send()
                        .await
                        .map_err(|err| format_object_store_error(err, location.as_ref()))?;
                    Ok(location)
                }
                .into_send()
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });

        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream
                .try_filter(|entry| futures::future::ready(!entry.metadata().is_dir()))
                .then(|res| async {
                    let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                    let meta = entry.metadata();

                    Ok(format_object_meta(entry.path(), meta))
                });
            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().into_send().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let offset = offset.clone();

        // clone self for 'static lifetime
        // clone self is cheap
        let this = self.clone();

        let fut = async move {
            let list_with_start_after = this.inner.info().capability().list_with_start_after;
            let mut fut = this.inner.lister_with(&path).recursive(true);

            // Use native start_after support if possible.
            if list_with_start_after {
                fut = fut.start_after(offset.as_ref());
            }

            let lister = fut
                .await
                .map_err(|err| format_object_store_error(err, &path))?
                .try_filter(|entry| futures::future::ready(!entry.metadata().is_dir()))
                .then(move |entry| {
                    let path = path.clone();
                    let this = this.clone();
                    async move {
                        let entry = entry.map_err(|err| format_object_store_error(err, &path))?;
                        let (path, metadata) = entry.into_parts();

                        // If last_modified is present, we can use it directly.
                        if metadata.last_modified().is_some() {
                            let object_meta = format_object_meta(&path, &metadata);
                            return Ok(object_meta);
                        }

                        let metadata = this
                            .inner
                            .stat(&path)
                            .await
                            .map_err(|err| format_object_store_error(err, &path))?;
                        let object_meta = format_object_meta(&path, &metadata);
                        Ok::<_, object_store::Error>(object_meta)
                    }
                })
                .into_send()
                .boxed();

            let stream = if list_with_start_after {
                lister
            } else {
                lister
                    .try_filter(move |entry| futures::future::ready(entry.location > offset))
                    .into_send()
                    .boxed()
            };

            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().into_send().try_flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
            .into_future()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, &path))?
            .into_send();

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().into_send().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else if meta.last_modified().is_some() {
                objects.push(format_object_meta(entry.path(), meta));
            } else {
                let meta = self
                    .inner
                    .stat(entry.path())
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, entry.path()))?;
                objects.push(format_object_meta(entry.path(), &meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Extensions::new(),
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: ObjectStoreCopyOptions,
    ) -> object_store::Result<()> {
        let if_not_exists = matches!(options.mode, ObjectStoreCopyMode::Create);
        self.copy_request(from, to, if_not_exists).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let RenameOptions {
            target_mode,
            extensions,
        } = options;
        let if_not_exists = matches!(target_mode, RenameTargetMode::Create);
        let capability = self.inner.info().capability();
        if capability.rename && (!if_not_exists || capability.rename_with_if_not_exists) {
            // Use OpenDAL's conditional rename API when the backend supports it.
            self.inner
                .rename_with(
                    &percent_decode_path(from.as_ref()),
                    &percent_decode_path(to.as_ref()),
                )
                .if_not_exists(if_not_exists)
                .into_send()
                .await
                .map_err(|err| {
                    if if_not_exists
                        && matches!(
                            err.kind(),
                            opendal::ErrorKind::AlreadyExists
                                | opendal::ErrorKind::ConditionNotMatch
                        )
                    {
                        object_store::Error::AlreadyExists {
                            path: to.to_string(),
                            source: Box::new(err),
                        }
                    } else {
                        format_object_store_error(err, from.as_ref())
                    }
                })?;
            return Ok(());
        }
        if if_not_exists && !capability.copy_with_if_not_exists {
            return Err(object_store::Error::NotSupported {
                source: Box::new(opendal::Error::new(
                    opendal::ErrorKind::Unsupported,
                    "atomic rename Create requires conditional copy support",
                )),
            });
        }

        // Preserve object_store's copy-then-delete fallback, deleting only the
        // source after a successful copy. An unconditional rename cannot implement Create.
        self.copy_opts(
            from,
            to,
            ObjectStoreCopyOptions {
                mode: if if_not_exists {
                    ObjectStoreCopyMode::Create
                } else {
                    ObjectStoreCopyMode::Overwrite
                },
                extensions,
            },
        )
        .await?;
        self.delete(from).await?;
        Ok(())
    }
}

/// `MultipartUpload`'s impl based on `Writer` in opendal
///
/// # Notes
///
/// OpenDAL writer can handle concurrent internally we don't generate real `UploadPart` like existing
/// implementation do. Instead, we just write the part and notify the next task to be written.
///
/// The lock here doesn't really involve the write process, it's just for the notify mechanism.
struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: oneshot::Receiver<()>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        // an immediately dropped sender for the first part to write without waiting
        let (_, rx) = oneshot::channel();

        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: rx,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();

        // Generate next notify which will be notified after the current part is written.
        let (tx, rx) = oneshot::channel();
        // Fetch the notify for current part to wait for it to be written.
        let last_rx = std::mem::replace(&mut self.next_notify, rx);

        async move {
            // Wait for the previous part to be written
            let _ = last_rx.await;

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            // Notify the next part to be written
            drop(tx);

            result
        }
        .into_send()
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        let e_tag = metadata.etag().map(|s| s.to_string());
        let version = metadata.version().map(|s| s.to_string());

        Ok(PutResult {
            e_tag,
            version,
            extensions: Extensions::new(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
    use opendal::services;
    use rand::prelude::*;
    use rand::rng;
    use std::sync::Arc;

    use super::*;

    async fn create_test_object_store() -> Arc<dyn ObjectStore> {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store = Arc::new(OpendalStore::new(op));

        let path: Path = "data/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.into()).await.unwrap();

        let path: Path = "data/nested/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world! I am nested.");
        object_store.put(&path, bytes.into()).await.unwrap();

        object_store
    }

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        // Retrieve a specific file
        let path: Path = "data/test.txt".into();

        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.clone().into()).await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, 13);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes
        );
    }

    mod rename_contract {
        use super::*;
        use opendal::raw::*;
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Default)]
        struct Calls {
            stat: AtomicUsize,
            copy: AtomicUsize,
            rename: AtomicUsize,
            delete: AtomicUsize,
        }

        #[derive(Debug)]
        struct ContractService {
            inner: Servicer,
            conditional_copy: bool,
            native_rename: bool,
            conditional_rename: Option<opendal::ErrorKind>,
            calls: Arc<Calls>,
            list_fixture: Option<bool>,
        }

        struct FixtureLister(VecDeque<opendal::Result<oio::Entry>>);

        impl oio::List for FixtureLister {
            async fn next(&mut self) -> opendal::Result<Option<oio::Entry>> {
                self.0.pop_front().transpose()
            }
        }

        impl Service for ContractService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;
            type Copier = oio::Copier;
            type Composer = oio::Composer;

            fn info(&self) -> ServiceInfo {
                self.inner.info()
            }
            fn capability(&self) -> opendal::Capability {
                let mut capability = self.inner.capability();
                capability.copy = true;
                capability.copy_with_if_not_exists = self.conditional_copy;
                capability.rename = self.native_rename;
                capability.rename_with_if_not_exists = self.conditional_rename.is_some();
                if let Some(has_start_after) = self.list_fixture {
                    capability.list_with_recursive = true;
                    capability.list_with_start_after = has_start_after;
                }
                capability
            }
            async fn create_dir(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpCreateDir,
            ) -> opendal::Result<RpCreateDir> {
                self.inner.create_dir(ctx, path, args).await
            }
            async fn stat(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpStat,
            ) -> opendal::Result<RpStat> {
                self.calls.stat.fetch_add(1, Ordering::SeqCst);
                self.inner.stat(ctx, path, args).await
            }
            fn read(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpRead,
            ) -> opendal::Result<Self::Reader> {
                self.inner.read(ctx, path, args)
            }
            fn write(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpWrite,
            ) -> opendal::Result<Self::Writer> {
                self.inner.write(ctx, path, args)
            }
            fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
                self.calls.delete.fetch_add(1, Ordering::SeqCst);
                self.inner.delete(ctx)
            }
            fn list(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpList,
            ) -> opendal::Result<Self::Lister> {
                if self.list_fixture.is_some() {
                    let mut file = opendal::MetadataBuilder::file(3);
                    file.last_modified(Timestamp::now());
                    let mut empty = opendal::MetadataBuilder::file(0);
                    empty.last_modified(Timestamp::now());
                    let mut unknown = opendal::MetadataBuilder::unknown();
                    unknown.last_modified(Timestamp::now());
                    let mut entries = VecDeque::new();
                    if args.recursive() {
                        entries.push_back(Ok(oio::Entry::new(
                            "/",
                            opendal::MetadataBuilder::dir().build(),
                        )));
                        entries.push_back(Ok(oio::Entry::new(
                            "data/",
                            opendal::MetadataBuilder::dir().build(),
                        )));
                    }
                    for (path, metadata) in [
                        ("data/empty", empty.build()),
                        ("data/file", file.build()),
                        ("data/nested/", opendal::MetadataBuilder::dir().build()),
                        ("data/unknown", unknown.build()),
                    ] {
                        if args.start_after().is_none_or(|offset| path > offset) {
                            entries.push_back(Ok(oio::Entry::new(path, metadata)));
                        }
                    }
                    if args.recursive() {
                        entries.push_back(Err(opendal::Error::new(
                            opendal::ErrorKind::PermissionDenied,
                            "listing denied after files",
                        )));
                    }
                    return Ok(Box::new(FixtureLister(entries)));
                }
                self.inner.list(ctx, path, args)
            }
            fn copy(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: OpCopy,
            ) -> opendal::Result<Self::Copier> {
                self.calls.copy.fetch_add(1, Ordering::SeqCst);
                let op = Operator::from_parts(ctx.clone(), self.inner.clone());
                let from = from.to_string();
                let to = to.to_string();
                Ok(Box::new(oio::OneShotCopier::new(async move {
                    let bytes = op.read(&from).await?;
                    let size = bytes.len() as u64;
                    op.write_options(
                        &to,
                        bytes,
                        opendal::options::WriteOptions {
                            if_not_exists: args.if_not_exists(),
                            ..Default::default()
                        },
                    )
                    .await?;
                    Ok(opendal::MetadataBuilder::file(size).build())
                })))
            }
            async fn rename(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: OpRename,
            ) -> opendal::Result<RpRename> {
                self.calls.rename.fetch_add(1, Ordering::SeqCst);
                let op = Operator::from_parts(ctx.clone(), self.inner.clone());
                let bytes = op.read(from).await?;
                if args.if_not_exists() {
                    tokio::task::yield_now().await;
                }
                op.write_options(
                    to,
                    bytes,
                    opendal::options::WriteOptions {
                        if_not_exists: args.if_not_exists(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| {
                    if args.if_not_exists()
                        && matches!(
                            error.kind(),
                            opendal::ErrorKind::AlreadyExists
                                | opendal::ErrorKind::ConditionNotMatch
                        )
                        && let Some(kind) = self.conditional_rename
                    {
                        opendal::Error::new(kind, "rename destination already exists")
                            .set_source(error)
                    } else {
                        error
                    }
                })?;
                op.delete(from).await?;
                Ok(RpRename::default())
            }
            async fn presign(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: OpPresign,
            ) -> opendal::Result<RpPresign> {
                self.inner.presign(ctx, path, args).await
            }
        }

        fn store(
            conditional_copy: bool,
            native_rename: bool,
            conditional_rename: Option<opendal::ErrorKind>,
        ) -> (OpendalStore, Arc<Calls>) {
            let (ctx, inner) = Operator::new(services::Memory::default())
                .unwrap()
                .into_parts();
            let calls = Arc::new(Calls::default());
            let op = Operator::from_parts(
                ctx,
                Arc::new(ContractService {
                    inner,
                    conditional_copy,
                    native_rename,
                    conditional_rename,
                    calls: calls.clone(),
                    list_fixture: None,
                }),
            );
            (OpendalStore::new(op), calls)
        }

        fn listing_store(has_start_after: bool) -> OpendalStore {
            let (ctx, inner) = Operator::new(services::Memory::default())
                .unwrap()
                .into_parts();
            OpendalStore::new(Operator::from_parts(
                ctx,
                Arc::new(ContractService {
                    inner,
                    conditional_copy: false,
                    native_rename: false,
                    conditional_rename: None,
                    calls: Arc::new(Calls::default()),
                    list_fixture: Some(has_start_after),
                }),
            ))
        }

        fn listed_files(results: &[object_store::Result<ObjectMeta>]) -> Vec<(&str, u64)> {
            results
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .map(|meta| (meta.location.as_ref(), meta.size))
                .collect()
        }

        fn assert_listing_error(results: &[object_store::Result<ObjectMeta>]) {
            assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
            let error = results.last().unwrap().as_ref().unwrap_err();
            assert!(matches!(error, object_store::Error::Generic { source, .. }
                if source.downcast_ref::<opendal::Error>().unwrap().kind()
                    == opendal::ErrorKind::PermissionDenied));
            assert!(error.to_string().contains("listing denied after files"));
        }

        #[tokio::test]
        async fn list_filters_only_directories_and_preserves_errors_and_delimiters() {
            let store = listing_store(false);
            let results = store.list(None).collect::<Vec<_>>().await;
            assert_eq!(
                listed_files(&results),
                [("data/empty", 0), ("data/file", 3), ("data/unknown", 0)]
            );
            assert_listing_error(&results);

            let delimiter = store
                .list_with_delimiter(Some(&Path::from("data")))
                .await
                .unwrap();
            assert_eq!(delimiter.common_prefixes, [Path::from("data/nested")]);
            assert_eq!(
                delimiter
                    .objects
                    .iter()
                    .map(|meta| meta.location.as_ref())
                    .collect::<Vec<_>>(),
                ["data/empty", "data/file", "data/unknown"]
            );
        }

        #[tokio::test]
        async fn list_with_offset_filters_directories_in_native_and_fallback_paths() {
            let offset = Path::from("data/empty");
            for has_start_after in [false, true] {
                let store = listing_store(has_start_after);
                let listed = store.list(None).collect::<Vec<_>>().await;
                let expected = listed_files(&listed)
                    .into_iter()
                    .filter(|(path, _)| *path > offset.as_ref())
                    .collect::<Vec<_>>();
                let results = store
                    .list_with_offset(None, &offset)
                    .collect::<Vec<_>>()
                    .await;
                assert_eq!(listed_files(&results), expected);
                assert_eq!(
                    listed_files(&results),
                    [("data/file", 3), ("data/unknown", 0)]
                );
                assert_listing_error(&results);
            }
        }

        fn create_options() -> RenameOptions {
            RenameOptions::new().with_target_mode(RenameTargetMode::Create)
        }

        #[tokio::test]
        async fn create_prefers_native_conditional_rename_with_or_without_conditional_copy() {
            for conditional_copy in [false, true] {
                let (store, calls) = store(
                    conditional_copy,
                    true,
                    Some(opendal::ErrorKind::ConditionNotMatch),
                );
                let from = Path::from("staged % source");
                let to = Path::from("final % target");
                store
                    .put(&from, Bytes::from_static(b"source").into())
                    .await
                    .unwrap();
                store
                    .rename_opts(&from, &to, create_options())
                    .await
                    .unwrap();
                assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
                assert_eq!(calls.copy.load(Ordering::SeqCst), 0);
                assert_eq!(calls.rename.load(Ordering::SeqCst), 1);
                assert_eq!(calls.delete.load(Ordering::SeqCst), 0);
                assert!(matches!(
                    store.head(&from).await.unwrap_err(),
                    object_store::Error::NotFound { .. }
                ));
                assert_eq!(
                    store.get(&to).await.unwrap().bytes().await.unwrap(),
                    b"source"[..]
                );
            }
        }

        #[tokio::test]
        async fn native_create_conflict_preserves_source_and_destination() {
            for conflict_kind in [
                opendal::ErrorKind::AlreadyExists,
                opendal::ErrorKind::ConditionNotMatch,
            ] {
                let (store, calls) = store(false, true, Some(conflict_kind));
                let from = Path::from("staged source");
                let to = Path::from("final target");
                store
                    .put(&from, Bytes::from_static(b"source").into())
                    .await
                    .unwrap();
                store
                    .put(&to, Bytes::from_static(b"winner").into())
                    .await
                    .unwrap();
                let error = store
                    .rename_opts(&from, &to, create_options())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(&error, object_store::Error::AlreadyExists { path, .. }
                        if path == &to.to_string())
                );
                assert!(
                    error
                        .to_string()
                        .contains("rename destination already exists")
                );
                assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
                assert_eq!(calls.copy.load(Ordering::SeqCst), 0);
                assert_eq!(calls.rename.load(Ordering::SeqCst), 1);
                assert_eq!(calls.delete.load(Ordering::SeqCst), 0);
                assert_eq!(
                    store.get(&from).await.unwrap().bytes().await.unwrap(),
                    b"source"[..]
                );
                assert_eq!(
                    store.get(&to).await.unwrap().bytes().await.unwrap(),
                    b"winner"[..]
                );
            }
        }

        #[tokio::test]
        async fn concurrent_native_create_has_one_winner_and_preserves_losing_source() {
            for conflict_kind in [
                opendal::ErrorKind::AlreadyExists,
                opendal::ErrorKind::ConditionNotMatch,
            ] {
                let (store, calls) = store(false, true, Some(conflict_kind));
                let first = Path::from("source-a");
                let second = Path::from("source-b");
                let to = Path::from("target");
                store
                    .put(&first, Bytes::from_static(b"first").into())
                    .await
                    .unwrap();
                store
                    .put(&second, Bytes::from_static(b"second").into())
                    .await
                    .unwrap();
                let (a, b) = tokio::join!(
                    store.rename_opts(&first, &to, create_options()),
                    store.rename_opts(&second, &to, create_options())
                );
                assert!(a.is_ok() ^ b.is_ok());
                let (winner, loser, content, loser_content, error) = if a.is_ok() {
                    (
                        &first,
                        &second,
                        b"first".as_slice(),
                        b"second".as_slice(),
                        b.unwrap_err(),
                    )
                } else {
                    (
                        &second,
                        &first,
                        b"second".as_slice(),
                        b"first".as_slice(),
                        a.unwrap_err(),
                    )
                };
                assert!(
                    matches!(&error, object_store::Error::AlreadyExists { path, .. }
                        if path == &to.to_string())
                );
                assert!(
                    error
                        .to_string()
                        .contains("rename destination already exists")
                );
                assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
                assert_eq!(calls.copy.load(Ordering::SeqCst), 0);
                assert_eq!(calls.rename.load(Ordering::SeqCst), 2);
                assert_eq!(calls.delete.load(Ordering::SeqCst), 0);
                assert!(matches!(
                    store.head(winner).await.unwrap_err(),
                    object_store::Error::NotFound { .. }
                ));
                assert_eq!(
                    store.get(loser).await.unwrap().bytes().await.unwrap(),
                    loser_content
                );
                assert_eq!(
                    store.get(&to).await.unwrap().bytes().await.unwrap(),
                    content
                );
            }
        }

        #[tokio::test]
        async fn create_conflict_preserves_both_objects_without_stat_or_rename() {
            let (store, calls) = store(true, true, None);
            let from = Path::from("staged source");
            let to = Path::from("final target");
            store
                .put(&from, Bytes::from_static(b"source").into())
                .await
                .unwrap();
            store
                .put(&to, Bytes::from_static(b"winner").into())
                .await
                .unwrap();
            let error = store
                .rename_opts(&from, &to, create_options())
                .await
                .unwrap_err();
            assert!(
                matches!(&error, object_store::Error::AlreadyExists { path, .. } if path == &to.to_string())
            );
            assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
            assert_eq!(calls.copy.load(Ordering::SeqCst), 1);
            assert_eq!(calls.rename.load(Ordering::SeqCst), 0);
            assert_eq!(calls.delete.load(Ordering::SeqCst), 0);
            assert_eq!(
                store.get(&from).await.unwrap().bytes().await.unwrap(),
                b"source"[..]
            );
            assert_eq!(
                store.get(&to).await.unwrap().bytes().await.unwrap(),
                b"winner"[..]
            );
        }

        #[tokio::test]
        async fn create_concurrent_winner_deletes_only_its_source() {
            let (store, calls) = store(true, true, None);
            let first = Path::from("source-a");
            let second = Path::from("source-b");
            let to = Path::from("target");
            store
                .put(&first, Bytes::from_static(b"first").into())
                .await
                .unwrap();
            store
                .put(&second, Bytes::from_static(b"second").into())
                .await
                .unwrap();
            let (a, b) = tokio::join!(
                store.rename_opts(&first, &to, create_options()),
                store.rename_opts(&second, &to, create_options())
            );
            assert!(a.is_ok() ^ b.is_ok());
            let (winner, loser, content, loser_content, error) = if a.is_ok() {
                (
                    &first,
                    &second,
                    b"first".as_slice(),
                    b"second".as_slice(),
                    b.unwrap_err(),
                )
            } else {
                (
                    &second,
                    &first,
                    b"second".as_slice(),
                    b"first".as_slice(),
                    a.unwrap_err(),
                )
            };
            assert!(matches!(error, object_store::Error::AlreadyExists { .. }));
            assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
            assert_eq!(calls.rename.load(Ordering::SeqCst), 0);
            assert_eq!(calls.delete.load(Ordering::SeqCst), 1);
            assert!(matches!(
                store.head(winner).await.unwrap_err(),
                object_store::Error::NotFound { .. }
            ));
            assert_eq!(
                store.get(loser).await.unwrap().bytes().await.unwrap(),
                loser_content
            );
            assert_eq!(
                store.get(&to).await.unwrap().bytes().await.unwrap(),
                content
            );
        }

        #[tokio::test]
        async fn unsupported_create_is_rejected_before_any_storage_operation() {
            let (store, calls) = store(false, true, None);
            let error = store
                .rename_opts(
                    &Path::from("source"),
                    &Path::from("target"),
                    create_options(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, object_store::Error::NotSupported { .. }));
            assert!(error.to_string().contains("conditional copy support"));
            assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
            assert_eq!(calls.copy.load(Ordering::SeqCst), 0);
            assert_eq!(calls.rename.load(Ordering::SeqCst), 0);
            assert_eq!(calls.delete.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        async fn overwrite_uses_native_rename_when_available() {
            let (store, calls) = store(true, true, None);
            let from = Path::from("source");
            let to = Path::from("target");
            store
                .put(&from, Bytes::from_static(b"new").into())
                .await
                .unwrap();
            store
                .put(&to, Bytes::from_static(b"old").into())
                .await
                .unwrap();
            store
                .rename_opts(&from, &to, RenameOptions::default())
                .await
                .unwrap();
            assert_eq!(calls.stat.load(Ordering::SeqCst), 0);
            assert_eq!(calls.rename.load(Ordering::SeqCst), 1);
            assert_eq!(calls.copy.load(Ordering::SeqCst), 0);
            assert_eq!(
                store.get(&to).await.unwrap().bytes().await.unwrap(),
                b"new"[..]
            );
        }

        #[tokio::test]
        async fn overwrite_keeps_object_store_copy_delete_without_native_rename() {
            let (store, calls) = store(true, false, None);
            let from = Path::from("source");
            let to = Path::from("target");
            store
                .put(&from, Bytes::from_static(b"new").into())
                .await
                .unwrap();
            store
                .rename_opts(&from, &to, RenameOptions::default())
                .await
                .unwrap();
            assert_eq!(calls.rename.load(Ordering::SeqCst), 0);
            assert_eq!(calls.copy.load(Ordering::SeqCst), 1);
            assert_eq!(calls.delete.load(Ordering::SeqCst), 1);
            assert_eq!(
                store.get(&to).await.unwrap().bytes().await.unwrap(),
                b"new"[..]
            );
        }
    }

    #[tokio::test]
    async fn test_put_multipart() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        let mut rng = rng();

        // Case complete
        let path: Path = "data/test_complete.txt".into();
        let upload = object_store.put_multipart(&path).await.unwrap();

        let mut write = WriteMultipart::new(upload);

        let mut all_bytes = vec![];
        let round = rng.random_range(1..=1024);
        for _ in 0..round {
            let size = rng.random_range(1..=1024);
            let mut bytes = vec![0; size];
            rng.fill_bytes(&mut bytes);

            all_bytes.extend_from_slice(&bytes);
            write.put(bytes.into());
        }

        let _ = write.finish().await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, all_bytes.len() as u64);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from(all_bytes)
        );

        // Case abort
        let path: Path = "data/test_abort.txt".into();
        let mut upload = object_store.put_multipart(&path).await.unwrap();
        upload.put_part(vec![1; 1024].into()).await.unwrap();
        upload.abort().await.unwrap();

        let res = object_store.head(&path).await;
        let err = res.unwrap_err();

        assert!(matches!(err, object_store::Error::NotFound { .. }))
    }

    #[tokio::test]
    async fn test_list() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let results = object_store.list(Some(&path)).collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        let mut locations = results
            .iter()
            .map(|x| x.as_ref().unwrap().location.as_ref())
            .collect::<Vec<_>>();

        let expected_files = vec![
            (
                "data/nested/test.txt",
                Bytes::from_static(b"hello, world! I am nested."),
            ),
            ("data/test.txt", Bytes::from_static(b"hello, world!")),
        ];

        let expected_locations = expected_files.iter().map(|x| x.0).collect::<Vec<&str>>();

        locations.sort();
        assert_eq!(locations, expected_locations);

        for (location, bytes) in expected_files {
            let path: Path = location.into();
            assert_eq!(
                object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let result = object_store.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.objects[0].location.as_ref(), "data/test.txt");
        assert_eq!(result.common_prefixes[0].as_ref(), "data/nested");
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let offset: Path = "data/nested/test.txt".into();
        let result = object_store
            .list_with_offset(Some(&path), &offset)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].as_ref().unwrap().location.as_ref(),
            "data/test.txt"
        );
    }

    /// Custom layer that counts stat operations and optionally records read ranges.
    mod stat_counter {
        use super::*;
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Clone)]
        pub struct StatCounterLayer {
            count: Arc<AtomicUsize>,
            read_ranges: Option<Arc<StdMutex<Vec<BytesRange>>>>,
        }

        impl StatCounterLayer {
            pub fn new(count: Arc<AtomicUsize>) -> Self {
                Self {
                    count,
                    read_ranges: None,
                }
            }

            pub fn with_read_ranges(mut self, read_ranges: Arc<StdMutex<Vec<BytesRange>>>) -> Self {
                self.read_ranges = Some(read_ranges);
                self
            }
        }

        impl opendal::raw::Layer for StatCounterLayer {
            fn apply_service(&self, srv: opendal::raw::Servicer) -> opendal::raw::Servicer {
                Arc::new(StatCounterService {
                    srv,
                    count: self.count.clone(),
                    read_ranges: self.read_ranges.clone(),
                })
            }
        }

        #[derive(Debug)]
        pub struct StatCounterService {
            srv: opendal::raw::Servicer,
            count: Arc<AtomicUsize>,
            read_ranges: Option<Arc<StdMutex<Vec<BytesRange>>>>,
        }

        struct ReadCounter {
            inner: opendal::raw::oio::Reader,
            ranges: Arc<StdMutex<Vec<BytesRange>>>,
        }

        impl opendal::raw::oio::Read for ReadCounter {
            async fn open(
                &self,
                range: BytesRange,
            ) -> opendal::Result<(
                opendal::raw::RpRead,
                Box<dyn opendal::raw::oio::ReadStreamDyn>,
            )> {
                self.inner.open(range).await
            }

            async fn read(
                &self,
                range: BytesRange,
            ) -> opendal::Result<(opendal::raw::RpRead, Buffer)> {
                self.ranges.lock().unwrap().push(range);
                self.inner.read(range).await
            }
        }

        impl opendal::raw::Service for StatCounterService {
            type Reader = opendal::raw::oio::Reader;
            type Writer = opendal::raw::oio::Writer;
            type Lister = opendal::raw::oio::Lister;
            type Deleter = opendal::raw::oio::Deleter;
            type Copier = opendal::raw::oio::Copier;
            type Composer = opendal::raw::oio::Composer;

            fn info(&self) -> opendal::raw::ServiceInfo {
                self.srv.info()
            }

            fn capability(&self) -> opendal::Capability {
                self.srv.capability()
            }

            async fn create_dir(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpCreateDir,
            ) -> opendal::Result<opendal::raw::RpCreateDir> {
                self.srv.create_dir(ctx, path, args).await
            }

            async fn stat(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpStat,
            ) -> opendal::Result<opendal::raw::RpStat> {
                self.count.fetch_add(1, Ordering::SeqCst);
                self.srv.stat(ctx, path, args).await
            }

            fn read(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpRead,
            ) -> opendal::Result<Self::Reader> {
                let reader = self.srv.read(ctx, path, args)?;
                match &self.read_ranges {
                    Some(ranges) => Ok(Box::new(ReadCounter {
                        inner: reader,
                        ranges: ranges.clone(),
                    })),
                    None => Ok(reader),
                }
            }

            fn write(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpWrite,
            ) -> opendal::Result<Self::Writer> {
                self.srv.write(ctx, path, args)
            }

            fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
                self.srv.delete(ctx)
            }

            fn list(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpList,
            ) -> opendal::Result<Self::Lister> {
                self.srv.list(ctx, path, args)
            }

            fn copy(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: opendal::raw::OpCopy,
            ) -> opendal::Result<Self::Copier> {
                self.srv.copy(ctx, from, to, args)
            }

            async fn rename(
                &self,
                ctx: &opendal::OperationContext,
                from: &str,
                to: &str,
                args: opendal::raw::OpRename,
            ) -> opendal::Result<opendal::raw::RpRename> {
                self.srv.rename(ctx, from, to, args).await
            }

            async fn presign(
                &self,
                ctx: &opendal::OperationContext,
                path: &str,
                args: opendal::raw::OpPresign,
            ) -> opendal::Result<opendal::raw::RpPresign> {
                self.srv.presign(ctx, path, args).await
            }
        }
    }

    #[tokio::test]
    async fn test_get_ranges_gap_configuration() {
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::AtomicUsize;

        let read_ranges = Arc::new(StdMutex::new(Vec::new()));
        let op = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .layer(
                stat_counter::StatCounterLayer::new(Arc::new(AtomicUsize::new(0)))
                    .with_read_ranges(read_ranges.clone()),
            );
        let store = OpendalStore::new(op);
        let location = "test_get_ranges_fetch.txt".into();
        store
            .put(&location, Bytes::from_static(b"0123456789abcdefgh").into())
            .await
            .unwrap();

        let ranges = [15..17, 0..4, 8..10];
        let result = store.get_ranges(&location, &ranges).await.unwrap();
        assert_eq!(
            result,
            vec![
                Bytes::from_static(b"fg"),
                Bytes::from_static(b"0123"),
                Bytes::from_static(b"89"),
            ]
        );
        assert_eq!(
            read_ranges.lock().unwrap().as_slice(),
            &[BytesRange::new(0, Some(17))]
        );

        read_ranges.lock().unwrap().clear();
        store
            .clone()
            .with_get_ranges_gap(0)
            .get_ranges(&location, &ranges)
            .await
            .unwrap();
        let mut actual = read_ranges.lock().unwrap().clone();
        actual.sort_unstable_by_key(BytesRange::offset);
        assert_eq!(
            actual,
            vec![
                BytesRange::new(0, Some(4)),
                BytesRange::new(8, Some(2)),
                BytesRange::new(15, Some(2)),
            ]
        );

        read_ranges.lock().unwrap().clear();
        store
            .with_get_ranges_gap(4)
            .get_ranges(&location, &ranges)
            .await
            .unwrap();
        let mut actual = read_ranges.lock().unwrap().clone();
        actual.sort_unstable_by_key(BytesRange::offset);
        assert_eq!(
            actual,
            vec![BytesRange::new(0, Some(10)), BytesRange::new(15, Some(2))]
        );
    }

    #[tokio::test]
    async fn test_get_range_no_stat() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Create a stat counter and operator with tracking layer
        let stat_count = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .layer(stat_counter::StatCounterLayer::new(stat_count.clone()));
        let store = OpendalStore::new(op);

        // Create a test file
        let location = "test_get_range.txt".into();
        let value = Bytes::from_static(b"Hello, world!");
        store.put(&location, value.clone().into()).await.unwrap();

        // Reset counter after put
        stat_count.store(0, Ordering::SeqCst);

        // Test 1: get_ranges should NOT call stat()
        #[allow(clippy::single_range_in_vec_init)]
        let ranges = [0u64..5];
        let ret = store.get_ranges(&location, &ranges).await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), ret[0]);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_ranges should not call stat()"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 2: get_opts should NOT call stat() for bounded range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Bounded(0..5)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 0..5);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for bounded range reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 3: get_opts should NOT call stat() for offset range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Offset(7)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 7..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"world!"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for offset range reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 4: get_opts should NOT call stat() for full reads
        let ret = store
            .get_opts(&location, object_store::GetOptions::default())
            .await
            .unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 0..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(value, data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for full reads"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 5: get_opts should NOT call stat() for suffix range reads
        let opts = object_store::GetOptions {
            range: Some(object_store::GetRange::Suffix(6)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        assert_eq!(ret.meta.size, value.len() as u64);
        assert_eq!(ret.range, 7..value.len() as u64);
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"world!"), data);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_opts should not call stat() for suffix range reads"
        );

        // Cleanup
        store.delete(&location).await.unwrap();
    }
}
