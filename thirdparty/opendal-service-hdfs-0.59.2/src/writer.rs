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

use std::io;
use std::sync::Arc;

use bytes::Buf;
use futures::{AsyncWrite, AsyncWriteExt};
use log::warn;

use opendal_core::raw::*;
use opendal_core::*;

pub(crate) trait HdfsRenameClient: Send + Sync {
    fn path_exists(&self, path: &str) -> io::Result<bool>;
    fn remove_file(&self, path: &str) -> io::Result<()>;
    fn rename_file(&self, from: &str, to: &str) -> io::Result<()>;
}

impl HdfsRenameClient for hdrs::Client {
    fn path_exists(&self, path: &str) -> io::Result<bool> {
        match self.metadata(path) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn remove_file(&self, path: &str) -> io::Result<()> {
        hdrs::Client::remove_file(self, path)
    }

    fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
        hdrs::Client::rename_file(self, from, to)
    }
}

fn publish_staged_file(
    client: &dyn HdfsRenameClient,
    staged: &str,
    target: &str,
    target_existed: bool,
    if_not_exists: bool,
) -> Result<()> {
    if !if_not_exists && target_existed {
        client.remove_file(target).map_err(new_std_io_error)?;
    }
    client.rename_file(staged, target).map_err(|error| {
        // Some JNI providers report a generic rename failure for an existing
        // file. Confirm the destination, but never mask permission failures.
        let conflict = if_not_exists
            && (error.kind() == io::ErrorKind::AlreadyExists
                || (error.kind() != io::ErrorKind::PermissionDenied
                    && client.path_exists(target).unwrap_or(false)));
        if conflict {
            Error::new(ErrorKind::ConditionNotMatch, "destination already exists")
                .with_context("path", target)
                .set_source(error)
        } else {
            new_std_io_error(error)
        }
    })
}

pub struct HdfsWriter<F> {
    target_path: String,
    tmp_path: Option<String>,
    f: Option<F>,
    client: Arc<dyn HdfsRenameClient>,
    target_path_exists: bool,
    if_not_exists: bool,
    size: u64,
}

/// # Safety
///
/// We will only take `&mut Self` reference for HdfsWriter.
unsafe impl<F: Send> Sync for HdfsWriter<F> {}

impl<F> HdfsWriter<F> {
    pub(crate) fn new(
        target_path: String,
        tmp_path: Option<String>,
        f: F,
        client: Arc<dyn HdfsRenameClient>,
        target_path_exists: bool,
        if_not_exists: bool,
        initial_size: u64,
    ) -> Self {
        Self {
            target_path,
            tmp_path,
            f: Some(f),
            client,
            target_path_exists,
            if_not_exists,
            size: initial_size,
        }
    }

    fn cleanup_staging(&mut self) -> Result<()> {
        let Some(staged) = self.tmp_path.as_ref() else {
            return Ok(());
        };
        match self.client.remove_file(staged) {
            Ok(()) => self.tmp_path = None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => self.tmp_path = None,
            Err(error) => return Err(new_std_io_error(error)),
        }
        Ok(())
    }
}

impl<F> Drop for HdfsWriter<F> {
    fn drop(&mut self) {
        // Close the owned handle before removing an uncommitted staging file.
        self.f.take();
        if let Err(error) = self.cleanup_staging() {
            warn!("failed to clean up HDFS staging writer: {error}");
        }
    }
}

impl<F: AsyncWrite + Unpin + Send> oio::Write for HdfsWriter<F> {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        let len = bs.len() as u64;
        let f = self.f.as_mut().ok_or_else(closed_writer_error)?;

        while bs.has_remaining() {
            let n = f.write(bs.chunk()).await.map_err(new_std_io_error)?;
            if n == 0 {
                return Err(new_std_io_error(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "HDFS writer accepted zero bytes",
                )));
            }
            bs.advance(n);
        }

        self.size = self
            .size
            .checked_add(len)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "HDFS writer size overflow"))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let f = self.f.as_mut().ok_or_else(closed_writer_error)?;
        f.close().await.map_err(new_std_io_error)?;
        self.f = None;

        // TODO: we need to make rename async.
        if let Some(tmp_path) = &self.tmp_path {
            let published = publish_staged_file(
                self.client.as_ref(),
                tmp_path,
                &self.target_path,
                self.target_path_exists,
                self.if_not_exists,
            );
            if let Err(error) = published {
                if let Err(cleanup_error) = self.cleanup_staging() {
                    warn!(
                        "failed to clean up HDFS staging file after publication failed: {cleanup_error}"
                    );
                }
                return Err(error);
            }
            self.tmp_path = None;
        }

        Ok({
            let mut metadata = MetadataBuilder::unknown();
            metadata.set_file(self.size);
            metadata.build()
        })
    }

    async fn abort(&mut self) -> Result<()> {
        if self.tmp_path.is_none() {
            return if self.f.is_none() {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "cannot abort a direct HDFS write",
                ))
            };
        }
        if let Some(f) = self.f.as_mut()
            && let Err(error) = f.close().await
        {
            warn!("failed to close HDFS staging writer during abort: {error}");
        }
        self.f = None;
        self.cleanup_staging()
    }
}

fn closed_writer_error() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "HDFS writer is already closed or aborted",
    )
}

#[cfg(test)]
mod tests {
    use opendal_core::raw::oio::Write;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use super::*;

    #[derive(Default)]
    struct State {
        paths: HashMap<String, Vec<u8>>,
        events: Vec<String>,
        rename_error: Option<io::ErrorKind>,
    }

    #[derive(Default)]
    struct FakeClient(Mutex<State>);

    impl FakeClient {
        fn staged(target_exists: bool, rename_error: Option<io::ErrorKind>) -> Arc<Self> {
            let client = Arc::new(Self::default());
            {
                let mut state = client.0.lock().unwrap();
                state.paths.insert("/staged".into(), b"new".to_vec());
                if target_exists {
                    state.paths.insert("/target".into(), b"original".to_vec());
                }
                state.rename_error = rename_error;
            }
            client
        }
    }

    impl HdfsRenameClient for FakeClient {
        fn path_exists(&self, path: &str) -> io::Result<bool> {
            Ok(self.0.lock().unwrap().paths.contains_key(path))
        }

        fn remove_file(&self, path: &str) -> io::Result<()> {
            let mut state = self.0.lock().unwrap();
            state.events.push(format!("remove:{path}"));
            state
                .paths
                .remove(path)
                .map(|_| ())
                .ok_or_else(|| io::ErrorKind::NotFound.into())
        }

        fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
            let mut state = self.0.lock().unwrap();
            state.events.push(format!("rename:{from}:{to}"));
            if let Some(kind) = state.rename_error {
                return Err(kind.into());
            }
            if state.paths.contains_key(to) {
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            let bytes = state.paths.remove(from).ok_or(io::ErrorKind::NotFound)?;
            state.paths.insert(to.into(), bytes);
            Ok(())
        }
    }

    struct StubFile {
        client: Arc<FakeClient>,
        close_error: bool,
        pending_close: bool,
        zero_write: bool,
    }

    impl Drop for StubFile {
        fn drop(&mut self) {
            self.client
                .0
                .lock()
                .unwrap()
                .events
                .push("file-drop".into());
        }
    }

    impl AsyncWrite for StubFile {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(if self.zero_write { 0 } else { bytes.len() }))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.pending_close {
                return Poll::Pending;
            }
            self.client
                .0
                .lock()
                .unwrap()
                .events
                .push("file-close".into());
            Poll::Ready(if self.close_error {
                Err(io::ErrorKind::Other.into())
            } else {
                Ok(())
            })
        }
    }

    fn writer(
        client: Arc<FakeClient>,
        conditional: bool,
        target_existed: bool,
    ) -> HdfsWriter<StubFile> {
        HdfsWriter::new(
            "/target".into(),
            Some("/staged".into()),
            StubFile {
                client: client.clone(),
                close_error: false,
                pending_close: false,
                zero_write: false,
            },
            client,
            target_existed,
            conditional,
            0,
        )
    }

    fn ready<T>(future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("hermetic writer fixture unexpectedly blocked"),
        }
    }

    #[test]
    fn conditional_publish_preserves_target_and_reports_failures() {
        for (target_exists, rename_error, expected) in [
            (false, None, None),
            (true, None, Some(ErrorKind::ConditionNotMatch)),
            (
                false,
                Some(io::ErrorKind::PermissionDenied),
                Some(ErrorKind::PermissionDenied),
            ),
            (
                true,
                Some(io::ErrorKind::PermissionDenied),
                Some(ErrorKind::PermissionDenied),
            ),
            (
                false,
                Some(io::ErrorKind::Other),
                Some(ErrorKind::Unexpected),
            ),
            (
                true,
                Some(io::ErrorKind::Other),
                Some(ErrorKind::ConditionNotMatch),
            ),
        ] {
            let client = FakeClient::staged(target_exists, rename_error);
            let mut writer = writer(client.clone(), true, false);
            let result = ready(writer.close());
            match expected {
                Some(kind) => assert_eq!(result.unwrap_err().kind(), kind),
                None => assert_eq!(result.unwrap().content_length(), 0),
            }
            drop(writer);
            let state = client.0.lock().unwrap();
            assert!(!state.paths.contains_key("/staged"));
            assert!(!state.events.iter().any(|event| event == "remove:/target"));
            if target_exists {
                assert_eq!(state.paths["/target"], b"original");
            } else if expected.is_none() {
                assert_eq!(state.paths["/target"], b"new");
            } else {
                assert!(!state.paths.contains_key("/target"));
            }
            let dropped = state
                .events
                .iter()
                .position(|event| event == "file-drop")
                .unwrap();
            let published = state
                .events
                .iter()
                .position(|event| event.starts_with("rename:"))
                .unwrap();
            assert!(
                dropped < published,
                "JNI file ownership must end before publication"
            );
        }
    }

    #[test]
    fn concurrent_conditional_publish_has_one_winner_and_no_staged_files() {
        let client = FakeClient::staged(false, None);
        client
            .0
            .lock()
            .unwrap()
            .paths
            .insert("/second".into(), b"other".to_vec());
        let outcomes = std::thread::scope(|scope| {
            let first_client = client.clone();
            let second_client = client.clone();
            let first = scope.spawn(move || ready(writer(first_client, true, false).close()));
            let second = scope.spawn(move || {
                let mut writer = writer(second_client, true, false);
                writer.tmp_path = Some("/second".into());
                ready(writer.close())
            });
            [first.join().unwrap(), second.join().unwrap()]
        });
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes.into_iter().find_map(Result::err).unwrap().kind(),
            ErrorKind::ConditionNotMatch
        );
        let state = client.0.lock().unwrap();
        assert_eq!(state.paths.len(), 1);
        assert!(state.paths.contains_key("/target"));
    }

    #[test]
    fn staged_abort_drop_and_close_error_never_publish() {
        for mode in ["abort", "drop", "close-error", "abort-close-error"] {
            let client = FakeClient::staged(true, None);
            let mut writer = writer(client.clone(), true, false);
            writer.f.as_mut().unwrap().close_error = mode.ends_with("error");
            if mode.starts_with("abort") {
                assert!(ready(writer.abort()).is_ok());
                assert!(ready(writer.abort()).is_ok());
                assert_eq!(
                    ready(writer.write(Buffer::from("later")))
                        .unwrap_err()
                        .kind(),
                    ErrorKind::Unexpected
                );
            } else if mode == "close-error" {
                assert!(ready(writer.close()).is_err());
            }
            drop(writer);
            let state = client.0.lock().unwrap();
            assert_eq!(state.paths.len(), 1);
            assert_eq!(state.paths["/target"], b"original");
            assert!(
                !state
                    .events
                    .iter()
                    .any(|event| event.starts_with("rename:"))
            );
            let dropped = state
                .events
                .iter()
                .position(|event| event == "file-drop")
                .unwrap();
            let removed = state
                .events
                .iter()
                .position(|event| event == "remove:/staged")
                .unwrap();
            assert!(dropped < removed);
        }
    }

    #[test]
    fn cancelling_pending_close_cleans_only_the_owned_staging_file() {
        let client = FakeClient::staged(true, None);
        let mut writer = writer(client.clone(), true, false);
        writer.f.as_mut().unwrap().pending_close = true;
        {
            let mut future = std::pin::pin!(writer.close());
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        drop(writer);
        let state = client.0.lock().unwrap();
        assert_eq!(state.paths.len(), 1);
        assert_eq!(state.paths["/target"], b"original");
        assert!(
            !state
                .events
                .iter()
                .any(|event| event.starts_with("rename:"))
        );
    }

    #[test]
    fn ordinary_overwrite_is_unchanged_and_direct_abort_never_deletes_target() {
        let client = FakeClient::staged(true, None);
        let mut staged_writer = writer(client.clone(), false, true);
        assert!(ready(staged_writer.close()).is_ok());
        assert_eq!(client.0.lock().unwrap().paths["/target"], b"new");
        let mut direct_writer = writer(client.clone(), false, true);
        direct_writer.tmp_path = None;
        assert_eq!(
            ready(direct_writer.abort()).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        drop(direct_writer);
        assert_eq!(client.0.lock().unwrap().paths["/target"], b"new");
    }

    #[test]
    fn zero_byte_write_fails_instead_of_looping_and_closed_writer_cannot_reopen() {
        let client = FakeClient::staged(false, None);
        let mut writer = writer(client.clone(), true, false);
        writer.f.as_mut().unwrap().zero_write = true;
        assert!(ready(writer.write(Buffer::from("data"))).is_err());
        assert!(ready(writer.abort()).is_ok());
        assert_eq!(
            ready(writer.close()).unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        assert!(!client.0.lock().unwrap().paths.contains_key("/target"));
    }
}
