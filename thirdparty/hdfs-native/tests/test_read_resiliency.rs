#[cfg(feature = "integration-test")]
mod common;

#[cfg(feature = "integration-test")]
mod test {
    use std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

    use crate::common::TEST_FILE_INTS;
    use bytes::Buf;
    use futures::{StreamExt, future::try_join_all};
    use hdfs_native::{
        Client, ClientBuilder, HdfsError, Result, WriteOptions,
        minidfs::MiniDfs,
        test::{
            DATANODE_CONNECT_FAULT_INJECTOR, DATANODE_READ_ACK_BLOCKER, DATANODE_READ_BLOCKER,
            DATANODE_READ_FAULT_AFTER_PACKETS, read_path_metrics, reset_read_path_metrics,
        },
    };
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_read_resiliency() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();

        let _dfs = MiniDfs::default();
        let client = Client::default();

        let mut file = client.create("/testfile", WriteOptions::default()).await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write_bytes(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        // Failing to connect to the first datanode should failover to the next
        DATANODE_CONNECT_FAULT_INJECTOR.store(true, Ordering::SeqCst);

        let reader = client.read("/testfile").await?;
        let mut buf = reader.read_range(0, 4).await?;
        assert_eq!(buf.get_i32(), 0);

        // Create a single replicated file to test IO failures retrying on the same DataNode
        let mut file = client
            .create(
                "/testfile",
                WriteOptions::default().replication(1).overwrite(true),
            )
            .await?;
        for i in 0..TEST_FILE_INTS as i32 {
            file.write_bytes(i.to_be_bytes().to_vec().into()).await?;
        }
        file.close().await?;

        let reader = client.read("/testfile").await?;
        let mut stream = reader.read_range_stream(0, reader.file_length());
        DATANODE_READ_FAULT_AFTER_PACKETS.store(1, Ordering::SeqCst);

        let mut actual = Vec::with_capacity(reader.file_length());
        while let Some(packet) = stream.next().await {
            actual.extend_from_slice(&packet?);
        }
        let expected = (0..TEST_FILE_INTS as i32)
            .flat_map(i32::to_be_bytes)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_dropped_read_releases_total_connection_budget() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create(
                "/cancelled-connection",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "1"),
                ("dfs.client.socketcache.maxTotal", "1"),
                ("dfs.client.socketcache.expiryMsec", "60000"),
                ("dfs.client.socket-timeout", "60000"),
            ])
            .build()?;
        let reader = Arc::new(client.read("/cancelled-connection").await?);

        let blocker = Arc::new(tokio::sync::Notify::new());
        *DATANODE_READ_BLOCKER
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::clone(&blocker));
        let blocked_read = tokio::spawn({
            let reader = Arc::clone(&reader);
            async move {
                let mut stream = reader.read_range_stream(0, 4);
                stream.next().await
            }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let is_blocked = DATANODE_READ_BLOCKER
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_none();
                if is_blocked {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("DataNode reader did not reach the injected blocking point");

        reset_read_path_metrics();
        let resumed_read = tokio::spawn({
            let reader = Arc::clone(&reader);
            async move { reader.read_range(0, 4).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while read_path_metrics().datanode_connection_pool_waits == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second read did not wait for the occupied connection permit");

        blocked_read.abort();
        let error = blocked_read
            .await
            .expect_err("blocked read should be cancelled");
        assert!(error.is_cancelled());

        let resumed_read = tokio::time::timeout(Duration::from_secs(5), resumed_read).await;
        blocker.notify_waiters();
        assert_eq!(
            resumed_read
                .expect("waiting read was not notified after cancellation")
                .expect("waiting read task failed")?,
            vec![1, 2, 3, 4]
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_cancelled_final_ack_releases_total_connection_budget() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create(
                "/cancelled-final-ack",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "1"),
                ("dfs.client.socketcache.maxTotal", "1"),
                ("dfs.client.socketcache.expiryMsec", "60000"),
                ("dfs.client.socket-timeout", "60000"),
            ])
            .build()?;
        let reader = Arc::new(client.read("/cancelled-final-ack").await?);

        let blocker = Arc::new(tokio::sync::Notify::new());
        *DATANODE_READ_ACK_BLOCKER
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::clone(&blocker));
        let blocked_read = tokio::spawn({
            let reader = Arc::clone(&reader);
            async move { reader.read_range(0, 4).await }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let is_blocked = DATANODE_READ_ACK_BLOCKER
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_none();
                if is_blocked {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("DataNode reader did not reach the final acknowledgement");

        reset_read_path_metrics();
        let resumed_read = tokio::spawn({
            let reader = Arc::clone(&reader);
            async move { reader.read_range(0, 4).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while read_path_metrics().datanode_connection_pool_waits == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second read did not wait for the occupied connection permit");

        blocked_read.abort();
        let error = blocked_read
            .await
            .expect_err("blocked read should be cancelled");
        assert!(error.is_cancelled());

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), resumed_read)
                .await
                .expect("waiting read was not notified after cancellation")
                .expect("waiting read task failed")?,
            vec![1, 2, 3, 4]
        );
        blocker.notify_waiters();

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_datanode_read_timeout_releases_total_connection_budget() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create(
                "/timed-out-connection",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "1"),
                ("dfs.client.socketcache.maxTotal", "1"),
                ("dfs.client.socketcache.expiryMsec", "60000"),
                ("dfs.client.socket-timeout", "25"),
                ("dfs.client.max.block.acquire.failures", "0"),
            ])
            .build()?;
        let reader = client.read("/timed-out-connection").await?;

        let blocker = Arc::new(tokio::sync::Notify::new());
        *DATANODE_READ_BLOCKER
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::clone(&blocker));
        let timed_out_read = tokio::time::timeout(Duration::from_secs(5), reader.read_range(0, 4))
            .await
            .expect("DataNode packet timeout did not terminate the read");
        blocker.notify_waiters();
        let error = timed_out_read.expect_err("blocked DataNode read should time out");
        assert!(
            matches!(&error, HdfsError::DataTransferError(message) if message.contains("All DataNodes failed")),
            "unexpected timeout error: {error:?}"
        );

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), reader.read_range(0, 4))
                .await
                .expect("connection permit was not released after timeout")?,
            vec![1, 2, 3, 4]
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_positioned_reads_reuse_reader_block_locations() -> Result<()> {
        let _dfs = MiniDfs::default();
        let client = ClientBuilder::new()
            .with_config([("dfs.client.retry.window.base", "0")])
            .build()?;

        let mut file = client
            .create("/cloned-file", WriteOptions::default().replication(1))
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        reset_read_path_metrics();
        let source = Arc::new(client.read("/cloned-file").await?);
        let contents = try_join_all((0..16).map(|_| {
            let source = Arc::clone(&source);
            async move { source.read_range(0, 4).await }
        }))
        .await?;
        assert!(contents.iter().all(|bytes| bytes.as_ref() == [1, 2, 3, 4]));

        let metrics = read_path_metrics();
        assert_eq!(metrics.file_reader_opens, 1);
        assert_eq!(metrics.block_locations_cache_misses, 1);
        assert_eq!(metrics.block_locations_cache_hits, 0);

        // Like DFSInputStream, a DataNode failure refreshes locations from the
        // NameNode and retries the current read.
        reset_read_path_metrics();
        DATANODE_CONNECT_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        assert_eq!(source.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let metrics = read_path_metrics();
        assert_eq!(metrics.block_locations_cache_hits, 0);
        assert_eq!(metrics.block_locations_cache_misses, 1);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_datanode_connection_cache_reuses_and_expires_connections() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create("/connection-cache", WriteOptions::default().replication(1))
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let cached = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "2"),
                ("dfs.client.socketcache.expiryMsec", "60000"),
                ("dfs.client.cached.conn.retry", "3"),
            ])
            .build()?;
        let reader = cached.read("/connection-cache").await?;
        reset_read_path_metrics();
        for _ in 0..3 {
            assert_eq!(reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        }
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 1);
        assert_eq!(metrics.datanode_connection_cache_misses, 1);
        assert_eq!(metrics.datanode_connection_cache_hits, 2);

        let uncached = ClientBuilder::new()
            .with_config([("dfs.client.socketcache.capacity", "0")])
            .build()?;
        let reader = uncached.read("/connection-cache").await?;
        reset_read_path_metrics();
        for _ in 0..2 {
            assert_eq!(reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        }
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 2);
        assert_eq!(metrics.datanode_connection_cache_misses, 2);
        assert_eq!(metrics.datanode_connection_cache_hits, 0);

        let expiring = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "1"),
                ("dfs.client.socketcache.expiryMsec", "10"),
            ])
            .build()?;
        let reader = expiring.read("/connection-cache").await?;
        assert_eq!(reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        reset_read_path_metrics();
        assert_eq!(reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 1);
        assert_eq!(metrics.datanode_connection_cache_hits, 0);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_datanode_connection_cache_is_client_scoped() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create(
                "/client-scoped-connection-cache",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let config = [
            ("dfs.client.socketcache.capacity", "1"),
            ("dfs.client.socketcache.expiryMsec", "60000"),
        ];
        let first_client = ClientBuilder::new().with_config(config).build()?;
        let second_client = ClientBuilder::new().with_config(config).build()?;
        let first_reader = first_client.read("/client-scoped-connection-cache").await?;
        let second_reader = second_client
            .read("/client-scoped-connection-cache")
            .await?;

        reset_read_path_metrics();
        assert_eq!(first_reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        assert_eq!(second_reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 2);
        assert_eq!(metrics.datanode_connection_cache_misses, 2);
        assert_eq!(metrics.datanode_connection_cache_hits, 0);

        reset_read_path_metrics();
        assert_eq!(first_reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        assert_eq!(second_reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 0);
        assert_eq!(metrics.datanode_connection_cache_misses, 0);
        assert_eq!(metrics.datanode_connection_cache_hits, 2);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_datanode_total_connection_budget_waits_and_reuses() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let mut file = writer_client
            .create(
                "/bounded-connection-cache",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "2"),
                ("dfs.client.socketcache.maxTotal", "2"),
                ("dfs.client.socketcache.expiryMsec", "60000"),
            ])
            .build()?;
        let reader = Arc::new(client.read("/bounded-connection-cache").await?);
        let barrier = Arc::new(tokio::sync::Barrier::new(16));

        reset_read_path_metrics();
        let reads = (0..16).map(|_| {
            let reader = Arc::clone(&reader);
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                reader.read_range(0, 4).await
            }
        });
        let results = try_join_all(reads).await?;
        assert!(results.iter().all(|bytes| bytes.as_ref() == [1, 2, 3, 4]));
        let metrics = read_path_metrics();
        assert!(metrics.datanode_new_connections <= 2);
        assert!(metrics.datanode_connection_pool_waits > 0);

        reset_read_path_metrics();
        assert_eq!(reader.read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let metrics = read_path_metrics();
        assert_eq!(metrics.datanode_new_connections, 0);
        assert_eq!(metrics.datanode_connection_cache_hits, 1);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_block_locations_cache_reuses_concurrent_opens() -> Result<()> {
        let _dfs = MiniDfs::default();
        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.read.block-locations-cache.capacity", "8"),
                ("dfs.client.read.block-locations-cache.ttl-ms", "60000"),
                ("dfs.client.retry.window.base", "0"),
            ])
            .build()?;

        let mut file = client
            .create("/cached-file", WriteOptions::default().replication(1))
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        reset_read_path_metrics();
        let readers = try_join_all((0..16).map(|_| client.read("/cached-file"))).await?;
        let metrics = read_path_metrics();
        assert_eq!(metrics.file_reader_opens, 16);
        assert_eq!(metrics.block_locations_cache_misses, 1);
        assert_eq!(metrics.block_locations_cache_hits, 15);

        let contents = try_join_all(readers.iter().map(|reader| reader.read_range(0, 4))).await?;
        assert!(contents.iter().all(|bytes| bytes.as_ref() == [1, 2, 3, 4]));

        // A DataNode failure refreshes the shared entry once; a later open
        // reuses the refreshed map instead of issuing another NameNode RPC.
        reset_read_path_metrics();
        DATANODE_CONNECT_FAULT_INJECTOR.store(true, Ordering::SeqCst);
        assert_eq!(readers[0].read_range(0, 4).await?, vec![1, 2, 3, 4]);
        let reopened = client.read("/cached-file").await?;
        assert_eq!(reopened.file_length(), 4);
        let metrics = read_path_metrics();
        assert_eq!(metrics.block_locations_cache_misses, 1);
        assert_eq!(metrics.block_locations_cache_hits, 1);

        assert!(client.delete("/cached-file", false).await?);
        for index in 0..8 {
            let path = format!("/hot-file-{index}");
            let mut file = client
                .create(&path, WriteOptions::default().replication(1))
                .await?;
            file.write_bytes(vec![index as u8].into()).await?;
            file.close().await?;
        }

        // Model two hot-query rounds that independently reopen the same set of
        // physical files. The cold round performs one RPC per file; the next
        // round performs none.
        reset_read_path_metrics();
        let first_round = try_join_all((0..8).map(|index| {
            let path = format!("/hot-file-{index}");
            let client = client.clone();
            async move { client.read(&path).await }
        }))
        .await?;
        assert_eq!(first_round.len(), 8);
        let metrics = read_path_metrics();
        assert_eq!(metrics.block_locations_cache_misses, 8);
        assert_eq!(metrics.block_locations_cache_hits, 0);

        reset_read_path_metrics();
        let second_round = try_join_all((0..8).map(|index| {
            let path = format!("/hot-file-{index}");
            let client = client.clone();
            async move { client.read(&path).await }
        }))
        .await?;
        assert_eq!(second_round.len(), 8);
        let metrics = read_path_metrics();
        assert_eq!(metrics.block_locations_cache_misses, 0);
        assert_eq!(metrics.block_locations_cache_hits, 8);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_same_client_mutations_invalidate_block_locations_cache() -> Result<()> {
        let _dfs = MiniDfs::default();
        let client = ClientBuilder::new()
            .with_config([
                ("dfs.client.read.block-locations-cache.capacity", "8"),
                ("dfs.client.read.block-locations-cache.ttl-ms", "60000"),
            ])
            .build()?;

        let mut file = client
            .create(
                "/append-invalidates",
                WriteOptions::default().replication(1),
            )
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let original = client.read("/append-invalidates").await?;
        let cached = client.read("/append-invalidates").await?;
        assert_eq!(original.file_length(), 4);
        assert_eq!(cached.file_length(), 4);

        let mut file = client.append("/append-invalidates").await?;
        file.write_bytes(vec![5, 6, 7, 8].into()).await?;
        file.close().await?;

        reset_read_path_metrics();
        let fresh = client.read("/append-invalidates").await?;
        assert_eq!(fresh.file_length(), 8);
        assert_eq!(fresh.read_range(4, 4).await?, vec![5, 6, 7, 8]);
        assert_eq!(read_path_metrics().block_locations_cache_misses, 1);

        // An already-open reader retains its original snapshot, like
        // DFSInputStream.
        assert_eq!(original.file_length(), 4);

        let mut file = client
            .create(
                "/append-invalidates",
                WriteOptions::default().replication(1).overwrite(true),
            )
            .await?;
        file.write_bytes(vec![9].into()).await?;
        file.close().await?;

        reset_read_path_metrics();
        let replaced = client.read("/append-invalidates").await?;
        assert_eq!(replaced.read_range(0, 1).await?, vec![9]);
        assert_eq!(read_path_metrics().block_locations_cache_misses, 1);

        let mut file = client
            .create("/rename-target", WriteOptions::default().replication(1))
            .await?;
        file.write_bytes(vec![0].into()).await?;
        file.close().await?;
        let _ = client.read("/rename-target").await?;

        client
            .rename("/append-invalidates", "/rename-target", true)
            .await?;
        reset_read_path_metrics();
        let renamed = client.read("/rename-target").await?;
        assert_eq!(renamed.read_range(0, 1).await?, vec![9]);
        assert_eq!(read_path_metrics().block_locations_cache_misses, 1);

        assert!(client.delete("/rename-target", false).await?);
        reset_read_path_metrics();
        assert!(client.read("/rename-target").await.is_err());
        assert_eq!(read_path_metrics().block_locations_cache_misses, 1);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_normal_open_is_fresh_after_external_append() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        let reader_client = Client::default();

        let mut file = writer_client
            .create("/external-append", WriteOptions::default().replication(1))
            .await?;
        file.write_bytes(vec![1, 2, 3, 4].into()).await?;
        file.close().await?;

        let original = reader_client.read("/external-append").await?;
        assert_eq!(original.file_length(), 4);

        let mut file = writer_client.append("/external-append").await?;
        file.write_bytes(vec![5, 6, 7, 8].into()).await?;
        file.close().await?;

        // A regular open always asks the NameNode, even while the old reader
        // is alive.
        let fresh_reader = reader_client.read("/external-append").await?;
        assert_eq!(fresh_reader.file_length(), 8);
        assert_eq!(fresh_reader.read_range(4, 4).await?, vec![5, 6, 7, 8]);
        assert_eq!(original.file_length(), 4);

        Ok(())
    }
}
