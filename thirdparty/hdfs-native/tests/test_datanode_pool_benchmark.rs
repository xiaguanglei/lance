#[cfg(feature = "integration-test")]
mod benchmark {
    use std::{hint::black_box, sync::Arc};

    use futures::future::try_join_all;
    use hdfs_native::{
        Client, ClientBuilder, Result, WriteOptions,
        file::FileReader,
        minidfs::MiniDfs,
        test::{read_path_metrics, reset_read_path_metrics},
    };
    use serial_test::serial;

    const CONCURRENCY: usize = 64;
    const RANGE_BYTES: usize = 2 * 1024;
    const FILE_BYTES: usize = 8 * 1024 * 1024;

    #[derive(Debug)]
    struct Sample {
        operations_per_second: f64,
        new_connections: u64,
        cache_hits: u64,
        cache_rejected: u64,
        pool_waits: u64,
    }

    async fn write_fixture(client: &Client) -> Result<()> {
        let mut writer = client
            .create(
                "/datanode-pool-benchmark",
                WriteOptions::default().replication(1).overwrite(true),
            )
            .await?;
        let data = (0..FILE_BYTES)
            .map(|offset| (offset % 251) as u8)
            .collect::<Vec<_>>();
        writer.write_bytes(data.into()).await?;
        writer.close().await
    }

    async fn run_load(reader: Arc<FileReader>, waves: usize) -> Result<u64> {
        let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
        let operations = try_join_all((0..CONCURRENCY).map(|worker| {
            let reader = Arc::clone(&reader);
            let barrier = Arc::clone(&barrier);
            async move {
                for wave in 0..waves {
                    // Model one query fan-out: all range reads begin together,
                    // then all connections are returned before the next query.
                    barrier.wait().await;
                    let slot = worker + wave;
                    let offset = (slot * RANGE_BYTES) % (FILE_BYTES - RANGE_BYTES);
                    let bytes = reader.read_range(offset, RANGE_BYTES).await?;
                    black_box(bytes);
                    barrier.wait().await;
                }
                Ok::<_, hdfs_native::HdfsError>(waves as u64)
            }
        }))
        .await?;
        Ok(operations.into_iter().sum())
    }

    async fn sample(reader: Arc<FileReader>, waves: usize) -> Result<Sample> {
        run_load(Arc::clone(&reader), 1).await?;
        reset_read_path_metrics();
        let started_at = std::time::Instant::now();
        let operations = run_load(reader, waves).await?;
        let elapsed = started_at.elapsed().as_secs_f64();
        let metrics = read_path_metrics();
        Ok(Sample {
            operations_per_second: operations as f64 / elapsed,
            new_connections: metrics.datanode_new_connections,
            cache_hits: metrics.datanode_connection_cache_hits,
            cache_rejected: metrics.datanode_connection_cache_rejected,
            pool_waits: metrics.datanode_connection_pool_waits,
        })
    }

    fn median(values: &mut [f64]) -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }

    /// Local-only throughput proxy for Lance's many small HDFS range reads.
    /// Run explicitly with:
    /// `cargo test --release --features integration-test --test test_datanode_pool_benchmark -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    #[ignore = "local MiniDFS throughput benchmark"]
    async fn compare_hadoop_default_and_gateway_pool() -> Result<()> {
        let _dfs = MiniDfs::default();
        let writer_client = Client::default();
        write_fixture(&writer_client).await?;

        let hadoop_default = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "16"),
                ("dfs.client.socketcache.expiryMsec", "3000"),
                ("dfs.client.socketcache.maxTotal", "0"),
            ])
            .build()?;
        let gateway = ClientBuilder::new()
            .with_config([
                ("dfs.client.socketcache.capacity", "512"),
                ("dfs.client.socketcache.expiryMsec", "3000"),
                ("dfs.client.socketcache.maxTotal", "2048"),
            ])
            .build()?;
        let hadoop_default = Arc::new(hadoop_default.read("/datanode-pool-benchmark").await?);
        let gateway = Arc::new(gateway.read("/datanode-pool-benchmark").await?);

        let waves = std::env::var("HDFS_POOL_BENCH_WAVES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100);
        let mut old_samples = Vec::new();
        let mut new_samples = Vec::new();
        for repetition in 0..3 {
            let order = if repetition % 2 == 0 {
                [
                    ("idle=16", Arc::clone(&hadoop_default)),
                    ("idle=512,total=2048", Arc::clone(&gateway)),
                ]
            } else {
                [
                    ("idle=512,total=2048", Arc::clone(&gateway)),
                    ("idle=16", Arc::clone(&hadoop_default)),
                ]
            };
            for (name, reader) in order {
                let result = sample(reader, waves).await?;
                println!(
                    "POOL_BENCH rep={} config={} ops_s={:.1} new_connections={} cache_hits={} cache_rejected={} pool_waits={}",
                    repetition + 1,
                    name,
                    result.operations_per_second,
                    result.new_connections,
                    result.cache_hits,
                    result.cache_rejected,
                    result.pool_waits,
                );
                if name == "idle=16" {
                    old_samples.push(result.operations_per_second);
                } else {
                    new_samples.push(result.operations_per_second);
                }
            }
        }

        let old_median = median(&mut old_samples);
        let new_median = median(&mut new_samples);
        println!(
            "POOL_BENCH_SUMMARY concurrency={} waves={} range_bytes={} old_ops_s={:.1} new_ops_s={:.1} improvement={:+.1}%",
            CONCURRENCY,
            waves,
            RANGE_BYTES,
            old_median,
            new_median,
            (new_median / old_median - 1.0) * 100.0,
        );
        Ok(())
    }
}
