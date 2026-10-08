use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use tokio::sync::Notify;

pub static DATANODE_CONNECT_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static NAMENODE_STANDBY_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static DATANODE_READ_FAULT_AFTER_PACKETS: AtomicU64 = AtomicU64::new(u64::MAX);
pub static DATANODE_READ_BLOCKER: Mutex<Option<Arc<Notify>>> = Mutex::new(None);
pub static DATANODE_READ_ACK_BLOCKER: Mutex<Option<Arc<Notify>>> = Mutex::new(None);
pub static EC_FAULT_INJECTOR: Mutex<Option<EcFaultInjection>> = Mutex::new(None);
pub static WRITE_CONNECTION_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static WRITE_REPLY_FAULT_INJECTOR: Mutex<Option<usize>> = Mutex::new(None);
pub static NAMENODE_RESPONSE_FAULT_INJECTOR: AtomicBool = AtomicBool::new(false);
pub static PROXY_CALLS: Mutex<Option<Vec<(&'static str, bool)>>> = Mutex::new(None);

/// Number of `Client::read` calls.
pub static FILE_READER_OPENS: AtomicU64 = AtomicU64::new(0);
/// Wall-clock nanoseconds spent opening readers, including getBlockLocations.
pub static FILE_READER_OPEN_NANOS: AtomicU64 = AtomicU64::new(0);
/// Number of `Client::read` calls served by the block-location cache.
pub static BLOCK_LOCATIONS_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
/// Number of `getBlockLocations` calls, including read-error refreshes.
pub static BLOCK_LOCATIONS_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_CONNECTION_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_CONNECTION_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_CONNECTION_CACHE_EXPIRED: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_CONNECTION_CACHE_REJECTED: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_CONNECTION_POOL_WAITS: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_NEW_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
pub static DATANODE_NEW_CONNECTION_NANOS: AtomicU64 = AtomicU64::new(0);
pub static CHECKSUM_BYTES_VERIFIED: AtomicU64 = AtomicU64::new(0);
pub static CHECKSUM_VERIFY_NANOS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn should_inject_datanode_read_fault() -> bool {
    let mut remaining = DATANODE_READ_FAULT_AFTER_PACKETS.load(Ordering::SeqCst);
    loop {
        if remaining == u64::MAX {
            return false;
        }
        let next = if remaining == 0 {
            u64::MAX
        } else {
            remaining - 1
        };
        match DATANODE_READ_FAULT_AFTER_PACKETS.compare_exchange(
            remaining,
            next,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return remaining == 0,
            Err(actual) => remaining = actual,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReadPathMetrics {
    pub file_reader_opens: u64,
    pub file_reader_open_nanos: u64,
    pub block_locations_cache_hits: u64,
    pub block_locations_cache_misses: u64,
    pub datanode_connection_cache_hits: u64,
    pub datanode_connection_cache_misses: u64,
    pub datanode_connection_cache_expired: u64,
    pub datanode_connection_cache_rejected: u64,
    pub datanode_connection_pool_waits: u64,
    pub datanode_new_connections: u64,
    pub datanode_new_connection_nanos: u64,
    pub checksum_bytes_verified: u64,
    pub checksum_verify_nanos: u64,
}

pub fn reset_read_path_metrics() {
    FILE_READER_OPENS.store(0, Ordering::Relaxed);
    FILE_READER_OPEN_NANOS.store(0, Ordering::Relaxed);
    BLOCK_LOCATIONS_CACHE_HITS.store(0, Ordering::Relaxed);
    BLOCK_LOCATIONS_CACHE_MISSES.store(0, Ordering::Relaxed);
    DATANODE_CONNECTION_CACHE_HITS.store(0, Ordering::Relaxed);
    DATANODE_CONNECTION_CACHE_MISSES.store(0, Ordering::Relaxed);
    DATANODE_CONNECTION_CACHE_EXPIRED.store(0, Ordering::Relaxed);
    DATANODE_CONNECTION_CACHE_REJECTED.store(0, Ordering::Relaxed);
    DATANODE_CONNECTION_POOL_WAITS.store(0, Ordering::Relaxed);
    DATANODE_NEW_CONNECTIONS.store(0, Ordering::Relaxed);
    DATANODE_NEW_CONNECTION_NANOS.store(0, Ordering::Relaxed);
    CHECKSUM_BYTES_VERIFIED.store(0, Ordering::Relaxed);
    CHECKSUM_VERIFY_NANOS.store(0, Ordering::Relaxed);
}

pub fn read_path_metrics() -> ReadPathMetrics {
    ReadPathMetrics {
        file_reader_opens: FILE_READER_OPENS.load(Ordering::Relaxed),
        file_reader_open_nanos: FILE_READER_OPEN_NANOS.load(Ordering::Relaxed),
        block_locations_cache_hits: BLOCK_LOCATIONS_CACHE_HITS.load(Ordering::Relaxed),
        block_locations_cache_misses: BLOCK_LOCATIONS_CACHE_MISSES.load(Ordering::Relaxed),
        datanode_connection_cache_hits: DATANODE_CONNECTION_CACHE_HITS.load(Ordering::Relaxed),
        datanode_connection_cache_misses: DATANODE_CONNECTION_CACHE_MISSES.load(Ordering::Relaxed),
        datanode_connection_cache_expired: DATANODE_CONNECTION_CACHE_EXPIRED
            .load(Ordering::Relaxed),
        datanode_connection_cache_rejected: DATANODE_CONNECTION_CACHE_REJECTED
            .load(Ordering::Relaxed),
        datanode_connection_pool_waits: DATANODE_CONNECTION_POOL_WAITS.load(Ordering::Relaxed),
        datanode_new_connections: DATANODE_NEW_CONNECTIONS.load(Ordering::Relaxed),
        datanode_new_connection_nanos: DATANODE_NEW_CONNECTION_NANOS.load(Ordering::Relaxed),
        checksum_bytes_verified: CHECKSUM_BYTES_VERIFIED.load(Ordering::Relaxed),
        checksum_verify_nanos: CHECKSUM_VERIFY_NANOS.load(Ordering::Relaxed),
    }
}

pub struct EcFaultInjection {
    pub fail_blocks: Vec<usize>,
}
