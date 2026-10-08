use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use crate::hdfs::protocol::NamenodeProtocol;
use crate::proto::hdfs::LocatedBlocksProto;
use crate::{HdfsError, Result};

const CACHE_SHARDS: usize = 16;
const MIN_ENTRIES_PER_SHARD: usize = 64;

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A block map shared by cached opens and concurrent positioned reads.
#[derive(Debug)]
pub(crate) struct LocatedBlocksState {
    locations: RwLock<Arc<LocatedBlocksProto>>,
    refresh_lock: AsyncMutex<()>,
    updated_at: Mutex<Instant>,
}

impl LocatedBlocksState {
    pub(crate) fn new(locations: Arc<LocatedBlocksProto>) -> Self {
        Self {
            locations: RwLock::new(locations),
            refresh_lock: AsyncMutex::new(()),
            updated_at: Mutex::new(Instant::now()),
        }
    }

    pub(crate) fn current(&self) -> Arc<LocatedBlocksProto> {
        match self.locations.read() {
            Ok(locations) => Arc::clone(&locations),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    fn is_fresh(&self, ttl: Duration) -> bool {
        lock_unpoisoned(&self.updated_at).elapsed() <= ttl
    }

    fn updated_at(&self) -> Instant {
        *lock_unpoisoned(&self.updated_at)
    }

    fn replace(&self, locations: Arc<LocatedBlocksProto>) {
        match self.locations.write() {
            Ok(mut current) => *current = locations,
            Err(poisoned) => *poisoned.into_inner() = locations,
        }
        *lock_unpoisoned(&self.updated_at) = Instant::now();
    }

    pub(crate) async fn refresh_if_current(
        &self,
        protocol: &Arc<NamenodeProtocol>,
        path: &str,
        expected: &Arc<LocatedBlocksProto>,
    ) -> Result<Arc<LocatedBlocksProto>> {
        let _guard = self.refresh_lock.lock().await;
        let current = self.current();
        if !Arc::ptr_eq(&current, expected) {
            return Ok(current);
        }

        let refreshed = fetch(protocol, path).await?;

        // DFSInputStream applies the same identity check before replacing the
        // block map of an already-open stream. A different block ID means the
        // path was replaced, so continuing would mix file generations.
        for (old, new) in expected.blocks.iter().zip(&refreshed.blocks) {
            if old.b.pool_id != new.b.pool_id || old.b.block_id != new.b.block_id {
                return Err(HdfsError::OperationFailed(format!(
                    "block list for '{path}' changed while reading"
                )));
            }
        }

        self.replace(Arc::clone(&refreshed));
        Ok(refreshed)
    }
}

async fn fetch(protocol: &Arc<NamenodeProtocol>, path: &str) -> Result<Arc<LocatedBlocksProto>> {
    #[cfg(feature = "integration-test")]
    crate::test::BLOCK_LOCATIONS_CACHE_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let locations = protocol
        .get_block_locations(path, 0, i64::MAX as u64)
        .await?
        .locations
        .ok_or_else(|| HdfsError::FileNotFound(path.to_string()))?;
    Ok(Arc::new(locations))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    protocol: usize,
    path: String,
}

impl CacheKey {
    fn new(protocol: &Arc<NamenodeProtocol>, path: &str) -> Self {
        Self {
            protocol: Arc::as_ptr(protocol) as usize,
            path: path.to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct LoadSlot {
    state: Mutex<Option<Arc<LocatedBlocksState>>>,
    load_lock: AsyncMutex<()>,
}

impl LoadSlot {
    fn get(&self, ttl: Duration) -> Option<Arc<LocatedBlocksState>> {
        lock_unpoisoned(&self.state)
            .as_ref()
            .filter(|state| state.is_fresh(ttl))
            .cloned()
    }

    fn set(&self, state: Arc<LocatedBlocksState>) {
        *lock_unpoisoned(&self.state) = Some(state);
    }

    fn updated_at(&self) -> Option<Instant> {
        lock_unpoisoned(&self.state)
            .as_ref()
            .map(|state| state.updated_at())
    }
}

/// A client-scoped metadata cache for complete files.
///
/// Empty slots act as per-key single-flight locks. Cache shards reduce lock
/// contention without serializing unrelated NameNode requests.
#[derive(Debug)]
pub(crate) struct BlockLocationsCache {
    shards: Vec<Mutex<HashMap<CacheKey, Arc<LoadSlot>>>>,
    shard_capacities: Vec<usize>,
    ttl: Duration,
}

/// Removes an empty single-flight slot if its loader is dropped before the
/// NameNode request completes. This keeps request cancellation from turning a
/// temporary capacity overflow into a permanent one.
struct PendingLoadCleanup<'a> {
    cache: &'a BlockLocationsCache,
    key: &'a CacheKey,
    slot: &'a Arc<LoadSlot>,
    armed: bool,
}

impl<'a> PendingLoadCleanup<'a> {
    fn new(cache: &'a BlockLocationsCache, key: &'a CacheKey, slot: &'a Arc<LoadSlot>) -> Self {
        Self {
            cache,
            key,
            slot,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingLoadCleanup<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.remove_slot(self.key, self.slot);
        }
    }
}

impl BlockLocationsCache {
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Option<Self> {
        if capacity == 0 || ttl.is_zero() {
            return None;
        }

        // Small caches use fewer shards so hash collisions cannot strand most
        // of the configured capacity. Larger caches still spread hot opens
        // across locks.
        let shard_count = CACHE_SHARDS.min((capacity / MIN_ENTRIES_PER_SHARD).max(1));
        let base_capacity = capacity / shard_count;
        let remainder = capacity % shard_count;
        Some(Self {
            shards: (0..shard_count)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
            shard_capacities: (0..shard_count)
                .map(|index| base_capacity + usize::from(index < remainder))
                .collect(),
            ttl,
        })
    }

    fn shard(&self, key: &CacheKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    fn get_or_insert_slot(&self, key: &CacheKey) -> Arc<LoadSlot> {
        let shard_index = self.shard(key);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        if let Some(slot) = shard.get(key) {
            if slot
                .updated_at()
                .is_none_or(|updated_at| updated_at.elapsed() <= self.ttl)
            {
                return Arc::clone(slot);
            }
            shard.remove(key);
        }

        if shard.len() >= self.shard_capacities[shard_index] {
            shard.retain(|_, slot| {
                slot.updated_at()
                    .is_none_or(|updated_at| updated_at.elapsed() <= self.ttl)
            });
        }
        while shard.len() >= self.shard_capacities[shard_index] {
            let Some(eviction_key) = shard
                .iter()
                .filter_map(|(key, slot)| slot.updated_at().map(|updated_at| (key, updated_at)))
                .min_by_key(|(_, updated_at)| *updated_at)
                .map(|(key, _)| key.clone())
            else {
                // Every resident slot is currently loading. Keep those slots
                // so callers for the same key still share one NameNode RPC;
                // the shard is trimmed again as each load completes.
                break;
            };
            shard.remove(&eviction_key);
        }

        let slot = Arc::new(LoadSlot::default());
        shard.insert(key.clone(), Arc::clone(&slot));
        slot
    }

    fn trim_loaded_entries(&self, key: &CacheKey) {
        let shard_index = self.shard(key);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        while shard.len() > self.shard_capacities[shard_index] {
            let Some(eviction_key) = shard
                .iter()
                .filter_map(|(key, slot)| slot.updated_at().map(|updated_at| (key, updated_at)))
                .min_by_key(|(_, updated_at)| *updated_at)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            shard.remove(&eviction_key);
        }
    }

    fn remove_slot(&self, key: &CacheKey, slot: &Arc<LoadSlot>) {
        let mut shard = lock_unpoisoned(&self.shards[self.shard(key)]);
        if shard
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            shard.remove(key);
        }
    }

    fn is_current_slot(&self, key: &CacheKey, slot: &Arc<LoadSlot>) -> bool {
        lock_unpoisoned(&self.shards[self.shard(key)])
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
    }

    pub(crate) async fn get_or_load(
        &self,
        protocol: &Arc<NamenodeProtocol>,
        path: &str,
    ) -> Result<(Arc<LocatedBlocksState>, bool)> {
        let key = CacheKey::new(protocol, path);
        loop {
            let slot = self.get_or_insert_slot(&key);
            if let Some(state) = slot.get(self.ttl) {
                return Ok((state, true));
            }

            let _load_guard = slot.load_lock.lock().await;

            // Recheck after taking the per-key lock: another opener may have
            // populated the slot while this task was waiting.
            if let Some(state) = slot.get(self.ttl) {
                return Ok((state, true));
            }

            // A cancelled loader removes its slot before releasing load_lock.
            // Waiters on that detached slot must join the replacement instead
            // of issuing a second NameNode request that cannot be cached.
            if !self.is_current_slot(&key, &slot) {
                continue;
            }

            let mut cleanup = PendingLoadCleanup::new(self, &key, &slot);
            let locations = fetch(protocol, path).await?;
            let cacheable = is_cacheable(&locations);
            let state = Arc::new(LocatedBlocksState::new(locations));
            if cacheable {
                slot.set(Arc::clone(&state));
                cleanup.disarm();
                self.trim_loaded_entries(&key);
            }
            return Ok((state, false));
        }
    }

    pub(crate) fn invalidate(&self, protocol: &Arc<NamenodeProtocol>, path: &str) {
        let protocol = Arc::as_ptr(protocol) as usize;
        let prefix = format!("{}/", path.trim_end_matches('/'));
        for shard in &self.shards {
            lock_unpoisoned(shard).retain(|key, _| {
                key.protocol != protocol || (key.path != path && !key.path.starts_with(&prefix))
            });
        }
    }
}

fn is_cacheable(locations: &LocatedBlocksProto) -> bool {
    !locations.under_construction && locations.is_last_block_complete
}

pub(crate) async fn load_uncached(
    protocol: &Arc<NamenodeProtocol>,
    path: &str,
) -> Result<Arc<LocatedBlocksState>> {
    Ok(Arc::new(LocatedBlocksState::new(
        fetch(protocol, path).await?,
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use tokio::runtime::Runtime;
    use url::Url;

    use super::*;
    use crate::common::config::Configuration;
    use crate::hdfs::proxy::NameServiceProxy;

    static RT: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

    fn create_protocol(url: &str) -> Arc<NamenodeProtocol> {
        let url = Url::parse(url).unwrap();
        let config = Arc::new(Configuration::new(None, None).unwrap());
        let proxy = NameServiceProxy::new(&url, config, RT.handle().clone(), None, None).unwrap();
        Arc::new(NamenodeProtocol::new(proxy, RT.handle().clone()))
    }

    #[test]
    fn caches_are_bounded_and_invalidate_subtrees() {
        let cache = BlockLocationsCache::new(8, Duration::from_secs(1)).unwrap();
        assert_eq!(cache.shards.len(), 1);
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let file_key = CacheKey::new(&protocol, "/table/data/file.lance");
        let sibling_key = CacheKey::new(&protocol, "/other/file.lance");
        let complete = Arc::new(LocatedBlocksState::new(Arc::new(LocatedBlocksProto {
            file_length: 42,
            is_last_block_complete: true,
            ..Default::default()
        })));

        cache
            .get_or_insert_slot(&file_key)
            .set(Arc::clone(&complete));
        cache
            .get_or_insert_slot(&sibling_key)
            .set(Arc::clone(&complete));
        assert_eq!(
            cache
                .get_or_insert_slot(&file_key)
                .get(cache.ttl)
                .unwrap()
                .current()
                .file_length,
            42
        );

        cache.invalidate(&protocol, "/table");
        assert!(cache.get_or_insert_slot(&file_key).get(cache.ttl).is_none());
        assert!(
            cache
                .get_or_insert_slot(&sibling_key)
                .get(cache.ttl)
                .is_some()
        );
    }

    #[test]
    fn only_complete_files_are_cacheable() {
        assert!(is_cacheable(&LocatedBlocksProto {
            is_last_block_complete: true,
            ..Default::default()
        }));
        assert!(!is_cacheable(&LocatedBlocksProto {
            under_construction: true,
            is_last_block_complete: true,
            ..Default::default()
        }));
        assert!(!is_cacheable(&LocatedBlocksProto::default()));
    }

    #[test]
    fn expired_entries_are_replaced_and_capacity_is_enforced() {
        let cache = BlockLocationsCache::new(8, Duration::from_secs(1)).unwrap();
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let state = Arc::new(LocatedBlocksState::new(Arc::new(LocatedBlocksProto {
            is_last_block_complete: true,
            ..Default::default()
        })));

        let expired_key = CacheKey::new(&protocol, "/expired");
        let expired_slot = cache.get_or_insert_slot(&expired_key);
        expired_slot.set(Arc::clone(&state));
        *lock_unpoisoned(&state.updated_at) = Instant::now() - Duration::from_secs(2);
        let replacement = cache.get_or_insert_slot(&expired_key);
        assert!(!Arc::ptr_eq(&expired_slot, &replacement));

        for index in 0..9 {
            let key = CacheKey::new(&protocol, &format!("/file-{index}"));
            cache
                .get_or_insert_slot(&key)
                .set(Arc::new(LocatedBlocksState::new(Arc::new(
                    LocatedBlocksProto {
                        is_last_block_complete: true,
                        ..Default::default()
                    },
                ))));
        }
        assert_eq!(lock_unpoisoned(&cache.shards[0]).len(), 8);
    }

    #[test]
    fn in_flight_slots_are_not_evicted() {
        let cache = BlockLocationsCache::new(1, Duration::from_secs(1)).unwrap();
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let first_key = CacheKey::new(&protocol, "/first");
        let second_key = CacheKey::new(&protocol, "/second");

        let first_slot = cache.get_or_insert_slot(&first_key);
        let _second_slot = cache.get_or_insert_slot(&second_key);
        let same_first_slot = cache.get_or_insert_slot(&first_key);

        assert!(Arc::ptr_eq(&first_slot, &same_first_slot));
        assert_eq!(lock_unpoisoned(&cache.shards[0]).len(), 2);

        first_slot.set(Arc::new(LocatedBlocksState::new(Arc::new(
            LocatedBlocksProto {
                is_last_block_complete: true,
                ..Default::default()
            },
        ))));
        cache.trim_loaded_entries(&first_key);
        assert_eq!(lock_unpoisoned(&cache.shards[0]).len(), 1);
    }

    #[test]
    fn cancelled_loads_remove_empty_slots() {
        let cache = BlockLocationsCache::new(1, Duration::from_secs(1)).unwrap();
        let protocol = create_protocol("hdfs://127.0.0.1:9000");
        let key = CacheKey::new(&protocol, "/cancelled");
        let slot = cache.get_or_insert_slot(&key);

        {
            // Dropping this guard models an async loader being cancelled while
            // it is awaiting the NameNode RPC.
            let _cleanup = PendingLoadCleanup::new(&cache, &key, &slot);
        }

        assert!(lock_unpoisoned(&cache.shards[0]).is_empty());
        let replacement = cache.get_or_insert_slot(&key);
        assert!(!Arc::ptr_eq(&slot, &replacement));
    }
}
