// Block cache — LRU cache of decompressed SSTable blocks
//
// Why cache at the block level?
//   SSTable reads are the bottleneck on the read path. Without a cache, every
//   point lookup opens a file, seeks to a block, reads and decompresses it.
//   A hot key queried repeatedly pays that cost every time.
//
//   Caching at block granularity (not key granularity) exploits spatial
//   locality: once a block is loaded, all keys in that block are served from
//   memory for free. Adjacent keys (e.g. sensor:000010..sensor:000099) share
//   blocks and benefit from each other's cache warming.
//
//   The cache stores DECOMPRESSED bytes, so cached hits also skip the LZ4
//   decompression step entirely.
//
// Key: (sst_path, block_file_offset) — uniquely identifies a block across all
//   SSTables at all levels. When an SSTable is deleted after compaction, its
//   blocks stop being requested and are naturally evicted by LRU over time.
//
// Eviction: monotonic tick counter. Each access refreshes the entry's tick.
//   On insert when at capacity, the entry with the lowest tick is evicted
//   (O(n) scan). A production implementation would use a linked-hash-map for
//   O(1) LRU, but the O(n) scan is acceptable at the default 512-block capacity.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub type BlockKey = (PathBuf, u64);

struct Inner {
    map: HashMap<BlockKey, (Arc<Vec<u8>>, u64)>, // value, lru_tick
    cap: usize,
    tick: u64,
}

pub struct BlockCache(Mutex<Inner>);

impl BlockCache {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Inner {
            map: HashMap::new(),
            cap: capacity,
            tick: 0,
        })))
    }

    pub fn get(&self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        let mut g = self.0.lock().unwrap();
        g.tick += 1;
        let tick = g.tick;
        g.map.get_mut(key).map(|(data, t)| {
            *t = tick;
            Arc::clone(data)
        })
    }

    pub fn insert(&self, key: BlockKey, data: Arc<Vec<u8>>) {
        let mut g = self.0.lock().unwrap();
        g.tick += 1;
        let tick = g.tick;
        if g.map.len() >= g.cap && !g.map.contains_key(&key) {
            if let Some(lru) = g
                .map
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| k.clone())
            {
                g.map.remove(&lru);
            }
        }
        g.map.insert(key, (data, tick));
    }
}
