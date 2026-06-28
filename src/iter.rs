// =============================================================
// Cursor — merge-heap iterator over all data sources (#8)
//
// An LSM-Tree stores data across multiple overlapping layers:
//   • The active MemTable (newest writes)
//   • Zero or more immutable MemTables (awaiting flush)
//   • L0..L6 SSTable files (progressively older data)
//
// To produce a correct sorted iteration we need a k-way merge.
// The standard algorithm:
//   1. Prime each source with its first entry.
//   2. Use a min-heap keyed on (entry_key, source_priority) to
//      always surface the globally smallest key next.
//   3. When an entry is popped, advance that source and push its
//      next entry into the heap.
//   4. For duplicate keys (same key in MemTable AND an SSTable),
//      lower source_id wins: MemTable = 0 (newest), older SSTable
//      = higher source_id (oldest).  We dedup by tracking the last
//      key yielded and skipping stale versions.
//   5. Tombstones (value = None) are silently skipped so the
//      caller only sees live key-value pairs.
//
// MVCC (#10):
//   Every source yields (key, write_seq, value_opt). Each source
//   is pre-filtered so it only emits entries with seq ≤ max_seq.
//   This means a Cursor created with max_seq=S sees the exact
//   snapshot as of write_seq S, even while new writes arrive.
//
// SstableBlockIter:
//   Lazily loads one 4 KiB compressed block at a time from disk.
//   The SstTable sparse index points to each block's file offset;
//   we only read the next block once all entries in the current
//   block are exhausted.  This bounds memory to O(block_size)
//   per open SSTable rather than O(file_size).
// =============================================================

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::block_cache::BlockCache;
use crate::sstable::load_block_entries_at;

// ---- Heap item ----------------------------------------------------------

#[derive(Eq, PartialEq)]
struct HeapItem {
    key: Vec<u8>,
    /// Lower = newer (MemTable = 0, older SSTables = higher numbers).
    source_id: usize,
    seq: u64,
    value: Option<Vec<u8>>,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary: ascending key (min-heap via Reverse below)
        // Secondary: ascending source_id so the newest source wins for equal keys
        self.key.cmp(&other.key)
            .then(self.source_id.cmp(&other.source_id))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// ---- Cursor -------------------------------------------------------------

/// A merged, sorted, deduplicated iterator over all data sources in one
/// column family.  Tombstones are suppressed — only live (key, value) pairs
/// are yielded.
///
/// Create via `LsmEngine::iter()` / `iter_cf()` / `snapshot.iter()`.
pub struct Cursor {
    sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, u64, Option<Vec<u8>>)> + Send>>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    last_key: Option<Vec<u8>>,
}

impl Cursor {
    /// Build a Cursor from an ordered list of sources.
    /// Source 0 must be the NEWEST (active MemTable); higher indices are older.
    pub(crate) fn new(
        mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, u64, Option<Vec<u8>>)> + Send>>,
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for (id, src) in sources.iter_mut().enumerate() {
            if let Some((key, seq, value)) = src.next() {
                heap.push(Reverse(HeapItem { key, source_id: id, seq, value }));
            }
        }
        Self { sources, heap, last_key: None }
    }
}

impl Iterator for Cursor {
    /// Yields live (key, value) pairs in ascending key order.
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Reverse(item) = self.heap.pop()?;

            // Advance this source so the heap stays primed
            if let Some((nk, ns, nv)) = self.sources[item.source_id].next() {
                self.heap.push(Reverse(HeapItem { key: nk, source_id: item.source_id, seq: ns, value: nv }));
            }

            // Skip stale versions: if we already yielded (or skipped) this key,
            // any further occurrences come from older sources and must be ignored.
            if self.last_key.as_deref() == Some(item.key.as_slice()) {
                continue;
            }
            self.last_key = Some(item.key.clone());

            // Skip tombstones — the key was deleted
            match item.value {
                Some(v) => return Some((item.key, v)),
                None    => continue,
            }
        }
    }
}

// ---- SstableBlockIter ---------------------------------------------------

/// Lazy block-by-block iterator over one SSTable file.
/// Loads a compressed 4 KiB block from disk only when the previous block's
/// entries are exhausted, bounding memory usage per open file.
pub(crate) struct SstableBlockIter {
    path: PathBuf,
    /// Sparse index: (first_key_in_block, block_file_offset)
    sparse_index: Vec<(Vec<u8>, u64)>,
    block_idx: usize,
    /// Currently loaded block's decoded entries
    entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    entry_idx: usize,
    cache: Arc<BlockCache>,
    /// MVCC filter: only yield entries with seq ≤ max_seq
    max_seq: u64,
}

impl SstableBlockIter {
    pub(crate) fn new(
        path: PathBuf,
        sparse_index: Vec<(Vec<u8>, u64)>,
        cache: Arc<BlockCache>,
        max_seq: u64,
    ) -> Self {
        Self {
            path,
            sparse_index,
            block_idx: 0,
            entries: Vec::new(),
            entry_idx: 0,
            cache,
            max_seq,
        }
    }

    /// Load the next block from disk. Returns false when all blocks exhausted.
    fn load_next_block(&mut self) -> bool {
        loop {
            if self.block_idx >= self.sparse_index.len() {
                return false;
            }
            let (_, block_offset) = self.sparse_index[self.block_idx];
            self.block_idx += 1;

            match load_block_entries_at(&self.path, block_offset, &self.cache) {
                Ok(entries) => {
                    // Apply MVCC filter at the source level
                    self.entries = entries.into_iter()
                        .filter(|(_, seq, _)| *seq <= self.max_seq)
                        .collect();
                    self.entry_idx = 0;
                    if !self.entries.is_empty() {
                        return true;
                    }
                    // Block had no visible entries at this snapshot — try next
                }
                Err(_) => return false,
            }
        }
    }
}

impl Iterator for SstableBlockIter {
    type Item = (Vec<u8>, u64, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.entry_idx < self.entries.len() {
                let entry = self.entries[self.entry_idx].clone();
                self.entry_idx += 1;
                return Some(entry);
            }
            if !self.load_next_block() {
                return None;
            }
        }
    }
}
