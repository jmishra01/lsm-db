// ==========================================================
// MemTable — in-memory sorted write buffer
//
// All writes land here first (after the WAL). Using a BTreeMap
// guarantees iteration in sorted key order, which is required
// by the SSTable writer: SSTables must be sorted so that binary
// search and the sparse index both work correctly.
//
// Each entry now carries a write sequence number (seq) alongside
// its value. Sequence numbers power MVCC (#10): a reader can
// "pin" a snapshot_seq and only see writes with seq ≤ snapshot_seq,
// providing a consistent point-in-time view regardless of concurrent
// writes that arrive later.
//
// Entries are either live (Some(bytes)) or tombstones (None).
// Tombstones exist because SSTables are immutable — you cannot
// remove a key from a file that is already on disk. Writing a
// tombstone is the only way to mark a key as deleted. The
// tombstone propagates through compaction until it reaches the
// deepest level, where it is finally dropped.
//
// When size_bytes crosses the engine's threshold (256 KiB), the
// engine swaps this MemTable out, writes it to an SSTable, and
// starts a fresh empty MemTable for new writes.
// ==========================================================

use std::collections::BTreeMap;

/// A MemTable entry: (write_seq, value_opt).
/// write_seq is the global monotonic sequence number assigned at write time.
/// value_opt is Some(bytes) for a live key, None for a tombstone.
pub type MemEntry = (u64, Option<Vec<u8>>);

pub struct MemTable {
    data: BTreeMap<Vec<u8>, MemEntry>,
    pub size_bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        Self { data: BTreeMap::new(), size_bytes: 0 }
    }

    /// Insert or overwrite a key with an explicit write sequence number.
    pub fn put_seq(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
        self.size_bytes += key.len() + value.len() + 8;
        self.data.insert(key, (seq, Some(value)));
    }

    /// Insert a tombstone with an explicit write sequence number.
    pub fn delete_seq(&mut self, key: Vec<u8>, seq: u64) {
        self.size_bytes += key.len() + 8;
        self.data.insert(key, (seq, None));
    }

    /// Insert or overwrite using seq = u64::MAX (always visible to any snapshot).
    /// Kept for internal paths that don't need MVCC tracking (WAL replay, compaction).
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.put_seq(key, value, u64::MAX);
    }

    /// Insert a tombstone using seq = u64::MAX.
    pub fn delete(&mut self, key: Vec<u8>) {
        self.delete_seq(key, u64::MAX);
    }

    /// Snapshot-aware point lookup: returns None if the entry's seq > max_seq.
    /// This lets a Snapshot "time-travel" to see only writes up to a pinned seq.
    pub fn get_at(&self, key: &[u8], max_seq: u64) -> Option<&Option<Vec<u8>>> {
        self.data.get(key).and_then(|(seq, val)| {
            if *seq <= max_seq { Some(val) } else { None }
        })
    }

    /// Latest-version lookup (seq-unaware). Returns the current value regardless
    /// of sequence number. Used by the normal (non-snapshot) read path.
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.data.get(key).map(|(_, v)| v)
    }

    /// Iterate entries in sorted key order (for flush to SSTable and for Cursor).
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &MemEntry)> {
        self.data.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for MemTable {
    fn default() -> Self { Self::new() }
}
