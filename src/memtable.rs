// ==========================================================
// MemTable - in-memory sorted write buffer.
//
// Backed by a BtreeMap so iteration is always in key order.
// Entries are either live values or tombstones (None).
// When `size_bytes` crosses the threshold the engine flushes
// this to disk as an SSTable.
// ==========================================================

use std::collections::BTreeMap;

/// A value in MemTable: Some(bytes) = live, None = tombstone.
pub type MemValue = Option<Vec<u8>>;

pub struct  MemTable {
    data: BTreeMap<Vec<u8>, MemValue>,
    pub size_bytes: usize
}

impl MemTable {
    pub fn new() -> MemTable {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0
        }
    }

    /// Insert or overwrite a key.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.size_bytes += key.len() + value.len();
        self.data.insert(key, Some(value));
    }

    /// Insert a tombstone.
    pub fn delete(&mut self, key: Vec<u8>) {
        self.size_bytes += key.len();
        self.data.insert(key, None);
    }

    /// Point lookup. Returns:
    /// - `Some(Some(v))`   -> found with value v
    /// - `Some(None)`      -> found, but deleted(tombstone)
    /// - `None`            -> not in this MemTable
    pub fn get(&self, key: &[u8]) -> Option<&MemValue> {
        self.data.get(key)
    }

    /// Iterate entries in sorted key order 9for flush to SSTable).
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &MemValue)> {
        self.data.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}