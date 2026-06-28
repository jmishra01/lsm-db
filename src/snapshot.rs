// =============================================================
// Snapshot — consistent point-in-time read (#10)
// WriteBatch — atomic multi-key write (#10)
//
// MVCC (Multi-Version Concurrency Control) overview
// --------------------------------------------------
// Every write is tagged with a global monotonic write_seq.
// A Snapshot captures the write_seq at creation time.  Reads
// through the Snapshot only see entries with seq ≤ snapshot_seq,
// giving an immutable, consistent view even while new writes land.
//
// How Snapshot is implemented here:
//   When you call `db.snapshot()`, the engine:
//     1. Reads the current write_seq (call it S).
//     2. Builds a Cursor with max_seq = S (all three sources —
//        MemTable, immutable MemTables, and SSTables — filter out
//        entries with seq > S).
//     3. Drains the cursor into a BTreeMap, producing a frozen
//        key-value map of the database as of seq S.
//   The resulting Snapshot is independent of the live engine;
//   concurrent writes do not affect it.
//
// WriteBatch — atomic multi-key transactions
// -------------------------------------------
// All entries in a WriteBatch are assigned the SAME write_seq.
// From a reader's perspective, they all happen simultaneously:
//   • A snapshot at seq < batch_seq sees NONE of the batch's writes.
//   • A snapshot at seq ≥ batch_seq sees ALL of them.
//
// This is the same semantics used by RocksDB's WriteBatch and
// LevelDB's WriteBatch: it provides "all-or-nothing" atomicity at
// the seq boundary, but does NOT provide serializable isolation
// across concurrent readers/writers (that would require a lock).
// =============================================================

use std::collections::BTreeMap;

// ---- Snapshot --------------------------------------------------------------

/// A frozen, consistent read-only view of one column family at a fixed
/// write_seq.  Reads do not go to disk — data was eagerly materialized
/// into a BTreeMap when the snapshot was taken.
pub struct Snapshot {
    seq: u64,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Snapshot {
    pub(crate) fn new(seq: u64, data: BTreeMap<Vec<u8>, Vec<u8>>) -> Self {
        Self { seq, data }
    }

    /// The write_seq this snapshot is pinned at.
    pub fn seq(&self) -> u64 { self.seq }

    /// Point lookup in the snapshot.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&[u8]> {
        self.data.get(key.as_ref()).map(|v| v.as_slice())
    }

    /// Iterate all (key, value) pairs in sorted key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.data.iter()
    }

    /// Return all (key, value) pairs whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = prefix.as_ref();
        self.data.iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Total number of live keys in the snapshot.
    pub fn key_count(&self) -> usize { self.data.len() }
}

// ---- WriteBatch ------------------------------------------------------------

/// A buffer of mutations to be committed atomically.
///
/// Usage:
/// ```
/// let mut batch = WriteBatch::new();
/// batch.put("default", "key_a", "val_a")
///      .put("default", "key_b", "val_b")
///      .delete("default", "key_c");
/// db.write_batch(batch)?;
/// ```
pub struct WriteBatch {
    /// (column_family, key, value_opt).  None = tombstone / delete.
    pub(crate) entries: Vec<(String, Vec<u8>, Option<Vec<u8>>)>,
}

impl WriteBatch {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    /// Queue a put (insert / overwrite).
    pub fn put(
        &mut self,
        cf: impl Into<String>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> &mut Self {
        self.entries.push((cf.into(), key.into(), Some(value.into())));
        self
    }

    /// Queue a delete (tombstone).
    pub fn delete(&mut self, cf: impl Into<String>, key: impl Into<Vec<u8>>) -> &mut Self {
        self.entries.push((cf.into(), key.into(), None));
        self
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
}

impl Default for WriteBatch {
    fn default() -> Self { Self::new() }
}
