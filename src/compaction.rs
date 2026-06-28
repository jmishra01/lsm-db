// ================================================================================================
// Compaction — Leveled compaction strategy
//
// Without compaction:
//   - L0 accumulates many overlapping files → every read must check all of them (read amp).
//   - Deleted keys and overwritten values stay in old SSTable files forever (space amp).
//   - L1+ files could overlap in key range, making it impossible to skip files during reads.
//
// Leveled strategy:
//   L0: special — files may overlap. Triggered when L0 reaches L0_MAX_FILES (4).
//   L1+: size budgets (1 MiB, 10 MiB, 100 MiB, …). When a level overflows, the oldest
//        SSTable is picked and merged with all overlapping SSTables in the next level.
//
// Why pick the OLDEST L(n) SSTable as the victim?
//   Round-robin over oldest ensures every SSTable eventually gets compacted. No single
//   SSTable grows arbitrarily large without being merged.
//
// Why drop tombstones ONLY at the deepest level?
//   A tombstone at L1 may be masking a live entry at L2. Dropping it early would make
//   the old value "reappear" — a consistency violation. We keep tombstones until we are
//   certain no older copy of the key exists at any deeper level.
//
// Write amplification: with SIZE_RATIO=10 and typical 3 active levels, each byte the
// user writes is rewritten ~10-30× total. This is the fundamental LSM trade-off:
// sequential write amplification in exchange for avoiding random writes.
// ================================================================================================

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::memtable::MemTable;
use crate::sstable::SSTable;

pub const L0_MAX_FILES: usize = 4;
pub const SIZE_RATIO: usize = 10;
pub const L1_BASE_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Merge two sorted iterators (stable: lower-level wins on tie).
/// `a` = newer (lower level), `b` = older.

pub fn merge_entries(
    a: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    b: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    drop_tombstones: bool,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    // Insert older first, then newer overwrites
    for (k, v) in b {
        map.insert(k, v);
    }

    for (k, v) in a {
        map.insert(k, v);
    }
    map.into_iter()
        .filter(|(_, v)| !drop_tombstones || v.is_some())
        .collect()
}

/// Write a merged entry list into a new SSTable on disk.
pub fn write_merged(
    path: impl AsRef<Path>,
    entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    level: u32,
) -> io::Result<SSTable> {
    // Build a temporary MemTable to reuse SSTable::write_from_memtable.
    let mut mem = MemTable::new();
    for (k, v) in entries {
        match v {
            Some(val) => mem.put(k, val),
            None => mem.delete(k)
        }
    }
    SSTable::write_from_memtable(path, &mem, level)
}

/// Decide whether L0 needs compaction.
pub fn l0_needs_compaction(l0: &[SSTable]) -> bool {
    l0.len() >= L0_MAX_FILES
}

/// Generate a fresh SSTable file name
pub fn next_sstable_path(dir: &Path, level: u32, seq:u64) -> PathBuf {
    dir.join(format!("L{level}_{seq:08}.sst"))
}
