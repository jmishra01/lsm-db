// ================================================================================================
// Compaction -- Leveled compaction strategy
//
// Rules
// -----
// * L0 may accumulate up to L0_MAX_FILES files (no size guarantee).
// * L1+ have a size budget: level N holds at most SIZE_RATIO^N x BASE_BYTES.
// *  When L(n) overflows, one SSTable from L(n) is merged with all overlapping SSTables in L(n+1),
//    producing new L(n + 1) files.
// * During merge, for duplicate keys the entry from the LOWER level (more recent) wins.
//   Tombstones are dropped only at the deepest level.
// ================================================================================================

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::mem_table::MemTable;
use crate::ss_table::SSTable;

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
pub fn next_ss_table_path(dir: &Path, level: u32, seq:u64) -> PathBuf {
    dir.join(format!("L{level}_{seq:08}.sst"))
}
