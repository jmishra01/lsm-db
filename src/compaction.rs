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
// MVCC notes (#10):
//   The compaction functions now carry the write_seq from each entry through the merge
//   so that snapshot reads on the resulting SSTable remain correct. The seq in each
//   merged entry is preserved unchanged — we only ever keep the LATEST version of a key,
//   retaining its original seq so snapshots can see or skip it appropriately.
// ================================================================================================

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::memtable::MemTable;
use crate::sstable::SSTable;

pub const L0_MAX_FILES: usize = 4;
pub const SIZE_RATIO: usize = 10;
pub const L1_BASE_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Merge two sorted entry lists (stable: `a` = newer, wins on key collision).
/// Each entry is (key, write_seq, value_opt). The winning entry's seq is preserved
/// so snapshot reads remain accurate after compaction.
pub fn merge_entries(
    a: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    b: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    drop_tombstones: bool,
) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
    // BTreeMap keeps one entry per key; inserting `a` after `b` lets `a` win.
    let mut map: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
    for (k, seq, v) in b { map.insert(k, (seq, v)); }
    for (k, seq, v) in a { map.insert(k, (seq, v)); }
    map.into_iter()
        .filter(|(_, (_, v))| !drop_tombstones || v.is_some())
        .map(|(k, (seq, v))| (k, seq, v))
        .collect()
}

/// Write a merged entry list into a new SSTable on disk.
/// Seqs are passed through verbatim so snapshot visibility is maintained.
pub fn write_merged(
    path: impl AsRef<Path>,
    entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    level: u32,
) -> io::Result<SSTable> {
    let mut mem = MemTable::new();
    for (k, seq, v) in entries {
        match v {
            Some(val) => mem.put_seq(k, val, seq),
            None      => mem.delete_seq(k, seq),
        }
    }
    SSTable::write_from_memtable(path, &mem, level)
}

/// Decide whether L0 needs compaction.
pub fn l0_needs_compaction(l0: &[SSTable]) -> bool {
    l0.len() >= L0_MAX_FILES
}

/// Generate a fresh SSTable file name.
pub fn next_sstable_path(dir: &Path, level: u32, seq: u64) -> PathBuf {
    dir.join(format!("L{level}_{seq:08}.sst"))
}
