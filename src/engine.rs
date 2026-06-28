// =====================================================
// LSM_Engine -- top-level DB engine
//
// Public API
// ----------
// open(dir)        -> LSM_Engine
// put(key, value)  -> Result<()>
// delete(key)      -> Result<()>
// get(key)         -> Result<Option<Vec<u8>>>
// scan(from, to)   -> Result<Vec<(Vec<u8>>, Vec<u8>)>>
// stats()          -> Stats
// =====================================================

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::compaction::{
    l0_needs_compaction, merge_entries, next_ss_table_path, write_merged
};

use crate::mem_table::MemTable;
use crate::ss_table::SSTable;
use crate::wal::{Wal, WalRecord};

/// Flush memtable when it exceeds this size.
const MEM_TABLE_SIZE_LIMIT: usize = 256 * 1024; // 256 KiB
const MAX_LEVELS: usize = 7;

pub struct LsmEngine {
    dir: PathBuf,
    wal: Wal,
    mem: MemTable,
    /// Imputable memtables waiting for flush (usually 0 or 1).
    imm: Vec<MemTable>,
    /// levels[0] = L0, levels[1] = L1, ...
    levels: Vec<Vec<SSTable>>,
    seq: Arc<AtomicU64>,
}

// -- Stats

#[derive(Clone, Debug)]
pub struct Stats {
    pub mem_table_size_bytes: usize,
    pub immutable_count: usize,
    pub level_file_counts: Vec<usize>,
    pub total_ss_table_files: usize,
}

// -- Engine impl
impl LsmEngine {
    /// Open or create an LSM database at `dir`
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<LsmEngine> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Load existing SSTables
        let mut levels: Vec<Vec<SSTable>> = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
        let mut max_seq = 0u64;

        let mut sst_paths: Vec<(u32, u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    // Parse "L{level}_{seq}"
                    if let Some((level_s, seq_s)) = stem.split_once('_') {
                        let level: u32 = level_s.trim_start_matches('L').parse().unwrap();
                        let seq: u64 =  seq_s.parse().unwrap_or(0);
                        if seq > max_seq { max_seq = seq; }
                        sst_paths.push((level, seq, path));
                    }
                }
            }
        }

        // Sort by seq so newer files come first within each level
        sst_paths.sort_by_key(|(l, s, _)| (*l, *s));
        for (level, _, path) in sst_paths {
            if let Ok(sst) = SSTable::open(&path, level) {
                if (level as usize) < MAX_LEVELS { levels[level as usize].push(sst); }
            }
        }

        // WAL recovery
        let wal_path = dir.join("wal.log");
        let records = Wal::recover(&wal_path)?;
        let mut mem = MemTable::new();
        for rec in records {
            match rec {
                WalRecord::Put {key, value} => {mem.put(key, value);},
                WalRecord::Delete { key } => {mem.delete(key);},
            }
        }

        let wal = Wal::open(&wal_path)?;
        Ok(Self {
            dir, wal, mem, imm: Vec::new(), levels, seq: Arc::new(AtomicU64::new(max_seq + 1))
        })
    }

    // -- Write path

    /// Insert or Update a key-value pair.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let value = value.into();
        self.wal.append_put(key.clone(), value.clone())?;
        self.mem.put(key, value);
        self.maybe_flush_and_compact()?;
        Ok(())
    }

    /// Delete a key (writes a tombstone).
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        self.wal.append_delete(key.clone())?;
        self.mem.delete(key);
        self.maybe_flush_and_compact()?;
        Ok(())
    }

    // ---- Read path
    /// Look up a key. Return `None` if not found or deleted.
    pub fn get(&self, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        // 1. MemTable
        if let Some(val_opt) = self.mem.get(key) {
            return Ok(val_opt.clone());
        }

        // 2. Immutable memtables (newest first)
        for imm in self.imm.iter().rev() {
            if let Some(val_opt) = imm.get(key) {
                return Ok(val_opt.clone());
            }
        }

        // 3. SSTables level by level(L0 = newest)
        for level_files in &self.levels {
            // Within L0 check all filess; within L1+ onlu one file can match
            for sst in level_files.iter().rev() {
                match sst.get(key)? {
                    Some(Some(v)) => return Ok(Some(v)),
                    Some(None) => return Ok(None),
                    None => ()
                }
            }
        }
        Ok(None)
    }

    /// Range scan [from, to). Returns live key-value pairs in sorted order.
    pub fn scan(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let from = from.as_ref();
        let to = to.as_ref();

        // Collect all versions into a BTreeMap (newest source wins)
        let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // SSTables (old first, so newer overwrites)
        for level_files in self.levels.iter().rev() {
            for sst in level_files.iter() {
                let entries = sst.scan_all()?;
                for (k, v) in entries {
                    if k.as_slice() >= from && k.as_slice() < to {
                        map.insert(k, v);
                    }
                }
            }
        }

        // Immutable MemTables
        for imm in self.imm.iter() {
            for (k, v) in imm.iter() {
                if k.as_slice() >= from && k.as_slice() < to {
                    map.insert(k.clone(), v.clone());
                }
            }
        }

        // Active memtable (highest priority)
        for (k, v) in self.mem.iter() {
            if k.as_slice() >= from && k.as_slice() < to {
                map.insert(k.clone(), v.clone());
            }
        }

        Ok(
            map
                .into_iter()
                .filter_map(|(k, v)| v.map(|val| (k, val)))
                .collect()
        )
    }


    // Flush & Compaction
    fn maybe_flush_and_compact(&mut self) -> io::Result<()> {
        if self.mem.size_bytes >= MEM_TABLE_SIZE_LIMIT {
            self.flush_memtable()?;
        }

        if l0_needs_compaction(&self.levels[0]) {
            self.compact_l0_to_l1()?;
        }

        // Check L1+ overflow
        for level in 1..MAX_LEVELS - 1 {
            let budget = crate::compaction::L1_BASE_BYTES * (crate::compaction::SIZE_RATIO as u64).pow(level as u32 - 1);
            let total: u64 = self.levels[level]
                .iter()
                .map(|s| std::fs::metadata(&s.path).map(|m| m.len()).unwrap_or(0))
                .sum();

            if total > budget && !self.levels[level].is_empty() {
                self.compact_level(level)?;
            }
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        // Swap active MemTable out
        let mut flushing = MemTable::new();
        std::mem::swap(&mut self.mem, &mut flushing);

        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let path = next_ss_table_path(&self.dir, 0, seq);
        let sst = SSTable::write_from_memtable(&path, &flushing, 0)?;
        self.levels[0].push(sst);

        // Truncate WAL (new one)
        let wal_path = self.dir.join("wal.log");
        fs::remove_file(&wal_path).ok();
        self.wal = Wal::open(&wal_path)?;

        Ok(())
    }

    fn compact_l0_to_l1(&mut self) -> io::Result<()> {
        // Merge all L0 files together (they may overlap) then merge with L1
        let mut l0_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        for sst in &self.levels[0] {
            let entries = sst.scan_all()?;
            l0_entries = merge_entries(entries, l0_entries, false);
        }

        let mut l1_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        for sst in &self.levels[1] {
            let entries = sst.scan_all()?;
            l1_entries.extend(entries);
        }

        l1_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let drop_tombstones = self.levels.iter().skip(2).all(|l| l.is_empty());
        let merged = merge_entries(l0_entries, l1_entries, drop_tombstones);

        // Delete old files
        for sst in self.levels[0].drain(..) {
            sst.delete_file().ok();
        }

        for sst in self.levels[1].drain(..) {
            sst.delete_file().ok();
        }

        if !merged.is_empty() {
            let seq = self.seq.fetch_add(1, Ordering::SeqCst);
            let path = next_ss_table_path(&self.dir, 0, seq);
            let sst = write_merged(path, merged, 1)?;
            self.levels[1].push(sst);
        }
        Ok(())
    }

    fn compact_level(&mut self, level: usize) -> io::Result<()> {
        // Take the oldest SSTable from level, merge into level+1
        if self.levels[level].is_empty() {
            return Ok(());
        }

        let victim = self.levels[level].remove(0);
        let new_entries = victim.scan_all()?;

        let mut next_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        for sst in &self.levels[level + 1] {
            next_entries.extend(sst.scan_all()?);
        }
        next_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let is_deepest = level + 1 == MAX_LEVELS - 1;
        let merged = merge_entries(new_entries, next_entries, is_deepest);

        victim.delete_file().ok();
        for sst in self.levels[level + 1].drain(..) {
            sst.delete_file().ok();
        }

        if !merged.is_empty() {
            let seq = self.seq.fetch_add(1, Ordering::SeqCst);
            let path = next_ss_table_path(&self.dir, (level + 1) as u32, seq);
            let sst = write_merged(path, merged, (level + 1) as u32)?;
            self.levels[level + 1].push(sst);
        }
        Ok(())
    }

    // -- Diagnostics
    pub fn stats(&self) -> Stats {
        let level_file_counts = self.levels.iter().map(|l| l.len()).collect::<Vec<usize>>();
        let total_sstable_files: usize = level_file_counts.iter().sum();
        Stats {
            mem_table_size_bytes: self.mem.size_bytes,
            immutable_count: self.imm.len(),
            level_file_counts,
            total_ss_table_files: total_sstable_files,
        }
    }
}











