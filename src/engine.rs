// =====================================================
// LSM Engine
//
// #1  SharedLsmEngine  — Arc<RwLock<>> concurrent wrapper
// #2  Background compaction thread
// #3  Block cache (hot blocks stay in memory)
// #4  SSTable v4 — sparse index + LZ4 blocks + CRC32
// #5  CRC32 checksums on WAL records and SSTable blocks
// #6  Column families — independent key spaces
// #7  Manifest — durable log of live SSTable files
// #8  Cursor / Iterator — merge-heap over all sources
// #9  Prefix scans
// #10 Snapshots + WriteBatch MVCC
// =====================================================

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crate::block_cache::BlockCache;
use crate::compaction::{l0_needs_compaction, merge_entries, next_sstable_path, write_merged, L1_BASE_BYTES, SIZE_RATIO};
use crate::iter::{Cursor, SstableBlockIter};
use crate::manifest::{Manifest, ManifestRecord};
use crate::memtable::MemTable;
use crate::snapshot::{Snapshot, WriteBatch};
use crate::sstable::SSTable;
use crate::wal::{Wal, WalRecord};

const MEM_TABLE_SIZE_LIMIT: usize = 256 * 1024;
const MAX_LEVELS: usize = 7;
const BLOCK_CACHE_CAPACITY: usize = 512;

// ---- Column-family state (#6) -----------------------------------------------

struct CfState {
    #[allow(dead_code)]
    name: String,
    cf_dir: PathBuf,
    wal: Wal,
    mem: MemTable,
    imm: Vec<MemTable>,
    levels: Vec<Vec<SSTable>>,
}

impl CfState {
    fn new(name: String, cf_dir: PathBuf, wal: Wal) -> Self {
        Self {
            name,
            cf_dir,
            wal,
            mem: MemTable::new(),
            imm: Vec::new(),
            levels: (0..MAX_LEVELS).map(|_| Vec::new()).collect(),
        }
    }
}

// ---- Background compaction messages ----------------------------------------

enum CompactionJob {
    L0ToL1 {
        cf_name: String,
        l0_paths: Vec<PathBuf>,
        l1_paths: Vec<PathBuf>,
        drop_tombstones: bool,
        seq: u64,
        dir: PathBuf,
    },
    CompactLevel {
        cf_name: String,
        level: usize,
        victim_path: PathBuf,
        next_paths: Vec<PathBuf>,
        is_deepest: bool,
        seq: u64,
        dir: PathBuf,
    },
    Shutdown,
}

struct CompactionResult {
    cf_name: String,
    target_level: usize,
    merged_source_paths: Vec<PathBuf>,
    merged_target_paths: Vec<PathBuf>,
    new_sst: Option<SSTable>,
}

struct CompactionWorker {
    tx: Sender<CompactionJob>,
    rx: Mutex<Receiver<CompactionResult>>,
    in_flight: bool,
}

// ---- Stats -----------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Stats {
    pub mem_table_size_bytes: usize,
    pub immutable_count: usize,
    pub level_file_counts: Vec<usize>,
    pub total_ss_table_files: usize,
    pub column_families: Vec<String>,
}

// ---- Engine ----------------------------------------------------------------

pub struct LsmEngine {
    dir: PathBuf,
    families: HashMap<String, CfState>,
    /// Naming seq: generates unique SSTable file names.
    seq: Arc<AtomicU64>,
    /// Write seq: incremented on every user write, powers MVCC (#10).
    write_seq: Arc<AtomicU64>,
    cache: Arc<BlockCache>,
    compact: CompactionWorker,
    manifest: Manifest,
}

impl LsmEngine {
    // -- Open / create

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with_cfs(path, &["default"])
    }

    pub fn open_with_cfs<P: AsRef<Path>>(path: P, cf_names: &[&str]) -> io::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let manifest_path = dir.join("MANIFEST");
        let mstate = Manifest::recover(&manifest_path)?;
        let mut manifest = Manifest::open(&manifest_path)?;

        let mut families: HashMap<String, CfState> = HashMap::new();
        let mut max_naming_seq: u64 = 0;
        let mut max_write_seq: u64 = 0;

        // Load CFs recorded in manifest
        for cf_name in &mstate.cfs {
            let cf_dir = dir.join(cf_name);
            fs::create_dir_all(&cf_dir)?;

            // WAL recovery: restore MemTable entries with their original seqs (#10)
            let wal_path = cf_dir.join("wal.log");
            let wal_records = Wal::recover(&wal_path)?;
            let mut mem = MemTable::new();
            for rec in wal_records {
                match rec {
                    WalRecord::Put { key, seq, value } => {
                        if seq > max_write_seq { max_write_seq = seq; }
                        mem.put_seq(key, value, seq);
                    }
                    WalRecord::Delete { key, seq } => {
                        if seq > max_write_seq { max_write_seq = seq; }
                        mem.delete_seq(key, seq);
                    }
                }
            }
            let wal = Wal::open(&wal_path)?;

            let mut cf = CfState::new(cf_name.clone(), cf_dir.clone(), wal);
            cf.mem = mem;

            let empty = vec![];
            let file_list = mstate.files.get(cf_name).unwrap_or(&empty);
            for (level, filename) in file_list {
                if let Some(seq) = seq_from_filename(filename) {
                    if seq > max_naming_seq { max_naming_seq = seq; }
                }
                let path = cf_dir.join(filename);
                match SSTable::open(&path, *level) {
                    Ok(sst) if (*level as usize) < MAX_LEVELS => {
                        // Track max write_seq seen in any SSTable for recovery (#10)
                        if sst.max_write_seq > max_write_seq {
                            max_write_seq = sst.max_write_seq;
                        }
                        cf.levels[*level as usize].push(sst);
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("WARNING: could not open {path:?}: {e}"),
                }
            }

            families.insert(cf_name.clone(), cf);
        }

        // Create any requested CFs that do not yet exist
        for &name in cf_names {
            if !families.contains_key(name) {
                let cf_dir = dir.join(name);
                fs::create_dir_all(&cf_dir)?;
                let wal_path = cf_dir.join("wal.log");
                let wal = Wal::open(&wal_path)?;
                families.insert(name.to_string(), CfState::new(name.to_string(), cf_dir, wal));
                manifest.append(&ManifestRecord::CreateCF { name: name.to_string() })?;
            }
        }

        let cache = BlockCache::new(BLOCK_CACHE_CAPACITY);
        let compact = spawn_compaction_worker();

        Ok(Self {
            dir,
            families,
            seq: Arc::new(AtomicU64::new(max_naming_seq + 1)),
            write_seq: Arc::new(AtomicU64::new(max_write_seq + 1)),
            cache,
            compact,
            manifest,
        })
    }

    // -- Column family management (#6)

    pub fn create_cf(&mut self, name: &str) -> io::Result<()> {
        if self.families.contains_key(name) { return Ok(()); }
        let cf_dir = self.dir.join(name);
        fs::create_dir_all(&cf_dir)?;
        let wal_path = cf_dir.join("wal.log");
        let wal = Wal::open(&wal_path)?;
        self.families.insert(name.to_string(), CfState::new(name.to_string(), cf_dir, wal));
        self.manifest.append(&ManifestRecord::CreateCF { name: name.to_string() })?;
        Ok(())
    }

    pub fn list_cfs(&self) -> Vec<String> {
        let mut names: Vec<String> = self.families.keys().cloned().collect();
        names.sort();
        names
    }

    // -- Write path

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        self.put_cf("default", key, value)
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        self.delete_cf("default", key)
    }

    pub fn put_cf(
        &mut self,
        cf: &str,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> io::Result<()> {
        let key = key.into();
        let value = value.into();
        // Assign a write_seq before taking the mutable borrow on the CF state.
        // AtomicU64::fetch_add only needs &self so the borrow is immediately released.
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        let cf_state = self.cf_mut(cf)?;
        cf_state.wal.append_put(key.clone(), seq, value.clone())?;
        cf_state.mem.put_seq(key, value, seq);
        self.maybe_flush_and_compact(cf)
    }

    pub fn delete_cf(&mut self, cf: &str, key: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);
        let cf_state = self.cf_mut(cf)?;
        cf_state.wal.append_delete(key.clone(), seq)?;
        cf_state.mem.delete_seq(key, seq);
        self.maybe_flush_and_compact(cf)
    }

    /// Atomic multi-key write (#10).
    /// All entries in the batch receive the SAME write_seq, making them atomically
    /// visible: a snapshot at seq < batch_seq sees none; seq ≥ batch_seq sees all.
    pub fn write_batch(&mut self, batch: WriteBatch) -> io::Result<()> {
        if batch.is_empty() { return Ok(()); }
        let seq = self.write_seq.fetch_add(1, Ordering::SeqCst);

        // Collect CF names to check for flush after all writes
        let mut cfs_touched: Vec<String> = Vec::new();

        for (cf, key, value_opt) in batch.entries {
            let cf_state = self.families.get_mut(&cf).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("column family '{cf}' not found"))
            })?;
            match value_opt {
                Some(value) => {
                    cf_state.wal.append_put(key.clone(), seq, value.clone())?;
                    cf_state.mem.put_seq(key, value, seq);
                }
                None => {
                    cf_state.wal.append_delete(key.clone(), seq)?;
                    cf_state.mem.delete_seq(key, seq);
                }
            }
            if !cfs_touched.contains(&cf) {
                cfs_touched.push(cf);
            }
        }

        let unique: Vec<String> = {
            let mut s: std::collections::HashSet<String> = std::collections::HashSet::new();
            cfs_touched.into_iter().filter(|c| s.insert(c.clone())).collect()
        };
        for cf in unique {
            self.maybe_flush_and_compact(&cf)?;
        }
        Ok(())
    }

    // -- Read path

    pub fn get(&self, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        self.get_cf("default", key)
    }

    pub fn scan(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_cf("default", from, to)
    }

    pub fn get_cf(&self, cf: &str, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let cf_state = self.cf(cf)?;

        if let Some(val) = cf_state.mem.get(key) {
            return Ok(val.clone());
        }
        for imm in cf_state.imm.iter().rev() {
            if let Some(val) = imm.get(key) {
                return Ok(val.clone());
            }
        }
        for level_files in &cf_state.levels {
            for sst in level_files.iter().rev() {
                match sst.get(key, u64::MAX, Some(&self.cache))? {
                    Some(Some(v)) => return Ok(Some(v)),
                    Some(None)    => return Ok(None),
                    None          => {}
                }
            }
        }
        Ok(None)
    }

    pub fn scan_cf(
        &self,
        cf: &str,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let from = from.as_ref().to_vec();
        let to   = to.as_ref().to_vec();
        // Use the merge-heap cursor for correctness and deduplication (#8)
        let cursor = self.make_cursor_at(cf, u64::MAX)?;
        Ok(cursor
            .skip_while(|(k, _)| k.as_slice() < from.as_slice())
            .take_while(|(k, _)| k.as_slice() < to.as_slice())
            .collect())
    }

    // -- Iterator / Cursor (#8)

    /// Return a Cursor over the "default" column family (current state).
    pub fn iter(&self) -> io::Result<Cursor> {
        self.iter_cf("default")
    }

    /// Return a Cursor over the named column family.
    pub fn iter_cf(&self, cf: &str) -> io::Result<Cursor> {
        self.make_cursor_at(cf, u64::MAX)
    }

    // -- Prefix scans (#9)

    /// Return all live (key, value) pairs whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_cf("default", prefix)
    }

    pub fn scan_prefix_cf(
        &self,
        cf: &str,
        prefix: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let prefix = prefix.as_ref().to_vec();
        let cursor = self.make_cursor_at(cf, u64::MAX)?;
        Ok(cursor
            .skip_while(|(k, _)| k.as_slice() < prefix.as_slice())
            .take_while(|(k, _)| k.starts_with(&prefix))
            .collect())
    }

    // -- Snapshots (#10)

    /// Capture a consistent point-in-time snapshot of the "default" CF.
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.snapshot_cf("default")
    }

    /// Capture a consistent point-in-time snapshot of the named CF.
    /// The snapshot_seq is pinned to the current write_seq; any writes
    /// arriving concurrently with or after this call are invisible to it.
    pub fn snapshot_cf(&self, cf: &str) -> io::Result<Snapshot> {
        let snapshot_seq = self.write_seq.load(Ordering::SeqCst).saturating_sub(1);
        let cursor = self.make_cursor_at(cf, snapshot_seq)?;
        let data: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = cursor.collect();
        Ok(Snapshot::new(snapshot_seq, data))
    }

    // -- Stats

    pub fn stats(&self) -> Stats {
        self.stats_cf("default").unwrap_or_else(|| Stats {
            mem_table_size_bytes: 0,
            immutable_count: 0,
            level_file_counts: vec![0; MAX_LEVELS],
            total_ss_table_files: 0,
            column_families: self.list_cfs(),
        })
    }

    pub fn stats_cf(&self, cf: &str) -> Option<Stats> {
        let cf_state = self.families.get(cf)?;
        let level_file_counts: Vec<usize> = cf_state.levels.iter().map(|l| l.len()).collect();
        let total_ss_table_files = level_file_counts.iter().sum();
        Some(Stats {
            mem_table_size_bytes: cf_state.mem.size_bytes,
            immutable_count: cf_state.imm.len(),
            level_file_counts,
            total_ss_table_files,
            column_families: self.list_cfs(),
        })
    }

    // ---- Internal helpers --------------------------------------------------

    fn cf(&self, name: &str) -> io::Result<&CfState> {
        self.families.get(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("column family '{name}' not found"))
        })
    }

    fn cf_mut(&mut self, name: &str) -> io::Result<&mut CfState> {
        self.families.get_mut(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("column family '{name}' not found"))
        })
    }

    /// Build a Cursor over `cf` that only sees entries with seq ≤ max_seq.
    /// Source ordering (lower index = newer, wins deduplication):
    ///   0:         active MemTable
    ///   1..n:      immutable MemTables, newest-flushed first
    ///   n+1..:     L0 SSTables, most recently created first
    ///   remaining: L1+ SSTables in level order
    fn make_cursor_at(&self, cf: &str, max_seq: u64) -> io::Result<Cursor> {
        let cf_state = self.cf(cf)?;
        let mut sources: Vec<Box<dyn Iterator<Item = (Vec<u8>, u64, Option<Vec<u8>>)> + Send>> =
            Vec::new();

        // Active MemTable — clone data so the cursor outlives the borrow
        let mem_entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = cf_state.mem.iter()
            .filter(|(_, (seq, _))| *seq <= max_seq)
            .map(|(k, (seq, v))| (k.clone(), *seq, v.clone()))
            .collect();
        sources.push(Box::new(mem_entries.into_iter()));

        // Immutable MemTables, newest first (imm is appended; last = newest)
        for imm in cf_state.imm.iter().rev() {
            let entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)> = imm.iter()
                .filter(|(_, (seq, _))| *seq <= max_seq)
                .map(|(k, (seq, v))| (k.clone(), *seq, v.clone()))
                .collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // L0 SSTables (may overlap): most recently added last → reverse
        for sst in cf_state.levels[0].iter().rev() {
            sources.push(Box::new(SstableBlockIter::new(
                sst.path.clone(),
                sst.sparse_index.clone(),
                Arc::clone(&self.cache),
                max_seq,
            )));
        }

        // L1+ SSTables (non-overlapping within each level)
        for level_files in cf_state.levels.iter().skip(1) {
            for sst in level_files.iter() {
                sources.push(Box::new(SstableBlockIter::new(
                    sst.path.clone(),
                    sst.sparse_index.clone(),
                    Arc::clone(&self.cache),
                    max_seq,
                )));
            }
        }

        Ok(Cursor::new(sources))
    }

    fn maybe_flush_and_compact(&mut self, cf_name: &str) -> io::Result<()> {
        self.drain_compaction_results();

        let needs_flush = self.families.get(cf_name)
            .map(|cf| cf.mem.size_bytes >= MEM_TABLE_SIZE_LIMIT)
            .unwrap_or(false);

        if needs_flush {
            self.flush_memtable_cf(cf_name)?;
        }

        if !self.compact.in_flight {
            self.try_schedule_compaction();
        }
        Ok(())
    }

    fn flush_memtable_cf(&mut self, cf_name: &str) -> io::Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let cf_dir = self.families[cf_name].cf_dir.clone();

        let mut flushing = MemTable::new();
        std::mem::swap(&mut self.families.get_mut(cf_name).unwrap().mem, &mut flushing);

        let filename = format!("L0_{seq:08}.sst");
        let path = cf_dir.join(&filename);
        let sst = SSTable::write_from_memtable(&path, &flushing, 0)?;

        self.manifest.append(&ManifestRecord::AddFile {
            cf: cf_name.to_string(),
            level: 0,
            filename: filename.clone(),
        })?;

        self.families.get_mut(cf_name).unwrap().levels[0].push(sst);

        let wal_path = cf_dir.join("wal.log");
        fs::remove_file(&wal_path).ok();
        let new_wal = Wal::open(&wal_path)?;
        self.families.get_mut(cf_name).unwrap().wal = new_wal;

        Ok(())
    }

    fn try_schedule_compaction(&mut self) {
        let job_opt = self.find_compaction_job();
        if let Some(job) = job_opt {
            if self.compact.tx.send(job).is_ok() {
                self.compact.in_flight = true;
            }
        }
    }

    fn find_compaction_job(&self) -> Option<CompactionJob> {
        for (cf_name, cf) in &self.families {
            if l0_needs_compaction(&cf.levels[0]) {
                let l0_paths: Vec<PathBuf> = cf.levels[0].iter().map(|s| s.path.clone()).collect();
                let l1_paths: Vec<PathBuf> = cf.levels[1].iter().map(|s| s.path.clone()).collect();
                let drop_tombstones = cf.levels.iter().skip(2).all(|l| l.is_empty());
                let seq = self.seq.fetch_add(1, Ordering::SeqCst);
                return Some(CompactionJob::L0ToL1 {
                    cf_name: cf_name.clone(),
                    l0_paths,
                    l1_paths,
                    drop_tombstones,
                    seq,
                    dir: cf.cf_dir.clone(),
                });
            }
            for level in 1..MAX_LEVELS - 1 {
                let budget = L1_BASE_BYTES * (SIZE_RATIO as u64).pow(level as u32 - 1);
                let total: u64 = cf.levels[level]
                    .iter()
                    .map(|s| fs::metadata(&s.path).map(|m| m.len()).unwrap_or(0))
                    .sum();
                if total > budget && !cf.levels[level].is_empty() {
                    let victim = cf.levels[level][0].path.clone();
                    let next_paths: Vec<PathBuf> =
                        cf.levels[level + 1].iter().map(|s| s.path.clone()).collect();
                    let seq = self.seq.fetch_add(1, Ordering::SeqCst);
                    return Some(CompactionJob::CompactLevel {
                        cf_name: cf_name.clone(),
                        level,
                        victim_path: victim,
                        next_paths,
                        is_deepest: level + 1 == MAX_LEVELS - 1,
                        seq,
                        dir: cf.cf_dir.clone(),
                    });
                }
            }
        }
        None
    }

    fn drain_compaction_results(&mut self) {
        while let Ok(result) = self.compact.rx.lock().unwrap().try_recv() {
            for path in result.merged_source_paths.iter().chain(result.merged_target_paths.iter()) {
                if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
                    let _ = self.manifest.append(&ManifestRecord::RemoveFile {
                        cf: result.cf_name.clone(),
                        filename: fname.to_string(),
                    });
                }
            }
            if let Some(ref sst) = result.new_sst {
                if let Some(fname) = sst.path.file_name().and_then(|n| n.to_str()) {
                    let _ = self.manifest.append(&ManifestRecord::AddFile {
                        cf: result.cf_name.clone(),
                        level: result.target_level as u32,
                        filename: fname.to_string(),
                    });
                }
            }

            if let Some(cf) = self.families.get_mut(&result.cf_name) {
                let src_level = result.target_level.saturating_sub(1);
                for path in &result.merged_source_paths {
                    cf.levels[src_level].retain(|s| &s.path != path);
                    let _ = fs::remove_file(path);
                }
                for path in &result.merged_target_paths {
                    cf.levels[result.target_level].retain(|s| &s.path != path);
                    let _ = fs::remove_file(path);
                }
                if let Some(sst) = result.new_sst {
                    cf.levels[result.target_level].push(sst);
                }
            }
            self.compact.in_flight = false;
        }
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let _ = self.compact.tx.send(CompactionJob::Shutdown);
    }
}

// ---- Background compaction worker ------------------------------------------

fn spawn_compaction_worker() -> CompactionWorker {
    let (job_tx, job_rx) = mpsc::channel::<CompactionJob>();
    let (res_tx, res_rx) = mpsc::channel::<CompactionResult>();

    thread::spawn(move || {
        for job in job_rx {
            match job {
                CompactionJob::Shutdown => break,
                CompactionJob::L0ToL1 { cf_name, l0_paths, l1_paths, drop_tombstones, seq, dir } => {
                    if let Ok(new_sst) = compact_l0_to_l1(&l0_paths, &l1_paths, drop_tombstones, seq, &dir) {
                        let _ = res_tx.send(CompactionResult {
                            cf_name,
                            target_level: 1,
                            merged_source_paths: l0_paths,
                            merged_target_paths: l1_paths,
                            new_sst,
                        });
                    }
                }
                CompactionJob::CompactLevel { cf_name, level, victim_path, next_paths, is_deepest, seq, dir } => {
                    if let Ok(new_sst) = compact_level(level, &victim_path, &next_paths, is_deepest, seq, &dir) {
                        let _ = res_tx.send(CompactionResult {
                            cf_name,
                            target_level: level + 1,
                            merged_source_paths: vec![victim_path],
                            merged_target_paths: next_paths,
                            new_sst,
                        });
                    }
                }
            }
        }
    });

    CompactionWorker { tx: job_tx, rx: Mutex::new(res_rx), in_flight: false }
}

fn compact_l0_to_l1(
    l0_paths: &[PathBuf],
    l1_paths: &[PathBuf],
    drop_tombstones: bool,
    seq: u64,
    dir: &Path,
) -> io::Result<Option<SSTable>> {
    let mut l0_entries = Vec::new();
    for path in l0_paths {
        if let Ok(sst) = SSTable::open(path, 0) {
            l0_entries = merge_entries(sst.scan_all()?, l0_entries, false);
        }
    }
    let mut l1_entries = Vec::new();
    for path in l1_paths {
        if let Ok(sst) = SSTable::open(path, 1) {
            l1_entries.extend(sst.scan_all()?);
        }
    }
    l1_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let merged = merge_entries(l0_entries, l1_entries, drop_tombstones);
    if merged.is_empty() { return Ok(None); }
    let path = next_sstable_path(dir, 1, seq);
    Ok(Some(write_merged(path, merged, 1)?))
}

fn compact_level(
    level: usize,
    victim_path: &Path,
    next_paths: &[PathBuf],
    is_deepest: bool,
    seq: u64,
    dir: &Path,
) -> io::Result<Option<SSTable>> {
    let victim = SSTable::open(victim_path, level as u32)?;
    let new_entries = victim.scan_all()?;
    let mut next_entries = Vec::new();
    for path in next_paths {
        if let Ok(sst) = SSTable::open(path, (level + 1) as u32) {
            next_entries.extend(sst.scan_all()?);
        }
    }
    next_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let merged = merge_entries(new_entries, next_entries, is_deepest);
    if merged.is_empty() { return Ok(None); }
    let path = next_sstable_path(dir, (level + 1) as u32, seq);
    Ok(Some(write_merged(path, merged, (level + 1) as u32)?))
}

// ---- Utility ---------------------------------------------------------------

fn seq_from_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_suffix(".sst")?;
    let (_, seq_s) = stem.split_once('_')?;
    seq_s.parse().ok()
}

// ---- SharedLsmEngine (#1) --------------------------------------------------

#[derive(Clone)]
pub struct SharedLsmEngine(Arc<RwLock<LsmEngine>>);

impl SharedLsmEngine {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self(Arc::new(RwLock::new(LsmEngine::open(path)?))))
    }

    pub fn open_with_cfs(path: impl AsRef<Path>, cfs: &[&str]) -> io::Result<Self> {
        Ok(Self(Arc::new(RwLock::new(LsmEngine::open_with_cfs(path, cfs)?))))
    }

    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().put(key, value)
    }
    pub fn put_cf(&self, cf: &str, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().put_cf(cf, key, value)
    }
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().delete(key)
    }
    pub fn delete_cf(&self, cf: &str, key: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().delete_cf(cf, key)
    }
    pub fn get(&self, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        self.0.read().unwrap().get(key)
    }
    pub fn get_cf(&self, cf: &str, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        self.0.read().unwrap().get_cf(cf, key)
    }
    pub fn scan(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.read().unwrap().scan(from, to)
    }
    pub fn scan_cf(&self, cf: &str, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.read().unwrap().scan_cf(cf, from, to)
    }
    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.read().unwrap().scan_prefix(prefix)
    }
    pub fn scan_prefix_cf(&self, cf: &str, prefix: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.read().unwrap().scan_prefix_cf(cf, prefix)
    }
    pub fn write_batch(&self, batch: WriteBatch) -> io::Result<()> {
        self.0.write().unwrap().write_batch(batch)
    }
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        self.0.read().unwrap().snapshot()
    }
    pub fn stats(&self) -> Stats {
        self.0.read().unwrap().stats()
    }
    pub fn list_cfs(&self) -> Vec<String> {
        self.0.read().unwrap().list_cfs()
    }
}
