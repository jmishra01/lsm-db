// =====================================================
// LSM Engine
//
// #1  SharedLsmEngine  — Arc<RwLock<>> concurrent wrapper
// #2  Background compaction thread
// #3  Block cache (hot blocks stay in memory)
// #4  SSTable v3 — sparse index + LZ4 blocks + CRC32
// #5  CRC32 checksums on WAL records and SSTable blocks
// #6  Column families — independent key spaces
// #7  Manifest — durable log of live SSTable files
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
use crate::manifest::{Manifest, ManifestRecord};
use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::wal::{Wal, WalRecord};

const MEM_TABLE_SIZE_LIMIT: usize = 256 * 1024;
const MAX_LEVELS: usize = 7;
const BLOCK_CACHE_CAPACITY: usize = 512;

// ---- Column-family state (#6) -----------------------------------------------
//
// Each column family is an independent key space with its own:
//   - WAL file       (dir/{cf_name}/wal.log)
//   - SSTable files  (dir/{cf_name}/L{n}_{seq}.sst)
//   - MemTable
//   - Level hierarchy
//
// The global seq counter and block cache are shared across all CFs because:
//   - Shared seq: guarantees unique filenames even if two CFs flush
//     simultaneously (future multi-threaded writes).
//   - Shared cache: a single LRU budget across all CFs is more efficient
//     than per-CF caches that can starve each other.

struct CfState {
    #[allow(dead_code)] // stored for introspection / future stats
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
    /// Names of all open column families.
    pub column_families: Vec<String>,
}

// ---- Engine ----------------------------------------------------------------

pub struct LsmEngine {
    dir: PathBuf,
    /// One entry per column family. (#6)
    families: HashMap<String, CfState>,
    seq: Arc<AtomicU64>,
    cache: Arc<BlockCache>,
    compact: CompactionWorker,
    /// Durable record of live SSTable files. (#7)
    manifest: Manifest,
}

impl LsmEngine {
    // -- Open / create

    /// Open the engine with only the "default" column family.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with_cfs(path, &["default"])
    }

    /// Open the engine, ensuring the listed column families exist.
    /// CFs not in the list but present in the manifest are also loaded.
    /// New names in the list that are absent from the manifest are created.
    pub fn open_with_cfs<P: AsRef<Path>>(path: P, cf_names: &[&str]) -> io::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // (#7) Replay manifest to determine the current set of live SSTable files.
        // This replaces the directory scan used before: the manifest is the single
        // authoritative source of which files are current, even after a crash.
        let manifest_path = dir.join("MANIFEST");
        let mstate = Manifest::recover(&manifest_path)?;
        let mut manifest = Manifest::open(&manifest_path)?;

        let mut families: HashMap<String, CfState> = HashMap::new();
        let mut max_seq: u64 = 0;

        // Load CFs recorded in manifest
        for cf_name in &mstate.cfs {
            let cf_dir = dir.join(cf_name);
            fs::create_dir_all(&cf_dir)?;

            // WAL recovery for this CF
            let wal_path = cf_dir.join("wal.log");
            let wal_records = Wal::recover(&wal_path)?;
            let mut mem = MemTable::new();
            for rec in wal_records {
                match rec {
                    WalRecord::Put { key, value } => mem.put(key, value),
                    WalRecord::Delete { key } => mem.delete(key),
                }
            }
            let wal = Wal::open(&wal_path)?;

            let mut cf = CfState::new(cf_name.clone(), cf_dir.clone(), wal);
            cf.mem = mem;

            // Load SSTables per manifest (not by directory scan)
            let empty = vec![];
            let file_list = mstate.files.get(cf_name).unwrap_or(&empty);
            for (level, filename) in file_list {
                // Advance global seq counter past all known seq numbers
                if let Some(seq) = seq_from_filename(filename) {
                    if seq > max_seq { max_seq = seq; }
                }
                let path = cf_dir.join(filename);
                match SSTable::open(&path, *level) {
                    Ok(sst) if (*level as usize) < MAX_LEVELS => {
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
            seq: Arc::new(AtomicU64::new(max_seq + 1)),
            cache,
            compact,
            manifest,
        })
    }

    // -- Column family management (#6)

    /// Create a new column family at runtime.
    pub fn create_cf(&mut self, name: &str) -> io::Result<()> {
        if self.families.contains_key(name) {
            return Ok(()); // already exists
        }
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

    /// Write to the "default" column family.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        self.put_cf("default", key, value)
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        self.delete_cf("default", key)
    }

    /// Write to an explicit column family.
    pub fn put_cf(
        &mut self,
        cf: &str,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> io::Result<()> {
        let key = key.into();
        let value = value.into();
        let cf_state = self.cf_mut(cf)?;
        cf_state.wal.append_put(key.clone(), value.clone())?;
        cf_state.mem.put(key, value);
        self.maybe_flush_and_compact(cf)
    }

    pub fn delete_cf(&mut self, cf: &str, key: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let cf_state = self.cf_mut(cf)?;
        cf_state.wal.append_delete(key.clone())?;
        cf_state.mem.delete(key);
        self.maybe_flush_and_compact(cf)
    }

    // -- Read path

    /// Read from the "default" column family.
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
                match sst.get(key, Some(&self.cache))? {
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
        use std::collections::BTreeMap;
        let from = from.as_ref();
        let to   = to.as_ref();
        let cf_state = self.cf(cf)?;
        let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        for level_files in cf_state.levels.iter().rev() {
            for sst in level_files.iter() {
                for (k, v) in sst.scan_all()? {
                    if k.as_slice() >= from && k.as_slice() < to {
                        map.insert(k, v);
                    }
                }
            }
        }
        for imm in cf_state.imm.iter() {
            for (k, v) in imm.iter() {
                if k.as_slice() >= from && k.as_slice() < to {
                    map.insert(k.clone(), v.clone());
                }
            }
        }
        for (k, v) in cf_state.mem.iter() {
            if k.as_slice() >= from && k.as_slice() < to {
                map.insert(k.clone(), v.clone());
            }
        }

        Ok(map.into_iter().filter_map(|(k, v)| v.map(|val| (k, val))).collect())
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

        // (#7) Write manifest BEFORE updating in-memory state.
        // If we crash after this line the new SST is recorded; on recovery the
        // WAL replay would add duplicates that the SST already covers — harmless.
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
        // Find the first CF that needs compaction.
        // Collect the job details without holding a borrow on self.families
        // so we can then borrow self.compact.tx.
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
            // (#7) Write manifest removes + add BEFORE touching in-memory state.
            // Order: RemoveFile old entries first, then AddFile for the new one.
            // If we crash mid-manifest write, the worst case is a partially
            // recorded edit — the CRC check in Manifest::recover will stop at the
            // corrupt record, leaving the previous committed state intact.
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

            // Update in-memory level lists
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

/// Thread-safe handle. Multiple clones share one underlying engine via
/// Arc<RwLock<>>. Reads acquire a shared lock; writes acquire an exclusive lock.
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
    pub fn stats(&self) -> Stats {
        self.0.read().unwrap().stats()
    }
    pub fn list_cfs(&self) -> Vec<String> {
        self.0.read().unwrap().list_cfs()
    }
}
