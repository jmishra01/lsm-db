// =====================================================
// LSM Engine -- top-level DB engine
//
// Improvements over v1:
//   #1  SharedLsmEngine  -- Arc<RwLock<>> wrapper for concurrent access
//   #2  Background compaction thread -- writes never block on merge I/O
//   #3  Block cache -- hot blocks stay in memory (via BlockCache)
//   #4  SSTable v2 -- sparse index + LZ4-compressed 4 KiB data blocks
// =====================================================

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crate::block_cache::BlockCache;
use crate::compaction::{l0_needs_compaction, merge_entries, next_sstable_path, write_merged, L1_BASE_BYTES, SIZE_RATIO};
use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::wal::{Wal, WalRecord};

const MEM_TABLE_SIZE_LIMIT: usize = 256 * 1024;
const MAX_LEVELS: usize = 7;
const BLOCK_CACHE_CAPACITY: usize = 512; // cached blocks (~2 MiB at 4 KiB/block)

// ---- Background compaction messages ----------------------------------------

enum CompactionJob {
    L0ToL1 {
        l0_paths: Vec<PathBuf>,
        l1_paths: Vec<PathBuf>,
        drop_tombstones: bool,
        seq: u64,
        dir: PathBuf,
    },
    CompactLevel {
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

// ---- Engine ----------------------------------------------------------------

pub struct LsmEngine {
    dir: PathBuf,
    wal: Wal,
    mem: MemTable,
    imm: Vec<MemTable>,
    levels: Vec<Vec<SSTable>>,
    seq: Arc<AtomicU64>,
    cache: Arc<BlockCache>,
    compact: CompactionWorker,
}

#[derive(Clone, Debug)]
pub struct Stats {
    pub mem_table_size_bytes: usize,
    pub immutable_count: usize,
    pub level_file_counts: Vec<usize>,
    pub total_ss_table_files: usize,
}

impl LsmEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<LsmEngine> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut levels: Vec<Vec<SSTable>> = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
        let mut max_seq = 0u64;
        let mut sst_paths: Vec<(u32, u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("sst") {
                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                    if let Some((ls, ss)) = stem.split_once('_') {
                        let level: u32 = ls.trim_start_matches('L').parse().unwrap_or(0);
                        let seq: u64 = ss.parse().unwrap_or(0);
                        if seq > max_seq { max_seq = seq; }
                        sst_paths.push((level, seq, p));
                    }
                }
            }
        }

        sst_paths.sort_by_key(|(l, s, _)| (*l, *s));
        for (level, _, p) in sst_paths {
            if let Ok(sst) = SSTable::open(&p, level) {
                if (level as usize) < MAX_LEVELS {
                    levels[level as usize].push(sst);
                }
            }
        }

        // WAL recovery
        let wal_path = dir.join("wal.log");
        let records = Wal::recover(&wal_path)?;
        let mut mem = MemTable::new();
        for rec in records {
            match rec {
                WalRecord::Put { key, value } => mem.put(key, value),
                WalRecord::Delete { key } => mem.delete(key),
            }
        }
        let wal = Wal::open(&wal_path)?;

        let cache = BlockCache::new(BLOCK_CACHE_CAPACITY);
        let compact = spawn_compaction_worker();

        Ok(Self { dir, wal, mem, imm: Vec::new(), levels, seq: Arc::new(AtomicU64::new(max_seq + 1)), cache, compact })
    }

    // -- Write path

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        let value = value.into();
        self.wal.append_put(key.clone(), value.clone())?;
        self.mem.put(key, value);
        self.maybe_flush_and_compact()
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        let key = key.into();
        self.wal.append_delete(key.clone())?;
        self.mem.delete(key);
        self.maybe_flush_and_compact()
    }

    // -- Read path

    pub fn get(&self, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        if let Some(val) = self.mem.get(key) {
            return Ok(val.clone());
        }
        for imm in self.imm.iter().rev() {
            if let Some(val) = imm.get(key) {
                return Ok(val.clone());
            }
        }
        for level_files in &self.levels {
            for sst in level_files.iter().rev() {
                match sst.get(key, Some(&self.cache))? {
                    Some(Some(v)) => return Ok(Some(v)),
                    Some(None)    => return Ok(None),   // tombstone
                    None          => {}
                }
            }
        }
        Ok(None)
    }

    pub fn scan(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeMap;
        let from = from.as_ref();
        let to   = to.as_ref();
        let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        for level_files in self.levels.iter().rev() {
            for sst in level_files.iter() {
                for (k, v) in sst.scan_all()? {
                    if k.as_slice() >= from && k.as_slice() < to {
                        map.insert(k, v);
                    }
                }
            }
        }
        for imm in self.imm.iter() {
            for (k, v) in imm.iter() {
                if k.as_slice() >= from && k.as_slice() < to {
                    map.insert(k.clone(), v.clone());
                }
            }
        }
        for (k, v) in self.mem.iter() {
            if k.as_slice() >= from && k.as_slice() < to {
                map.insert(k.clone(), v.clone());
            }
        }

        Ok(map.into_iter().filter_map(|(k, v)| v.map(|val| (k, val))).collect())
    }

    // -- Stats

    pub fn stats(&self) -> Stats {
        let level_file_counts: Vec<usize> = self.levels.iter().map(|l| l.len()).collect();
        let total_ss_table_files = level_file_counts.iter().sum();
        Stats {
            mem_table_size_bytes: self.mem.size_bytes,
            immutable_count: self.imm.len(),
            level_file_counts,
            total_ss_table_files,
        }
    }

    // -- Flush & compaction

    fn maybe_flush_and_compact(&mut self) -> io::Result<()> {
        self.drain_compaction_results();

        if self.mem.size_bytes >= MEM_TABLE_SIZE_LIMIT {
            self.flush_memtable()?;
        }

        if !self.compact.in_flight {
            self.try_schedule_compaction();
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        let mut flushing = MemTable::new();
        std::mem::swap(&mut self.mem, &mut flushing);

        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let path = next_sstable_path(&self.dir, 0, seq);
        let sst = SSTable::write_from_memtable(&path, &flushing, 0)?;
        self.levels[0].push(sst);

        let wal_path = self.dir.join("wal.log");
        fs::remove_file(&wal_path).ok();
        self.wal = Wal::open(&wal_path)?;
        Ok(())
    }

    fn try_schedule_compaction(&mut self) {
        if l0_needs_compaction(&self.levels[0]) {
            let l0_paths: Vec<PathBuf> = self.levels[0].iter().map(|s| s.path.clone()).collect();
            let l1_paths: Vec<PathBuf> = self.levels[1].iter().map(|s| s.path.clone()).collect();
            let drop_tombstones = self.levels.iter().skip(2).all(|l| l.is_empty());
            let seq = self.seq.fetch_add(1, Ordering::SeqCst);
            if self.compact.tx.send(CompactionJob::L0ToL1 {
                l0_paths, l1_paths, drop_tombstones, seq, dir: self.dir.clone()
            }).is_ok() {
                self.compact.in_flight = true;
            }
            return;
        }

        for level in 1..MAX_LEVELS - 1 {
            let budget = L1_BASE_BYTES * (SIZE_RATIO as u64).pow(level as u32 - 1);
            let total: u64 = self.levels[level]
                .iter()
                .map(|s| fs::metadata(&s.path).map(|m| m.len()).unwrap_or(0))
                .sum();
            if total > budget && !self.levels[level].is_empty() {
                let victim   = self.levels[level][0].path.clone();
                let next_paths: Vec<PathBuf> = self.levels[level + 1].iter().map(|s| s.path.clone()).collect();
                let seq = self.seq.fetch_add(1, Ordering::SeqCst);
                if self.compact.tx.send(CompactionJob::CompactLevel {
                    level,
                    victim_path: victim,
                    next_paths,
                    is_deepest: level + 1 == MAX_LEVELS - 1,
                    seq,
                    dir: self.dir.clone(),
                }).is_ok() {
                    self.compact.in_flight = true;
                }
                return;
            }
        }
    }

    /// Apply any compaction results that the background thread has finished.
    fn drain_compaction_results(&mut self) {
        while let Ok(result) = self.compact.rx.lock().unwrap().try_recv() {
            // Remove merged source SSTables
            for path in &result.merged_source_paths {
                let src_level = result.target_level.saturating_sub(1);
                self.levels[src_level].retain(|s| &s.path != path);
                let _ = fs::remove_file(path);
            }
            // Remove merged target SSTables
            for path in &result.merged_target_paths {
                self.levels[result.target_level].retain(|s| &s.path != path);
                let _ = fs::remove_file(path);
            }
            // Install new SSTable
            if let Some(sst) = result.new_sst {
                self.levels[result.target_level].push(sst);
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

                CompactionJob::L0ToL1 { l0_paths, l1_paths, drop_tombstones, seq, dir } => {
                    let result = compact_l0_to_l1(&l0_paths, &l1_paths, drop_tombstones, seq, &dir);
                    if let Ok(new_sst) = result {
                        let _ = res_tx.send(CompactionResult {
                            target_level: 1,
                            merged_source_paths: l0_paths,
                            merged_target_paths: l1_paths,
                            new_sst,
                        });
                    }
                }

                CompactionJob::CompactLevel { level, victim_path, next_paths, is_deepest, seq, dir } => {
                    let result = compact_level(level, &victim_path, &next_paths, is_deepest, seq, &dir);
                    if let Ok(new_sst) = result {
                        let _ = res_tx.send(CompactionResult {
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
    if merged.is_empty() {
        return Ok(None);
    }
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
    if merged.is_empty() {
        return Ok(None);
    }
    let path = next_sstable_path(dir, (level + 1) as u32, seq);
    Ok(Some(write_merged(path, merged, (level + 1) as u32)?))
}

// ---- SharedLsmEngine  (#1 -- concurrent access via Arc<RwLock<>>) ----------

/// Thread-safe handle to an `LsmEngine`.
/// Multiple clones can be held across threads. Reads take a shared lock;
/// writes take an exclusive lock.
#[derive(Clone)]
pub struct SharedLsmEngine(Arc<RwLock<LsmEngine>>);

impl SharedLsmEngine {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(Self(Arc::new(RwLock::new(LsmEngine::open(path)?))))
    }

    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().put(key, value)
    }

    pub fn delete(&self, key: impl Into<Vec<u8>>) -> io::Result<()> {
        self.0.write().unwrap().delete(key)
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> io::Result<Option<Vec<u8>>> {
        self.0.read().unwrap().get(key)
    }

    pub fn scan(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.0.read().unwrap().scan(from, to)
    }

    pub fn stats(&self) -> Stats {
        self.0.read().unwrap().stats()
    }
}
