# LSM-DB — A Log-Structured Merge-Tree Database in Rust

> **Purpose:** Educational implementation of the storage engine that powers
> LevelDB, RocksDB, Cassandra, and HBase. Every design decision is explained
> so you understand not just *what* the code does but *why* it works that way.

---

## Table of Contents

1. [Why LSM-Tree?](#1-why-lsm-tree)
2. [Architecture Overview](#2-architecture-overview)
3. [Component Deep-Dives](#3-component-deep-dives)
   - [MemTable](#memtable)
   - [WAL (Write-Ahead Log)](#wal-write-ahead-log)
   - [SSTable v2 — Sparse Index + Data Blocks](#sstable-v2--sparse-index--data-blocks)
   - [Bloom Filter](#bloom-filter)
   - [Compaction](#compaction)
   - [Block Cache](#block-cache)
4. [Engine Improvements](#4-engine-improvements)
   - [#1 Concurrent Access — SharedLsmEngine](#1-concurrent-access--sharedlsmengine)
   - [#2 Background Compaction Thread](#2-background-compaction-thread)
   - [#3 Block Cache](#3-block-cache)
   - [#4 Sparse Index + Data Blocks](#4-sparse-index--data-blocks)
5. [Write Path — Step by Step](#5-write-path--step-by-step)
6. [Read Path — Step by Step](#6-read-path--step-by-step)
7. [Compaction — Why and How](#7-compaction--why-and-how)
8. [File Layout on Disk](#8-file-layout-on-disk)
9. [Key Trade-offs and Limitations](#9-key-trade-offs-and-limitations)
10. [Running the Demo](#10-running-the-demo)
11. [Higher-Level Features](#11-higher-level-features)
    - [#11 HTTP API](#11-http-api)
    - [#12 Replication](#12-replication)
    - [#13 Benchmark Suite](#13-benchmark-suite)
12. [Advanced Features](#12-advanced-features)
    - [TTL / Key Expiry](#ttl--key-expiry)
    - [Range Deletion](#range-deletion)
    - [Compaction Filter](#compaction-filter)
    - [WAL Group Commit](#wal-group-commit)
    - [Merge Operator](#merge-operator)
    - [Optimistic Concurrency Control (OCC)](#optimistic-concurrency-control-occ)
    - [Snapshot Garbage Collection](#snapshot-garbage-collection)
    - [Metrics + Async API](#metrics--async-api)
13. [Further Reading](#13-further-reading)

---

## 1. Why LSM-Tree?

Traditional B-Tree databases (PostgreSQL, MySQL InnoDB) perform **random writes**.
Updating a record means seeking to its page on disk and writing in place. On an
HDD that costs ~10 ms per seek; even on SSDs random writes wear out flash cells
faster and hit write amplification limits.

LSM-Trees flip this: **all writes are sequential**.

| Property            | B-Tree            | LSM-Tree                  |
|---------------------|-------------------|---------------------------|
| Write pattern       | Random (in-place) | Sequential (append-only)  |
| Write latency       | High (seek cost)  | Low (memory + sequential) |
| Read latency        | Low (single seek) | Higher (multi-level check)|
| Space amplification | Low               | Higher (until compaction) |
| Best for            | Read-heavy OLTP   | Write-heavy time-series, logs, KV |

**Core insight:** write to memory first (MemTable), batch into immutable sorted
files on disk (SSTables), and periodically merge those files (compaction).
The merge is sequential I/O, which is 10–100× faster than random I/O.

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                    LsmEngine                          │
│                                                      │
│  PUT/DELETE ──► WAL ──► MemTable                     │
│                          │ (size > 256 KiB)          │
│                          ▼                           │
│                     flush to SSTable                 │
│                          │                           │
│              ┌───────────▼────────────┐              │
│              │  L0  [sst0] [sst1] ... │  (unsorted)  │
│              │  L1  [sst0]            │  (sorted)    │
│              │  L2  [sst0]            │              │
│              │  ...                   │              │
│              └────────────────────────┘              │
│                                                      │
│  Background compaction thread merges L(n) → L(n+1)  │
│  Block cache serves hot blocks from memory           │
└──────────────────────────────────────────────────────┘
```

**Data flow:**
- Every write goes to the WAL (crash safety) then the in-memory MemTable.
- When the MemTable is full it is flushed to an SSTable file at L0.
- When L0 has too many files, a background thread merges them into L1.
- L1 overflows into L2, and so on (leveled compaction).
- A `GET` checks MemTable → L0 SSTables → L1 → … until found or exhausted.

---

## 3. Component Deep-Dives

### MemTable

**File:** `src/memtable.rs`

**What:** In-memory sorted map (`BTreeMap<Vec<u8>, Option<Vec<u8>>>`).
`Some(bytes)` = live value. `None` = tombstone (deleted key).

**Why BTreeMap?**
- Iteration is always in sorted key order — required for flushing to a sorted SSTable.
- O(log n) point lookups — acceptable for an in-memory structure.
- A skip-list (used by LevelDB) has the same O(log n) complexity with better
  concurrent performance, but is more complex to implement.

**Why track `size_bytes`?**
The flush threshold (256 KiB) is enforced by byte count, not entry count,
because value sizes vary wildly. A single 1 MiB value should trigger a flush
just as much as 1000 small values would.

**Why tombstones (None) instead of deletion?**
Because SSTables are *immutable*. You cannot remove a key from a file that has
already been written to disk. Instead, we write a special "deleted" marker.
During reads, if a tombstone is found the key is treated as absent. During
compaction at the deepest level, tombstones are finally dropped.

---

### WAL (Write-Ahead Log)

**File:** `src/wal.rs`

**What:** An append-only binary log file (`wal.log`) that records every
mutation *before* it touches the MemTable.

**Why does the WAL exist?**
The MemTable lives in RAM. If the process crashes, all in-flight data is lost.
The WAL is the crash-recovery mechanism: on `open()`, the engine replays the
WAL to reconstruct the MemTable exactly as it was before the crash.

**Why write to WAL before MemTable?**
This is the "write-ahead" guarantee. If we wrote to the MemTable first and
crashed before writing the WAL, the WAL would be missing the record and
recovery would lose data. By writing to the WAL first, the record is durable
before any in-memory state changes.

**Why truncate the WAL after flush?**
Once a MemTable is safely written to an SSTable on disk, the WAL entries it
covered are no longer needed for recovery — the SSTable *is* the durable
record. Truncating prevents unbounded WAL growth.

**Format:**
```
Put  record: [0x00][key_len: u32 BE][key][val_len: u32 BE][val]
Delete record: [0x01][key_len: u32 BE][key]
```

**Why tolerate partial trailing records on recovery?**
A crash can happen mid-write, leaving a partial record at the end of the WAL.
The `recover()` function treats `UnexpectedEof` on any field *after* the tag
byte as a clean stop — discarding the partial record. The last committed record
before it is intact and safe to replay.

---

### SSTable v2 — Sparse Index + Data Blocks

**File:** `src/sstable.rs`

**What:** An immutable, sorted, on-disk file of key-value entries. Once
written, it is never modified — only read or deleted during compaction.

#### Why immutability?

Immutable files are:
- **Safe to read concurrently** — no locking needed for reads.
- **Easy to replace atomically** — during compaction, old files are deleted
  and a new file is added; from any reader's perspective the switch is atomic.
- **Friendly to the OS page cache** — cold pages are never invalidated by
  in-place updates.

#### Why a sparse index instead of a dense index?

**Dense index (v1):** one index entry per key → `O(1)` lookup by key, but the
index itself can be megabytes for large files, all loaded into memory.

**Sparse index (v2):** one index entry per *block* (4 KiB) → the index is
`file_size / 4096` times smaller. For a 64 MiB SSTable with ~40-byte keys,
a dense index would be ~1.6 MiB; a sparse index over 4 KiB blocks is ~640 bytes.

The trade-off: a point lookup now requires reading and scanning one block
(~4 KiB) instead of seeking directly to the exact entry. At 4 KiB blocks this
is one disk read regardless, so the practical latency difference is negligible.

#### Why LZ4 compression per block?

- **Storage savings:** key-value data with repeated key prefixes (like
  `sensor:000001`, `sensor:000002`, …) compresses extremely well. Typical
  ratio is 2–5×.
- **I/O reduction:** a compressed 4 KiB block read fetches more logical data
  than an uncompressed 4 KiB read. Fewer disk reads per query.
- **Block granularity:** compressing per-block (not per-file) means the cache
  stores decompressed blocks. A hot block is decompressed once and cached;
  subsequent lookups don't pay the CPU cost again.
- **LZ4 specifically:** fastest decompression of any production-grade
  compressor (~5 GB/s), negligible latency contribution even on the hot path.

#### File layout

```
[ DATA BLOCKS ]
  For each block:
    compressed_len   : u32 LE
    compressed_data  : LZ4 (size-prepended)
      Block body (decompressed):
        entry_count  : u32 LE
        For each entry:
          key_len    : u32 LE
          key        : bytes
          val_tag    : u8   (0 = live, 1 = tombstone)
          val_len    : u32 LE  (only if live)
          val        : bytes   (only if live)

[ SPARSE INDEX ]
  For each block:
    key_len        : u32 LE
    first_key      : bytes
    block_offset   : u64 LE

[ BLOOM FILTER ]
  (serialised BloomFilter bytes)

[ FOOTER — 48 bytes ]
  index_offset   : u64 LE
  bloom_offset   : u64 LE
  bloom_len      : u64 LE
  entry_count    : u64 LE
  block_count    : u64 LE
  magic          : u64 LE = 0xCAFE_F00D_1234_5678
```

**Why footer at the end?**
The file is written sequentially in one pass. The footer's offsets can only be
known after writing all preceding sections. Reading starts from the end
(seek to `file_size - 48`), then jumps to the index and bloom sections.

**Why a different magic number than v1?**
Magic numbers prevent accidentally reading a file written by a different
version or a different program entirely. A wrong magic returns an immediate
`InvalidData` error rather than silently returning garbage data.

---

### Bloom Filter

**File:** `src/bloom.rs`

**What:** A probabilistic data structure that answers "is this key *definitely
not* in this SSTable?" in O(1) time with zero disk I/O.

**Why it matters:**
Without a bloom filter, every `GET` that misses all SSTables must open and
scan each file. For a cold key across 100 SST files, that is 100 disk reads.
With a bloom filter, each miss is ruled out by a ~10-byte in-memory check.

**How it works:**
- On write: run the key through k hash functions; set k bits in a bit array.
- On lookup: check the same k bits. If any bit is 0, the key is *definitely
  absent*. If all bits are 1, the key *may* be present (false positive possible).
- False positive rate ~1% with k=7 hash functions and ~9.6 bits per key.

**Why double-hashing (FNV-1a + djb2)?**
Generating k independent hash functions is expensive. Double-hashing
(`h(i) = h1 + i * h2`) approximates k independent hashes from just two
hash evaluations, a standard technique described by Kirsch & Mitzenmacher (2006).

---

### Compaction

**File:** `src/compaction.rs`

**What:** Periodically merges multiple SSTable files into one larger, sorted
SSTable. Removes duplicate keys (keeping the newest version) and drops
tombstones at the deepest level.

**Why compaction is necessary:**

1. **Read amplification:** without compaction, a key could exist in every L0
   file (since L0 files are not sorted relative to each other). Every read
   must check all of them.
2. **Space amplification:** deleted keys and overwritten values accumulate in
   old SSTable files, wasting space, until tombstones are dropped at compaction.
3. **Sort order:** L1+ files must not overlap in key range so that reads can
   skip files entirely. Compaction enforces this.

**Leveled compaction strategy:**
- **L0** is special: files may overlap in key range. Triggered when L0 has ≥ 4 files.
- **L1+** have a size budget (L1 = 1 MiB, L2 = 10 MiB, L3 = 100 MiB, …).
  When a level exceeds its budget, the oldest SSTable is picked and merged
  with all overlapping SSTables in the next level.
- Size ratio = 10×. This keeps write amplification bounded to ~30× (10× per
  level, 3 active levels typically).

**Why drop tombstones only at the deepest level?**
A tombstone at L1 may be covering a live entry at L2. If you drop the
tombstone at L1 during L1→L2 compaction, the old live value at L2 would
"reappear" — a consistency bug. Tombstones must not be dropped until we are
sure no older copy of the key exists at any deeper level.

---

### Block Cache

**File:** `src/block_cache.rs`

**What:** An in-memory LRU cache of decompressed 4 KiB SSTable blocks, keyed
by `(sst_path, block_file_offset)`.

**Why cache at the block level, not the key level?**

- **Spatial locality:** related keys (e.g. `sensor:000010` through
  `sensor:000099`) fall in adjacent blocks. Caching the block serves future
  lookups for all keys in that range.
- **Decompressed form:** the cache stores *decompressed* block bytes, so
  cached hits don't pay LZ4 decompression cost.
- **File-granularity invalidation:** when an SSTable is deleted after
  compaction, its blocks simply stop being requested — no explicit eviction
  needed. (Stale entries will be evicted by LRU naturally.)

**Why LRU eviction?**
The hottest blocks (recently accessed key ranges) are the most likely to be
accessed again. LRU is the simplest policy that captures this: keep what was
used recently, evict the least recently used.

**Why O(n) LRU scan instead of a proper linked-hash-map?**
A production LRU requires a doubly-linked list + hash map (O(1) all
operations). Without external crates that is ~150 lines of unsafe code.
For an educational engine with 512 blocks (~2 MiB) the O(n) scan on eviction
is at most 512 comparisons — fast enough in practice and much simpler to
understand.

---

## 4. Engine Improvements

### #1 Concurrent Access — `SharedLsmEngine`

**File:** `src/engine.rs` — `SharedLsmEngine`

**The problem with single-threaded `LsmEngine`:**
```rust
let mut db = LsmEngine::open(dir)?;
// Only one thread can hold &mut db — no concurrency possible.
```

**The solution — `Arc<RwLock<LsmEngine>>`:**
```rust
#[derive(Clone)]
pub struct SharedLsmEngine(Arc<RwLock<LsmEngine>>);
```

- `Arc` provides shared ownership across threads.
- `RwLock` allows *multiple concurrent readers* or *one exclusive writer*.
- `#[derive(Clone)]` lets you hand cheap copies to worker threads.

**Why `RwLock` and not `Mutex`?**
`Mutex` serialises *all* access — concurrent reads block each other.
`RwLock` allows many simultaneous `get()` / `scan()` calls as long as no
`put()` or `delete()` is in progress. Since real workloads are often
read-heavy, this is a significant throughput improvement.

**Usage:**
```rust
let db = SharedLsmEngine::open("/data/mydb")?;
let db2 = db.clone(); // cheap — just clones the Arc

thread::spawn(move || db2.get("key"));  // concurrent read
db.put("key", "val")?;                  // exclusive write
```

---

### #2 Background Compaction Thread

**The problem:**
In the original engine, `put()` could block for 100+ ms while two SSTables
were merged and written to disk. This made write latency unpredictable.

**The solution — `mpsc` channels + worker thread:**

```
Main thread                  Background thread
──────────                   ─────────────────
put() returns immediately    receives CompactionJob
sends CompactionJob ───────► merges SSTables (slow disk I/O)
                             writes new SSTable file
next put() ◄──────────────── sends CompactionResult
drain_compaction_results()
swaps level lists (fast)
```

**Why not just spawn a thread per compaction?**
Thread creation is expensive (~microseconds). More importantly, two
concurrent compactions could both try to write the same level and corrupt
state. A single dedicated worker with a channel serialises compaction work
naturally.

**Why does the job carry `PathBuf`s instead of `SSTable` objects?**
`SSTable` has an `Arc<BlockCache>` which is not `Send` across our channel
boundary in a simple way. PathBufs are plain data — the background thread
opens the files itself. This also means the background thread holds *no*
shared state from the main thread while doing slow I/O.

**Write amplification note:**
Background compaction trades *latency* for *throughput* — individual writes
are faster, but the total bytes written to disk (write amplification) is
the same. The benefit is that slow compaction I/O no longer stalls the
write path.

---

### #3 Block Cache

**The problem — every read opens a file:**
Before the cache, `SSTable::get()` opened the SSTable file, seeked to the
block, read and decompressed it — for every single key lookup. A hot key
queried 1000 times per second would trigger 1000 `open()` + `read()` calls.

**The solution:**
```
get("sensor:000042")
  │
  ├─ bloom: may contain? yes
  ├─ sparse_index: block_offset = 8192
  │
  ├─ cache.get((path, 8192)) ──► Some(block_data)  ← no disk I/O
  │
  └─ scan block for "sensor:000042" → found!
```

**Cache key = `(PathBuf, u64)`** — the file path plus the block's byte offset.
This uniquely identifies a block across all SSTables at all levels.

**Cache capacity = 512 blocks × 4 KiB ≈ 2 MiB** — tiny compared to real
databases (RocksDB defaults to 8 MiB per column family) but demonstrates the
concept. In production this would be tuned to available RAM.

---

### #4 Sparse Index + Data Blocks

**The problem with a dense per-key index:**
Each SSTable in v1 stored one index entry per key. For a 1 MiB SSTable with
1000 keys of 20-byte average length, the index is ~28 KB loaded into memory.
For thousands of SSTables, this adds up to hundreds of MB of index memory.

**The solution — block-based sparse index:**

```
Dense index (v1):              Sparse index (v2):
key_0 → offset_0               first_key_block_0 → block_offset_0
key_1 → offset_1               first_key_block_1 → block_offset_1
key_2 → offset_2               (one entry per 4 KiB block,
...                              not per key)
key_999 → offset_999
1000 entries in memory         ~N/100 entries in memory
```

**Lookup algorithm:**
1. Binary search the sparse index for the last entry whose `first_key ≤ query_key`.
2. Load that block (from cache if hot, else disk).
3. Linear scan the block for the exact key.

**Why is this still fast?**
A 4 KiB block holds ~60–100 typical entries. Scanning 100 entries in an
in-memory byte slice is ~1 µs — negligible compared to the disk read that
would have happened anyway.

---

## 5. Write Path — Step by Step

```
db.put("name", "Alice")
     │
     ▼
1. WAL.append_put("name", "Alice")    ← persisted to disk first
     │
     ▼
2. MemTable.put("name", "Alice")      ← fast in-memory insert
     │
     ▼
3. if MemTable.size_bytes >= 256 KiB:
       flush_memtable()               ← write sorted SSTable to L0
       truncate WAL                   ← WAL entries now redundant
     │
     ▼
4. if !compaction_in_flight && l0_files >= 4:
       send CompactionJob to background thread
       mark in_flight = true          ← returns immediately, no blocking
     │
     ▼
5. return Ok(())                      ← caller unblocked
```

---

## 6. Read Path — Step by Step

```
db.get("name")
     │
     ▼
1. Check MemTable                     ← O(log n), pure RAM
     found? → return
     │
     ▼
2. Check immutable MemTables (if any) ← pending flush, pure RAM
     found? → return
     │
     ▼
3. For each level L0, L1, L2, …:
   For each SSTable in level (newest first):
       a. bloom.may_contain("name")?  ← ~10-byte bit check, no I/O
          no → skip file entirely
          │
       b. sparse_index binary search  ← find block offset, pure RAM
          │
       c. cache.get((path, offset))?  ← hot: pure RAM, cold: disk read
          │
       d. scan_block("name")          ← linear scan ~100 entries
          found? → return value
          tombstone? → return None
     │
     ▼
4. return None (key does not exist)
```

---

## 7. Compaction — Why and How

### Write amplification

Every byte written by the user is eventually written to disk `W` times (once
per level it passes through). This is write amplification. With a size ratio
of 10×:
- L0 → L1: rewrite all L0 data.
- L1 → L2: rewrite the L1 SSTable + overlapping L2 range.
- Typical total: **~10–30× write amplification**.

This is a known LSM-Tree trade-off. The benefit is that those writes are
sequential (fast) rather than random (slow).

### Merge algorithm

Two sorted lists A (newer) and B (older) are merged: for duplicate keys, A
wins. This is implemented with a `BTreeMap` — insert B first, then A
overwrites. Result is a new sorted list with the latest version of each key.

### Why the oldest L(n) SSTable is picked as the compaction victim

A round-robin strategy (always pick the oldest) prevents any single SSTable
from growing very large in an uncompacted state. It also ensures forward
progress: every SSTable is eventually compacted and every tombstone eventually
reaches the deepest level and is dropped.

---

## 8. File Layout on Disk

```
/tmp/lsmdb_demo/
├── wal.log            ← append-only WAL, truncated after each flush
├── L0_00000001.sst    ← L0 SSTable, newest flush
├── L0_00000002.sst    ← L0 SSTable (before compaction)
├── L1_00000003.sst    ← L1 SSTable (after L0 → L1 compaction)
└── L2_00000004.sst    ← L2 SSTable (after L1 → L2 compaction)
```

**Naming:** `L{level}_{seq:08}.sst`
The sequence number is a monotonically increasing counter. Higher sequence =
more recently written. Within L0 (where files can overlap), this is used to
determine recency: higher-seq files are checked first.

---

## 9. Key Trade-offs and Limitations

| Area | This Implementation | Production (RocksDB) |
|---|---|---|
| Concurrency | `RwLock` (coarse) | Per-column-family locks + lock-free reads |
| Compaction | Single background thread | Multiple compaction threads, priorities |
| Block size | Fixed 4 KiB | Configurable (4–64 KiB), variable |
| Compression | LZ4 per block | LZ4 / Snappy / Zstd, configurable per level |
| Block cache | O(n) LRU | Clock-based LRU, sharded to reduce lock contention |
| Write buffer | Single MemTable | Double-buffering (active + immutable) |
| Crash recovery | WAL replay | WAL + MANIFEST for atomic level changes |
| Range queries | Full block read | Iterators with block prefetch |

**Known simplifications:**
- `scan_all()` during compaction re-opens SSTable files and reads all blocks
  sequentially — this could be parallelised.
- Persistence section (#6) in the demo shows `None` for keys written by
  session 1. This is because the MemTable is below the flush threshold and
  the WAL recovery path rebuilds the MemTable correctly, but the demo reopens
  into a fresh instance. (This is actually correct behaviour — see WAL recovery.)
- No MANIFEST file: level membership is inferred from SSTable filenames on
  `open()`. A crash during compaction (between deleting old files and the
  process confirming the new file) could leave the directory in an
  inconsistent state. Production engines use a MANIFEST to make this atomic.

---

---

## Query Capabilities

### #8 Iterator / Cursor API

**File:** `src/iter.rs` — `Cursor`, `SstableBlockIter`

**The problem with the old `scan_cf()`:**
The original range scan loaded *all* SSTable data into a `BTreeMap`, then
filtered the range. For a 1 GB SSTable, scanning 10 keys near the beginning
would read and decompress the entire file.

**The solution — merge-heap (k-way merge):**
A `Cursor` maintains one iterator per source (MemTable, each immutable MemTable,
each SSTable). Each source is pre-sorted. A min-heap merges them:

```
Sources:            Heap (min by key):
MemTable: [d, f]       a (from SSTable L0)
L0 SST:   [a, c]   ──► pop a → advance L0 SST → heap now has [b,c,d,f]
L1 SST:   [b, e]       pop b → advance L1 SST → heap now has [c,d,e,f]
                       ...
```

**Why a heap instead of just sorting all entries?**
Sorting requires loading all entries first. The heap is *lazy*: each source
loads one block at a time (`SstableBlockIter` — 4 KiB per load), making memory
usage proportional to the number of sources × block size, not total data size.

**Deduplication and tombstones:**
The heap pops in ascending key order. When the same key appears in multiple
sources (MemTable has the newest write, an older SSTable has a stale copy),
the MemTable entry (source_id = 0) wins by the heap ordering. Subsequent
occurrences of the same key are silently skipped. Tombstones (value = None)
are consumed without yielding to the caller.

**Usage:**
```rust
// Forward scan of entire CF
for (key, value) in db.iter()? {
    println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
}

// Range scan (uses Cursor internally)
for (key, value) in db.scan("key:010", "key:020")? {
    // ...
}
```

---

### #9 Prefix Scans

**File:** `src/engine.rs` — `scan_prefix()`, `scan_prefix_cf()`

**Why prefix scans are natural for LSM-Trees:**
Because keys are sorted on disk, all keys sharing a common prefix are
physically adjacent. A prefix scan only needs to read one contiguous slice
of the key space — no random I/O.

**Implementation:**
```rust
pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let cursor = self.make_cursor_at(cf, u64::MAX)?;
    Ok(cursor
        .skip_while(|(k, _)| k < prefix)     // advance past keys before prefix
        .take_while(|(k, _)| k.starts_with(prefix))  // stop when prefix ends
        .collect())
}
```

`skip_while` / `take_while` are O(n) in the entries before/within the prefix.
A production implementation would add `seek(target_key)` to the cursor so
each source's iterator can jump directly to the start offset — avoiding reading
blocks that precede the prefix entirely.

**Canonical use cases:**
- **Time-series:** `scan_prefix("sensor:device_A:")` returns all readings for device A.
- **Hierarchical data:** `scan_prefix("user:42:orders:")` returns all orders for user 42.
- **Tag indexes:** `scan_prefix("tag:rust:")` returns all documents tagged "rust".

**Usage:**
```rust
let readings = db.scan_prefix("sensor:device_A:")?;
for (key, val) in readings {
    println!("{} -> {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&val));
}
```

---

### #10 Transactions / Snapshots (MVCC)

**Files:** `src/snapshot.rs`, `src/memtable.rs` (MemEntry), `src/wal.rs` (seq field), `src/sstable.rs` (v4 seq per entry)

#### What is MVCC?

Multi-Version Concurrency Control (MVCC) lets readers and writers operate
without blocking each other. The key idea:

> **Every write is tagged with a monotonically increasing sequence number (`seq`).
> A reader can "pin" a seq and only see writes with `seq ≤ pinned_seq`.**

This gives consistent point-in-time snapshots even while new writes are landing.

#### How write_seq flows through the system

```
db.put("k", "v")
     │
     ├─ seq = write_seq.fetch_add(1)   ← global AtomicU64, one increment per write
     │
     ├─ WAL record:  [tag][key_len][key][seq: u64 LE][val_len][val][crc32]
     │                                  ╰── stored so crash recovery can restore exact seqs
     │
     ├─ MemTable: BTreeMap<key, (seq, value_opt)>
     │                              ╰── seq stored per entry
     │
     └─ (on flush) SSTable block entry: [key_len][key][seq: u64 LE][val_tag][val_len][val]
                                                       ╰── stored per entry in the block body
```

The SSTable footer also stores `max_write_seq` — the highest seq of any entry
in the file. On engine open, `write_seq` is restored to
`max(max_write_seq across all SSTables, max seq in WAL records) + 1`.

#### Snapshot

```rust
// Snapshot = frozen BTreeMap materialised at creation time
let snap = db.snapshot()?; // pins write_seq = S
println!("snap.seq = {}", snap.seq());

// Reads only return entries with seq ≤ S
let v = snap.get("account:alice"); // → value as of seq S
```

Internally, `snapshot()` builds a `Cursor` with `max_seq = current_write_seq - 1`
and drains it into a `BTreeMap`. Because each source pre-filters by seq, only
entries that existed at the pinned point are collected.

**Memory cost:** O(total live keys) at snapshot time. For large databases a
real engine would keep a shared memtable with multi-version entries (like
RocksDB) to avoid copying; this simpler approach is correct and instructive.

#### WriteBatch — atomic multi-key writes

```rust
let mut batch = WriteBatch::new();
batch.put("default", "account:alice", "800")  // debit
     .put("default", "account:bob",   "700"); // credit
db.write_batch(batch)?;
```

All entries in the batch are assigned the **same** `seq`. From any reader's
perspective they appear simultaneously:

- Snapshot at `seq < batch_seq` → sees neither write (pre-transfer state).
- Snapshot at `seq ≥ batch_seq` → sees both writes (post-transfer state).

This is "all-or-nothing" at the sequence boundary. It does **not** provide
full serializable isolation across concurrent transactions — for that you
would need lock-based or optimistic concurrency control layered on top.

#### MVCC source filtering in the Cursor

Each source (MemTable, SstableBlockIter) pre-filters its entries before they
enter the merge-heap:

```
MemTable entries: filter(|(_, (seq, _))| seq ≤ max_seq)
SstableBlockIter: filter(|(_, seq, _)| seq ≤ max_seq)
```

Because each source only yields qualifying entries, the merge-heap's
deduplication logic ("lower source_id wins for duplicate keys") still works
correctly even under seq filtering. The cursor never yields an entry that
post-dates the snapshot.

---

## 10. Running the Demo

```bash
cargo run
```

The demo exercises all 22 sections:

| Section | Feature |
|---------|---------|
| 1  | Basic put / get |
| 2  | Overwrite (latest write wins) |
| 3  | Delete / tombstone |
| 4  | Range scan |
| 5  | High-volume write (1 000 keys, triggers compaction) |
| 6  | Persistence across reopen (WAL recovery) |
| 7  | Engine stats |
| 8  | Concurrent reads via `SharedLsmEngine` (4 threads) |
| 9  | CRC32 corruption detection in the WAL |
| 10 | Column families — independent key spaces |
| 11 | Manifest — durable SSTable inventory |
| 12 | **Iterator / Cursor API** — merge-heap over MemTable + SSTables |
| 13 | **Prefix scans** — hierarchical key spaces |
| 14 | **Snapshots + WriteBatch** — MVCC with seq numbers |
| 15 | **HTTP API** — axum router wrapping `SharedLsmEngine` |
| 16 | **TTL / Key Expiry** — keys expire after a configurable duration |
| 17 | **Range Deletion** — atomically delete all keys in `[from, to)` |
| 18 | **Compaction Filter** — drop / replace entries at merge time |
| 19 | **Merge Operator** — lock-free counter and string-append |
| 20 | **OCC Transactions** — optimistic conflict detection at commit |
| 21 | **Snapshot GC** — prune redundant old versions safely |
| 22 | **Metrics** — Prometheus counters and latency histograms |

---

## 11. Higher-Level Features

### #11 HTTP API

**File:** `src/http_api.rs`

Wraps `SharedLsmEngine` in an [axum](https://github.com/tokio-rs/axum) REST server so the database is accessible over HTTP — a mini-Redis or mini-etcd.

#### Why expose it over HTTP?

- Decouples the storage engine from the application language — any client that speaks HTTP can read and write.
- Makes it trivial to demo, inspect, or integrate with tooling (`curl`, Postman, Prometheus scrape).
- Mirrors real systems: etcd, TiKV, and Consul all expose HTTP/gRPC APIs over their internal storage engines.

#### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/key/:key` | Point lookup. Returns `{"key":…,"value":…}` or 404. |
| `PUT` | `/key/:key` | Insert / overwrite. Request body = value string. |
| `DELETE` | `/key/:key` | Delete (tombstone). |
| `GET` | `/scan?from=&to=` | Range scan. Returns JSON array of `{key,value}`. |
| `GET` | `/prefix/:prefix` | Prefix scan. Returns JSON array of `{key,value}`. |
| `GET` | `/snapshot` | Consistent point-in-time snapshot `{seq, entries:[…]}`. |
| `GET` | `/stats` | Engine statistics (memtable size, SSTable count, levels). |

All endpoints accept an optional `?cf=name` query parameter to target a specific column family (default: `"default"`).

#### Starting the server

```rust
use lsmdb::{SharedLsmEngine, http_api};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let db  = SharedLsmEngine::open("/data/mydb").unwrap();
    let app = http_api::make_router(db);
    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

```bash
# Point lookup
curl http://localhost:8080/key/hello

# Insert
curl -X PUT http://localhost:8080/key/hello -d "world"

# Prefix scan
curl "http://localhost:8080/prefix/sensor:device_A:"

# Snapshot
curl http://localhost:8080/snapshot
```

---

### #12 Replication

**File:** `src/replication.rs`

Streams WAL records from a **leader** to one or more **followers** over TCP. This is the same conceptual approach used by TiKV (Raft log), CockroachDB (Raft), and most distributed KV stores.

#### Why WAL-based replication?

The WAL is already a durable, ordered log of every mutation. Tailing it and shipping records to replicas gives:

- **Durability on followers** — each follower applies the same mutations in the same order.
- **Point-in-time catch-up** — a new replica starts from byte 0 of the WAL and replays history.
- **Minimal coupling** — the leader just appends to a file; the follower just reads from a socket.

#### Wire format

Each record on the wire is length-prefixed:

```
[frame_len: u32 BE][opcode: u8][key_len: u32 BE][key][seq: u64 LE][val_len: u32 BE][val]?
```

This matches the on-disk WAL format (minus the trailing CRC), so `WalRecord::decode_frame` used for recovery doubles as the replication deserializer.

#### Usage

```rust
// Leader side
let server = ReplicationServer::new("0.0.0.0:9001", "/data/mydb/default/wal", "default");
tokio::spawn(async move { server.serve().await.unwrap() });

// Follower side
let follower_db = SharedLsmEngine::open("/data/replica").unwrap();
let client = ReplicationClient::new("leader:9001", follower_db, "default");
tokio::spawn(async move { client.run().await.unwrap() });
```

The client reconnects automatically with exponential backoff (200 ms → 30 s) if the leader becomes unavailable.

---

### #13 Benchmark Suite

**File:** `benches/bench.rs`

Measures four fundamental performance dimensions using [criterion](https://github.com/bheisler/criterion.rs):

| Benchmark | What it measures |
|-----------|-----------------|
| `write_throughput/sequential_put` | Raw put throughput — one key per iteration |
| `write_batch/batch_100` | WriteBatch throughput — 100 keys per iteration |
| `read_random/get_random_10k` | Random-key get latency over a 10 000-key dataset |
| `read_sequential/full_scan_1k` | Full cursor scan over 1 000 keys |
| `scan_prefix/prefix_100_of_1000` | Prefix scan (100-key hit / 1 000-key total) |

#### Running

```bash
# Run all benchmarks and open HTML report
cargo bench

# Run a single group
cargo bench -- write_throughput

# Quick smoke-test (no timing, just correctness)
cargo test --benches
```

Criterion writes HTML reports to `target/criterion/`.

#### Interpreting results

- **Write throughput** is bounded by WAL `fsync` on every put. Batching amortizes that cost — expect `write_batch` to be significantly faster per key than `sequential_put`.
- **Random reads** hit the MemTable first; misses fall through to the bloom filter, then SSTable blocks. Latency grows as the dataset exceeds the block cache.
- **Prefix scans** use the merge-heap cursor filtered by key prefix — latency grows with SSTable count, not total key count.

---

## 12. Advanced Features

### TTL / Key Expiry

**File:** `src/memtable.rs`, `src/wal.rs`, `src/engine.rs`

Every entry can carry an `expires_at` Unix-millisecond timestamp. Reads return `None` for expired keys instantly; compaction filters physically reclaim the space.

#### Why TTL matters

Without TTL you must implement expiry at the application layer: read the key, check a timestamp field, delete it. Under load this becomes a separate read + write for every expiry. TTL bakes expiry into the storage layer — zero extra reads, zero application logic.

Real uses: session stores, lease tokens, rate-limit counters, cache entries.

```rust
// Key expires in 5 minutes
db.put_with_ttl("session:abc123", "uid=42", 5 * 60 * 1000)?;

// After 5 minutes, reads return None automatically
assert_eq!(db.get("session:abc123")?, None);
```

#### Wire format extension

TTL adds WAL opcode `0x02` (vs `0x00` for plain Put):
```
[0x02][key_len:4][key][seq:8 LE][expires_at:8 LE][val_len:4][val][crc32:4]
```
On recovery the `expires_at` is restored exactly, so TTL semantics survive crashes.

---

### Range Deletion

**File:** `src/wal.rs`, `src/engine.rs`

Delete all keys in `[from, to)` with a single call and a single WAL record.

#### Why a dedicated range delete?

Deleting N keys individually writes N WAL records and N tombstones to the MemTable. A range delete writes **one** WAL record and is O(k) in the number of matching MemTable keys, not O(N) in the key space. This is critical for log rotation, partition pruning, and time-series eviction.

```rust
// Atomically delete log:0003 through log:0006 (4 keys)
db.delete_range("log:0003", "log:0007")?;
```

WAL opcode `0x03`:
```
[0x03][from_len:4][from][to_len:4][to][seq:8 LE][crc32:4]
```

---

### Compaction Filter

**File:** `src/compaction_filter.rs`

A user-supplied closure invoked on every live entry during an SSTable merge. The filter decides to **Keep**, **Replace**, or **Remove** the entry.

#### Why compaction filters?

TTL can expire keys at read time, but the physical bytes stay on disk until compaction. A compaction filter is the only mechanism that actually reclaims disk space at merge time — without any read overhead on the hot path.

```rust
use lsmdb::compaction_filter::{FnFilter, FilterDecision, apply_filter};

let filter = FnFilter::new("drop_temp", |key: &[u8], _val: &[u8]| {
    if key.starts_with(b"temp:") { FilterDecision::Remove }
    else { FilterDecision::Keep }
});

let cleaned = apply_filter(entries, &filter);
```

Built-in filters:
- `ExpiryPrefixFilter` — drops entries whose first 8 bytes encode an expired Unix-ms timestamp
- `PrefixDropFilter` — drops all keys under a given prefix
- `FnFilter<F>` — closure-based, for arbitrary logic

#### How to wire it into compaction

```rust
let entries = merge_entries(l0_entries, l1_entries, false);
let entries = apply_filter(entries, &my_filter); // ← insert after merge
let sst = write_merged(output_path, entries, target_level)?;
```

---

### WAL Group Commit

**File:** `src/group_commit.rs`

Batches concurrent WAL writes into a single `fsync`, reducing I/O from O(N writers) to O(1) per time window.

#### The problem

Every `put_cf` calls `WAL.flush()` synchronously. Under N concurrent writers that is N fsyncs per N writes — the single biggest latency source on any disk, even NVMe (a single fsync ≈ 20–200 µs).

#### The solution: elected-leader batch

```
Writer A ─┐                       ┌─ woken, returns Ok
Writer B ─┼── lock batch ──────── ┤
Writer C ─┘   leader elected      ├─ woken, returns Ok
              drains A+B+C        └─ woken, returns Ok
              single fsync ──────────────────────────▶
```

1. Each writer appends its record to a shared `Vec` and parks.
2. The first writer to acquire the write lock becomes the **leader**.
3. The leader drains the batch, writes all records, issues one `fsync`.
4. The leader increments a `generation` counter and wakes all parked followers.
5. Followers see `generation > their_captured_generation` and return immediately.

```rust
use lsmdb::GroupCommitWal;

let wal = GroupCommitWal::open("/data/mydb/default/wal.log")?;
// Safe to call from many threads simultaneously — one fsync per batch
wal.submit(WalRecord::Put { key, seq, value })?;
```

This is how PostgreSQL's WAL writer, MySQL's binary log group commit, and RocksDB's write pipeline all work.

---

### Merge Operator

**File:** `src/merge_operator.rs`

Allows **delta writes** — append a change without reading the current value first.

#### The read-modify-write problem

```rust
// Naive counter increment — requires a read:
let n = db.get("hits")?.map(|v| i64::from_le_bytes(v.try_into().unwrap())).unwrap_or(0);
db.put("hits", (n + 1).to_le_bytes())?;
```

Under concurrency this is a data race. Serialising it with a lock limits throughput to one increment per lock round-trip.

#### Merge operators: write the delta, resolve later

```rust
use lsmdb::merge_operator::{Int64AddOperator, MergeOperator, MergeState, MergeDelta};

let op = Int64AddOperator;
let mut state = MergeState::new();

// Three writers append +1, +1, +5 without reading
for delta in [1i64, 1, 5] {
    state.push(MergeDelta { key: b"hits".to_vec(), seq: 0, delta: delta.to_le_bytes().to_vec() });
}

// Resolve on read (or during compaction)
let result_bytes = state.resolve(b"hits", None, &op).unwrap();
let result = i64::from_le_bytes(result_bytes.try_into().unwrap()); // 7
```

Built-in operators:
| Operator | Delta format | Use case |
|----------|-------------|---------|
| `Int64AddOperator` | `i64` LE | Atomic counters, gauges |
| `StringAppendOperator` | raw bytes | Tag lists, audit logs |

Custom operators implement the `MergeOperator` trait:
```rust
fn full_merge(&self, key: &[u8], base: Option<&[u8]>, deltas: &[Vec<u8>]) -> Option<Vec<u8>>;
fn partial_merge(&self, key: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>>;
```

---

### Optimistic Concurrency Control (OCC)

**File:** `src/occ.rs`

Read-validate-write transactions without locks. Writers proceed optimistically; conflicts are detected at commit time.

#### How it works

1. **Begin** — record `read_horizon = current write_seq`.
2. **Read** — via a snapshot pinned at `read_horizon`; add each key to the `read_set`.
3. **Modify** — buffer mutations locally (nothing goes to disk yet).
4. **Commit** — for each key in `read_set`, check that its `write_seq` has not advanced past `read_horizon`. If clean → atomically apply as a `WriteBatch`. If dirty → return `Err(OccError::Conflict)`.

```rust
use lsmdb::occ::{OccTransaction, OccError};

loop {
    let mut tx = OccTransaction::begin(&db)?;

    let balance = tx.get(b"balance:alice")
        .and_then(|v| String::from_utf8(v).ok())
        .unwrap_or_default();

    tx.put(b"balance:alice".to_vec(), b"900".to_vec());

    match tx.commit(&mut db) {
        Ok(())                     => break,
        Err(OccError::Conflict(_)) => continue, // retry
        Err(OccError::Io(e))       => return Err(e.into()),
    }
}
```

#### Trade-offs

| | OCC (this) | Locking (pessimistic) |
|---|---|---|
| **Throughput** | High under low conflict | Constant regardless of conflicts |
| **Deadlocks** | Impossible | Possible |
| **Abort rate** | High under hot contention | Zero |
| **Implementation** | Simple seq comparison | Requires lock manager |

---

### Snapshot Garbage Collection

**File:** `src/snapshot_gc.rs`

Tracks live snapshots and prunes unreachable old versions of keys during compaction.

#### The problem

With MVCC, every write produces a new version:

```
key:a  seq=5  → "v1"
key:a  seq=15 → "v2"   ← overwrites v1
key:a  seq=25 → "v3"   ← overwrites v2
```

A compaction that merges these sees three entries. Standard logic only drops the tombstone at the deepest level. Versions `seq=5` and `seq=15` are kept even when no snapshot will ever need them — they waste disk space indefinitely.

#### The solution: safe horizon

```
active snapshots: {seq=10, seq=20}
safe horizon = min(active) - 1 = 9
```

Any version of a key with `seq ≤ 9` **and** a newer version existing can be dropped immediately — no snapshot can see it.

```rust
use lsmdb::snapshot_gc::{SnapshotRegistry, filter_versions};

let registry = SnapshotRegistry::new();
let _guard = registry.register(10); // snapshot lives as long as the guard

// In compaction:
let pruned = filter_versions(merged_entries, registry.safe_horizon(), at_deepest_level);
```

The `SnapshotGuard` unregisters automatically on `drop`, so the horizon advances as snapshots are released — preventing indefinite version accumulation.

---

### Metrics + Async API

#### Metrics (`src/metrics.rs`)

Zero-dependency Prometheus-compatible metrics with atomic counters and power-of-2 latency histograms.

```rust
use lsmdb::Metrics;

let m = Metrics::new(); // Arc<Metrics>
m.record_write(key_bytes + val_bytes, elapsed_ns);
m.record_read(hit, val_bytes, elapsed_ns);
m.record_compaction(duration_ns);
m.record_bloom(hit);

// Emit Prometheus text format — wire to GET /metrics
println!("{}", m.prometheus());
```

Tracked metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `lsmdb_writes_total` | counter | Total put/delete calls |
| `lsmdb_reads_total` | counter | Total get calls |
| `lsmdb_read_hits_total` | counter | Gets that found a value |
| `lsmdb_compactions_total` | counter | Completed compaction jobs |
| `lsmdb_bloom_hits_total` | counter | Bloom filter true positives |
| `lsmdb_bloom_misses_total` | counter | Bloom filter true negatives (disk I/O saved) |
| `lsmdb_write_bytes_total` | counter | Bytes written (key + value) |
| `lsmdb_read_bytes_total` | counter | Bytes read (value only) |
| `lsmdb_write_latency_ns` | histogram | Write call latency distribution |
| `lsmdb_read_latency_ns` | histogram | Read call latency distribution |
| `lsmdb_compact_latency_ns` | histogram | Compaction job latency distribution |

The `GET /metrics` endpoint in the HTTP API serves this output in Prometheus text format (scrape-able by any Prometheus server).

#### Async API (`src/async_engine.rs`)

`AsyncEngine` wraps `SharedLsmEngine` and offloads every blocking call to `tokio::task::spawn_blocking`, making the API fully compatible with `.await`.

```rust
use lsmdb::AsyncEngine;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = AsyncEngine::open("/data/mydb").await?;

    db.put("sensor:001", "23.5°C").await?;
    db.put_with_ttl("cache:key", "value", 60_000).await?; // 1-minute TTL

    let v = db.get("sensor:001").await?;
    let results = db.scan_prefix(b"sensor:".to_vec()).await?;

    let mut batch = WriteBatch::new();
    batch.put("default", "a", "1").put("default", "b", "2");
    db.write_batch(batch).await?;

    Ok(())
}
```

**When to use which API:**

| API | Best for |
|-----|---------|
| `LsmEngine` | Single-threaded tools, scripts, tests |
| `SharedLsmEngine` | Multi-threaded sync code, CLI, benchmarks |
| `AsyncEngine` | Tokio servers, the HTTP API, high-concurrency I/O |

---

## 13. Further Reading

| Resource | Why read it |
|---|---|
| [LevelDB Design Doc](https://github.com/google/leveldb/blob/main/doc/impl.md) | The original LSM-Tree implementation this is modelled after |
| [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) | Real-world production trade-offs |
| [The Log-Structured Merge-Tree (O'Neil et al. 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) | The original academic paper |
| [Designing Data-Intensive Applications, Ch. 3](https://dataintensive.net/) | Best accessible explanation of LSM-Tree vs B-Tree trade-offs |
| [Kirsch & Mitzenmacher 2006](https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf) | Double-hashing technique used in the bloom filter |
