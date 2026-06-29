// =============================================================
// Per-operation metrics (#new)
//
// Tracks four counters/histograms in a single lock-free struct:
//
//   write_count    — total put + delete + merge calls
//   read_count     — total get calls
//   read_hits      — get calls that found a value
//   compaction_count — number of compaction rounds completed
//   bloom_hits     — bloom filter true-positive (key may exist)
//   bloom_misses   — bloom filter true-negative (key definitely absent)
//   write_bytes    — cumulative bytes written (key + value)
//   read_bytes     — cumulative bytes read (value bytes returned)
//
// Latency histograms (nanoseconds, power-of-2 buckets):
//   write_latency_ns  — distribution of write call latency
//   read_latency_ns   — distribution of read call latency
//   compact_latency_ns — distribution of compaction job latency
//
// Prometheus text format
// ----------------------
// Call `metrics.prometheus()` to get a `String` in the standard
// Prometheus exposition format.  Wire it up with the HTTP API:
//   GET /metrics → prometheus()
//
// Why this approach vs. a crate like `prometheus`?
// ------------------------------------------------
//   Keeping it in-house means zero additional dependencies and
//   makes the instrumentation visible in the source.  A production
//   system would use `prometheus` or `opentelemetry`, but the
//   concepts (counters, histograms, labels) are identical.
// =============================================================

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const BUCKETS: usize = 64;

fn zero_buckets() -> [AtomicU64; BUCKETS] {
    // AtomicU64 is not Copy/Clone, so we can't use array initialiser syntax directly.
    // SAFETY: AtomicU64 has the same layout as u64; zeroed memory is valid for u64.
    unsafe { std::mem::zeroed() }
}

/// All metrics live in one struct behind an Arc so they can be
/// cloned cheaply and shared with the HTTP API.
pub struct Metrics {
    pub write_count:       AtomicU64,
    pub read_count:        AtomicU64,
    pub read_hits:         AtomicU64,
    pub compaction_count:  AtomicU64,
    pub bloom_hits:        AtomicU64,
    pub bloom_misses:      AtomicU64,
    pub write_bytes:       AtomicU64,
    pub read_bytes:        AtomicU64,
    /// 64 latency buckets: bucket[i] counts samples in [2^i ns, 2^(i+1) ns).
    pub write_latency_ns:  [AtomicU64; BUCKETS],
    pub read_latency_ns:   [AtomicU64; BUCKETS],
    pub compact_latency_ns:[AtomicU64; BUCKETS],
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            write_count:       AtomicU64::new(0),
            read_count:        AtomicU64::new(0),
            read_hits:         AtomicU64::new(0),
            compaction_count:  AtomicU64::new(0),
            bloom_hits:        AtomicU64::new(0),
            bloom_misses:      AtomicU64::new(0),
            write_bytes:       AtomicU64::new(0),
            read_bytes:        AtomicU64::new(0),
            write_latency_ns:  zero_buckets(),
            read_latency_ns:   zero_buckets(),
            compact_latency_ns:zero_buckets(),
        }
    }
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_write(&self, bytes: u64, duration_ns: u64) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.write_latency_ns[bucket(duration_ns)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_read(&self, hit: bool, bytes: u64, duration_ns: u64) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.read_hits.fetch_add(1, Ordering::Relaxed);
            self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        self.read_latency_ns[bucket(duration_ns)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction(&self, duration_ns: u64) {
        self.compaction_count.fetch_add(1, Ordering::Relaxed);
        self.compact_latency_ns[bucket(duration_ns)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bloom(&self, hit: bool) {
        if hit { self.bloom_hits.fetch_add(1, Ordering::Relaxed); }
        else   { self.bloom_misses.fetch_add(1, Ordering::Relaxed); }
    }

    /// Emit Prometheus text format.
    pub fn prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        macro_rules! counter {
            ($name:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP {n} {h}\n# TYPE {n} counter\n{n} {v}\n",
                    n = $name, h = $help, v = $val.load(Ordering::Relaxed)
                ));
            };
        }

        counter!("lsmdb_writes_total",      "Total write operations",         self.write_count);
        counter!("lsmdb_reads_total",        "Total read operations",          self.read_count);
        counter!("lsmdb_read_hits_total",    "Read operations that found a key", self.read_hits);
        counter!("lsmdb_compactions_total",  "Total compaction jobs completed", self.compaction_count);
        counter!("lsmdb_bloom_hits_total",   "Bloom filter true positives",    self.bloom_hits);
        counter!("lsmdb_bloom_misses_total", "Bloom filter true negatives",    self.bloom_misses);
        counter!("lsmdb_write_bytes_total",  "Bytes written (key+value)",      self.write_bytes);
        counter!("lsmdb_read_bytes_total",   "Bytes read (value only)",        self.read_bytes);

        // Histograms (manual; each bucket is a separate gauge for simplicity)
        for (prefix, buckets) in [
            ("lsmdb_write_latency_ns", &self.write_latency_ns   as &[AtomicU64]),
            ("lsmdb_read_latency_ns",  &self.read_latency_ns    as &[AtomicU64]),
            ("lsmdb_compact_latency_ns",&self.compact_latency_ns as &[AtomicU64]),
        ] {
            out.push_str(&format!("# HELP {prefix} Latency histogram (ns)\n# TYPE {prefix} histogram\n"));
            let mut cumulative: u64 = 0;
            for (i, b) in buckets.iter().enumerate() {
                let count = b.load(Ordering::Relaxed);
                cumulative += count;
                let le = if i == 63 { "+Inf".to_string() } else { (1u64 << i).to_string() };
                out.push_str(&format!("{prefix}_bucket{{le=\"{le}\"}} {cumulative}\n"));
            }
        }

        out
    }
}

/// Map a nanosecond duration to a power-of-2 bucket index (0..63).
fn bucket(ns: u64) -> usize {
    if ns == 0 { return 0; }
    let b = (63 - ns.leading_zeros()) as usize;
    b.min(63)
}

// ---- Timer helper ----------------------------------------------------------

/// RAII timer that records a write latency on drop.
pub struct WriteTimer<'a> {
    start:   Instant,
    bytes:   u64,
    metrics: &'a Metrics,
}

impl<'a> WriteTimer<'a> {
    pub fn start(metrics: &'a Metrics, bytes: u64) -> Self {
        Self { start: Instant::now(), bytes, metrics }
    }
}

impl Drop for WriteTimer<'_> {
    fn drop(&mut self) {
        self.metrics.record_write(self.bytes, self.start.elapsed().as_nanos() as u64);
    }
}

/// RAII timer that records a read latency on drop.
pub struct ReadTimer<'a> {
    start:   Instant,
    result:  Option<u64>, // bytes returned (None = miss)
    metrics: &'a Metrics,
}

impl<'a> ReadTimer<'a> {
    pub fn start(metrics: &'a Metrics) -> Self {
        Self { start: Instant::now(), result: None, metrics }
    }

    pub fn hit(mut self, bytes: u64) -> u64 {
        self.result = Some(bytes);
        bytes
    }
}

impl Drop for ReadTimer<'_> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        match self.result {
            Some(b) => self.metrics.record_read(true,  b, elapsed),
            None    => self.metrics.record_read(false, 0, elapsed),
        }
    }
}
