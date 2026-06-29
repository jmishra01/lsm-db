// Unit tests — metrics

use lsmdb::Metrics;
use std::sync::atomic::Ordering;

#[test]
fn counters_start_at_zero() {
    let m = Metrics::new();
    assert_eq!(m.write_count.load(Ordering::Relaxed), 0);
    assert_eq!(m.read_count.load(Ordering::Relaxed), 0);
    assert_eq!(m.read_hits.load(Ordering::Relaxed), 0);
    assert_eq!(m.compaction_count.load(Ordering::Relaxed), 0);
    assert_eq!(m.bloom_hits.load(Ordering::Relaxed), 0);
    assert_eq!(m.bloom_misses.load(Ordering::Relaxed), 0);
}

#[test]
fn record_write_increments_count_and_bytes() {
    let m = Metrics::new();
    m.record_write(100, 5_000);
    m.record_write(200, 3_000);
    assert_eq!(m.write_count.load(Ordering::Relaxed), 2);
    assert_eq!(m.write_bytes.load(Ordering::Relaxed), 300);
}

#[test]
fn record_read_hit_increments_hits_and_bytes() {
    let m = Metrics::new();
    m.record_read(true, 64, 1_000);
    assert_eq!(m.read_count.load(Ordering::Relaxed), 1);
    assert_eq!(m.read_hits.load(Ordering::Relaxed), 1);
    assert_eq!(m.read_bytes.load(Ordering::Relaxed), 64);
}

#[test]
fn record_read_miss_does_not_increment_hits_or_bytes() {
    let m = Metrics::new();
    m.record_read(false, 0, 800);
    assert_eq!(m.read_count.load(Ordering::Relaxed), 1);
    assert_eq!(m.read_hits.load(Ordering::Relaxed), 0);
    assert_eq!(m.read_bytes.load(Ordering::Relaxed), 0);
}

#[test]
fn record_compaction_increments_count() {
    let m = Metrics::new();
    m.record_compaction(1_000_000);
    m.record_compaction(2_000_000);
    assert_eq!(m.compaction_count.load(Ordering::Relaxed), 2);
}

#[test]
fn record_bloom_increments_hit_or_miss() {
    let m = Metrics::new();
    m.record_bloom(true);
    m.record_bloom(true);
    m.record_bloom(false);
    assert_eq!(m.bloom_hits.load(Ordering::Relaxed), 2);
    assert_eq!(m.bloom_misses.load(Ordering::Relaxed), 1);
}

#[test]
fn prometheus_output_contains_all_metric_names() {
    let m = Metrics::new();
    m.record_write(10, 500);
    m.record_read(true, 8, 200);
    m.record_compaction(50_000);
    m.record_bloom(true);

    let output = m.prometheus();
    let required = [
        "lsmdb_writes_total",
        "lsmdb_reads_total",
        "lsmdb_read_hits_total",
        "lsmdb_compactions_total",
        "lsmdb_bloom_hits_total",
        "lsmdb_bloom_misses_total",
        "lsmdb_write_bytes_total",
        "lsmdb_read_bytes_total",
        "lsmdb_write_latency_ns",
        "lsmdb_read_latency_ns",
        "lsmdb_compact_latency_ns",
    ];
    for name in required {
        assert!(output.contains(name), "missing metric: {name}");
    }
}

#[test]
fn prometheus_output_contains_help_and_type_lines() {
    let m = Metrics::new();
    let output = m.prometheus();
    let help_lines = output.lines().filter(|l| l.starts_with("# HELP")).count();
    let type_lines = output.lines().filter(|l| l.starts_with("# TYPE")).count();
    assert!(help_lines >= 8, "expected at least 8 HELP lines, got {help_lines}");
    assert!(type_lines >= 8, "expected at least 8 TYPE lines, got {type_lines}");
}

#[test]
fn write_latency_bucket_incremented() {
    let m = Metrics::new();
    m.record_write(0, 1_024); // 2^10 ns → bucket 10
    // At least one bucket must be non-zero
    let total: u64 = m.write_latency_ns.iter()
        .map(|b| b.load(Ordering::Relaxed))
        .sum();
    assert_eq!(total, 1);
}

#[test]
fn metrics_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Metrics>();
}
