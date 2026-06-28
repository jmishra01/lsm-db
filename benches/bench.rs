// =============================================================
// Criterion benchmark suite (#13)
//
// Run with:   cargo bench
//
// Groups
// ------
//   write_throughput   — sequential puts (1 B values, 8 B keys)
//   write_batch        — WriteBatch of 100 entries each iteration
//   read_random        — random-key get after 10 000 pre-loaded keys
//   read_sequential    — full scan (iter()) across 1 000 keys
//   scan_prefix        — scan_prefix on a 100-key subset
// =============================================================

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lsmdb::{LsmEngine, WriteBatch};
use std::hint::black_box;
use tempfile::TempDir;

// ---- helpers ---------------------------------------------------------------

fn open_fresh() -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

fn key(i: u64) -> Vec<u8> { format!("key:{i:016}").into_bytes() }
fn val(i: u64) -> Vec<u8> { format!("v{i}").into_bytes() }

fn preload(db: &mut LsmEngine, n: u64) {
    for i in 0..n {
        db.put(key(i), val(i)).unwrap();
    }
}

// ---- write_throughput ------------------------------------------------------

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequential_put", |b| {
        let (_dir, mut db) = open_fresh();
        let mut i = 0u64;
        b.iter(|| {
            db.put(black_box(key(i)), black_box(val(i))).unwrap();
            i += 1;
        });
    });

    group.finish();
}

// ---- write_batch -----------------------------------------------------------

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");
    const BATCH: u64 = 100;
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function("batch_100", |b| {
        let (_dir, mut db) = open_fresh();
        let mut base = 0u64;
        b.iter(|| {
            let mut batch = WriteBatch::new();
            for i in base..base + BATCH {
                batch.put("default", key(i), val(i));
            }
            db.write_batch(batch).unwrap();
            base += BATCH;
        });
    });

    group.finish();
}

// ---- read_random -----------------------------------------------------------

fn bench_read_random(c: &mut Criterion) {
    const N: u64 = 10_000;
    let mut group = c.benchmark_group("read_random");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_random_10k", |b| {
        let (_dir, mut db) = open_fresh();
        preload(&mut db, N);

        // Simple LCG for reproducible pseudo-random key order.
        let mut rng = 6364136223846793005u64;
        b.iter(|| {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let i = rng % N;
            let _ = black_box(db.get(key(i)).unwrap());
        });
    });

    group.finish();
}

// ---- read_sequential -------------------------------------------------------

fn bench_read_sequential(c: &mut Criterion) {
    const N: u64 = 1_000;
    let mut group = c.benchmark_group("read_sequential");
    group.throughput(Throughput::Elements(N));

    group.bench_function("full_scan_1k", |b| {
        let (_dir, mut db) = open_fresh();
        preload(&mut db, N);

        b.iter(|| {
            let cursor = db.iter().unwrap();
            let count = cursor.count();
            black_box(count)
        });
    });

    group.finish();
}

// ---- scan_prefix -----------------------------------------------------------

fn bench_scan_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_prefix");

    // Load 1 000 keys: 100 under "hot:" prefix, rest under "cold:".
    group.bench_function("prefix_100_of_1000", |b| {
        let (_dir, mut db) = open_fresh();
        for i in 0u64..100 {
            db.put(format!("hot:{i:08}").into_bytes(), val(i)).unwrap();
        }
        for i in 0u64..900 {
            db.put(format!("cold:{i:08}").into_bytes(), val(i)).unwrap();
        }

        b.iter(|| {
            let results = db.scan_prefix(black_box(b"hot:")).unwrap();
            black_box(results.len())
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_write_batch,
    bench_read_random,
    bench_read_sequential,
    bench_scan_prefix,
);
criterion_main!(benches);
