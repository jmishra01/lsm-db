/// Demonstrates the lock-free metrics system and Prometheus output.
use lsmdb::Metrics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

fn main() {
    let m = Arc::new(Metrics::new());

    // Simulate concurrent writes across threads
    let handles: Vec<_> = (0..4).map(|_| {
        let m = Arc::clone(&m);
        thread::spawn(move || {
            for _ in 0..250 {
                m.record_write(128, 1_000); // 128-byte key, 1µs latency
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }

    m.record_read(true,  64, 500);
    m.record_read(false, 0,  800);
    m.record_compaction(5_000_000);
    m.record_bloom(true);
    m.record_bloom(false);

    println!("writes:      {}", m.write_count.load(Ordering::Relaxed));
    println!("write bytes: {}", m.write_bytes.load(Ordering::Relaxed));
    println!("reads:       {}", m.read_count.load(Ordering::Relaxed));
    println!("read hits:   {}", m.read_hits.load(Ordering::Relaxed));
    println!("compactions: {}", m.compaction_count.load(Ordering::Relaxed));

    // Latency histogram — which bucket did the 1µs (1000ns) writes land in?
    // power-of-2 bucket: 2^9=512 < 1000 ≤ 2^10=1024 → bucket 10
    let bucket_10 = m.write_latency_ns[10].load(Ordering::Relaxed);
    println!("write latency bucket[10] = {bucket_10}"); // 1000

    // Prometheus exposition format
    println!("\n── Prometheus output (first 8 lines) ──");
    for line in m.prometheus().lines().take(8) {
        println!("{line}");
    }
}
