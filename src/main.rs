// LSM-DB - demo driver

use lsmdb::{LsmEngine, SharedLsmEngine, WriteBatch};
use std::time::Instant;


fn main() -> std::io::Result<()> {
    let dir = "/tmp/lsmdb_demo";
    let _ = std::fs::remove_dir_all(dir);

    println!("==========================================");
    println!("          LSM-Tree DB - Rust Demo         ");
    println!("==========================================");

    // 1. Basic put / get
    println!("-- 1. Basic put / get");
    {
        let mut db = LsmEngine::open(dir)?;
        db.put("name", "Jitendra")?;
        db.put("city", "Bangalore")?;
        db.put("country", "India")?;
        println!("name -> {:?}", db.get("name")?.map(|v| String::from_utf8(v).unwrap()));
        println!("city -> {:?}", db.get("city")?.map(|v| String::from_utf8(v).unwrap()));
        println!("country -> {:?}", db.get("country")?.map(|v| String::from_utf8(v).unwrap()));
        println!("missing -> {:?}", db.get("missing")?);
        println!("Stats\n{:#?}", db.stats());
    }
    // 2. Overwrite
    println!("\n -- 2. Overwirte");
    {
        let mut db = LsmEngine::open(dir)?;
        db.put("city", "Bangalore")?;
        println!(" before -> {:#?}", db.get("city")?.map(|v| String::from_utf8(v).unwrap()));
        db.put("city", "Mumbai")?;
        println!(" after -> {:#?}", db.get("city")?.map(|v| String::from_utf8(v).unwrap()));
    }

    // 3. Delete / tombstone
    println!("\n-- 3. Delete / tombstone");
    {
        let mut db = LsmEngine::open(dir)?;
        db.put("tmp_key", "will be deleted")?;
        println!(" before delete -> {:?}", db.get("tmp_key")?.map(|v| String::from_utf8(v).unwrap()));
        db.delete("tmp_key")?;
        println!(" after delete -> {:?}", db.get("tmp_key"));
    }

    // 4. Range scan
    println!("\n-- 4. Range Scan");
    {
        let mut db = LsmEngine::open(dir)?;
        for i in 0..10u32 {
            db.put(format!("key:{:03}", i), format!("value:{}", i))?;
        }
        let results = db.scan("key:002", "key:007")?;
        println!(" scan key:002..key:007 ({} results)", results.len());

        for (k, v) in results {
            println!(" {} -> {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
        }
    }

    // 5. High-volume write
    println!("\n-- 5. High-volume write (1000 keys)");
    {
        let _ = std::fs::remove_dir_all(dir);
        let mut db = LsmEngine::open(dir)?;
        let t = Instant::now();
        for i in 0..1_000u32 {
            db.put(
                format!("sensor:{:06}", i),
                format!("{{\"ts\": {},\"val\":{:.2}}}", 1_700_000_000u64 + i as u64, i as f64 * 0.1),
            )?;
        }
        println!(" Wrote 1000 keys in {:?}", t.elapsed());
        for i in 0..500u32 {
            db.put(
                format!("sensor:{:06}", i),
                format!("{{\"ts\":{},\"val\":{:.2}}}", 1_700_001_000u64 + i as u64, i as f64 * 0.2),
            )?;
        }

        println!(" Overwrote first 500 keys");
        let s = db.stats();
        println!(" memtable={} bytes files/level={:?}", s.mem_table_size_bytes, s.level_file_counts);
        let v0 = db.get("sensor:000000")?.map(|v| String::from_utf8(v).unwrap()).unwrap_or_default();
        let v999 = db.get("sensor:000999")?.map(|v| String::from_utf8(v).unwrap()).unwrap_or_default();
        println!(" sensor:000000 -> {}", v0);
        println!(" sensor:000999 -> {}", v999);
    }

    // 6. Persistence
    println!("\n -- 6. Persistence across reopen");
    {
        let _ = std::fs::remove_dir_all(dir);
        {
            let mut db = LsmEngine::open(dir)?;
            db.put("persistent_key", "I survive restarts")?;
            db.put("another", "also here")?;
            println!(" [session 1] wrote 2 keys, closing...");
        }
        {
            let db = LsmEngine::open(dir)?;
            println!(" [session 2] persistent_key -> {:?}", db.get("persistent_key")?.map(|v| String::from_utf8(v).unwrap()));
            println!(" [session 2] another -> {:?}", db.get("another")?.map(|v| String::from_utf8(v).unwrap()));
        }
    }

    // 7. Stats
    println!("\n-- 7. Engine stats");
    {
        let db = LsmEngine::open(dir)?;
        let s = db.stats();
        println!(" memtable     : {} bytes", s.mem_table_size_bytes);
        println!(" immutable    : {}", s.immutable_count);
        println!(" total sst    : {} file(s)", s.total_ss_table_files);
        for (i, cnt) in s.level_file_counts.iter().enumerate() {
            if *cnt > 0 {
                println!("  L{}     : {} file(s)", i, cnt);
            }
        }
    }

    // 8. Concurrent reads with SharedLsmEngine
    println!("\n-- 8. Concurrent reads (SharedLsmEngine + Arc<RwLock<>>)");
    {
        use std::thread;

        let _ = std::fs::remove_dir_all(dir);
        let db = SharedLsmEngine::open(dir)?;

        for i in 0..20u32 {
            db.put(format!("ckey:{:03}", i), format!("cval:{}", i))?;
        }

        let handles: Vec<_> = (0..4)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    let mut found = 0usize;
                    for i in 0..20u32 {
                        if db.get(format!("ckey:{:03}", i)).unwrap().is_some() {
                            found += 1;
                        }
                    }
                    println!(" thread {} found {}/20 keys", t, found);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    // 9. CRC32 checksums — corruption detection
    println!("\n-- 9. CRC32 checksums");
    {
        let _ = std::fs::remove_dir_all(dir);
        {
            let mut db = LsmEngine::open(dir)?;
            db.put("alpha", "value_alpha")?;
            db.put("beta",  "value_beta")?;
            db.put("gamma", "value_gamma")?;
            println!(" wrote 3 records to WAL");
        }

        // Corrupt the middle of the WAL file by flipping bytes in the second record.
        // The WAL replays until the CRC mismatch, then stops — recovering only
        // the records that were written before the corruption.
        let wal_path = format!("{}/default/wal.log", dir);
        let mut wal_bytes = std::fs::read(&wal_path)?;
        // Flip 4 bytes starting at offset 20 (well inside the second record)
        if wal_bytes.len() > 24 {
            wal_bytes[20] ^= 0xFF;
            wal_bytes[21] ^= 0xFF;
            wal_bytes[22] ^= 0xFF;
            wal_bytes[23] ^= 0xFF;
        }
        std::fs::write(&wal_path, &wal_bytes)?;
        println!(" WAL corrupted (bytes 20-23 flipped)");

        // Re-open: recovery should print a CRC mismatch warning and stop
        let db = LsmEngine::open(dir)?;
        // "alpha" was the first record — may survive depending on where corruption landed
        println!(" alpha -> {:?}", db.get("alpha")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" beta  -> {:?}", db.get("beta")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" gamma -> {:?}", db.get("gamma")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" (any None above = that record was past the corrupt point — safely discarded)");
    }

    // 10. Column families — independent key spaces
    println!("\n-- 10. Column families");
    {
        let _ = std::fs::remove_dir_all(dir);
        let mut db = LsmEngine::open_with_cfs(dir, &["default", "meta", "events"])?;

        println!(" open CFs: {:?}", db.list_cfs());

        // Each CF is a completely isolated key space
        db.put_cf("meta",    "schema_version", "3")?;
        db.put_cf("meta",    "db_created_at",  "2025-01-01")?;
        db.put_cf("events",  "evt:0001", r#"{"type":"login","user":"alice"}"#)?;
        db.put_cf("events",  "evt:0002", r#"{"type":"logout","user":"alice"}"#)?;
        db.put("app_config", "debug_mode=false")?;   // default CF

        // Reads are scoped to their CF — no cross-CF bleed
        println!(" meta.schema_version -> {:?}",
            db.get_cf("meta", "schema_version")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" events.evt:0001 -> {:?}",
            db.get_cf("events", "evt:0001")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" default.app_config -> {:?}",
            db.get("app_config")?.map(|v| String::from_utf8(v).unwrap()));

        // A key written to one CF is invisible in another
        println!(" events.schema_version (cross-CF read) -> {:?}",
            db.get_cf("events", "schema_version")?);

        // Scan within a CF
        let evts = db.scan_cf("events", "evt:0000", "evt:9999")?;
        println!(" events scan: {} result(s)", evts.len());
        for (k, v) in &evts {
            println!("   {} -> {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
        }

        // Create a new CF at runtime
        db.create_cf("audit")?;
        db.put_cf("audit", "log:001", "admin changed schema_version")?;
        println!(" audit.log:001 -> {:?}",
            db.get_cf("audit", "log:001")?.map(|v| String::from_utf8(v).unwrap()));
        println!(" all CFs after create_cf: {:?}", db.list_cfs());
    }

    // 11. Manifest — durable SSTable inventory
    println!("\n-- 11. Manifest");
    {
        let _ = std::fs::remove_dir_all(dir);

        // Session 1: write enough data to trigger a MemTable flush → SSTable file created
        {
            // 256 KiB threshold; each entry is ~70 bytes so ~3800 entries triggers flush
            let mut db = LsmEngine::open(dir)?;
            for i in 0..4000u32 {
                db.put(
                    format!("mkey:{:06}", i),
                    format!("{{\"index\":{},\"payload\":\"{}\"}}", i, "x".repeat(40)),
                )?;
            }
            println!(" [session 1] wrote 4000 keys (triggers flush to SSTable)");
            let s = db.stats();
            println!(" [session 1] L0 files: {}", s.level_file_counts[0]);
        }

        // Show manifest file exists and has records
        let manifest_path = format!("{}/MANIFEST", dir);
        let manifest_size = std::fs::metadata(&manifest_path)?.len();
        println!(" MANIFEST file size: {} bytes", manifest_size);

        // The manifest lists which SST files exist at which levels.
        // Read it back to show the records.
        let mstate = lsmdb::manifest::Manifest::recover(&manifest_path)?;
        println!(" Manifest knows {} CF(s): {:?}", mstate.cfs.len(), {
            let mut v: Vec<_> = mstate.cfs.iter().collect();
            v.sort();
            v
        });
        for (cf, files) in &mstate.files {
            println!(" CF '{}': {} SSTable file(s) recorded", cf, files.len());
            for (level, filename) in files {
                println!("   L{} — {}", level, filename);
            }
        }

        // Session 2: reopen — the engine loads SSTables from the manifest, not
        // a directory scan. If the manifest were missing (old behaviour), the
        // engine would have to re-scan the directory and could not distinguish
        // current files from compaction leftovers.
        {
            let db = LsmEngine::open(dir)?;
            let first = db.get("mkey:000000")?.map(|v| String::from_utf8(v).unwrap());
            let last  = db.get("mkey:003999")?.map(|v| String::from_utf8(v).unwrap());
            println!(" [session 2] mkey:000000 -> {:?}", first);
            println!(" [session 2] mkey:003999 -> {:?}", last);
            println!(" [session 2] data survived reopen via manifest ✓");
        }
    }

    // 12. Iterator / Cursor API — merge-heap over all data sources
    println!("\n-- 12. Iterator / Cursor API (merge-heap)");
    {
        let _ = std::fs::remove_dir_all(dir);
        let mut db = LsmEngine::open(dir)?;

        // Write enough to span both MemTable and SSTable
        for i in 0..5u32 {
            db.put(format!("item:{:03}", i), format!("first_{}", i))?;
        }
        // Overwrite some keys — cursor must return the latest version
        for i in 2..4u32 {
            db.put(format!("item:{:03}", i), format!("updated_{}", i))?;
        }
        db.delete("item:001")?; // tombstone — must not appear in cursor output

        println!(" Written 5 keys, updated 2, deleted 1.");
        println!(" Cursor output (sorted, deduped, tombstones suppressed):");

        let cursor = db.iter()?;
        for (k, v) in cursor {
            println!("   {} -> {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
        }
        // item:001 must be absent (tombstone), item:002/003 must show "updated_*"

        // Range iteration using the cursor
        println!(" Range item:002..item:004:");
        for (k, v) in db.scan("item:002", "item:004")? {
            println!("   {} -> {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
        }
    }

    // 13. Prefix scans — hierarchical key spaces
    println!("\n-- 13. Prefix scans");
    {
        let _ = std::fs::remove_dir_all(dir);
        let mut db = LsmEngine::open(dir)?;

        // Time-series sensor data with hierarchical keys
        let sensors = ["device_A", "device_B", "device_C"];
        for (ts, &sensor) in sensors.iter().enumerate() {
            for reading in 0..3u32 {
                db.put(
                    format!("sensor:{}:{:04}", sensor, reading),
                    format!("{{\"ts\":{},\"val\":{:.1}}}", ts * 1000 + reading as usize, reading as f32 * 0.5),
                )?;
            }
        }
        db.put("config:threshold", "0.8")?;
        db.put("config:interval_ms", "500")?;

        // Prefix scan returns only matching keys, sorted
        let device_a = db.scan_prefix("sensor:device_A:")?;
        println!(" sensor:device_A:* ({} results):", device_a.len());
        for (k, v) in &device_a {
            println!("   {} -> {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
        }

        let all_sensors = db.scan_prefix("sensor:")?;
        println!(" sensor:* ({} results total)", all_sensors.len());

        let config = db.scan_prefix("config:")?;
        println!(" config:* ({} results):", config.len());
        for (k, v) in &config {
            println!("   {} -> {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
        }
    }

    // 14. Snapshots + WriteBatch — MVCC (#10)
    println!("\n-- 14. Snapshots and WriteBatch (MVCC)");
    {
        let _ = std::fs::remove_dir_all(dir);
        let mut db = LsmEngine::open(dir)?;

        // Initial state
        db.put("account:alice", "1000")?;
        db.put("account:bob",   "500")?;
        println!(" Initial: alice=1000, bob=500");

        // Take a snapshot BEFORE the transfer
        let snap_before = db.snapshot()?;
        println!(" Snapshot taken at seq={}", snap_before.seq());

        // Simulate a transfer: alice → bob (atomic via WriteBatch)
        // Both updates share the same write_seq so they're invisible to
        // any snapshot pinned before this seq.
        let mut batch = WriteBatch::new();
        batch.put("default", "account:alice", "800")   // alice -200
             .put("default", "account:bob",   "700");  // bob   +200
        db.write_batch(batch)?;
        println!(" Transfer complete: alice=800, bob=700");

        // Live reads see the transfer
        let alice_live = db.get("account:alice")?.map(|v| String::from_utf8(v).unwrap());
        let bob_live   = db.get("account:bob")?.map(|v| String::from_utf8(v).unwrap());
        println!(" Live   — alice: {:?}, bob: {:?}", alice_live, bob_live);

        // Snapshot BEFORE the transfer sees the original values
        let alice_snap = snap_before.get("account:alice").map(|v| String::from_utf8_lossy(v).to_string());
        let bob_snap   = snap_before.get("account:bob").map(|v| String::from_utf8_lossy(v).to_string());
        println!(" Snap@{} — alice: {:?}, bob: {:?}", snap_before.seq(), alice_snap, bob_snap);

        // Take a snapshot AFTER the transfer
        let snap_after = db.snapshot()?;
        let alice_after = snap_after.get("account:alice").map(|v| String::from_utf8_lossy(v).to_string());
        let bob_after   = snap_after.get("account:bob").map(|v| String::from_utf8_lossy(v).to_string());
        println!(" Snap@{} — alice: {:?}, bob: {:?}", snap_after.seq(), alice_after, bob_after);

        println!(" ✓ Snapshot isolation: pre-transfer snapshot is unaffected by the WriteBatch.");

        // Snapshot iteration and prefix scan
        let snap = db.snapshot()?;
        println!(" Snapshot key count: {}", snap.key_count());
        for (k, v) in snap.iter() {
            println!("   {} -> {}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
        }
    }

    // ── Section 15: HTTP API (#11) ──────────────────────────────────────────
    {
        println!("\n=== Section 15: HTTP API ===");

        let dir = std::env::temp_dir().join(format!("lsmdb-http-demo-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let shared = SharedLsmEngine::open(&dir)?;
        shared.put("http:hello", "world")?;
        shared.put("http:foo",   "bar")?;

        // Build the router — in production you'd do:
        //   let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
        //   axum::serve(listener, app).await?;
        let _app = lsmdb::http_api::make_router(shared.clone());
        println!(" Router created with routes:");
        println!("   GET    /key/:key          -> point lookup");
        println!("   PUT    /key/:key          -> insert  (body = value)");
        println!("   DELETE /key/:key          -> delete");
        println!("   GET    /scan?from=&to=    -> range scan");
        println!("   GET    /prefix/:prefix    -> prefix scan");
        println!("   GET    /snapshot          -> consistent snapshot (JSON)");
        println!("   GET    /stats             -> engine stats (JSON)");
        println!(" To start:  axum::serve(TcpListener::bind(addr).await?, app).await?");
        println!(" ✓ HTTP API compiled and router constructed.");
    }

    Ok(())
}
