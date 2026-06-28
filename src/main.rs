// LSM-DB - demo driver

use lsmdb::LsmEngine;
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
        println!(" memtable={} bytes files/level={:?}", s.memtable_size_bytes, s.level_file_counts);
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
        println!(" memtable     : {} bytes", s.memtable_size_bytes);
        println!(" immutable    : {}", s.immutable_count);
        println!(" total sst    : {} file(s)", s.total_sstable_files);
        for (i, cnt) in s.level_file_counts.iter().enumerate() {
            if *cnt > 0 {
                println!("  L{}     : {} file(s)", i, cnt);
            }
        }
    }

    Ok(())
}
