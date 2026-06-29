/// Demonstrates TTL (time-to-live) keys and range deletion.
use lsmdb::LsmEngine;
use std::time::Duration;
use tempfile::TempDir;

fn main() -> std::io::Result<()> {
    let dir = TempDir::new().unwrap();
    let mut db = LsmEngine::open(dir.path())?;

    // ── TTL ────────────────────────────────────────────────────
    // Key expires in 200 ms
    db.put_with_ttl("session:abc", "user:42", 200)?;

    println!("before expiry → {:?}", db.get("session:abc")?);
    std::thread::sleep(Duration::from_millis(300));
    println!("after  expiry → {:?}", db.get("session:abc")?); // None

    // Non-expiring key (ttl_ms = 0)
    db.put_with_ttl("persistent", "stays forever", 0)?;
    std::thread::sleep(Duration::from_millis(100));
    println!("persistent    → {:?}", db.get("persistent")?);

    // ── range deletion ─────────────────────────────────────────
    for i in 0..6u32 {
        db.put(format!("log:{i:04}"), format!("entry {i}"))?;
    }
    println!("before range delete:");
    for (k, v) in db.scan("log:", "log:~")? {
        println!("  {} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    // Delete log:0001..log:0004 (exclusive upper bound)
    db.delete_range("log:0001", "log:0004")?;

    println!("after range delete [0001, 0004):");
    for (k, v) in db.scan("log:", "log:~")? {
        println!("  {} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    Ok(())
}
