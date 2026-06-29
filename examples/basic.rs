/// Demonstrates core CRUD operations, scans, and persistence.
use lsmdb::LsmEngine;
use tempfile::TempDir;

fn main() -> std::io::Result<()> {
    let dir = TempDir::new()?;
    let mut db = LsmEngine::open(dir.path())?;

    // ── put / get ──────────────────────────────────────────────
    db.put("user:alice", "Alice Smith")?;
    db.put("user:bob", "Bob Jones")?;
    db.put("user:carol", "Carol White")?;

    println!("alice → {:?}", db.get("user:alice")?);

    // ── delete ──────────────────────────────────────────────────
    db.delete("user:bob")?;
    println!("bob after delete → {:?}", db.get("user:bob")?);

    // ── range scan ──────────────────────────────────────────────
    println!("scan user:a..user:z:");
    for (k, v) in db.scan("user:a", "user:z")? {
        println!("  {} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    // ── prefix scan ─────────────────────────────────────────────
    db.put("order:001", "laptop")?;
    db.put("order:002", "keyboard")?;
    println!("prefix 'order:':");
    for (k, v) in db.scan_prefix("order:")? {
        println!("  {} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    // ── re-open (persistence) ────────────────────────────────────
    drop(db);
    let db2 = LsmEngine::open(dir.path())?;
    println!("alice after reopen → {:?}", db2.get("user:alice")?);

    Ok(())
}
