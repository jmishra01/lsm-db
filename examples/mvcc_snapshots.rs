/// Demonstrates MVCC snapshots — point-in-time consistent reads.
use lsmdb::LsmEngine;
use tempfile::TempDir;

fn main() -> std::io::Result<()> {
    let dir = TempDir::new()?;
    let mut db = LsmEngine::open(dir.path())?;

    db.put("balance", "100")?;

    // Take a snapshot at seq=1
    let snap = db.snapshot()?;
    println!("snapshot seq = {}", snap.seq());

    // Mutate after the snapshot
    db.put("balance", "200")?;
    db.put("new_key", "appeared later")?;

    // Snapshot still sees the old state
    println!("snap sees balance = {:?}", snap.get("balance"));
    println!("snap sees new_key = {:?}", snap.get("new_key")); // None

    // Current DB sees the latest
    println!("db   sees balance = {:?}", db.get("balance")?);
    println!("db   sees new_key = {:?}", db.get("new_key")?);

    // WriteBatch — atomic multi-key update
    let mut batch = lsmdb::WriteBatch::new();
    batch.put("default", "x", "10")
         .put("default", "y", "20")
         .delete("default", "balance");
    db.write_batch(batch)?;

    println!("after batch: x={:?} y={:?} balance={:?}",
        db.get("x")?, db.get("y")?, db.get("balance")?);

    Ok(())
}
