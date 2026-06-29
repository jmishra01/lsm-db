/// Demonstrates Optimistic Concurrency Control transactions.
use lsmdb::occ::{OccError, OccTransaction};
use lsmdb::LsmEngine;
use tempfile::TempDir;

fn main() -> std::io::Result<()> {
    let dir = TempDir::new()?;
    let mut db = LsmEngine::open(dir.path())?;

    db.put("alice_balance", "500")?;
    db.put("bob_balance",   "300")?;

    // ── successful transfer ────────────────────────────────────
    transfer(&mut db, "alice_balance", "bob_balance", 100)?;
    println!("alice = {:?}", db.get("alice_balance")?);
    println!("bob   = {:?}", db.get("bob_balance")?);

    // ── read-your-own-writes ───────────────────────────────────
    let mut tx = OccTransaction::begin(&db)?;
    tx.put(b"temp".to_vec(), b"draft_value".to_vec());
    println!("tx sees temp = {:?}", tx.get(b"temp")); // Some before commit
    tx.commit(&mut db).map_err(occ_to_io)?;

    // ── simulated conflict + automatic retry ──────────────────
    db.put("stock", "10")?;
    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut tx = OccTransaction::begin(&db)?;
        let qty: i64 = tx.get(b"stock")
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if attempts == 1 {
            // Concurrent write — will cause a conflict on first attempt
            db.put("stock", "9")?;
        }

        tx.put(b"stock".to_vec(), (qty - 1).to_string().into_bytes());
        match tx.commit(&mut db) {
            Ok(())                     => break,
            Err(OccError::Conflict(_)) => { eprintln!("conflict — retrying (attempt {attempts})"); continue; }
            Err(OccError::Io(e))       => return Err(e),
        }
    }
    println!("stock after reservation = {:?}", db.get("stock")?);
    println!("total attempts = {attempts}");

    Ok(())
}

fn occ_to_io(e: OccError) -> std::io::Error {
    match e {
        OccError::Io(e)       => e,
        OccError::Conflict(c) => std::io::Error::other(format!("conflict: {:?}", c.conflicting_keys)),
    }
}

fn transfer(db: &mut LsmEngine, from: &str, to: &str, amount: i64) -> std::io::Result<()> {
    loop {
        let mut tx = OccTransaction::begin(db)?;
        let from_bal: i64 = tx.get(from.as_bytes())
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let to_bal: i64 = tx.get(to.as_bytes())
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        tx.put(from.as_bytes().to_vec(), (from_bal - amount).to_string().into_bytes());
        tx.put(to.as_bytes().to_vec(),   (to_bal   + amount).to_string().into_bytes());

        match tx.commit(db) {
            Ok(())                     => return Ok(()),
            Err(OccError::Conflict(_)) => continue,
            Err(OccError::Io(e))       => return Err(e),
        }
    }
}
