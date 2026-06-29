/// Demonstrates column families — independent key spaces in one DB.
use lsmdb::LsmEngine;
use tempfile::TempDir;

fn main() -> std::io::Result<()> {
    let dir = TempDir::new()?;
    let mut db = LsmEngine::open(dir.path())?;

    db.create_cf("users")?;
    db.create_cf("products")?;

    db.put_cf("users",    "u:1", "Alice")?;
    db.put_cf("users",    "u:2", "Bob")?;
    db.put_cf("products", "p:1", "Laptop")?;
    db.put_cf("products", "p:2", "Keyboard")?;

    // Same key in different CFs → independent values
    db.put_cf("users",    "shared", "user-side")?;
    db.put_cf("products", "shared", "product-side")?;

    println!("users/shared    → {:?}", db.get_cf("users",    "shared")?);
    println!("products/shared → {:?}", db.get_cf("products", "shared")?);

    // Listing CFs
    println!("column families: {:?}", db.list_cfs());

    // Scan within a single CF
    println!("scan users:");
    for (k, v) in db.scan_cf("users", "u:0", "u:9")? {
        println!("  {} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    Ok(())
}
