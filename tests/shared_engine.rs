// Integration tests — SharedLsmEngine (concurrent access)

use lsmdb::SharedLsmEngine;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn open() -> (TempDir, SharedLsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = SharedLsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn concurrent_puts_all_visible() {
    let (_dir, db) = open();
    let db = Arc::new(db);
    const THREADS: u32 = 4;
    const KEYS: u32 = 50;

    let handles: Vec<_> = (0..THREADS).map(|t| {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            for i in 0..KEYS {
                db.put(format!("t{t}:k{i:04}"), format!("v{i}")).unwrap();
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }

    for t in 0..THREADS {
        for i in 0..KEYS {
            assert!(
                db.get(format!("t{t}:k{i:04}")).unwrap().is_some(),
                "missing t{t}:k{i:04}"
            );
        }
    }
}

#[test]
fn concurrent_reads_do_not_panic() {
    let (_dir, db) = open();
    let db = Arc::new(db);

    for i in 0..20u32 {
        db.put(format!("key:{i}"), format!("val:{i}")).unwrap();
    }

    let handles: Vec<_> = (0..8).map(|_| {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            for i in 0..20u32 {
                let _ = db.get(format!("key:{i}")).unwrap();
            }
        })
    }).collect();
    for h in handles { h.join().unwrap(); }
}

#[test]
fn write_batch_via_shared_engine() {
    let (_dir, db) = open();
    let mut batch = lsmdb::WriteBatch::new();
    batch.put("default", "x", "1")
         .put("default", "y", "2")
         .delete("default", "z");
    db.write_batch(batch).unwrap();

    assert_eq!(db.get("x").unwrap().as_deref(), Some(b"1".as_ref()));
    assert_eq!(db.get("y").unwrap().as_deref(), Some(b"2".as_ref()));
    assert_eq!(db.get("z").unwrap(), None);
}

#[test]
fn snapshot_via_shared_engine_is_consistent() {
    let (_dir, db) = open();
    db.put("snap_key", "snap_val").unwrap();
    let snap = db.snapshot().unwrap();
    db.put("snap_key", "new_val").unwrap();

    assert_eq!(snap.get("snap_key"), Some(b"snap_val".as_ref()));
    assert_eq!(db.get("snap_key").unwrap().as_deref(), Some(b"new_val".as_ref()));
}

#[test]
fn ttl_via_shared_engine() {
    let (_dir, db) = open();
    db.put_with_ttl("tk", "tv", 100).unwrap();
    assert_eq!(db.get("tk").unwrap().as_deref(), Some(b"tv".as_ref()));
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert_eq!(db.get("tk").unwrap(), None);
}

#[test]
fn delete_range_via_shared_engine() {
    let (_dir, db) = open();
    for i in 0..5u32 {
        db.put(format!("r:{i}"), "v").unwrap();
    }
    db.delete_range("r:1", "r:4").unwrap();

    assert!(db.get("r:0").unwrap().is_some());
    assert!(db.get("r:1").unwrap().is_none());
    assert!(db.get("r:3").unwrap().is_none());
    assert!(db.get("r:4").unwrap().is_some());
}
