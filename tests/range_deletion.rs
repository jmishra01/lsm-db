// Integration tests — range deletion

use lsmdb::LsmEngine;
use tempfile::TempDir;

fn open() -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

fn load(db: &mut LsmEngine, n: u32) {
    for i in 0..n {
        db.put(format!("log:{i:04}"), format!("entry-{i}")).unwrap();
    }
}

#[test]
fn delete_range_removes_keys_in_bounds() {
    let (_dir, mut db) = open();
    load(&mut db, 10);

    db.delete_range("log:0003", "log:0007").unwrap();

    for i in 0..10u32 {
        let k = format!("log:{i:04}");
        let present = db.get(&k).unwrap().is_some();
        if (3..7).contains(&i) {
            assert!(!present, "key {k} should be deleted");
        } else {
            assert!(present, "key {k} should be present");
        }
    }
}

#[test]
fn delete_range_exclusive_upper_bound() {
    let (_dir, mut db) = open();
    load(&mut db, 5);

    db.delete_range("log:0001", "log:0004").unwrap();

    // log:0004 must survive (exclusive upper bound)
    assert!(db.get("log:0004").unwrap().is_some());
    // log:0001..log:0003 must be gone
    assert!(db.get("log:0001").unwrap().is_none());
    assert!(db.get("log:0003").unwrap().is_none());
}

#[test]
fn delete_range_with_empty_range_is_no_op() {
    let (_dir, mut db) = open();
    load(&mut db, 5);

    // from == to → nothing deleted
    db.delete_range("log:0002", "log:0002").unwrap();
    assert!(db.get("log:0002").unwrap().is_some());
}

#[test]
fn delete_range_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = LsmEngine::open(dir.path()).unwrap();
        load(&mut db, 8);
        db.delete_range("log:0002", "log:0005").unwrap();
    }
    let db = LsmEngine::open(dir.path()).unwrap();
    assert!(db.get("log:0002").unwrap().is_none());
    assert!(db.get("log:0004").unwrap().is_none());
    assert!(db.get("log:0005").unwrap().is_some()); // not in range
}

#[test]
fn scan_after_delete_range_skips_deleted_keys() {
    let (_dir, mut db) = open();
    load(&mut db, 10);

    db.delete_range("log:0004", "log:0008").unwrap();

    let results = db.scan("log:", "log:~").unwrap();
    let keys: Vec<String> = results.iter()
        .map(|(k, _)| String::from_utf8(k.clone()).unwrap())
        .collect();
    let expected: Vec<String> = [0u32, 1, 2, 3, 8, 9]
        .iter().map(|i| format!("log:{i:04}")).collect();
    assert_eq!(keys, expected);
}

#[test]
fn delete_range_cf_scoped_to_target_family() {
    let dir = TempDir::new().unwrap();
    let mut db = LsmEngine::open_with_cfs(dir.path(), &["default", "logs"]).unwrap();

    for i in 0..5u32 {
        db.put_cf("default", format!("k:{i}"), "default").unwrap();
        db.put_cf("logs",    format!("k:{i}"), "logs").unwrap();
    }

    db.delete_range_cf("logs", "k:1", "k:4").unwrap();

    // "default" CF untouched
    assert!(db.get_cf("default", "k:1").unwrap().is_some());
    assert!(db.get_cf("default", "k:3").unwrap().is_some());

    // "logs" CF has the range deleted
    assert!(db.get_cf("logs", "k:1").unwrap().is_none());
    assert!(db.get_cf("logs", "k:4").unwrap().is_some());
}
