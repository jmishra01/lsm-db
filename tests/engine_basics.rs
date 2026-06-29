// Integration tests — core engine: put, get, delete, scan, overwrite, persistence

use lsmdb::LsmEngine;
use tempfile::TempDir;

fn open() -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn put_and_get() {
    let (_dir, mut db) = open();
    db.put("hello", "world").unwrap();
    assert_eq!(db.get("hello").unwrap().as_deref(), Some(b"world".as_ref()));
}

#[test]
fn get_missing_returns_none() {
    let (_dir, db) = open();
    assert_eq!(db.get("missing").unwrap(), None);
}

#[test]
fn overwrite_returns_latest() {
    let (_dir, mut db) = open();
    db.put("k", "v1").unwrap();
    db.put("k", "v2").unwrap();
    assert_eq!(db.get("k").unwrap().as_deref(), Some(b"v2".as_ref()));
}

#[test]
fn delete_makes_key_invisible() {
    let (_dir, mut db) = open();
    db.put("k", "v").unwrap();
    db.delete("k").unwrap();
    assert_eq!(db.get("k").unwrap(), None);
}

#[test]
fn scan_returns_sorted_range() {
    let (_dir, mut db) = open();
    for i in 0u8..10 {
        db.put(format!("key:{i:02}"), format!("val:{i}")).unwrap();
    }
    let results = db.scan("key:03", "key:07").unwrap();
    let keys: Vec<String> = results.iter()
        .map(|(k, _)| String::from_utf8(k.clone()).unwrap())
        .collect();
    assert_eq!(keys, ["key:03", "key:04", "key:05", "key:06"]);
}

#[test]
fn scan_prefix_filters_correctly() {
    let (_dir, mut db) = open();
    db.put("user:alice", "1").unwrap();
    db.put("user:bob",   "2").unwrap();
    db.put("order:001",  "3").unwrap();

    let users = db.scan_prefix("user:").unwrap();
    assert_eq!(users.len(), 2);
    assert!(users.iter().all(|(k, _)| k.starts_with(b"user:")));
}

#[test]
fn persistence_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = LsmEngine::open(dir.path()).unwrap();
        db.put("persist", "yes").unwrap();
    }
    let db = LsmEngine::open(dir.path()).unwrap();
    assert_eq!(db.get("persist").unwrap().as_deref(), Some(b"yes".as_ref()));
}

#[test]
fn delete_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = LsmEngine::open(dir.path()).unwrap();
        db.put("gone", "soon").unwrap();
        db.delete("gone").unwrap();
    }
    let db = LsmEngine::open(dir.path()).unwrap();
    assert_eq!(db.get("gone").unwrap(), None);
}

#[test]
fn high_volume_write_and_read() {
    let (_dir, mut db) = open();
    for i in 0u32..500 {
        db.put(format!("k:{i:06}"), format!("v{i}")).unwrap();
    }
    for i in 0u32..500 {
        assert!(db.get(format!("k:{i:06}")).unwrap().is_some(), "missing key {i}");
    }
}

#[test]
fn empty_scan_returns_empty() {
    let (_dir, db) = open();
    assert!(db.scan("a", "z").unwrap().is_empty());
}

#[test]
fn iter_deduplicates_and_suppresses_tombstones() {
    let (_dir, mut db) = open();
    db.put("a", "1").unwrap();
    db.put("b", "2").unwrap();
    db.put("a", "3").unwrap(); // overwrite
    db.delete("b").unwrap();   // tombstone

    let live: Vec<_> = db.iter().unwrap().collect();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].0, b"a");
    assert_eq!(live[0].1, b"3");
}
