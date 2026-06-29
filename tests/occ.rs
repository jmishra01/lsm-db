// Integration tests — Optimistic Concurrency Control

use lsmdb::occ::{OccError, OccTransaction};
use lsmdb::LsmEngine;
use tempfile::TempDir;

fn open() -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

#[test]
fn commit_without_conflict_succeeds() {
    let (_dir, mut db) = open();
    db.put("k", "initial").unwrap();

    let mut tx = OccTransaction::begin(&db).unwrap();
    let v = tx.get(b"k");
    assert_eq!(v.as_deref(), Some(b"initial".as_ref()));

    tx.put(b"k".to_vec(), b"updated".to_vec());
    tx.commit(&mut db).unwrap();

    assert_eq!(db.get("k").unwrap().as_deref(), Some(b"updated".as_ref()));
}

#[test]
fn commit_detects_concurrent_write_conflict() {
    let (_dir, mut db) = open();
    db.put("account", "100").unwrap();

    let mut tx = OccTransaction::begin(&db).unwrap();
    let _ = tx.get(b"account"); // add to read set

    // Concurrent write advances the seq past read_horizon
    db.put("account", "200").unwrap();

    tx.put(b"account".to_vec(), b"150".to_vec());
    let result = tx.commit(&mut db);

    assert!(
        matches!(result, Err(OccError::Conflict(_))),
        "expected conflict, got {:?}", result
    );
}

#[test]
fn write_without_read_does_not_conflict() {
    let (_dir, mut db) = open();
    db.put("k", "v0").unwrap();

    // Transaction only writes, never reads k
    let tx = OccTransaction::begin(&db).unwrap();
    // No reads — empty read_set → no conflict check

    // External write after begin
    db.put("k", "v2").unwrap();

    // No key in read_set → no conflict
    tx.commit(&mut db).unwrap();
}

#[test]
fn read_your_own_writes_in_transaction() {
    let (_dir, mut db) = open();
    let mut tx = OccTransaction::begin(&db).unwrap();

    tx.put(b"k".to_vec(), b"local_write".to_vec());
    let v = tx.get(b"k");

    assert_eq!(v.as_deref(), Some(b"local_write".as_ref()));
}

#[test]
fn transaction_delete_is_visible_via_get() {
    let (_dir, mut db) = open();
    db.put("k", "v").unwrap();

    let mut tx = OccTransaction::begin(&db).unwrap();
    tx.delete(b"k".to_vec());
    assert_eq!(tx.get(b"k"), None);
}

#[test]
fn conflicting_transaction_can_be_retried_successfully() {
    let (_dir, mut db) = open();
    db.put("counter", "0").unwrap();

    let mut attempts = 0;
    loop {
        attempts += 1;
        let mut tx = OccTransaction::begin(&db).unwrap();
        let cur: i64 = tx.get(b"counter")
            .and_then(|v| String::from_utf8(v).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if attempts == 1 {
            // Simulate a concurrent write on the first attempt only
            db.put("counter", "99").unwrap();
        }

        tx.put(b"counter".to_vec(), (cur + 1).to_string().into_bytes());
        match tx.commit(&mut db) {
            Ok(()) => break,
            Err(OccError::Conflict(_)) => continue,
            Err(OccError::Io(e)) => panic!("io error: {e}"),
        }
    }
    // After first conflict + retry, final value depends on the retry read
    assert!(db.get("counter").unwrap().is_some());
    assert!(attempts >= 2, "should have retried at least once");
}

#[test]
fn conflict_error_reports_conflicting_keys() {
    let (_dir, mut db) = open();
    db.put("a", "1").unwrap();
    db.put("b", "2").unwrap();

    let mut tx = OccTransaction::begin(&db).unwrap();
    let _ = tx.get(b"a");
    let _ = tx.get(b"b");

    db.put("a", "modified").unwrap();

    let err = tx.commit(&mut db).unwrap_err();
    if let OccError::Conflict(e) = err {
        assert!(!e.conflicting_keys.is_empty());
        assert!(e.conflicting_keys.contains(&b"a".to_vec()));
    } else {
        panic!("expected conflict error");
    }
}
