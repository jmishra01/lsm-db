// Integration tests — MVCC snapshots and WriteBatch

use lsmdb::{LsmEngine, WriteBatch};
use tempfile::TempDir;

fn open() -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open(dir.path()).unwrap();
    (dir, db)
}

// ---- Snapshot isolation ----------------------------------------------------

#[test]
fn snapshot_is_isolated_from_later_writes() {
    let (_dir, mut db) = open();
    db.put("k", "v1").unwrap();

    let snap = db.snapshot().unwrap();
    db.put("k", "v2").unwrap(); // write after snapshot

    // Snapshot still sees v1
    assert_eq!(snap.get("k"), Some(b"v1".as_ref()));
    // Live engine sees v2
    assert_eq!(db.get("k").unwrap().as_deref(), Some(b"v2".as_ref()));
}

#[test]
fn snapshot_does_not_see_post_snapshot_deletes() {
    let (_dir, mut db) = open();
    db.put("k", "alive").unwrap();
    let snap = db.snapshot().unwrap();
    db.delete("k").unwrap();

    assert_eq!(snap.get("k"), Some(b"alive".as_ref()));
    assert_eq!(db.get("k").unwrap(), None);
}

#[test]
fn snapshot_get_returns_none_for_pre_existing_absent_key() {
    let (_dir, mut db) = open();
    let snap = db.snapshot().unwrap();
    db.put("added_after", "value").unwrap();

    assert_eq!(snap.get("added_after"), None);
}

#[test]
fn snapshot_scan_prefix_is_consistent() {
    let (_dir, mut db) = open();
    db.put("sensor:001", "10").unwrap();
    db.put("sensor:002", "20").unwrap();
    let snap = db.snapshot().unwrap();
    db.put("sensor:003", "30").unwrap(); // after snapshot

    let results = snap.scan_prefix("sensor:");
    assert_eq!(results.len(), 2);
}

#[test]
fn snapshot_key_count_reflects_live_keys_at_creation() {
    let (_dir, mut db) = open();
    db.put("a", "1").unwrap();
    db.put("b", "2").unwrap();
    db.put("c", "3").unwrap();
    let snap = db.snapshot().unwrap();
    db.delete("c").unwrap();

    assert_eq!(snap.key_count(), 3); // snapshot taken before delete
}

#[test]
fn snapshot_iter_returns_sorted_pairs() {
    let (_dir, mut db) = open();
    for ch in ["zebra", "apple", "mango"] {
        db.put(ch, ch).unwrap();
    }
    let snap = db.snapshot().unwrap();
    let keys: Vec<&Vec<u8>> = snap.iter().map(|(k, _)| k).collect();
    let key_strs: Vec<&str> = keys.iter()
        .map(|k| std::str::from_utf8(k).unwrap())
        .collect();
    assert_eq!(key_strs, ["apple", "mango", "zebra"]);
}

// ---- WriteBatch atomicity --------------------------------------------------

#[test]
fn write_batch_all_or_nothing_visibility() {
    let (_dir, mut db) = open();
    db.put("a", "before").unwrap();
    db.put("b", "before").unwrap();

    let snap_pre = db.snapshot().unwrap();

    let mut batch = WriteBatch::new();
    batch.put("default", "a", "after")
         .put("default", "b", "after");
    db.write_batch(batch).unwrap();

    let snap_post = db.snapshot().unwrap();

    // Pre-batch snapshot sees neither update
    assert_eq!(snap_pre.get("a"), Some(b"before".as_ref()));
    assert_eq!(snap_pre.get("b"), Some(b"before".as_ref()));
    // Post-batch snapshot sees both
    assert_eq!(snap_post.get("a"), Some(b"after".as_ref()));
    assert_eq!(snap_post.get("b"), Some(b"after".as_ref()));
}

#[test]
fn write_batch_delete_is_visible_after_commit() {
    let (_dir, mut db) = open();
    db.put("del_me", "value").unwrap();

    let mut batch = WriteBatch::new();
    batch.delete("default", "del_me");
    db.write_batch(batch).unwrap();

    assert_eq!(db.get("del_me").unwrap(), None);
}

#[test]
fn empty_write_batch_is_no_op() {
    let (_dir, mut db) = open();
    db.put("k", "v").unwrap();
    db.write_batch(WriteBatch::new()).unwrap();
    assert_eq!(db.get("k").unwrap().as_deref(), Some(b"v".as_ref()));
}
