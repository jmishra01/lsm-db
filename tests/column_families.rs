// Integration tests — column families

use lsmdb::LsmEngine;
use tempfile::TempDir;

fn open_cfs(cfs: &[&str]) -> (TempDir, LsmEngine) {
    let dir = TempDir::new().unwrap();
    let db  = LsmEngine::open_with_cfs(dir.path(), cfs).unwrap();
    (dir, db)
}

#[test]
fn separate_cfs_do_not_share_keys() {
    let (_dir, mut db) = open_cfs(&["default", "meta"]);
    db.put_cf("default", "key", "from_default").unwrap();
    db.put_cf("meta",    "key", "from_meta").unwrap();

    assert_eq!(db.get_cf("default", "key").unwrap().as_deref(), Some(b"from_default".as_ref()));
    assert_eq!(db.get_cf("meta",    "key").unwrap().as_deref(), Some(b"from_meta".as_ref()));
}

#[test]
fn delete_in_one_cf_does_not_affect_another() {
    let (_dir, mut db) = open_cfs(&["default", "shadow"]);
    db.put_cf("default", "k", "v").unwrap();
    db.put_cf("shadow",  "k", "v").unwrap();
    db.delete_cf("shadow", "k").unwrap();

    assert_eq!(db.get_cf("default", "k").unwrap().as_deref(), Some(b"v".as_ref()));
    assert_eq!(db.get_cf("shadow",  "k").unwrap(), None);
}

#[test]
fn list_cfs_returns_all_families() {
    let (_dir, db) = open_cfs(&["default", "logs", "meta"]);
    let mut cfs = db.list_cfs();
    cfs.sort();
    assert_eq!(cfs, ["default", "logs", "meta"]);
}

#[test]
fn scan_prefix_scoped_to_cf() {
    let (_dir, mut db) = open_cfs(&["default", "idx"]);
    db.put_cf("default", "user:1", "a").unwrap();
    db.put_cf("idx",     "user:1", "b").unwrap();
    db.put_cf("idx",     "user:2", "c").unwrap();

    let idx_users = db.scan_prefix_cf("idx", "user:").unwrap();
    assert_eq!(idx_users.len(), 2);

    let default_users = db.scan_prefix_cf("default", "user:").unwrap();
    assert_eq!(default_users.len(), 1);
}

#[test]
fn cfs_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = LsmEngine::open_with_cfs(dir.path(), &["default", "archive"]).unwrap();
        db.put_cf("archive", "rec:1", "data").unwrap();
    }
    let db = LsmEngine::open_with_cfs(dir.path(), &["default", "archive"]).unwrap();
    assert_eq!(
        db.get_cf("archive", "rec:1").unwrap().as_deref(),
        Some(b"data".as_ref())
    );
}
