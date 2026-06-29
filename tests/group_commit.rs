// Integration tests — WAL group commit

use lsmdb::group_commit::GroupCommitWal;
use lsmdb::wal::{Wal, WalRecord};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn single_submit_is_recovered() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("gc.wal");

    let gc = GroupCommitWal::open(&path).unwrap();
    gc.submit(WalRecord::Put {
        key:   b"hello".to_vec(),
        seq:   1,
        value: b"world".to_vec(),
    }).unwrap();
    drop(gc);

    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(&records[0], WalRecord::Put { seq: 1, .. }));
}

#[test]
fn all_record_types_survive_group_commit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("gc.wal");
    let gc = GroupCommitWal::open(&path).unwrap();

    gc.submit(WalRecord::Put    { key: b"k".to_vec(), seq: 1, value: b"v".to_vec() }).unwrap();
    gc.submit(WalRecord::Delete { key: b"k".to_vec(), seq: 2 }).unwrap();
    gc.submit(WalRecord::PutTtl { key: b"t".to_vec(), seq: 3, value: b"v".to_vec(), expires_at: 9999 }).unwrap();
    gc.submit(WalRecord::DeleteRange { from: b"a".to_vec(), to: b"z".to_vec(), seq: 4 }).unwrap();
    drop(gc);

    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(&records[0], WalRecord::Put    { seq: 1, .. }));
    assert!(matches!(&records[1], WalRecord::Delete { seq: 2, .. }));
    assert!(matches!(&records[2], WalRecord::PutTtl { seq: 3, expires_at: 9999, .. }));
    assert!(matches!(&records[3], WalRecord::DeleteRange { seq: 4, .. }));
}

#[test]
fn concurrent_submits_all_recovered() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("concurrent.wal");
    let gc = Arc::new(GroupCommitWal::open(&path).unwrap());

    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 25;

    let handles: Vec<_> = (0..THREADS).map(|t| {
        let gc = Arc::clone(&gc);
        thread::spawn(move || {
            for i in 0..PER_THREAD {
                let seq = t * PER_THREAD + i;
                gc.submit(WalRecord::Put {
                    key:   format!("k{seq}").into_bytes(),
                    seq,
                    value: b"v".to_vec(),
                }).unwrap();
            }
        })
    }).collect();

    for h in handles { h.join().unwrap(); }
    drop(gc);

    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len() as u64, THREADS * PER_THREAD,
        "all {} records must be recovered", THREADS * PER_THREAD);
}
