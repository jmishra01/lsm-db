// Unit tests — WAL: write, recover, CRC integrity, all opcodes

use lsmdb::wal::{Wal, WalRecord};
use tempfile::TempDir;

fn wal_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("test.wal")
}

#[test]
fn put_is_recovered() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_put(b"key".to_vec(), 1, b"value".to_vec()).unwrap();
    }
    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 1);
    if let WalRecord::Put { key, seq, value } = &records[0] {
        assert_eq!(key, b"key");
        assert_eq!(*seq, 1);
        assert_eq!(value, b"value");
    } else {
        panic!("expected Put");
    }
}

#[test]
fn delete_is_recovered() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_delete(b"key".to_vec(), 42).unwrap();
    }
    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 1);
    if let WalRecord::Delete { key, seq } = &records[0] {
        assert_eq!(key, b"key");
        assert_eq!(*seq, 42);
    } else {
        panic!("expected Delete");
    }
}

#[test]
fn put_ttl_is_recovered() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_put_ttl(b"k".to_vec(), 7, b"v".to_vec(), 9_999_999).unwrap();
    }
    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 1);
    if let WalRecord::PutTtl { key, seq, value, expires_at } = &records[0] {
        assert_eq!(key, b"k");
        assert_eq!(*seq, 7);
        assert_eq!(value, b"v");
        assert_eq!(*expires_at, 9_999_999);
    } else {
        panic!("expected PutTtl");
    }
}

#[test]
fn delete_range_is_recovered() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_delete_range(b"from".to_vec(), b"to".to_vec(), 5).unwrap();
    }
    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 1);
    if let WalRecord::DeleteRange { from, to, seq } = &records[0] {
        assert_eq!(from, b"from");
        assert_eq!(to, b"to");
        assert_eq!(*seq, 5);
    } else {
        panic!("expected DeleteRange");
    }
}

#[test]
fn multiple_records_all_recovered_in_order() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_put(b"a".to_vec(), 1, b"va".to_vec()).unwrap();
        wal.append_delete(b"b".to_vec(), 2).unwrap();
        wal.append_put_ttl(b"c".to_vec(), 3, b"vc".to_vec(), 12345).unwrap();
        wal.append_delete_range(b"d".to_vec(), b"e".to_vec(), 4).unwrap();
    }
    let records = Wal::recover(&path).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(&records[0], WalRecord::Put { seq: 1, .. }));
    assert!(matches!(&records[1], WalRecord::Delete { seq: 2, .. }));
    assert!(matches!(&records[2], WalRecord::PutTtl { seq: 3, .. }));
    assert!(matches!(&records[3], WalRecord::DeleteRange { seq: 4, .. }));
}

#[test]
fn recover_missing_file_returns_empty() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nonexistent.wal");
    let records = Wal::recover(&path).unwrap();
    assert!(records.is_empty());
}

#[test]
fn crc_corruption_stops_recovery() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_put(b"good".to_vec(), 1, b"value".to_vec()).unwrap();
    }
    // Flip a byte in the CRC (last 4 bytes of the file)
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let records = Wal::recover(&path).unwrap();
    assert!(records.is_empty(), "CRC corruption should stop recovery before any record");
}

#[test]
fn truncated_file_returns_partial_records() {
    let dir = TempDir::new().unwrap();
    let path = wal_path(&dir);
    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append_put(b"k1".to_vec(), 1, b"v1".to_vec()).unwrap();
        wal.append_put(b"k2".to_vec(), 2, b"v2".to_vec()).unwrap();
    }
    // Truncate 5 bytes off the end (corrupts the second record)
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.truncate(bytes.len() - 5);
    std::fs::write(&path, &bytes).unwrap();

    let records = Wal::recover(&path).unwrap();
    // First record intact, second is truncated / corrupt
    assert_eq!(records.len(), 1);
    assert!(matches!(&records[0], WalRecord::Put { seq: 1, .. }));
}
