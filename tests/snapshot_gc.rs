// Unit tests — snapshot garbage collection

use lsmdb::snapshot_gc::{filter_versions, SnapshotRegistry};

fn entry(key: &str, seq: u64, val: &str) -> (Vec<u8>, u64, Option<Vec<u8>>) {
    (key.as_bytes().to_vec(), seq, Some(val.as_bytes().to_vec()))
}

fn tombstone(key: &str, seq: u64) -> (Vec<u8>, u64, Option<Vec<u8>>) {
    (key.as_bytes().to_vec(), seq, None)
}

// ---- SnapshotRegistry ------------------------------------------------------

#[test]
fn safe_horizon_is_max_u64_with_no_snapshots() {
    let reg = SnapshotRegistry::new();
    assert_eq!(reg.safe_horizon(), u64::MAX);
}

#[test]
fn safe_horizon_is_min_seq_minus_one() {
    let reg = SnapshotRegistry::new();
    let _g1 = reg.register(10);
    let _g2 = reg.register(20);
    assert_eq!(reg.safe_horizon(), 9); // min(10,20) - 1
}

#[test]
fn safe_horizon_updates_after_guard_dropped() {
    let reg = SnapshotRegistry::new();
    let g1 = reg.register(5);
    let _g2 = reg.register(15);
    assert_eq!(reg.safe_horizon(), 4);
    drop(g1);
    assert_eq!(reg.safe_horizon(), 14); // min is now 15
}

#[test]
fn active_count_tracks_live_snapshots() {
    let reg = SnapshotRegistry::new();
    assert_eq!(reg.active_count(), 0);
    let g1 = reg.register(1);
    let g2 = reg.register(2);
    assert_eq!(reg.active_count(), 2);
    drop(g1);
    assert_eq!(reg.active_count(), 1);
    drop(g2);
    assert_eq!(reg.active_count(), 0);
}

#[test]
fn horizon_is_zero_when_snapshot_at_seq_one() {
    let reg = SnapshotRegistry::new();
    let _g = reg.register(1);
    assert_eq!(reg.safe_horizon(), 0);
}

// ---- filter_versions -------------------------------------------------------

#[test]
fn keeps_latest_version_always() {
    let entries = vec![entry("k", 100, "latest")];
    let out = filter_versions(entries, 200, false);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].2.as_deref(), Some(b"latest".as_ref()));
}

#[test]
fn drops_old_version_below_safe_horizon() {
    // Two versions of same key; older one is below safe_horizon
    let entries = vec![
        entry("k", 5, "old"),
        entry("k", 15, "new"),
    ];
    // safe_horizon = 10 → seq=5 is below horizon and not the latest → drop it
    let out = filter_versions(entries, 10, false);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].1, 15);
    assert_eq!(out[0].2.as_deref(), Some(b"new".as_ref()));
}

#[test]
fn keeps_old_version_above_safe_horizon() {
    let entries = vec![
        entry("k", 15, "old"),
        entry("k", 25, "new"),
    ];
    // safe_horizon = 10 → seq=15 is above horizon → keep both
    let out = filter_versions(entries, 10, false);
    assert_eq!(out.len(), 2);
}

#[test]
fn tombstone_kept_at_non_deepest_level() {
    let entries = vec![tombstone("k", 5)];
    let out = filter_versions(entries, 100, false); // not deepest
    assert_eq!(out.len(), 1);
    assert!(out[0].2.is_none());
}

#[test]
fn tombstone_dropped_at_deepest_level_below_horizon() {
    let entries = vec![tombstone("k", 5)];
    // at deepest level + seq below safe_horizon → physically drop
    let out = filter_versions(entries, 10, true);
    assert!(out.is_empty());
}

#[test]
fn tombstone_kept_at_deepest_level_above_horizon() {
    let entries = vec![tombstone("k", 15)];
    // seq above safe_horizon → some snapshot might need it
    let out = filter_versions(entries, 10, true);
    assert_eq!(out.len(), 1);
}

#[test]
fn multiple_keys_handled_independently() {
    let entries = vec![
        entry("a", 3, "old_a"),
        entry("a", 10, "new_a"),
        entry("b", 5, "only_b"),
    ];
    // safe_horizon = 7 → a@3 prunable, a@10 kept; b@5 is latest → kept
    let out = filter_versions(entries, 7, false);
    let a_versions: Vec<_> = out.iter().filter(|(k, _, _)| k == b"a").collect();
    let b_versions: Vec<_> = out.iter().filter(|(k, _, _)| k == b"b").collect();
    assert_eq!(a_versions.len(), 1, "only new_a should remain");
    assert_eq!(b_versions.len(), 1, "b has only one version, must be kept");
}

#[test]
fn output_is_sorted_by_key() {
    let entries = vec![
        entry("z", 1, "last"),
        entry("a", 1, "first"),
        entry("m", 1, "mid"),
    ];
    let out = filter_versions(entries, u64::MAX, false);
    let keys: Vec<&[u8]> = out.iter().map(|(k, _, _)| k.as_slice()).collect();
    assert_eq!(keys, [b"a".as_ref(), b"m".as_ref(), b"z".as_ref()]);
}
