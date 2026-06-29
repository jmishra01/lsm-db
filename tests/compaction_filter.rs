// Unit tests — compaction filter

use lsmdb::compaction_filter::{
    apply_filter, CompactionFilter, ExpiryPrefixFilter, FilterDecision, FnFilter, PrefixDropFilter,
};

fn entry(key: &str, seq: u64, val: &str) -> (Vec<u8>, u64, Option<Vec<u8>>) {
    (key.as_bytes().to_vec(), seq, Some(val.as_bytes().to_vec()))
}

fn tombstone(key: &str, seq: u64) -> (Vec<u8>, u64, Option<Vec<u8>>) {
    (key.as_bytes().to_vec(), seq, None)
}

#[test]
fn fn_filter_removes_matching_entries() {
    let entries = vec![
        entry("keep:a", 1, "v"),
        entry("drop:b", 2, "v"),
        entry("keep:c", 3, "v"),
    ];
    let filter = FnFilter::new("drop_prefix", |key: &[u8], _| {
        if key.starts_with(b"drop:") { FilterDecision::Remove }
        else { FilterDecision::Keep }
    });
    let out = apply_filter(entries, &filter);
    assert_eq!(out.len(), 2);
    assert!(out.iter().all(|(k, _, _)| k.starts_with(b"keep:")));
}

#[test]
fn fn_filter_replaces_value() {
    let entries = vec![entry("k", 1, "old")];
    let filter = FnFilter::new("replace", |_, _| FilterDecision::Replace(b"new".to_vec()));
    let out = apply_filter(entries, &filter);
    assert_eq!(out[0].2.as_deref(), Some(b"new".as_ref()));
}

#[test]
fn tombstones_pass_through_unchanged() {
    let entries = vec![tombstone("dead", 1), entry("live", 2, "v")];
    let filter = FnFilter::new("keep_all", |_, _| FilterDecision::Keep);
    let out = apply_filter(entries, &filter);
    assert_eq!(out.len(), 2);
    assert!(out[0].2.is_none()); // tombstone preserved
}

#[test]
fn prefix_drop_filter_removes_by_prefix() {
    let entries = vec![
        entry("temp:x", 1, "v"),
        entry("perm:y", 2, "v"),
        entry("temp:z", 3, "v"),
    ];
    let filter = PrefixDropFilter::new("temp:");
    let out = apply_filter(entries, &filter);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, b"perm:y");
}

#[test]
fn expiry_prefix_filter_drops_expired_entries() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

    let expired_ts = (now_ms - 1000).to_be_bytes().to_vec();   // 1 s ago
    let future_ts  = (now_ms + 60_000).to_be_bytes().to_vec(); // 60 s from now
    let no_expiry  = 0u64.to_be_bytes().to_vec();

    let entries = vec![
        (b"expired".to_vec(), 1u64, Some(expired_ts)),
        (b"future".to_vec(),  2u64, Some(future_ts)),
        (b"permanent".to_vec(), 3u64, Some(no_expiry)),
    ];
    let filter = ExpiryPrefixFilter;
    let out = apply_filter(entries, &filter);
    assert_eq!(out.len(), 2);
    assert!(out.iter().any(|(k, _, _)| k == b"future"));
    assert!(out.iter().any(|(k, _, _)| k == b"permanent"));
    assert!(!out.iter().any(|(k, _, _)| k == b"expired"));
}

#[test]
fn empty_input_returns_empty() {
    let filter = FnFilter::new("noop", |_, _| FilterDecision::Keep);
    let out = apply_filter(vec![], &filter);
    assert!(out.is_empty());
}

#[test]
fn filter_name_is_accessible() {
    let f = FnFilter::new("my_filter", |_, _| FilterDecision::Keep);
    assert_eq!(f.name(), "my_filter");
}
