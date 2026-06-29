// Unit tests — merge operator

use lsmdb::merge_operator::{
    Int64AddOperator, MergeOperator, MergeState, MergeDelta, StringAppendOperator,
};

fn delta(key: &[u8], seq: u64, d: Vec<u8>) -> MergeDelta {
    MergeDelta { key: key.to_vec(), seq, delta: d }
}

// ---- Int64AddOperator ------------------------------------------------------

#[test]
fn int64_add_no_base_sums_deltas() {
    let op = Int64AddOperator;
    let deltas: Vec<Vec<u8>> = [1i64, 2, 3]
        .iter().map(|n| n.to_le_bytes().to_vec()).collect();
    let result = op.full_merge(b"k", None, &deltas).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 6);
}

#[test]
fn int64_add_with_base() {
    let op = Int64AddOperator;
    let base = 10i64.to_le_bytes();
    let deltas: Vec<Vec<u8>> = [5i64, -3].iter().map(|n| n.to_le_bytes().to_vec()).collect();
    let result = op.full_merge(b"k", Some(&base), &deltas).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 12);
}

#[test]
fn int64_add_partial_merge() {
    let op = Int64AddOperator;
    let l = 7i64.to_le_bytes();
    let r = 3i64.to_le_bytes();
    let result = op.partial_merge(b"k", &l, &r).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 10);
}

#[test]
fn int64_add_empty_deltas_returns_base() {
    let op = Int64AddOperator;
    let base = 42i64.to_le_bytes();
    let result = op.full_merge(b"k", Some(&base), &[]).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 42);
}

#[test]
fn int64_add_no_base_no_deltas_returns_zero() {
    let op = Int64AddOperator;
    let result = op.full_merge(b"k", None, &[]).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 0);
}

#[test]
fn int64_add_wraps_on_overflow() {
    let op = Int64AddOperator;
    let base = i64::MAX.to_le_bytes();
    let delta = 1i64.to_le_bytes();
    let result = op.full_merge(b"k", Some(&base), &[delta.to_vec()]).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), i64::MIN);
}

// ---- StringAppendOperator --------------------------------------------------

#[test]
fn string_append_comma_joins_deltas() {
    let op = StringAppendOperator::comma();
    let deltas: Vec<Vec<u8>> = ["rust", "lsm", "fast"]
        .iter().map(|s| s.as_bytes().to_vec()).collect();
    let result = op.full_merge(b"tags", None, &deltas).unwrap();
    assert_eq!(result, b"rust,lsm,fast");
}

#[test]
fn string_append_with_base() {
    let op = StringAppendOperator::comma();
    let base = b"existing";
    let deltas: Vec<Vec<u8>> = vec![b"new".to_vec()];
    let result = op.full_merge(b"tags", Some(base), &deltas).unwrap();
    assert_eq!(result, b"existing,new");
}

#[test]
fn string_append_partial_merge() {
    let op = StringAppendOperator::new(b"|");
    let result = op.partial_merge(b"k", b"a", b"b").unwrap();
    assert_eq!(result, b"a|b");
}

#[test]
fn string_append_empty_deltas_returns_base() {
    let op = StringAppendOperator::comma();
    let result = op.full_merge(b"k", Some(b"base"), &[]).unwrap();
    assert_eq!(result, b"base");
}

#[test]
fn string_append_custom_separator() {
    let op = StringAppendOperator::new(b" | ");
    let deltas: Vec<Vec<u8>> = vec![b"alpha".to_vec(), b"beta".to_vec()];
    let result = op.full_merge(b"k", None, &deltas).unwrap();
    assert_eq!(result, b"alpha | beta");
}

// ---- MergeState ------------------------------------------------------------

#[test]
fn merge_state_resolves_pending_deltas() {
    let op = Int64AddOperator;
    let mut state = MergeState::new();
    for n in [10i64, 20, 30] {
        state.push(delta(b"counter", 0, n.to_le_bytes().to_vec()));
    }
    let result = state.resolve(b"counter", None, &op).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 60);
}

#[test]
fn merge_state_empty_returns_base() {
    let op = Int64AddOperator;
    let state = MergeState::new();
    let base = 99i64.to_le_bytes();
    let result = state.resolve(b"k", Some(&base), &op).unwrap();
    assert_eq!(i64::from_le_bytes(result.try_into().unwrap()), 99);
}

#[test]
fn merge_state_empty_no_base_returns_none() {
    let op = Int64AddOperator;
    let state = MergeState::new();
    // No base, no deltas → full_merge called with empty deltas and no base
    // Int64AddOperator returns 0 (not None)
    let result = state.resolve(b"k", None, &op);
    // With no base and no deltas, resolve() returns base as-is (None)
    assert!(result.is_none());
}
