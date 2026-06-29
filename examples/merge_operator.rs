/// Demonstrates merge operators for read-modify-write without locking.
use lsmdb::merge_operator::{Int64AddOperator, MergeState, MergeDelta, StringAppendOperator};

fn delta(key: &[u8], seq: u64, value: Vec<u8>) -> MergeDelta {
    MergeDelta { key: key.to_vec(), seq, delta: value }
}

fn main() {
    // ── Int64Add: atomic counter accumulation ──────────────────
    let op = Int64AddOperator;
    let mut state = MergeState::new();

    state.push(delta(b"page_views",  1, 5_i64.to_le_bytes().to_vec()));
    state.push(delta(b"page_views",  2, 3_i64.to_le_bytes().to_vec()));
    state.push(delta(b"page_views",  3, (-2_i64).to_le_bytes().to_vec()));

    let result = state.resolve(b"page_views", None, &op);
    let total = i64::from_le_bytes(result.unwrap().try_into().unwrap());
    println!("counter total = {total}"); // 6

    // With a base value
    let base = 100_i64.to_le_bytes().to_vec();
    let mut state2 = MergeState::new();
    state2.push(delta(b"page_views", 4, 10_i64.to_le_bytes().to_vec()));
    let result2 = state2.resolve(b"page_views", Some(&base), &op);
    let total2 = i64::from_le_bytes(result2.unwrap().try_into().unwrap());
    println!("counter with base = {total2}"); // 110

    // ── StringAppend: log-style append ────────────────────────
    let append_op = StringAppendOperator { separator: b",".to_vec() };
    let mut log = MergeState::new();
    log.push(delta(b"audit:user:42", 1, b"login".to_vec()));
    log.push(delta(b"audit:user:42", 2, b"view_dashboard".to_vec()));
    log.push(delta(b"audit:user:42", 3, b"logout".to_vec()));

    let log_result = log.resolve(b"audit:user:42", None, &append_op);
    println!("audit log = {}", String::from_utf8_lossy(&log_result.unwrap()));
    // "login,view_dashboard,logout"
}
