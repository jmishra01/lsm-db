// =============================================================
// Merge Operator (#new)
//
// Problem: read-modify-write patterns are expensive on LSM trees.
//   current_val = db.get("counter")?;   // ← disk read
//   db.put("counter", current_val + 1); // ← disk write + WAL
// Under high concurrency this serialises all writers on the key.
//
// Merge operator: the user defines a *commutative* partial update
// operation ("delta").  Deltas are written directly without reading
// the current value.  During compaction (or on read) the engine
// applies the merge function to fold all deltas onto the base value.
//
// Examples
// --------
//   Counter increment : delta = +1 (i64 LE)
//   Set union         : delta = new member bytes
//   JSON patch        : delta = {"op":"add","path":"/k","value":…}
//
// How it fits into the engine
// ---------------------------
//   db.merge("counter", b"+1") writes a MergeRecord to the WAL.
//   On read, the engine collects base + all pending deltas,
//   feeds them to MergeOperator::full_merge, and returns the result.
//   Compaction calls partial_merge to collapse adjacent deltas.
//
// This mirrors RocksDB's MergeOperator interface (simplified).
// =============================================================

/// A pending delta written via `db.merge()`.
#[derive(Debug, Clone)]
pub struct MergeDelta {
    pub key:   Vec<u8>,
    pub seq:   u64,
    pub delta: Vec<u8>,
}

/// Trait that defines how deltas are combined.
pub trait MergeOperator: Send + Sync {
    /// Combine a base value (optional — key may not exist yet) with an
    /// ordered list of deltas (oldest first) into a final value.
    fn full_merge(
        &self,
        key:     &[u8],
        base:    Option<&[u8]>,
        deltas:  &[Vec<u8>],
    ) -> Option<Vec<u8>>;

    /// Collapse two adjacent deltas into one.  Called during compaction
    /// to reduce the number of merge records without a base value present.
    /// Return None if partial merging is not supported.
    fn partial_merge(
        &self,
        _key:     &[u8],
        _left:    &[u8],
        _right:   &[u8],
    ) -> Option<Vec<u8>> { None }

    fn name(&self) -> &str { "unnamed_operator" }
}

// ---- Built-in operators ----------------------------------------------------

/// Integer counter: deltas are i64 LE-encoded signed increments.
/// Base value is also i64 LE.  Missing base = 0.
pub struct Int64AddOperator;

impl MergeOperator for Int64AddOperator {
    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, deltas: &[Vec<u8>]) -> Option<Vec<u8>> {
        let mut acc: i64 = base
            .and_then(|b| b.try_into().ok())
            .map(i64::from_le_bytes)
            .unwrap_or(0);
        for d in deltas {
            if let Ok(arr) = d.as_slice().try_into() {
                acc = acc.wrapping_add(i64::from_le_bytes(arr));
            }
        }
        Some(acc.to_le_bytes().to_vec())
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
        let l: i64 = left.try_into().ok().map(i64::from_le_bytes)?;
        let r: i64 = right.try_into().ok().map(i64::from_le_bytes)?;
        Some(l.wrapping_add(r).to_le_bytes().to_vec())
    }

    fn name(&self) -> &str { "int64_add" }
}

/// String append: deltas are appended to the base with an optional separator.
pub struct StringAppendOperator {
    pub separator: Vec<u8>,
}

impl StringAppendOperator {
    pub fn new(separator: impl Into<Vec<u8>>) -> Self {
        Self { separator: separator.into() }
    }
    pub fn comma() -> Self { Self::new(b",") }
}

impl MergeOperator for StringAppendOperator {
    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, deltas: &[Vec<u8>]) -> Option<Vec<u8>> {
        let mut out = base.map(|b| b.to_vec()).unwrap_or_default();
        for d in deltas {
            if !out.is_empty() { out.extend_from_slice(&self.separator); }
            out.extend_from_slice(d);
        }
        Some(out)
    }

    fn partial_merge(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Option<Vec<u8>> {
        let mut out = left.to_vec();
        out.extend_from_slice(&self.separator);
        out.extend_from_slice(right);
        Some(out)
    }

    fn name(&self) -> &str { "string_append" }
}

// ---- MergeState: pending deltas for a key ----------------------------------

/// Collects pending deltas for one key, applies them on demand.
pub struct MergeState {
    pub deltas: Vec<MergeDelta>,
}

impl MergeState {
    pub fn new() -> Self { Self { deltas: Vec::new() } }

    pub fn push(&mut self, delta: MergeDelta) {
        self.deltas.push(delta);
    }

    /// Apply all pending deltas on top of `base`, using `op`.
    pub fn resolve(&self, key: &[u8], base: Option<&[u8]>, op: &dyn MergeOperator) -> Option<Vec<u8>> {
        if self.deltas.is_empty() { return base.map(|b| b.to_vec()); }
        let raw: Vec<Vec<u8>> = self.deltas.iter().map(|d| d.delta.clone()).collect();
        op.full_merge(key, base, &raw)
    }
}

impl Default for MergeState { fn default() -> Self { Self::new() } }
