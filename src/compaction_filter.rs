// =============================================================
// Compaction Filter (#new)
//
// A user-supplied closure called on every (key, value) pair
// during an SSTable merge.  The filter can:
//   - Keep(value)       — emit the entry unchanged
//   - Replace(new_val)  — change the value (e.g. re-encode, strip fields)
//   - Remove            — drop the entry entirely (like a tombstone)
//
// The most common use cases:
//   1. TTL — drop entries whose expiry timestamp is in the past.
//      (Already handled inside the MemTable/cursor layer, but compaction
//       filters are the only way to physically reclaim space in SSTables.)
//   2. Secondary index cleanup — when the primary record is deleted,
//      the filter can remove corresponding index entries.
//   3. Value transformation — strip PII fields from old records at compaction
//      time without rewriting the whole database.
//
// This matches RocksDB's CompactionFilter interface (simplified).
// =============================================================

/// Decision returned by a CompactionFilter for one entry.
#[derive(Debug)]
pub enum FilterDecision {
    /// Keep the entry as-is.
    Keep,
    /// Replace the stored value with a new one.
    Replace(Vec<u8>),
    /// Drop the entry entirely (physically deleted during compaction).
    Remove,
}

/// Trait for user-defined compaction filters.
pub trait CompactionFilter: Send + Sync {
    /// Called for every live (non-tombstone) entry during a compaction merge.
    /// `key` and `value` are the entry being evaluated.
    fn filter(&self, key: &[u8], value: &[u8]) -> FilterDecision;

    /// Human-readable name for logging / debugging.
    fn name(&self) -> &str { "unnamed_filter" }
}

// ---- Built-in filters ------------------------------------------------------

/// Filter that drops entries whose value encodes a Unix-ms expiry timestamp
/// in the first 8 bytes (big-endian u64).  Values shorter than 8 bytes are kept.
pub struct ExpiryPrefixFilter;

impl CompactionFilter for ExpiryPrefixFilter {
    fn filter(&self, _key: &[u8], value: &[u8]) -> FilterDecision {
        if value.len() < 8 { return FilterDecision::Keep; }
        let ts = u64::from_be_bytes(value[..8].try_into().unwrap());
        if ts == 0 { return FilterDecision::Keep; }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if now > ts { FilterDecision::Remove } else { FilterDecision::Keep }
    }
    fn name(&self) -> &str { "expiry_prefix_filter" }
}

/// Prefix-based filter: remove all keys that start with a given prefix.
pub struct PrefixDropFilter {
    prefix: Vec<u8>,
}

impl PrefixDropFilter {
    pub fn new(prefix: impl Into<Vec<u8>>) -> Self {
        Self { prefix: prefix.into() }
    }
}

impl CompactionFilter for PrefixDropFilter {
    fn filter(&self, key: &[u8], _value: &[u8]) -> FilterDecision {
        if key.starts_with(&self.prefix) { FilterDecision::Remove }
        else { FilterDecision::Keep }
    }
    fn name(&self) -> &str { "prefix_drop_filter" }
}

/// Closure-based filter for ad-hoc use.
pub struct FnFilter<F> {
    name: String,
    f: F,
}

impl<F> FnFilter<F>
where
    F: Fn(&[u8], &[u8]) -> FilterDecision + Send + Sync,
{
    pub fn new(name: impl Into<String>, f: F) -> Self {
        Self { name: name.into(), f }
    }
}

impl<F> CompactionFilter for FnFilter<F>
where
    F: Fn(&[u8], &[u8]) -> FilterDecision + Send + Sync,
{
    fn filter(&self, key: &[u8], value: &[u8]) -> FilterDecision {
        (self.f)(key, value)
    }
    fn name(&self) -> &str { &self.name }
}

// ---- Helper: apply filter to a merged entry list ---------------------------

/// Run `filter` over a sorted entry list (output of compaction::merge_entries).
/// Tombstones are passed through unchanged; only live entries are filtered.
pub fn apply_filter(
    entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    filter: &dyn CompactionFilter,
) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
    entries.into_iter().filter_map(|(key, seq, val_opt)| {
        match val_opt {
            None => Some((key, seq, None)), // tombstone: pass through
            Some(val) => match filter.filter(&key, &val) {
                FilterDecision::Keep           => Some((key, seq, Some(val))),
                FilterDecision::Replace(new_v) => Some((key, seq, Some(new_v))),
                FilterDecision::Remove         => None, // physically drop
            }
        }
    }).collect()
}
