// =============================================================
// Snapshot Garbage Collection (#new)
//
// Problem: with MVCC, every write produces a new version of a key.
// Compaction currently drops tombstones only at the deepest level.
// But intermediate versions of a key (version at seq=5, then at
// seq=10) can be dropped as soon as no live snapshot is pinned
// at a seq between 5 and 10 — they are invisible to every reader.
//
// Snapshot GC tracks the "oldest live snapshot seq" (also called
// the "safe horizon" in TiKV / CockroachDB terminology).  During
// compaction the merge step can drop any version of a key whose
// seq is below the safe horizon AND a newer version of the same
// key exists.
//
// Implementation
// --------------
// SnapshotRegistry: a shared set of active snapshot seqs.
//   - db.snapshot() registers the seq before returning.
//   - Snapshot::drop unregisters it.
//   - safe_horizon() = min(active_seqs) - 1 (or u64::MAX if empty).
//
// The compaction merge can then call:
//   filter_versions(entries, safe_horizon)
// to drop redundant old versions.
// =============================================================

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

// ---- SnapshotRegistry ------------------------------------------------------

#[derive(Clone, Default)]
pub struct SnapshotRegistry(Arc<Mutex<BTreeSet<u64>>>);

impl SnapshotRegistry {
    pub fn new() -> Self { Self::default() }

    /// Register a snapshot at `seq`.  Returns a guard that unregisters on drop.
    pub fn register(&self, seq: u64) -> SnapshotGuard {
        self.0.lock().unwrap().insert(seq);
        SnapshotGuard { registry: self.clone(), seq }
    }

    /// The oldest seq still held by a live snapshot.
    /// Versions strictly older than this can be GC'd during compaction.
    pub fn safe_horizon(&self) -> u64 {
        self.0.lock().unwrap().iter().copied().next()
            .map(|s| s.saturating_sub(1))
            .unwrap_or(u64::MAX) // no live snapshots → everything is safe to GC
    }

    /// Number of currently active snapshots.
    pub fn active_count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

/// RAII guard: releases a snapshot registration on drop.
pub struct SnapshotGuard {
    registry: SnapshotRegistry,
    seq:      u64,
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.registry.0.lock().unwrap().remove(&self.seq);
    }
}

// ---- Version pruning -------------------------------------------------------

/// Remove redundant old versions from a sorted, merged entry list.
///
/// An entry (key, seq, val) is redundant if:
///   1. A newer entry for the same key exists (seq_newer > seq), AND
///   2. seq ≤ safe_horizon  (no live snapshot needs this version).
///
/// Tombstones are handled correctly: if the newest version of a key is a
/// tombstone AND tombstone_seq ≤ safe_horizon AND `at_deepest_level`, the
/// tombstone itself is also dropped (standard tombstone compaction).
pub fn filter_versions(
    entries: Vec<(Vec<u8>, u64, Option<Vec<u8>>)>,
    safe_horizon: u64,
    at_deepest_level: bool,
) -> Vec<(Vec<u8>, u64, Option<Vec<u8>>)> {
    // Group by key; for each key keep only the versions that are either:
    //   - the latest version, OR
    //   - have seq > safe_horizon (still needed by some snapshot)
    use std::collections::BTreeMap;

    // Build per-key version lists (already sorted by key, but may have multiple entries per key
    // from different levels).
    let mut per_key: BTreeMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>> = BTreeMap::new();
    for (key, seq, val) in entries {
        per_key.entry(key).or_default().push((seq, val));
    }

    let mut out = Vec::new();
    for (key, mut versions) in per_key {
        // Sort newest-first.
        versions.sort_by(|a, b| b.0.cmp(&a.0));

        let newest_seq = versions[0].0;
        let newest_is_tombstone = versions[0].1.is_none();

        // Skip the tombstone at the deepest level if GC-safe.
        if at_deepest_level && newest_is_tombstone && newest_seq <= safe_horizon {
            continue; // drop the tombstone — the key is fully gone
        }

        for (seq, val) in versions {
            let is_latest = seq == newest_seq;
            // Keep if: (a) latest version, or (b) still needed by a live snapshot.
            if is_latest || seq > safe_horizon {
                out.push((key.clone(), seq, val));
            }
            // else: redundant old version, drop it
        }
    }

    // Re-sort by key (BTreeMap preserves key order but versions within a key
    // were reversed above).
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}
