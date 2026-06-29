// =============================================================
// Optimistic Concurrency Control — OCC (#new)
//
// WriteBatch provides atomicity ("all or nothing at the seq
// boundary") but not conflict detection: two concurrent
// read-modify-write transactions can silently overwrite each other.
//
// OCC adds a lightweight conflict layer on top of the engine:
//
//   1. Begin: record the current write_seq as the "read horizon".
//   2. Read:  use the engine's MVCC snapshot at read_horizon.
//   3. Modify: buffer mutations locally (no engine writes yet).
//   4. Commit: verify that none of the keys you read have been
//              written since read_horizon.  If clean, atomically
//              apply the batch.  If dirty, return Err(Conflict).
//
// This is the same approach used by FoundationDB's transaction
// layer, and is the basis of Serializable Snapshot Isolation (SSI)
// in PostgreSQL and CockroachDB.
//
// Trade-offs vs. locking (pessimistic CC)
// ----------------------------------------
//   + No lock contention under low conflict rates → higher throughput.
//   + Deadlock-free by construction.
//   - Under high conflict rates → high abort rate → wasted work.
//   - Caller must implement retry logic on Conflict errors.
// =============================================================

use std::collections::HashMap;
use std::io;

use crate::engine::LsmEngine;
use crate::snapshot::WriteBatch;

/// Error returned when a transaction conflicts with a concurrent write.
#[derive(Debug)]
pub struct ConflictError {
    pub conflicting_keys: Vec<Vec<u8>>,
}

impl std::fmt::Display for ConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OCC conflict on {} key(s)", self.conflicting_keys.len())
    }
}

impl std::error::Error for ConflictError {}

/// An OCC transaction against a single `LsmEngine`.
///
/// Usage:
/// ```no_run
/// let mut tx = OccTransaction::begin(&db);
/// let val = tx.get(b"balance")?;
/// tx.put("balance".into(), b"new_val".to_vec());
/// match tx.commit(&mut db) {
///     Ok(())                => println!("committed"),
///     Err(OccError::Conflict(e)) => println!("retry: {e}"),
///     Err(OccError::Io(e))       => return Err(e.into()),
/// }
/// ```
pub struct OccTransaction {
    /// Snapshot seq at which this transaction started.
    read_horizon: u64,
    /// CF → keys read by this transaction.
    read_set: HashMap<String, Vec<Vec<u8>>>,
    /// Mutations buffered locally.
    write_buf: WriteBatch,
    /// Snapshot data at read_horizon (used for local reads).
    snapshot: HashMap<String, crate::snapshot::Snapshot>,
}

#[derive(Debug)]
pub enum OccError {
    Conflict(ConflictError),
    Io(io::Error),
}

impl From<io::Error> for OccError {
    fn from(e: io::Error) -> Self { OccError::Io(e) }
}

impl OccTransaction {
    /// Start a new transaction.  Captures a snapshot of the engine.
    pub fn begin(db: &LsmEngine) -> io::Result<Self> {
        let read_horizon = db.write_seq().saturating_sub(1);

        // Capture snapshots for the default CF (extend for multi-CF if needed).
        let snap = db.snapshot_cf("default")?;
        let mut snapshot = HashMap::new();
        snapshot.insert("default".to_string(), snap);

        Ok(Self {
            read_horizon,
            read_set: HashMap::new(),
            write_buf: WriteBatch::new(),
            snapshot,
        })
    }

    /// Read a key through the transaction's snapshot.
    /// Adds the key to the read set so it will be checked on commit.
    pub fn get(&mut self, key: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        self.get_cf("default", key)
    }

    pub fn get_cf(&mut self, cf: &str, key: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        let key = key.as_ref();
        self.read_set.entry(cf.to_string()).or_default().push(key.to_vec());

        // Look in write_buf first (read-your-own-writes).
        for (wcf, wkey, wval) in &self.write_buf.entries {
            if wcf == cf && wkey == key {
                return wval.clone();
            }
        }

        // Fall back to snapshot.
        self.snapshot.get(cf)?.get(key).map(|v| v.to_vec())
    }

    /// Buffer a put (does not write to engine yet).
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.put_cf("default", key, value);
    }

    pub fn put_cf(&mut self, cf: &str, key: Vec<u8>, value: Vec<u8>) {
        self.write_buf.put(cf, key, value);
    }

    /// Buffer a delete (does not write to engine yet).
    pub fn delete(&mut self, key: Vec<u8>) {
        self.delete_cf("default", key);
    }

    pub fn delete_cf(&mut self, cf: &str, key: Vec<u8>) {
        self.write_buf.delete(cf, key);
    }

    /// Validate and commit.
    ///
    /// Checks that no key in the read_set has been written since
    /// `read_horizon`.  If the check passes, the write_buf is committed
    /// atomically via `write_batch`.
    pub fn commit(self, db: &mut LsmEngine) -> Result<(), OccError> {
        // Conflict check: for each key in the read_set, verify that the
        // latest version's write_seq ≤ read_horizon.  We approximate this
        // by taking a fresh snapshot (at the current write_seq - 1) and
        // checking the seq field exposed by the cursor.
        let mut conflicts: Vec<Vec<u8>> = Vec::new();

        for (cf, keys) in &self.read_set {
            for key in keys {
                // Check using the low-level cursor API.
                let seq = db.key_write_seq(cf, key).map_err(OccError::Io)?;
                if let Some(s) = seq {
                    if s > self.read_horizon {
                        conflicts.push(key.clone());
                    }
                }
            }
        }

        if !conflicts.is_empty() {
            return Err(OccError::Conflict(ConflictError { conflicting_keys: conflicts }));
        }

        db.write_batch(self.write_buf).map_err(OccError::Io)
    }
}
