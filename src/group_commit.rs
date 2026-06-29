// =============================================================
// WAL Group Commit (#new)
//
// Problem: every `put_cf` calls `WAL.flush()` (an fsync) on its
// own.  Under concurrent writes that is N fsyncs per N writers —
// the dominant latency on any spinning disk or even NVMe.
//
// Group commit: multiple writers queue their mutation into a
// pending batch; one elected "leader" writer issues a single
// fsync on behalf of all of them.  This is how PostgreSQL's WAL
// writer, MySQL's group commit, and RocksDB's write pipeline work.
//
// Implementation
// --------------
//   1. Each writer calls `GroupCommitWal::submit(record)`.
//   2. The call appends the record to an in-memory queue protected
//      by a Mutex, increments a generation counter, and parks the
//      calling thread.
//   3. The first writer to acquire the lock becomes the "leader"
//      for that batch.  It drains the queue, writes and flushes
//      the WAL once, then wakes up all parked writers.
//   4. Writers that arrived while the flush was in progress
//      ("followers") see the generation advance past their own and
//      return immediately — their data was included in the leader's
//      flush.
//
// The net result: under N concurrent writers only 1 fsync is
// issued per "window" (≈ the duration of one flush), giving
// O(1) fsyncs per time unit instead of O(N).
// =============================================================

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use crate::wal::WalRecord;

struct Batch {
    records: Vec<WalRecord>,
    /// Every flush increments this.  Writers compare their captured
    /// generation to know if their data has already been flushed.
    generation: u64,
}

struct Inner {
    batch: Mutex<Batch>,
    flush_done: Condvar,
    writer: Mutex<BufWriter<File>>,
}

/// A WAL wrapper that batches concurrent writes into a single fsync.
///
/// Drop-in replacement for `Wal` when multiple threads write
/// through a `SharedLsmEngine`.  The single-writer path (tests,
/// sequential benchmarks) has virtually identical latency.
pub struct GroupCommitWal {
    inner: Arc<Inner>,
}

impl GroupCommitWal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Inner {
                batch: Mutex::new(Batch { records: Vec::new(), generation: 0 }),
                flush_done: Condvar::new(),
                writer: Mutex::new(BufWriter::new(file)),
            }),
        })
    }

    /// Submit a WAL record.  Blocks until the record (plus any other
    /// concurrently submitted records) has been written and flushed.
    pub fn submit(&self, record: WalRecord) -> io::Result<()> {
        let my_gen = {
            let mut batch = self.inner.batch.lock().unwrap();
            batch.records.push(record);
            batch.generation
        };

        // Try to become the leader.  The leader is the first thread
        // to successfully lock `writer` while there are pending records.
        let flush_result = {
            let mut writer = self.inner.writer.lock().unwrap();
            let mut batch = self.inner.batch.lock().unwrap();

            // Another thread may have flushed our record already.
            if batch.generation > my_gen {
                return Ok(());
            }

            // We are the leader — drain and flush.
            let to_flush = std::mem::replace(&mut batch.records, Vec::new());
            drop(batch); // release batch lock while doing I/O

            let mut write_err: Option<io::Error> = None;
            for rec in to_flush {
                if let Err(e) = write_record(&mut *writer, &rec) {
                    write_err = Some(e);
                    break;
                }
            }
            if write_err.is_none() {
                if let Err(e) = writer.flush() { write_err = Some(e); }
            }
            write_err
        };

        // Advance generation and wake followers.
        {
            let mut batch = self.inner.batch.lock().unwrap();
            batch.generation += 1;
        }
        self.inner.flush_done.notify_all();

        // Wait if we were a follower who missed the leader's flush.
        // (This path is hit if the batch.lock() line above blocks until
        //  the leader is already writing — we'll see generation > my_gen.)
        {
            let batch = self.inner.batch.lock().unwrap();
            let _guard = self.inner.flush_done.wait_while(batch, |b| b.generation <= my_gen);
        }

        match flush_result {
            None    => Ok(()),
            Some(e) => Err(e),
        }
    }
}

fn write_record(w: &mut impl Write, rec: &WalRecord) -> io::Result<()> {
    match rec {
        WalRecord::Put { key, seq, value } => {
            let mut p = Vec::new();
            p.push(0x00u8);
            p.extend_from_slice(&(key.len() as u32).to_be_bytes());
            p.extend_from_slice(key);
            p.extend_from_slice(&seq.to_le_bytes());
            p.extend_from_slice(&(value.len() as u32).to_be_bytes());
            p.extend_from_slice(value);
            let crc = crc32fast::hash(&p);
            w.write_all(&p)?;
            w.write_all(&crc.to_le_bytes())
        }
        WalRecord::PutTtl { key, seq, value, expires_at } => {
            let mut p = Vec::new();
            p.push(0x02u8);
            p.extend_from_slice(&(key.len() as u32).to_be_bytes());
            p.extend_from_slice(key);
            p.extend_from_slice(&seq.to_le_bytes());
            p.extend_from_slice(&expires_at.to_le_bytes());
            p.extend_from_slice(&(value.len() as u32).to_be_bytes());
            p.extend_from_slice(value);
            let crc = crc32fast::hash(&p);
            w.write_all(&p)?;
            w.write_all(&crc.to_le_bytes())
        }
        WalRecord::Delete { key, seq } => {
            let mut p = Vec::new();
            p.push(0x01u8);
            p.extend_from_slice(&(key.len() as u32).to_be_bytes());
            p.extend_from_slice(key);
            p.extend_from_slice(&seq.to_le_bytes());
            let crc = crc32fast::hash(&p);
            w.write_all(&p)?;
            w.write_all(&crc.to_le_bytes())
        }
        WalRecord::DeleteRange { from, to, seq } => {
            let mut p = Vec::new();
            p.push(0x03u8);
            p.extend_from_slice(&(from.len() as u32).to_be_bytes());
            p.extend_from_slice(from);
            p.extend_from_slice(&(to.len() as u32).to_be_bytes());
            p.extend_from_slice(to);
            p.extend_from_slice(&seq.to_le_bytes());
            let crc = crc32fast::hash(&p);
            w.write_all(&p)?;
            w.write_all(&crc.to_le_bytes())
        }
    }
}
