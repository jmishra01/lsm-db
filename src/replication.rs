// =============================================================
// Replication (#12) — WAL-based leader → follower streaming
//
// Architecture
// ------------
//   ReplicationServer  — runs on the leader.  When a follower
//     connects it tails the WAL file of the requested CF and
//     streams WalRecord-encoded bytes over TCP.  New records are
//     detected by polling file size; an optional notify channel
//     allows zero-latency wake-up when the engine writes.
//
//   ReplicationClient  — runs on the follower.  Connects to the
//     leader, reads the stream, and applies each record to a
//     local SharedLsmEngine (follower's copy).
//
// Wire format (length-delimited framing)
// ----------------------------------------
//   Each record on the wire:
//     [frame_len: u32 BE][opcode: u8][key_len: u32 BE][key]
//                        [seq: u64 LE][val_len: u32 BE][val]?
//   opcode 0x00 = Put, 0x01 = Delete (same as WAL on-disk)
//   The receiver applies the record directly via put_cf /
//   delete_cf.  Sequence numbers are intentionally NOT forwarded
//   to the follower's engine (the follower's own write_seq is
//   monotonic and independent).
//
// This mirrors how real systems work: TiKV replicates Raft log
// entries (which are essentially WAL records); each follower
// advances its own state machine.
// =============================================================

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::wal::WalRecord;
use crate::SharedLsmEngine;

// ---- ReplicationServer -----------------------------------------------------

/// Tails one CF's WAL and streams records to every connected follower.
pub struct ReplicationServer {
    bind_addr: String,
    wal_path:  PathBuf,
    cf:        String,
}

impl ReplicationServer {
    /// `wal_path` should be the path to the WAL file for `cf`
    /// (e.g., `<db_dir>/default/wal`).
    pub fn new(bind_addr: impl Into<String>, wal_path: impl AsRef<Path>, cf: impl Into<String>) -> Self {
        Self {
            bind_addr: bind_addr.into(),
            wal_path:  wal_path.as_ref().to_path_buf(),
            cf:        cf.into(),
        }
    }

    /// Start accepting connections.  Each connection gets its own task that
    /// sends all existing WAL records first, then tails for new ones.
    pub async fn serve(self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        println!("[repl-server] listening on {} (cf={})", self.bind_addr, self.cf);

        let wal_path = Arc::new(self.wal_path.clone());

        loop {
            let (socket, peer) = listener.accept().await?;
            println!("[repl-server] follower connected: {peer}");
            let wal_path = Arc::clone(&wal_path);
            tokio::spawn(async move {
                if let Err(e) = stream_wal(socket, &wal_path).await {
                    eprintln!("[repl-server] stream error for {peer}: {e}");
                }
            });
        }
    }
}

/// Read all WAL records from `wal_path`, encode them, and send over `socket`.
/// Then poll for new records every 100 ms indefinitely.
async fn stream_wal(mut socket: TcpStream, wal_path: &Path) -> io::Result<()> {
    let mut sent_bytes: u64 = 0;

    loop {
        // Re-read file from `sent_bytes` to pick up new records.
        let file_len = match tokio::fs::metadata(wal_path).await {
            Ok(m)  => m.len(),
            Err(_) => { tokio::time::sleep(Duration::from_millis(100)).await; continue; }
        };

        if file_len > sent_bytes {
            // Read the tail we haven't sent yet.
            let tail = {
                let path = wal_path.to_path_buf();
                let from = sent_bytes;
                tokio::task::spawn_blocking(move || read_wal_tail(&path, from)).await
                    .map_err(|e| io::Error::other(e))??
            };

            for (record, raw) in tail {
                let _ = record; // we only need the raw bytes for framing
                let frame_len = raw.len() as u32;
                socket.write_all(&frame_len.to_be_bytes()).await?;
                socket.write_all(&raw).await?;
                sent_bytes += raw.len() as u64;
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Blocking: read raw WAL frames starting at byte offset `from`.
/// Returns (WalRecord, raw_bytes_for_that_record).
fn read_wal_tail(path: &Path, from: u64) -> io::Result<Vec<(WalRecord, Vec<u8>)>> {
    use std::fs::File;
    use std::io::{BufReader, Seek, SeekFrom};

    let file = match File::open(path) {
        Ok(f)  => f,
        Err(_) => return Ok(vec![]),
    };
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(from))?;

    // Re-use WalRecord recovery logic: recover reads the whole file, but we
    // only need records after `from`.  For simplicity we re-recover from `from`
    // using the same raw reader and capture the raw bytes alongside.
    WalRecord::recover_from_reader_with_raw(&mut reader)
}

// ---- ReplicationClient -----------------------------------------------------

/// Connects to a ReplicationServer and applies records to a local engine.
pub struct ReplicationClient {
    server_addr: String,
    db:          SharedLsmEngine,
    cf:          String,
}

impl ReplicationClient {
    pub fn new(server_addr: impl Into<String>, db: SharedLsmEngine, cf: impl Into<String>) -> Self {
        Self { server_addr: server_addr.into(), db, cf: cf.into() }
    }

    /// Connect and start applying records.  Reconnects on error with backoff.
    pub async fn run(self) -> io::Result<()> {
        let mut backoff = Duration::from_millis(200);
        loop {
            println!("[repl-client] connecting to {}", self.server_addr);
            match TcpStream::connect(&self.server_addr).await {
                Ok(mut socket) => {
                    backoff = Duration::from_millis(200);
                    if let Err(e) = self.receive_loop(&mut socket).await {
                        eprintln!("[repl-client] disconnected: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("[repl-client] connect error: {e}");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    async fn receive_loop(&self, socket: &mut TcpStream) -> io::Result<()> {
        loop {
            // Read 4-byte frame length.
            let mut len_buf = [0u8; 4];
            socket.read_exact(&mut len_buf).await?;
            let frame_len = u32::from_be_bytes(len_buf) as usize;

            let mut frame = vec![0u8; frame_len];
            socket.read_exact(&mut frame).await?;

            // Decode the frame as a WAL record.
            let record = WalRecord::decode_frame(&frame)?;
            match record {
                WalRecord::Put { key, value, .. } => {
                    self.db.put_cf(&self.cf, key, value)
                        .map_err(io::Error::other)?;
                }
                WalRecord::Delete { key, .. } => {
                    self.db.delete_cf(&self.cf, key)
                        .map_err(io::Error::other)?;
                }
            }
        }
    }
}
