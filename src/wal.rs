// WAL — Write-Ahead Log
//
// Every mutation is written here BEFORE touching the MemTable.
// This is the "write-ahead" guarantee: if the process crashes
// between the WAL write and the MemTable update, the WAL record
// still exists on disk and will be replayed on next open().
//
// If we wrote to the MemTable first and crashed before the WAL
// write, the record would be silently lost — an unacceptable
// data-loss scenario.
//
// The WAL is truncated (replaced with a fresh empty file) each
// time the MemTable is flushed to an SSTable. After a successful
// flush the SSTable IS the durable record; the WAL entries it
// covered are no longer needed for recovery.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// A single WAL record.
#[derive(Debug)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    /// Open (or create) a WAL file at `path`.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Wal> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a Put record.
    pub fn append_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        // Format: [type:1][key_len:4][val_len:4][val]
        self.writer.write_all(&[0u8])?;
        self.writer.write_all(&key.len().to_be_bytes())?;
        self.writer.write_all(&key)?;
        self.writer.write_all(&value.len().to_be_bytes())?;
        self.writer.write_all(&value)?;
        self.writer.flush()
    }

    /// Append a Delete (tombstone) record.
    pub fn append_delete(&mut self, key: Vec<u8>) -> io::Result<()> {
        // Format: [type:1][key_len:4][key]
        self.writer.write_all(&[1u8])?;
        self.writer.write_all(&key.len().to_be_bytes())?;
        self.writer.write_all(&key)?;
        self.writer.flush()
    }

    /// Read all  records from an existing WAL file (for recovery).
    pub fn recover(path: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let key = match read_bytes(&mut reader) {
                Ok(k) => k,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            if tag[0] == 0 {
                let value = match read_bytes(&mut reader) {
                    Ok(v) => v,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                };
                records.push(WalRecord::Put { key, value });
            } else {
                records.push(WalRecord::Delete { key });
            }
        }
        Ok(records)
    }
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}