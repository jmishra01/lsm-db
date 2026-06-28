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
//
// CRC32 checksums (#5)
// --------------------
// Each record has a 4-byte CRC32 appended. The CRC covers all
// payload bytes in that record (tag + key_len + key [+ val_len + val]).
//
// On recovery, if the CRC of a record does not match the stored value
// we stop replaying immediately. Everything before the corrupt record
// is safe; everything from that point on is discarded. This handles:
//   - Bit-rot: silent hardware corruption of stored bytes.
//   - Torn writes: crash mid-write leaves a partial record whose
//     bytes happen to decode as valid lengths but with wrong content.
//
// Record wire format
// ------------------
//  Put:    [0x00][key_len: u32 BE][key][val_len: u32 BE][val][crc32: u32 LE]
//  Delete: [0x01][key_len: u32 BE][key][crc32: u32 LE]

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

#[derive(Debug)]
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Wal> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { writer: BufWriter::new(file) })
    }

    pub fn append_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let mut payload = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
        payload.push(0u8);
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(&key);
        payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
        payload.extend_from_slice(&value);
        let crc = crc32fast::hash(&payload);
        self.writer.write_all(&payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.flush()
    }

    pub fn append_delete(&mut self, key: Vec<u8>) -> io::Result<()> {
        let mut payload = Vec::with_capacity(1 + 4 + key.len());
        payload.push(1u8);
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(&key);
        let crc = crc32fast::hash(&payload);
        self.writer.write_all(&payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.flush()
    }

    pub fn recover(path: impl AsRef<Path>) -> io::Result<Vec<WalRecord>> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            // Tag byte — clean EOF here means we're done
            let mut tag = [0u8; 1];
            match reader.read_exact(&mut tag) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let key = match read_bytes(&mut reader) {
                Ok(k) => k,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            let value = if tag[0] == 0 {
                match read_bytes(&mut reader) {
                    Ok(v) => Some(v),
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
            } else {
                None
            };

            // Read stored CRC
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let stored_crc = u32::from_le_bytes(crc_buf);

            // Recompute CRC from the exact bytes that were written
            let mut payload = vec![tag[0]];
            payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
            payload.extend_from_slice(&key);
            if let Some(ref v) = value {
                payload.extend_from_slice(&(v.len() as u32).to_be_bytes());
                payload.extend_from_slice(v);
            }
            let computed_crc = crc32fast::hash(&payload);

            if stored_crc != computed_crc {
                // Bit-rot or torn write detected. Stop here — do not replay
                // anything further; the bytes after this point are untrustworthy.
                eprintln!("WAL: CRC mismatch — stopping recovery at corrupt record (recovered {} records before it)", records.len());
                break;
            }

            match value {
                Some(v) => records.push(WalRecord::Put { key, value: v }),
                None => records.push(WalRecord::Delete { key }),
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
