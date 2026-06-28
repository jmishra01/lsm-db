// =====================================================================
// Manifest — durable log of which SSTable files exist at which level.
//
// Why does the manifest exist?
// ----------------------------
// Without it, the engine re-scans the db directory on open() to
// discover SSTable files. That has two problems:
//
//  1. No atomicity during compaction. Compaction writes a new file then
//     deletes old ones. If the process crashes between those two steps,
//     on the next open() the engine sees *both* the old and new files —
//     it cannot tell which represents the current committed state.
//
//  2. No source of truth. Any .sst file on disk looks valid. There is no
//     way to distinguish "current file" from "leftover garbage from a
//     previous crashed compaction".
//
// The manifest solves both problems. It is an append-only binary log.
// Every change to the set of live SSTables (flush, compaction result) is
// recorded as a manifest edit *before* the old files are deleted. On
// recovery, replaying the manifest gives the exact set of live files.
//
// Crash-safety argument
// ---------------------
//  Flush:
//    1. Write new SST file to disk.
//    2. Append AddFile(new) to manifest. ← durable after flush()
//    3. Truncate WAL.
//    If crash between 1 and 2: manifest has no record of new file; engine
//      re-flushes from WAL on next open. The orphan SST is harmless garbage.
//    If crash between 2 and 3: manifest records the file; WAL replay adds
//      duplicate keys that the SST already covers — harmless (MemTable wins).
//
//  Compaction:
//    1. Write new merged SST file.
//    2. Append RemoveFile(old…) + AddFile(new) to manifest.
//    3. Delete old SST files from disk.
//    If crash between 1 and 2: old files still in manifest, old state intact.
//    If crash between 2 and 3: manifest is already updated (new state); old
//      files still on disk but not in manifest — they're unreferenced garbage.
//
// Wire format (each record)
// -------------------------
//  [payload_len : u32 LE]
//  [payload     : [u8; payload_len]]
//    [type : u8]
//    ... type-specific fields ...
//  [crc32       : u32 LE]   covers payload bytes only
//
//  CreateCF (0x01):  [cf_name: lp-string]
//  AddFile  (0x02):  [level: u8][cf_name: lp-string][filename: lp-string]
//  RemoveFile(0x03): [cf_name: lp-string][filename: lp-string]
//
//  lp-string = [len: u16 LE][utf8 bytes]
// =====================================================================

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub enum ManifestRecord {
    CreateCF { name: String },
    AddFile  { cf: String, level: u32, filename: String },
    RemoveFile { cf: String, filename: String },
}

/// Replayed state: which CFs exist and which files are live at which level.
#[derive(Default)]
pub struct ManifestState {
    pub cfs: HashSet<String>,
    /// cf_name → Vec<(level, filename)> of current live files
    pub files: HashMap<String, Vec<(u32, String)>>,
}

pub struct Manifest {
    writer: BufWriter<File>,
}

impl Manifest {
    /// Open (or create) the manifest for writing.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { writer: BufWriter::new(file) })
    }

    /// Append one record to the manifest. Flushes to disk immediately.
    pub fn append(&mut self, record: &ManifestRecord) -> io::Result<()> {
        let payload = encode(record);
        let crc = crc32fast::hash(&payload);
        self.writer.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.flush()
    }

    /// Replay all records from an existing manifest file.
    /// Returns an empty `ManifestState` if the file does not exist.
    pub fn recover(path: impl AsRef<Path>) -> io::Result<ManifestState> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ManifestState::default()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut state = ManifestState::default();

        loop {
            // Record length prefix
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Payload
            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            // CRC
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);

            if stored_crc != computed_crc {
                eprintln!(
                    "MANIFEST: CRC mismatch — stopping recovery at corrupt record \
                     (applied {} CFs, {} total file entries so far)",
                    state.cfs.len(),
                    state.files.values().map(|v| v.len()).sum::<usize>(),
                );
                break;
            }

            if let Some(record) = decode(&payload) {
                apply_record(&mut state, record);
            }
        }

        Ok(state)
    }
}

fn apply_record(state: &mut ManifestState, record: ManifestRecord) {
    match record {
        ManifestRecord::CreateCF { name } => {
            state.cfs.insert(name);
        }
        ManifestRecord::AddFile { cf, level, filename } => {
            state.files.entry(cf).or_default().push((level, filename));
        }
        ManifestRecord::RemoveFile { cf, filename } => {
            if let Some(files) = state.files.get_mut(&cf) {
                files.retain(|(_, f)| f != &filename);
            }
        }
    }
}

// ---- Encode / decode -------------------------------------------------------

fn write_lp_string(s: &str, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn encode(record: &ManifestRecord) -> Vec<u8> {
    let mut buf = Vec::new();
    match record {
        ManifestRecord::CreateCF { name } => {
            buf.push(0x01);
            write_lp_string(name, &mut buf);
        }
        ManifestRecord::AddFile { cf, level, filename } => {
            buf.push(0x02);
            buf.push(*level as u8);
            write_lp_string(cf, &mut buf);
            write_lp_string(filename, &mut buf);
        }
        ManifestRecord::RemoveFile { cf, filename } => {
            buf.push(0x03);
            write_lp_string(cf, &mut buf);
            write_lp_string(filename, &mut buf);
        }
    }
    buf
}

fn read_lp_string(data: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > data.len() { return None; }
    let len = u16::from_le_bytes(data[*pos..*pos + 2].try_into().ok()?) as usize;
    *pos += 2;
    if *pos + len > data.len() { return None; }
    let s = String::from_utf8(data[*pos..*pos + len].to_vec()).ok()?;
    *pos += len;
    Some(s)
}

fn decode(payload: &[u8]) -> Option<ManifestRecord> {
    if payload.is_empty() { return None; }
    let mut pos = 0usize;
    let tag = payload[pos]; pos += 1;
    match tag {
        0x01 => {
            let name = read_lp_string(payload, &mut pos)?;
            Some(ManifestRecord::CreateCF { name })
        }
        0x02 => {
            if pos >= payload.len() { return None; }
            let level = payload[pos] as u32; pos += 1;
            let cf = read_lp_string(payload, &mut pos)?;
            let filename = read_lp_string(payload, &mut pos)?;
            Some(ManifestRecord::AddFile { cf, level, filename })
        }
        0x03 => {
            let cf = read_lp_string(payload, &mut pos)?;
            let filename = read_lp_string(payload, &mut pos)?;
            Some(ManifestRecord::RemoveFile { cf, filename })
        }
        _ => None,
    }
}
