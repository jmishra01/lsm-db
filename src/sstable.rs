// =============================================================
// SSTable -- Sorted String Table (immutable on-disk file)
//
// File layout
// -----------
//  [ DATA SECTION ]
//  For each entry (sorted by key):
//      key_len     : u32 LE
//      key         : [u8; key_len]
//      val_tag     : u8    (0 = live, 1 = tombstone)
//      val_len     : u32 LE    (only if tag == 0)
//      val         : [u8; val_len] (only if tag == 0)
//
//  [ INDEX SECTION ]
//  For each entry:
//      key_len     : u32 LE
//      key         : [u8; key_len]
//      offset      : u64 LE (byte offset of this entry in DATA)
//
//  [ FOOTER (fixed 40 bytes) ]
//      index_offset    : u64 LE
//      bloom_offset    : u64 LE
//      bloom_len       : u64 LE
//      entry_count     : u64 LE
//      magic           : u64 LE = 0xDEAD_BEEF_CAFE_BABE
// =============================================================

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{ Path, PathBuf};

use crate::bloom::BloomFilter;
use crate::memtable::MemTable;

const MAGIC: u64 = 0xDEAD_BEEF_CAFE_BABE;
const FOOTER_SIZE: usize = 40;

/// An in-memory handle to an on-disk SSTable.
pub struct SSTable {
    pub path: PathBuf,
    /// Sparse in-memory index: key -> data-section byte offset.
    pub index: BTreeMap<Vec<u8>, u64>,
    pub bloom: BloomFilter,
    pub entry_count: usize,
    pub level: u32
}


impl SSTable {
    // -- Writer

    /// Flush a MemTable to a new SSTable at `path`.
    pub fn write_from_memtable(path: impl AsRef<Path>, mem: &MemTable, level: u32) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let entry_count = mem.iter().count();
        let mut bloom = BloomFilter::new(entry_count.max(1));
        let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut data_offset: u64 = 0;

        // -- DATA SECTION
        for (key, val_opt) in mem.iter() {
            index.insert(key.clone(), data_offset);
            bloom.insert(key);

            let klen = key.len() as u32;
            w.write_all(&klen.to_le_bytes())?;
            w.write_all(key)?;
            data_offset += 4 + key.len() as u64;

            match val_opt {
                Some(val) => {
                    w.write_all(&[0u8])?;
                    w.write_all(&val)?;
                    data_offset += 1 + 4 + val.len() as u64;
                },
                None => {
                    w.write_all(&[1u8])?;
                    data_offset += 1;
                }
            }
        }


        // -- INDEX SECTION
        let index_offset = data_offset;
        for (key, &off) in &index {
            let klen = key.len() as u32;
            w.write_all(&klen.to_le_bytes())?;
            w.write_all(key)?;
            w.write_all(&off.to_le_bytes())?;
        }

        // -- BLOOM SECTION
        let bloom_bytes = bloom.to_bytes();
        let bloom_offset = index_offset + index.iter().map(|(k, _)| 4 + k.len() as u64 + 8).sum::<u64>();
        let bloom_len = bloom_bytes.len() as u64;
        w.write_all(&bloom_bytes)?;

        // -- FOOTER
        w.write_all(&index_offset.to_le_bytes())?;
        w.write_all(&bloom_offset.to_le_bytes())?;
        w.write_all(&bloom_len.to_le_bytes())?;
        w.write_all(&(entry_count as u64).to_le_bytes())?;
        w.write_all(&MAGIC.to_le_bytes())?;
        w.flush()?;

        Ok(Self { path, index, bloom, entry_count, level })

    }
    // -- Reader

    /// Load SSTable metadata (index + bloom) from disk.
    pub fn open(path: impl AsRef<Path>, level: u32) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut f = File::open(&path)?;
        let file_size = f.seek(SeekFrom::End(0))?;

        // Read footer
        f.seek(SeekFrom::Start(file_size - FOOTER_SIZE as u64))?;
        let mut footer = [0u8; 40];
        f.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let entry_count = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let magic = u64::from_le_bytes(footer[32..40].try_into().unwrap());

        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Bad SSTable magic"));
        }

        // Read bloom
        f.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_bytes = vec![0u8; bloom_len as usize];
        f.read_exact(&mut bloom_bytes)?;
        let bloom = BloomFilter::from_bytes(&bloom_bytes);

        // Read index
        f.seek(SeekFrom::Start(index_offset))?;
        let index_end = bloom_offset;
        let mut reader = BufReader::new(&mut f);
        let mut index = BTreeMap::new();
        let mut pos = index_offset;
        while pos < index_end {
            let key = read_bytes_r(&mut reader)?;
            let mut off_buf = [0u8; 8];
            reader.read_exact(&mut off_buf)?;
            let off = u64::from_le_bytes(off_buf);
            pos += 4 + key.len() as u64 + 8;
            index.insert(key, off);
        }

        Ok(Self { path, index, bloom, entry_count, level })
    }

    /// Look up a key. Returns `Some(Some(v))`, `Some(None)` (tombstone), or `None`.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        // Find the largest index key =< query key
        let offset = match self.index.range(..=key.to_vec()).next_back() {
            Some((_, &offset)) => offset,
            None => return Ok(None)
        };
        // Scan forward from the offset
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(f);

        // Scan until we either find the key or pass it
        let index_start = self.index_section_start();
        let mut cursor = offset;
        loop {
            if cursor >= index_start {
                break;
            }

            let k = read_bytes_r(&mut reader)?;
            let tag_buf = &mut [0u8; 1];
            reader.read_exact(tag_buf)?;
            let entry_start_cursor = cursor;
            cursor += 4 + k.len() as u64 + 1;

            if tag_buf[0] == 0 {
                // live value
                let v = read_bytes_r(&mut reader)?;
                cursor += 4 + v.len() as u64;
                if k == key {
                    return Ok(Some(Some(v)));
                }
            } else {
                // tombstone
                if k == key {
                    return Ok(Some(None));
                }
            }

            if k.as_slice() > key {
                break; // passed the key
            }
            let _ = entry_start_cursor; // silence warning
        }
        Ok(None)
    }

    /// Full scan -- returns all entries in sorted order (for compaction).
    pub fn scan_all(&self) -> io::Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(f);
        let mut entries = Vec::new();
        let index_start = self.index_section_start();
        let mut cursor = 0u64;

        loop {
            if cursor >= index_start {
                break;
            }

            let k = read_bytes_r(&mut reader)?;
            let mut tag = [0u8; 1];
            reader.read_exact(&mut tag)?;
            cursor += 4 + k.len() as u64 + 1;

            if tag[0] == 0 {
                let v = read_bytes_r(&mut reader)?;
                cursor += 4 + v.len() as u64;
                entries.push((k, Some(v)));
            } else {
                entries.push((k, None));
            }
        }
        Ok(entries)
    }

    fn index_section_start(&self) -> u64 {
        // Sum of all data entries = first index key's stored offset + its entry size
        // Easiest: just re-derive from the index (first entry offset is always 0)
        // We store index_offset in the footer; re-read it here lazilu via bloom trick:
        // Instread, scan: max data offset + size of that entry. But simpler -- the
        // smallest index key always has offset 0, so we use stored offsets.
        // Actually the footer has index_offset. We need to open the file.
        // To avoid re-opening, we precompute and store it.
        // For now: we'll compute it from the index map (keys map to data offsets).
        // The section starts where the last data entry ends -- which equals the
        // maximum offset + that entry's byte size. We don't store that here, so read footer
        // from disk.
        // *** Simpler: store index_offset at construction time. ***
        // This is a design shortcut -- in production you'd cache it.
        // We'll open + read footer on demand. For performance this is fine since scan_all
        // is only called during compaction.
        read_index_offset_from_file(&self.path).unwrap_or(u64::MAX)
    }

    pub fn delete_file(&self) -> io::Result<()> {
        fs::remove_file(&self.path)
    }
}


fn read_index_offset_from_file(path: &Path) -> io::Result<u64> {
    let mut f = File::open(path)?;
    let size = f.seek(SeekFrom::End(0))?;
    f.seek(SeekFrom::Start(size - FOOTER_SIZE as u64))?;
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}


fn read_bytes_r(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}