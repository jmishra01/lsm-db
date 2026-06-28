// =============================================================
// SSTable v2 -- block-compressed Sorted String Table
//
// File layout
// -----------
//  [ DATA BLOCKS ]
//    For each block:
//      compressed_len   : u32 LE
//      compressed_data  : [u8; compressed_len]
//        (LZ4-decompress to get block body)
//        Block body:
//          entry_count  : u32 LE
//          For each entry:
//            key_len    : u32 LE
//            key        : [u8; key_len]
//            val_tag    : u8          (0 = live, 1 = tombstone)
//            val_len    : u32 LE      (only if tag == 0)
//            val        : [u8; val_len] (only if tag == 0)
//
//  [ SPARSE INDEX ]
//    For each block:
//      key_len          : u32 LE
//      first_key        : [u8; key_len]
//      block_offset     : u64 LE
//
//  [ BLOOM FILTER ]
//    bloom_bytes
//
//  [ FOOTER (48 bytes) ]
//    index_offset       : u64 LE
//    bloom_offset       : u64 LE
//    bloom_len          : u64 LE
//    entry_count        : u64 LE
//    block_count        : u64 LE
//    magic              : u64 LE = 0xCAFE_F00D_1234_5678
// =============================================================

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::block_cache::{BlockCache, BlockKey};
use crate::bloom::BloomFilter;
use crate::memtable::MemTable;

const MAGIC: u64 = 0xCAFE_F00D_1234_5678;
const FOOTER_SIZE: usize = 48;
const BLOCK_TARGET_BYTES: usize = 4 * 1024; // 4 KiB uncompressed target

// ---- SSTable struct --------------------------------------------------------

pub struct SSTable {
    pub path: PathBuf,
    /// Sparse index: (first_key_of_block, byte_offset_of_block_in_file)
    sparse_index: Vec<(Vec<u8>, u64)>,
    bloom: BloomFilter,
    pub entry_count: usize,
    pub level: u32,
    // Stored for informational / future use (e.g. stats, pre-allocating sparse_index).
    #[allow(dead_code)]
    block_count: usize,
}

// ---- Block builder (accumulates raw entry bytes until threshold) -----------

struct BlockBuilder {
    buf: Vec<u8>,       // raw serialised entries
    entry_count: u32,
    first_key: Option<Vec<u8>>,
}

impl BlockBuilder {
    fn new() -> Self {
        Self { buf: Vec::new(), entry_count: 0, first_key: None }
    }

    fn add(&mut self, key: &[u8], val_opt: &Option<Vec<u8>>) {
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(key);
        match val_opt {
            Some(v) => {
                self.buf.push(0u8);
                self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.buf.extend_from_slice(v);
            }
            None => self.buf.push(1u8),
        }
        self.entry_count += 1;
    }

    fn uncompressed_size(&self) -> usize {
        4 + self.buf.len() // entry_count header + entries
    }

    fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Consume builder, returning (first_key, block_body_with_count_header).
    fn finish(self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.entry_count == 0 {
            return None;
        }
        let mut body = Vec::with_capacity(4 + self.buf.len());
        body.extend_from_slice(&self.entry_count.to_le_bytes());
        body.extend_from_slice(&self.buf);
        Some((self.first_key.unwrap(), body))
    }
}

// ---- SSTable impl ----------------------------------------------------------

impl SSTable {
    // -- Writer

    /// Flush a MemTable to a new SSTable at `path`.
    pub fn write_from_memtable(path: impl AsRef<Path>, mem: &MemTable, level: u32) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let entry_count = mem.iter().count();
        let mut bloom = BloomFilter::new(entry_count.max(1));
        let mut sparse_index: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut file_offset: u64 = 0;
        let mut block_count: usize = 0;
        let mut builder = BlockBuilder::new();

        for (key, val_opt) in mem.iter() {
            bloom.insert(key);
            builder.add(key, val_opt);

            if builder.uncompressed_size() >= BLOCK_TARGET_BYTES {
                file_offset = flush_block(&mut w, builder, &mut sparse_index, file_offset, &mut block_count)?;
                builder = BlockBuilder::new();
            }
        }
        if !builder.is_empty() {
            file_offset = flush_block(&mut w, builder, &mut sparse_index, file_offset, &mut block_count)?;
        }

        // Sparse index section
        let index_offset = file_offset;
        for (first_key, block_off) in &sparse_index {
            w.write_all(&(first_key.len() as u32).to_le_bytes())?;
            w.write_all(first_key)?;
            w.write_all(&block_off.to_le_bytes())?;
        }

        // Bloom section
        let bloom_bytes = bloom.to_bytes();
        let bloom_offset = index_offset
            + sparse_index
                .iter()
                .map(|(k, _)| 4 + k.len() as u64 + 8)
                .sum::<u64>();
        let bloom_len = bloom_bytes.len() as u64;
        w.write_all(&bloom_bytes)?;

        // Footer
        w.write_all(&index_offset.to_le_bytes())?;
        w.write_all(&bloom_offset.to_le_bytes())?;
        w.write_all(&bloom_len.to_le_bytes())?;
        w.write_all(&(entry_count as u64).to_le_bytes())?;
        w.write_all(&(block_count as u64).to_le_bytes())?;
        w.write_all(&MAGIC.to_le_bytes())?;
        w.flush()?;

        Ok(Self { path, sparse_index, bloom, entry_count, level, block_count })
    }

    // -- Reader

    /// Load SSTable metadata (sparse index + bloom) from disk.
    pub fn open(path: impl AsRef<Path>, level: u32) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut f = File::open(&path)?;
        let file_size = f.seek(SeekFrom::End(0))?;

        // Footer
        f.seek(SeekFrom::Start(file_size - FOOTER_SIZE as u64))?;
        let mut footer = [0u8; FOOTER_SIZE];
        f.read_exact(&mut footer)?;
        let index_offset  = u64::from_le_bytes(footer[ 0.. 8].try_into().unwrap());
        let bloom_offset  = u64::from_le_bytes(footer[ 8..16].try_into().unwrap());
        let bloom_len     = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let entry_count   = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let block_count   = u64::from_le_bytes(footer[32..40].try_into().unwrap()) as usize;
        let magic         = u64::from_le_bytes(footer[40..48].try_into().unwrap());

        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad SSTable magic (wrong version?)"));
        }

        // Bloom
        f.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_bytes = vec![0u8; bloom_len as usize];
        f.read_exact(&mut bloom_bytes)?;
        let bloom = BloomFilter::from_bytes(&bloom_bytes);

        // Sparse index
        f.seek(SeekFrom::Start(index_offset))?;
        let mut reader = BufReader::new(&mut f);
        let mut sparse_index: Vec<(Vec<u8>, u64)> = Vec::with_capacity(block_count);
        let mut pos = index_offset;
        while pos < bloom_offset {
            let key = read_bytes_io(&mut reader)?;
            let mut off_buf = [0u8; 8];
            reader.read_exact(&mut off_buf)?;
            let off = u64::from_le_bytes(off_buf);
            pos += 4 + key.len() as u64 + 8;
            sparse_index.push((key, off));
        }

        Ok(Self { path, sparse_index, bloom, entry_count, level, block_count })
    }

    /// Point lookup. Returns `Some(Some(v))`, `Some(None)` (tombstone), or `None`.
    pub fn get(&self, key: &[u8], cache: Option<&Arc<BlockCache>>) -> io::Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // partition_point returns first idx where first_key > key; we want idx-1
        let idx = self.sparse_index.partition_point(|(fk, _)| fk.as_slice() <= key);
        if idx == 0 {
            return Ok(None);
        }
        let (_, block_offset) = &self.sparse_index[idx - 1];

        let data = self.load_block(*block_offset, cache)?;
        scan_block_for_key(&data, key)
    }

    /// Full scan -- returns all (key, val_opt) in sorted order (used by compaction).
    pub fn scan_all(&self) -> io::Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut f = File::open(&self.path)?;
        let mut results = Vec::with_capacity(self.entry_count);

        for (_, block_offset) in &self.sparse_index {
            f.seek(SeekFrom::Start(*block_offset))?;
            let mut len_buf = [0u8; 4];
            f.read_exact(&mut len_buf)?;
            let clen = u32::from_le_bytes(len_buf) as usize;
            let mut compressed = vec![0u8; clen];
            f.read_exact(&mut compressed)?;

            let data = decompress(&compressed)?;
            let mut cursor = Cursor::new(&data);
            let count = read_u32_le(&mut cursor)?;
            for _ in 0..count {
                let k = read_bytes_cursor(&mut cursor)?;
                let tag = read_u8_cursor(&mut cursor)?;
                let v = if tag == 0 { Some(read_bytes_cursor(&mut cursor)?) } else { None };
                results.push((k, v));
            }
        }
        Ok(results)
    }

    pub fn delete_file(&self) -> io::Result<()> {
        fs::remove_file(&self.path)
    }

    // -- Internal helpers

    fn load_block(&self, block_offset: u64, cache: Option<&Arc<BlockCache>>) -> io::Result<Arc<Vec<u8>>> {
        let cache_key: BlockKey = (self.path.clone(), block_offset);

        if let Some(c) = cache {
            if let Some(data) = c.get(&cache_key) {
                return Ok(data);
            }
        }

        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(block_offset))?;
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let clen = u32::from_le_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; clen];
        f.read_exact(&mut compressed)?;

        let data = Arc::new(decompress(&compressed)?);

        if let Some(c) = cache {
            c.insert(cache_key, Arc::clone(&data));
        }

        Ok(data)
    }
}

// ---- Free functions --------------------------------------------------------

/// Compress, write block to file, update sparse_index and file_offset.
fn flush_block(
    w: &mut BufWriter<File>,
    builder: BlockBuilder,
    sparse_index: &mut Vec<(Vec<u8>, u64)>,
    file_offset: u64,
    block_count: &mut usize,
) -> io::Result<u64> {
    if let Some((first_key, body)) = builder.finish() {
        let compressed = lz4_flex::compress_prepend_size(&body);
        let clen = compressed.len() as u32;
        sparse_index.push((first_key, file_offset));
        w.write_all(&clen.to_le_bytes())?;
        w.write_all(&compressed)?;
        *block_count += 1;
        Ok(file_offset + 4 + compressed.len() as u64)
    } else {
        Ok(file_offset)
    }
}

fn decompress(compressed: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(compressed)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Scan a decompressed block body for a specific key.
fn scan_block_for_key(data: &[u8], key: &[u8]) -> io::Result<Option<Option<Vec<u8>>>> {
    let mut cursor = Cursor::new(data);
    let count = read_u32_le(&mut cursor)?;
    for _ in 0..count {
        let k = read_bytes_cursor(&mut cursor)?;
        let tag = read_u8_cursor(&mut cursor)?;
        let v = if tag == 0 { Some(read_bytes_cursor(&mut cursor)?) } else { None };
        if k.as_slice() == key {
            return Ok(Some(v));
        }
        if k.as_slice() > key {
            return Ok(None);
        }
    }
    Ok(None)
}

fn read_u32_le(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u8_cursor(r: &mut impl Read) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_bytes_cursor(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let len = read_u32_le(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_bytes_io(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}
