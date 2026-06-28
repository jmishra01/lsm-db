// ================================================================
// Bloom Filter - probabilistic membership test
//
// Used per-SSTable to skip files that definitely do NOT contain
// the queried key, cutting disk reads on the read path.
//
// Two independent hash functions are simulated via:
// h1 = FNV-1a
// h2 = djb2 variant
// with double-hashing h(i) = h1 + i * h2 for k probes.
// ================================================================


// ----- Hash helpers ---------------------------------------------

fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 14695881039346656037;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

fn djb2(data: &[u8]) -> u64 {
    let mut h: u64 = 5381;
    for &b in data {
        h = h
            .wrapping_shl(5)
            .wrapping_add(h)
            .wrapping_add(b as u64);
    }
    if h == 0 { 1 } else { h }
}



fn hashes(key: &[u8]) -> (u64, u64) {
    (fnv1a(key), djb2(key))
}




pub struct BloomFilter {
    bits: Vec<u64>,     // bit array stored as u64 words
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a filter sized for `capacity` items at -1 % FPR.
    pub fn new(capacity: usize) -> BloomFilter {
        // m = -n * ln(p) / (ln2)^2 with p=0.01
        let num_bits = ((capacity as f64 * 9.585) as usize).max(64);
        let num_words = (num_bits + 63)/64;
        // k = m / n * ln2
        let num_hashes = 7_usize;   // optimal for p =0.01
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = hashes(key);
        for i in 0..self.num_hashes {
            let idx = (h1.wrapping_add(i as u64 * h2)) as usize % self.num_bits;
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
    }

    /// Returns `false` if the key is DEFINITELY absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = hashes(key);
        for i in 0..self.num_hashes {
            let idx = (h1.wrapping_add(i as u64 * h2)) as usize % self.num_bits;
            if self.bits[idx / 64] & (1u64 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to bytes (for embedding in SSTable footer).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 8 + self.bits.len() * 8);
        out.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        out.extend_from_slice(&(self.num_hashes as u64).to_le_bytes());
        for word in &self.bits {
            out.extend_from_slice(&word.to_le_bytes());
        }
        out
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> BloomFilter {
        let num_bits = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
        let num_hashes = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
        let words = data[16..]
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect::<Vec<_>>();
        Self {
            bits: words,
            num_bits,
            num_hashes,
        }
    }
}