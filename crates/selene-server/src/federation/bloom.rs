//! Compact Bloom filter for federation label+property summaries.
//!
//! Each peer computes a Bloom filter from its schema labels and property keys,
//! exchanges it during handshake, and coordinators check it before forwarding
//! queries to skip peers that definitely lack matching data.

/// A simple Bloom filter using two hash functions (double hashing).
///
/// Tuned for ~10K items at 1% false positive rate = ~12 KB.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new Bloom filter for the expected number of items and
    /// desired false positive probability.
    pub fn new(expected_items: usize, fpp: f64) -> Self {
        let num_bits = optimal_num_bits(expected_items, fpp);
        let num_hashes = optimal_num_hashes(expected_items, num_bits);
        let words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
        }
    }

    /// Insert an item into the filter.
    pub fn insert(&mut self, item: &str) {
        let (h1, h2) = hash_pair(item);
        for i in 0..self.num_hashes {
            let bit = combined_hash(h1, h2, i) % self.num_bits as u64;
            self.bits[bit as usize / 64] |= 1 << (bit % 64);
        }
    }

    /// Check if an item might be in the filter.
    /// Returns false if definitely absent, true if possibly present.
    pub fn might_contain(&self, item: &str) -> bool {
        let (h1, h2) = hash_pair(item);
        for i in 0..self.num_hashes {
            let bit = combined_hash(h1, h2, i) % self.num_bits as u64;
            if self.bits[bit as usize / 64] & (1 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to bytes for wire transmission.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len() * 8);
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?);
        if num_bits == 0 || num_hashes == 0 {
            return None;
        }
        let words = num_bits.div_ceil(64);
        if data.len() != 8 + words * 8 {
            return None;
        }
        let bits = (0..words)
            .map(|i| {
                let offset = 8 + i * 8;
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
            })
            .collect();
        Some(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

/// Build a Bloom filter from a collection of label and property key strings.
pub fn build_filter(labels: &[String], property_keys: &[String]) -> BloomFilter {
    let total = labels.len() + property_keys.len();
    let expected = total.max(10); // minimum 10 to avoid degenerate sizes
    let mut filter = BloomFilter::new(expected, 0.01);
    for label in labels {
        filter.insert(&format!("label:{label}"));
    }
    for key in property_keys {
        filter.insert(&format!("prop:{key}"));
    }
    filter
}

fn hash_pair(item: &str) -> (u64, u64) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h1 = DefaultHasher::new();
    item.hash(&mut h1);
    let v1 = h1.finish();
    let mut h2 = DefaultHasher::new();
    v1.hash(&mut h2);
    (v1, h2.finish())
}

fn combined_hash(h1: u64, h2: u64, i: u32) -> u64 {
    h1.wrapping_add(u64::from(i).wrapping_mul(h2))
}

fn optimal_num_bits(n: usize, fpp: f64) -> usize {
    let m = -(n as f64 * fpp.ln()) / (2.0_f64.ln().powi(2));
    (m.ceil() as usize).max(64)
}

fn optimal_num_hashes(n: usize, m: usize) -> u32 {
    let k = (m as f64 / n as f64) * 2.0_f64.ln();
    (k.ceil() as u32).clamp(1, 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_check() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert("label:sensor");
        bf.insert("label:ahu");
        bf.insert("prop:temperature");

        assert!(bf.might_contain("label:sensor"));
        assert!(bf.might_contain("label:ahu"));
        assert!(bf.might_contain("prop:temperature"));
        assert!(!bf.might_contain("label:nonexistent"));
    }

    #[test]
    fn round_trip_serialization() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert("label:sensor");
        bf.insert("prop:temp");

        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(bf2.might_contain("label:sensor"));
        assert!(bf2.might_contain("prop:temp"));
        assert!(!bf2.might_contain("label:missing"));
    }

    #[test]
    fn build_filter_from_schema() {
        let labels = vec!["sensor".into(), "ahu".into(), "floor".into()];
        let props = vec!["temperature".into(), "humidity".into()];
        let bf = build_filter(&labels, &props);

        assert!(bf.might_contain("label:sensor"));
        assert!(bf.might_contain("label:ahu"));
        assert!(bf.might_contain("prop:temperature"));
        assert!(!bf.might_contain("label:chiller"));
    }

    #[test]
    fn empty_data_deserialize_fails() {
        assert!(BloomFilter::from_bytes(&[]).is_none());
        assert!(BloomFilter::from_bytes(&[0; 4]).is_none());
    }

    #[test]
    fn from_bytes_rejects_zero_num_bits() {
        let data = [0u8, 0, 0, 0, 3, 0, 0, 0];
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn from_bytes_rejects_zero_num_hashes() {
        let mut data = vec![64u8, 0, 0, 0, 0, 0, 0, 0];
        data.extend_from_slice(&[0u8; 8]);
        assert!(BloomFilter::from_bytes(&data).is_none());
    }

    #[test]
    fn false_positive_rate_bounded() {
        // Insert 1000 items, check 10000 random strings for false positives
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..1000 {
            bf.insert(&format!("item:{i}"));
        }

        let mut false_positives = 0;
        for i in 1000..11000 {
            if bf.might_contain(&format!("other:{i}")) {
                false_positives += 1;
            }
        }
        // With 1% fpp, expect ~100 FPs out of 10000 checks. Allow 3x margin.
        assert!(
            false_positives < 300,
            "too many false positives: {false_positives}/10000"
        );
    }
}
