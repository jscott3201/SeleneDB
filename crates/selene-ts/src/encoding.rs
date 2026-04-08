//! Time-series block encoding.
//!
//! Supports multiple value encoding strategies:
//! - **Gorilla (default):** XOR with leading/trailing zero optimization (Pelkonen et al., 2015)
//! - **RLE:** Run-length encoding for binary/digital sensors
//! - **Dictionary:** Codebook encoding for discrete/enum sensors
//!
//! Timestamps always use delta-of-delta with variable-length prefix codes.
//! Two separate columnar bit-streams per block (timestamps + values).
//! No external dependencies - pure bit manipulation on `Vec<u64>` word buffers.

use crate::hot::TimeSample;
use rustc_hash::FxHashMap;
use selene_core::ValueEncoding;

// ── TsBlock ──────────────────────────────────────────────────

/// A compressed block of time-series samples.
///
/// Holds two parallel bit-streams (timestamps and values) plus metadata.
/// Timestamps always use delta-of-delta encoding. Values use one of three
/// strategies selected via `encoding`: Gorilla XOR (default), RLE, or Dictionary.
/// Typically covers a 30-minute window.
#[derive(Debug, Clone)]
pub struct TsBlock {
    /// First timestamp in the block.
    pub start_nanos: i64,
    /// Last timestamp in the block.
    pub end_nanos: i64,
    /// Number of samples encoded.
    pub sample_count: u32,
    /// Value encoding strategy used for this block.
    pub encoding: ValueEncoding,
    /// Delta-of-delta encoded timestamp bit-stream.
    pub ts_words: Vec<u64>,
    /// Encoding-specific value bit-stream.
    pub val_words: Vec<u64>,
}

impl TsBlock {
    /// Compress a slice of samples into a block with the given encoding.
    ///
    /// Samples must be sorted by timestamp. Empty input returns a block
    /// with sample_count=0.
    pub fn encode(samples: &[TimeSample], encoding: ValueEncoding) -> Self {
        if samples.is_empty() {
            return Self {
                start_nanos: 0,
                end_nanos: 0,
                sample_count: 0,
                encoding,
                ts_words: vec![],
                val_words: vec![],
            };
        }

        assert!(
            u32::try_from(samples.len()).is_ok(),
            "TsBlock: sample count {} exceeds u32::MAX",
            samples.len()
        );
        let ts_words = encode_timestamps(samples);
        let (actual_encoding, val_words) = match encoding {
            ValueEncoding::Gorilla => (encoding, encode_values(samples)),
            ValueEncoding::Rle => (encoding, encode_values_rle(samples)),
            ValueEncoding::Dictionary => match encode_values_dictionary(samples) {
                Some(words) => (encoding, words),
                None => (ValueEncoding::Gorilla, encode_values(samples)),
            },
        };
        Self {
            start_nanos: samples[0].timestamp_nanos,
            end_nanos: samples.last().unwrap().timestamp_nanos,
            sample_count: samples.len() as u32,
            encoding: actual_encoding,
            ts_words,
            val_words,
        }
    }

    /// Decompress all samples from this block.
    pub fn decode_all(&self) -> Vec<TimeSample> {
        if self.sample_count == 0 {
            return vec![];
        }
        let timestamps = decode_timestamps(&self.ts_words, self.sample_count);
        let values = match self.encoding {
            ValueEncoding::Gorilla => decode_values(&self.val_words, self.sample_count),
            ValueEncoding::Rle => decode_values_rle(&self.val_words, self.sample_count),
            ValueEncoding::Dictionary => {
                decode_values_dictionary(&self.val_words, self.sample_count)
            }
        };
        timestamps
            .into_iter()
            .zip(values)
            .map(|(t, v)| TimeSample {
                timestamp_nanos: t,
                value: v,
            })
            .collect()
    }

    /// Decompress and filter samples within a time range.
    ///
    /// For 30-minute blocks (~1,800 samples at 1 Hz), full decode + filter
    /// is ~50us -- not worth partial decoding complexity.
    #[cfg(test)]
    pub fn decode_range(&self, start: i64, end: i64) -> Vec<TimeSample> {
        self.decode_all()
            .into_iter()
            .filter(|s| s.timestamp_nanos >= start && s.timestamp_nanos <= end)
            .collect()
    }

    /// Total memory footprint of this block in bytes.
    pub fn compressed_size(&self) -> usize {
        // 3 metadata fields (start, end, count) + word buffers
        24 + (self.ts_words.len() + self.val_words.len()) * 8
    }

    /// Serialize to bytes with a version envelope.
    /// Format: `[0x01, encoding_byte, start(8), end(8), count(4),
    ///          ts_len(4), ts_words..., val_len(4), val_words...]`
    pub fn serialize(&self) -> Vec<u8> {
        let capacity = 2 + 8 + 8 + 4 + 4 + self.ts_words.len() * 8 + 4 + self.val_words.len() * 8;
        let mut buf = Vec::with_capacity(capacity);
        buf.push(0x01); // version tag
        buf.push(match self.encoding {
            ValueEncoding::Gorilla => 0,
            ValueEncoding::Rle => 1,
            ValueEncoding::Dictionary => 2,
        });
        buf.extend_from_slice(&self.start_nanos.to_le_bytes());
        buf.extend_from_slice(&self.end_nanos.to_le_bytes());
        buf.extend_from_slice(&self.sample_count.to_le_bytes());
        buf.extend_from_slice(&(self.ts_words.len() as u32).to_le_bytes());
        for &w in &self.ts_words {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        buf.extend_from_slice(&(self.val_words.len() as u32).to_le_bytes());
        for &w in &self.val_words {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes, handling both versioned and legacy formats.
    ///
    /// V1 format starts with `[0x01, encoding_byte]` where encoding is 0-2.
    /// Legacy format starts with `start_nanos` as i64 LE. A collision requires
    /// `start_nanos % 256 == 1` AND `(start_nanos >> 8) % 256 <= 2`, which only
    /// occurs for timestamps 1, 257, 513 (first 513ns of Unix epoch - unrealistic).
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        if data[0] == 0x01 && data[1] <= 2 {
            Self::deserialize_v1(data)
        } else {
            Self::deserialize_legacy(data)
        }
    }

    fn deserialize_v1(data: &[u8]) -> Option<Self> {
        if data.len() < 2 + 8 + 8 + 4 + 4 {
            return None;
        }
        let encoding = match data[1] {
            0 => ValueEncoding::Gorilla,
            1 => ValueEncoding::Rle,
            2 => ValueEncoding::Dictionary,
            _ => return None,
        };
        let mut pos = 2;
        let start_nanos = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let end_nanos = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let sample_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let ts_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if ts_len > data.len().saturating_sub(pos) / 8 {
            return None;
        }
        let mut ts_words = Vec::with_capacity(ts_len);
        for _ in 0..ts_len {
            ts_words.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
        }
        if pos + 4 > data.len() {
            return None;
        }
        let val_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if val_len > data.len().saturating_sub(pos) / 8 {
            return None;
        }
        let mut val_words = Vec::with_capacity(val_len);
        for _ in 0..val_len {
            val_words.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
        }
        Some(Self {
            start_nanos,
            end_nanos,
            sample_count,
            encoding,
            ts_words,
            val_words,
        })
    }

    /// Deserialize legacy format (no version tag) as Gorilla.
    fn deserialize_legacy(data: &[u8]) -> Option<Self> {
        if data.len() < 8 + 8 + 4 + 4 {
            return None;
        }
        let mut pos = 0;
        let start_nanos = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let end_nanos = i64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;
        let sample_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let ts_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if ts_len > data.len().saturating_sub(pos) / 8 {
            return None;
        }
        let mut ts_words = Vec::with_capacity(ts_len);
        for _ in 0..ts_len {
            ts_words.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
        }
        if pos + 4 > data.len() {
            return None;
        }
        let val_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if val_len > data.len().saturating_sub(pos) / 8 {
            return None;
        }
        let mut val_words = Vec::with_capacity(val_len);
        for _ in 0..val_len {
            val_words.push(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
            pos += 8;
        }
        Some(Self {
            start_nanos,
            end_nanos,
            sample_count,
            encoding: ValueEncoding::Gorilla,
            ts_words,
            val_words,
        })
    }

    /// Serialize in legacy format (no version tag). For testing backward compat.
    #[cfg(test)]
    pub fn serialize_legacy(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.start_nanos.to_le_bytes());
        buf.extend_from_slice(&self.end_nanos.to_le_bytes());
        buf.extend_from_slice(&self.sample_count.to_le_bytes());
        buf.extend_from_slice(&(self.ts_words.len() as u32).to_le_bytes());
        for &w in &self.ts_words {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        buf.extend_from_slice(&(self.val_words.len() as u32).to_le_bytes());
        for &w in &self.val_words {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        buf
    }
}

// ── BitWriter ─────────────────────────────────────────────────────

/// Packs variable-width bit fields into a Vec<u64> word buffer.
struct BitWriter {
    words: Vec<u64>,
    current: u64,
    /// Bits remaining in the current word (64 = empty, 0 = full).
    remaining: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            words: Vec::new(),
            current: 0,
            remaining: 64,
        }
    }

    /// Write `num_bits` (1..=64) from the low bits of `value`.
    fn write_bits(&mut self, value: u64, num_bits: u8) {
        debug_assert!(num_bits > 0 && num_bits <= 64);
        let masked = if num_bits == 64 {
            value
        } else {
            value & ((1u64 << num_bits) - 1)
        };

        if num_bits <= self.remaining {
            // Fits in current word
            self.current |= masked << (self.remaining - num_bits);
            self.remaining -= num_bits;
            if self.remaining == 0 {
                self.words.push(self.current);
                self.current = 0;
                self.remaining = 64;
            }
        } else {
            // Split across current and next word
            let first_bits = self.remaining;
            let second_bits = num_bits - first_bits;
            self.current |= masked >> second_bits;
            self.words.push(self.current);
            self.current = masked << (64 - second_bits);
            self.remaining = 64 - second_bits;
        }
    }

    fn write_bit(&mut self, bit: bool) {
        self.write_bits(u64::from(bit), 1);
    }

    fn finish(mut self) -> Vec<u64> {
        if self.remaining < 64 {
            self.words.push(self.current);
        }
        self.words
    }
}

// ── BitReader ─────────────────────────────────────────────────────

/// Reads variable-width bit fields from a &[u64] word buffer.
struct BitReader<'a> {
    words: &'a [u64],
    word_idx: usize,
    /// Bits remaining in current word (64 = start of word).
    remaining: u8,
}

impl<'a> BitReader<'a> {
    fn new(words: &'a [u64]) -> Self {
        Self {
            words,
            word_idx: 0,
            remaining: 64,
        }
    }

    /// Read `num_bits` (1..=64) and return in the low bits.
    fn read_bits(&mut self, num_bits: u8) -> u64 {
        debug_assert!(num_bits > 0 && num_bits <= 64);

        let word = if self.word_idx < self.words.len() {
            self.words[self.word_idx]
        } else {
            0
        };

        if num_bits <= self.remaining {
            let shift = self.remaining - num_bits;
            let mask = if num_bits == 64 {
                u64::MAX
            } else {
                (1u64 << num_bits) - 1
            };
            let value = (word >> shift) & mask;
            self.remaining -= num_bits;
            if self.remaining == 0 {
                self.word_idx += 1;
                self.remaining = 64;
            }
            value
        } else {
            // Split across two words
            let first_bits = self.remaining;
            let second_bits = num_bits - first_bits;
            let high = if first_bits == 64 {
                word
            } else {
                word & ((1u64 << first_bits) - 1)
            };
            self.word_idx += 1;
            self.remaining = 64;
            let next_word = if self.word_idx < self.words.len() {
                self.words[self.word_idx]
            } else {
                0
            };
            let low = next_word >> (64 - second_bits);
            self.remaining -= second_bits;
            if self.remaining == 0 {
                self.word_idx += 1;
                self.remaining = 64;
            }
            (high << second_bits) | low
        }
    }

    fn read_bit(&mut self) -> bool {
        self.read_bits(1) != 0
    }
}

// ── Timestamp Encoding ────────────────────────────────────────────

/// Encode timestamps using delta-of-delta variable-length coding.
fn encode_timestamps(samples: &[TimeSample]) -> Vec<u64> {
    let mut w = BitWriter::new();

    // First timestamp: raw 64 bits
    w.write_bits(samples[0].timestamp_nanos as u64, 64);

    if samples.len() == 1 {
        return w.finish();
    }

    // Second timestamp: store delta (using 14 bits for typical ~1s deltas,
    // but we use the same dod scheme with prev_delta=0 for simplicity)
    let mut prev_delta = samples[1].timestamp_nanos - samples[0].timestamp_nanos;
    write_dod(&mut w, prev_delta); // first dod is just the delta itself (prev_delta was 0)

    // Subsequent: delta-of-delta
    for i in 2..samples.len() {
        let delta = samples[i].timestamp_nanos - samples[i - 1].timestamp_nanos;
        let dod = delta - prev_delta;
        write_dod(&mut w, dod);
        prev_delta = delta;
    }

    w.finish()
}

/// Write a delta-of-delta value with variable-length prefix encoding.
fn write_dod(w: &mut BitWriter, dod: i64) {
    if dod == 0 {
        w.write_bit(false); // 0 = same delta
    } else if (-63..=64).contains(&dod) {
        w.write_bits(0b10, 2);
        w.write_bits((dod + 63) as u64, 7);
    } else if (-255..=256).contains(&dod) {
        w.write_bits(0b110, 3);
        w.write_bits((dod + 255) as u64, 9);
    } else if (-2047..=2048).contains(&dod) {
        w.write_bits(0b1110, 4);
        w.write_bits((dod + 2047) as u64, 12);
    } else {
        w.write_bits(0b1111, 4);
        w.write_bits(dod as u64, 64);
    }
}

/// Decode timestamps from a delta-of-delta encoded bit-stream.
fn decode_timestamps(words: &[u64], count: u32) -> Vec<i64> {
    let mut r = BitReader::new(words);
    let mut result = Vec::with_capacity(count as usize);

    // First timestamp: raw 64 bits
    let first = r.read_bits(64) as i64;
    result.push(first);

    if count == 1 {
        return result;
    }

    // Second: decode dod (prev_delta was 0)
    let mut prev_delta = read_dod(&mut r);
    result.push(first + prev_delta);

    // Subsequent
    for _ in 2..count {
        let dod = read_dod(&mut r);
        prev_delta += dod;
        let ts = *result.last().unwrap() + prev_delta;
        result.push(ts);
    }

    result
}

/// Read a delta-of-delta value from the bit-stream.
fn read_dod(r: &mut BitReader) -> i64 {
    if !r.read_bit() {
        return 0; // prefix 0
    }
    if !r.read_bit() {
        // prefix 10
        let v = r.read_bits(7) as i64 - 63;
        return v;
    }
    if !r.read_bit() {
        // prefix 110
        let v = r.read_bits(9) as i64 - 255;
        return v;
    }
    if !r.read_bit() {
        // prefix 1110
        let v = r.read_bits(12) as i64 - 2047;
        return v;
    }
    // prefix 1111
    r.read_bits(64) as i64
}

// ── Value Encoding ────────────────────────────────────────────────

/// Encode f64 values using XOR with leading/trailing zero optimization.
fn encode_values(samples: &[TimeSample]) -> Vec<u64> {
    let mut w = BitWriter::new();

    // First value: raw 64 bits
    let mut prev_bits = samples[0].value.to_bits();
    w.write_bits(prev_bits, 64);

    let mut prev_leading: u8 = 64; // no previous pattern
    let mut prev_trailing: u8 = 0;

    for sample in &samples[1..] {
        let bits = sample.value.to_bits();
        let xor = prev_bits ^ bits;

        if xor == 0 {
            w.write_bit(false); // 0 = identical value
        } else {
            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            if leading >= prev_leading && trailing >= prev_trailing {
                // Reuse previous leading/trailing pattern
                w.write_bits(0b10, 2);
                let meaningful_bits = 64 - prev_leading - prev_trailing;
                // No mask needed: xor has at least prev_leading leading zeros,
                // so right-shifting by prev_trailing leaves at most meaningful_bits
                // significant bits. A mask would overflow when meaningful_bits == 64.
                let meaningful_value = xor >> prev_trailing;
                w.write_bits(meaningful_value, meaningful_bits);
            } else {
                // New pattern
                w.write_bits(0b11, 2);
                let leading_capped = leading.min(31); // 5 bits max
                let meaningful_bits = 64 - leading_capped - trailing;
                w.write_bits(u64::from(leading_capped), 5);
                w.write_bits(u64::from(meaningful_bits - 1), 6); // -1 since meaningful >= 1
                let meaningful_value = xor >> trailing;
                w.write_bits(meaningful_value, meaningful_bits);
                prev_leading = leading_capped;
                prev_trailing = trailing;
            }
        }

        prev_bits = bits;
    }

    w.finish()
}

/// Decode f64 values from an XOR encoded bit-stream.
fn decode_values(words: &[u64], count: u32) -> Vec<f64> {
    let mut r = BitReader::new(words);
    let mut result = Vec::with_capacity(count as usize);

    // First value: raw 64 bits
    let mut prev_bits = r.read_bits(64);
    result.push(f64::from_bits(prev_bits));

    let mut prev_leading: u8 = 64;
    let mut prev_trailing: u8 = 0;

    for _ in 1..count {
        if !r.read_bit() {
            // 0 = identical value
            result.push(f64::from_bits(prev_bits));
            continue;
        }

        if r.read_bit() {
            // 11 = new pattern
            let leading = r.read_bits(5) as u8;
            let meaningful_bits = r.read_bits(6) as u8 + 1;
            let trailing = 64 - leading - meaningful_bits;
            let meaningful_value = r.read_bits(meaningful_bits);
            let xor = meaningful_value << trailing;
            prev_bits ^= xor;
            prev_leading = leading;
            prev_trailing = trailing;
        } else {
            // 10 = reuse leading/trailing pattern
            let meaningful_bits = 64 - prev_leading - prev_trailing;
            let meaningful_value = r.read_bits(meaningful_bits);
            let xor = meaningful_value << prev_trailing;
            prev_bits ^= xor;
        }

        result.push(f64::from_bits(prev_bits));
    }

    result
}

/// Advance the XOR value decoder by one sample without returning a value.
///
/// Must be called for every sample in order because XOR encoding is stateful:
/// each value depends on the previous XOR pattern (prev_bits, prev_leading,
/// prev_trailing).
fn skip_one_value(
    r: &mut BitReader,
    prev_bits: &mut u64,
    prev_leading: &mut u8,
    prev_trailing: &mut u8,
) {
    if !r.read_bit() {
        // 0 = identical value, state unchanged
        return;
    }

    if r.read_bit() {
        // 11 = new pattern
        let leading = r.read_bits(5) as u8;
        let meaningful_bits = r.read_bits(6) as u8 + 1;
        let trailing = 64 - leading - meaningful_bits;
        let meaningful_value = r.read_bits(meaningful_bits);
        let xor = meaningful_value << trailing;
        *prev_bits ^= xor;
        *prev_leading = leading;
        *prev_trailing = trailing;
    } else {
        // 10 = reuse leading/trailing pattern
        let meaningful_bits = 64 - *prev_leading - *prev_trailing;
        let meaningful_value = r.read_bits(meaningful_bits);
        let xor = meaningful_value << *prev_trailing;
        *prev_bits ^= xor;
    }
}

/// Decode one value from the XOR stream and return the f64.
fn decode_one_value(
    r: &mut BitReader,
    prev_bits: &mut u64,
    prev_leading: &mut u8,
    prev_trailing: &mut u8,
) -> f64 {
    skip_one_value(r, prev_bits, prev_leading, prev_trailing);
    f64::from_bits(*prev_bits)
}

// ── RLE Value Encoding ───────────────────────────────────────────

/// Encode values as run-length pairs: (f64_bits, run_length) per run.
/// Each run is two u64 entries in the output vector. Runs are detected
/// by exact bitwise equality (`to_bits()`).
fn encode_values_rle(samples: &[TimeSample]) -> Vec<u64> {
    let mut words = Vec::new();
    let mut i = 0;
    while i < samples.len() {
        let value_bits = samples[i].value.to_bits();
        let mut run_len: u64 = 1;
        while i + (run_len as usize) < samples.len()
            && samples[i + run_len as usize].value.to_bits() == value_bits
        {
            run_len += 1;
        }
        words.push(value_bits);
        words.push(run_len);
        i += run_len as usize;
    }
    words
}

/// Decode RLE-encoded values: pairs of (f64_bits, run_length).
fn decode_values_rle(words: &[u64], count: u32) -> Vec<f64> {
    let mut result = Vec::with_capacity(count as usize);
    let mut i = 0;
    while i + 1 < words.len() && result.len() < count as usize {
        let value = f64::from_bits(words[i]);
        let run_len = words[i + 1] as usize;
        let remaining = (count as usize) - result.len();
        let actual_run = run_len.min(remaining);
        result.extend(std::iter::repeat_n(value, actual_run));
        i += 2;
    }
    result
}

// ── Dictionary Value Encoding ────────────────────────────────────

/// Maximum number of distinct values for dictionary encoding.
/// Beyond this, we fall back to Gorilla.
const DICTIONARY_MAX_DISTINCT: usize = 256;

/// Minimum number of bits needed to represent `n` distinct values.
/// Uses integer math instead of floating-point log2 for exact results.
fn dict_bit_width(n: usize) -> u8 {
    if n <= 1 {
        1
    } else {
        (usize::BITS - (n - 1).leading_zeros()) as u8
    }
}

/// Encode values using a codebook of distinct f64 values.
///
/// Layout: `[codebook_len, val0_bits, val1_bits, ..., packed_indices...]`
/// - `words[0]` = number of distinct values N
/// - `words[1..=N]` = the distinct f64 values as `to_bits()`
/// - remaining words = indices packed at `ceil(log2(N))` bits each
///
/// Returns `None` if there are more than 256 distinct values (caller should
/// fall back to Gorilla).
fn encode_values_dictionary(samples: &[TimeSample]) -> Option<Vec<u64>> {
    // Build codebook: map f64 bits -> index
    let mut codebook: Vec<u64> = Vec::new();
    let mut value_to_idx: FxHashMap<u64, usize> = FxHashMap::default();

    for s in samples {
        let bits = s.value.to_bits();
        if let std::collections::hash_map::Entry::Vacant(e) = value_to_idx.entry(bits) {
            if codebook.len() >= DICTIONARY_MAX_DISTINCT {
                return None; // too many distinct values
            }
            e.insert(codebook.len());
            codebook.push(bits);
        }
    }

    let n = codebook.len();
    // Bit width for indices: at least 1 bit even for a single value
    let bit_width = dict_bit_width(n);

    // Build output: header + codebook + packed indices
    let mut words = Vec::with_capacity(1 + n + (samples.len() * bit_width as usize).div_ceil(64));
    words.push(n as u64);
    words.extend_from_slice(&codebook);

    // Pack indices using BitWriter
    let mut w = BitWriter::new();
    for s in samples {
        let bits = s.value.to_bits();
        let idx = value_to_idx[&bits];
        w.write_bits(idx as u64, bit_width);
    }
    words.extend(w.finish());

    Some(words)
}

/// Decode dictionary-encoded values.
fn decode_values_dictionary(words: &[u64], count: u32) -> Vec<f64> {
    if words.is_empty() {
        return vec![];
    }

    let n = words[0] as usize;
    if n + 1 > words.len() {
        return vec![];
    }
    let codebook = &words[1..=n];
    let index_words = &words[n + 1..];

    let bit_width = dict_bit_width(n);

    let mut r = BitReader::new(index_words);
    let mut result = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let idx = r.read_bits(bit_width) as usize;
        if idx >= codebook.len() {
            break;
        }
        result.push(f64::from_bits(codebook[idx]));
    }
    result
}

impl TsBlock {
    /// Decode only samples within [start, end], optimized per encoding.
    ///
    /// For Gorilla: skips value decoding before the range (XOR is stateful).
    /// For RLE/Dictionary: full decode + filter (values are not stateful in
    /// the same way, and blocks are small enough that this is fast).
    pub fn decode_range_partial(&self, start: i64, end: i64) -> Vec<TimeSample> {
        if self.sample_count == 0 {
            return vec![];
        }
        if self.end_nanos < start || self.start_nanos > end {
            return vec![];
        }

        match self.encoding {
            ValueEncoding::Gorilla => self.decode_range_partial_gorilla(start, end),
            _ => self
                .decode_all()
                .into_iter()
                .filter(|s| s.timestamp_nanos >= start && s.timestamp_nanos <= end)
                .collect(),
        }
    }

    /// Gorilla-specific range decode with XOR skip optimization.
    fn decode_range_partial_gorilla(&self, start: i64, end: i64) -> Vec<TimeSample> {
        let timestamps = decode_timestamps(&self.ts_words, self.sample_count);
        let first_match = timestamps.partition_point(|&t| t < start);
        let last_match_exclusive = timestamps.partition_point(|&t| t <= end);

        if first_match >= last_match_exclusive {
            return vec![];
        }

        let mut val_reader = BitReader::new(&self.val_words);
        let mut prev_bits = val_reader.read_bits(64);
        let mut prev_leading: u8 = 64;
        let mut prev_trailing: u8 = 0;

        let mut result = Vec::with_capacity(last_match_exclusive - first_match);

        if first_match == 0 {
            result.push(TimeSample {
                timestamp_nanos: timestamps[0],
                value: f64::from_bits(prev_bits),
            });
        } else {
            for _ in 1..first_match {
                skip_one_value(
                    &mut val_reader,
                    &mut prev_bits,
                    &mut prev_leading,
                    &mut prev_trailing,
                );
            }
        }

        let decode_start = if first_match == 0 { 1 } else { first_match };
        for &ts in &timestamps[decode_start..last_match_exclusive] {
            let value = decode_one_value(
                &mut val_reader,
                &mut prev_bits,
                &mut prev_leading,
                &mut prev_trailing,
            );
            result.push(TimeSample {
                timestamp_nanos: ts,
                value,
            });
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: i64, val: f64) -> TimeSample {
        TimeSample {
            timestamp_nanos: ts,
            value: val,
        }
    }

    // ── BitWriter/BitReader ───────────────────────────────────────

    #[test]
    fn bit_writer_reader_round_trip() {
        let mut w = BitWriter::new();
        w.write_bits(0b101, 3);
        w.write_bits(0xFF, 8);
        w.write_bits(0, 1);
        w.write_bits(0xDEAD, 16);
        w.write_bit(true);
        let words = w.finish();

        let mut r = BitReader::new(&words);
        assert_eq!(r.read_bits(3), 0b101);
        assert_eq!(r.read_bits(8), 0xFF);
        assert_eq!(r.read_bits(1), 0);
        assert_eq!(r.read_bits(16), 0xDEAD);
        assert!(r.read_bit());
    }

    #[test]
    fn bit_writer_64bit_value() {
        let mut w = BitWriter::new();
        w.write_bits(u64::MAX, 64);
        w.write_bits(42, 7);
        let words = w.finish();

        let mut r = BitReader::new(&words);
        assert_eq!(r.read_bits(64), u64::MAX);
        assert_eq!(r.read_bits(7), 42);
    }

    // ── TsBlock ──────────────────────────────────────────────

    #[test]
    fn single_sample_round_trip() {
        let samples = vec![sample(1_000_000_000, 72.5)];
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        assert_eq!(block.sample_count, 1);
        assert_eq!(block.start_nanos, 1_000_000_000);
        assert_eq!(block.end_nanos, 1_000_000_000);

        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn two_samples_round_trip() {
        let samples = vec![sample(1_000_000_000, 72.5), sample(2_000_000_000, 73.1)];
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        assert_eq!(block.sample_count, 2);

        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn regular_1hz_round_trip() {
        let base = 1_700_000_000_000_000_000i64; // realistic epoch nanos
        let samples: Vec<TimeSample> = (0..1000)
            .map(|i| sample(base + i * 1_000_000_000, 72.0 + (i as f64 * 0.01).sin()))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        assert_eq!(block.sample_count, 1000);
        assert_eq!(block.start_nanos, samples[0].timestamp_nanos);
        assert_eq!(block.end_nanos, samples[999].timestamp_nanos);

        let decoded = block.decode_all();
        assert_eq!(decoded.len(), 1000);
        for (orig, dec) in samples.iter().zip(&decoded) {
            assert_eq!(orig.timestamp_nanos, dec.timestamp_nanos);
            assert_eq!(orig.value.to_bits(), dec.value.to_bits());
        }
    }

    #[test]
    fn identical_values() {
        let samples: Vec<TimeSample> = (0..100).map(|i| sample(i * 1_000_000_000, 42.0)).collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);

        // Identical values should compress extremely well: 1 bit per value
        // 100 values = ~100 bits = ~13 bytes of value data
        let val_bytes = block.val_words.len() * 8;
        assert!(val_bytes < 100, "val_bytes={val_bytes}, expected < 100");
    }

    #[test]
    fn identical_deltas() {
        // Perfectly regular 1 Hz: all dod = 0
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, 70.0 + i as f64))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);

        // Identical deltas: 1 bit per timestamp after the first two
        let ts_bytes = block.ts_words.len() * 8;
        assert!(ts_bytes < 50, "ts_bytes={ts_bytes}, expected < 50");
    }

    #[test]
    fn random_values() {
        // Worst case for XOR encoding but must still round-trip
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let samples: Vec<TimeSample> = (0..500)
            .map(|i| {
                let mut h = DefaultHasher::new();
                i.hash(&mut h);
                let bits = h.finish();
                sample(
                    i * 1_000_000_000,
                    f64::from_bits(bits & 0x7FEF_FFFF_FFFF_FFFF),
                ) // avoid NaN/Inf
            })
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();
        for (orig, dec) in samples.iter().zip(&decoded) {
            assert_eq!(orig.timestamp_nanos, dec.timestamp_nanos);
            assert_eq!(orig.value.to_bits(), dec.value.to_bits());
        }
    }

    #[test]
    fn special_floats() {
        let samples = vec![
            sample(100, 0.0),
            sample(200, -0.0),
            sample(300, f64::INFINITY),
            sample(400, f64::NEG_INFINITY),
            sample(500, f64::NAN),
        ];

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();

        assert_eq!(decoded[0].value.to_bits(), 0.0f64.to_bits());
        assert_eq!(decoded[1].value.to_bits(), (-0.0f64).to_bits());
        assert_eq!(decoded[2].value, f64::INFINITY);
        assert_eq!(decoded[3].value, f64::NEG_INFINITY);
        assert!(decoded[4].value.is_nan());
        // NaN bit pattern must be preserved
        assert_eq!(decoded[4].value.to_bits(), f64::NAN.to_bits());
    }

    #[test]
    fn large_delta_of_delta() {
        // Exercise all dod buckets with irregular gaps
        let samples = vec![
            sample(0, 1.0),
            sample(1_000_000_000, 2.0),   // delta = 1s
            sample(2_000_000_000, 3.0),   // dod = 0 (same delta)
            sample(2_000_000_050, 4.0),   // dod = -999999950 (small gap)
            sample(100_000_000_000, 5.0), // dod = huge positive
            sample(100_000_000_001, 6.0), // dod = -97999999999 (back to small)
        ];

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn empty_input() {
        let block = TsBlock::encode(&[], ValueEncoding::Gorilla);
        assert_eq!(block.sample_count, 0);
        assert!(block.ts_words.is_empty());
        assert!(block.val_words.is_empty());

        let decoded = block.decode_all();
        assert!(decoded.is_empty());
    }

    #[test]
    fn xor_full_width_round_trip() {
        // Regression test for C1: shift overflow when meaningful_bits == 64.
        // Consecutive values differing at both MSB (sign) and LSB trigger
        // the full-width XOR path that previously caused silent data corruption.
        let samples = vec![
            sample(100, 0.0),
            sample(200, f64::from_bits(0x8000_0000_0000_0001)),
            sample(300, 0.0),
            sample(400, f64::from_bits(u64::MAX)), // all bits set
            sample(500, 0.0),
        ];
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let decoded = block.decode_all();
        for (orig, dec) in samples.iter().zip(&decoded) {
            assert_eq!(
                orig.value.to_bits(),
                dec.value.to_bits(),
                "mismatch at ts={}: expected 0x{:016X}, got 0x{:016X}",
                orig.timestamp_nanos,
                orig.value.to_bits(),
                dec.value.to_bits()
            );
        }
    }

    #[test]
    fn decode_range_filters() {
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, i as f64))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);

        // Request middle 20 samples (timestamps 40s to 59s)
        let range = block.decode_range(40_000_000_000, 59_000_000_000);
        assert_eq!(range.len(), 20);
        assert_eq!(range[0].timestamp_nanos, 40_000_000_000);
        assert_eq!(range[19].timestamp_nanos, 59_000_000_000);
    }

    #[test]
    fn compressed_size_reflects_data() {
        let block = TsBlock::encode(&[sample(100, 42.0)], ValueEncoding::Gorilla);
        assert!(block.compressed_size() > 24); // at least metadata
        assert!(block.compressed_size() < 200); // not absurdly large for 1 sample
    }

    #[test]
    fn compression_ratio_regular_1hz() {
        let base = 1_700_000_000_000_000_000i64;
        let samples: Vec<TimeSample> = (0..1000)
            .map(|i| sample(base + i * 1_000_000_000, 72.0 + (i as f64 * 0.01).sin()))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let raw_bytes = samples.len() * 16;
        let compressed = block.compressed_size();
        let ratio = raw_bytes as f64 / compressed as f64;

        // Regular 1 Hz with sine-wave values: expect 2-8x compression
        // (sine produces high XOR entropy; real IoT sensors compress better)
        assert!(
            ratio > 2.0,
            "compression ratio {ratio:.1}x too low (raw={raw_bytes}, compressed={compressed})"
        );
    }

    #[test]
    fn compression_ratio_binary_sensor() {
        // Binary sensor: same value repeated, regular interval
        let samples: Vec<TimeSample> = (0..1000).map(|i| sample(i * 1_000_000_000, 1.0)).collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let raw_bytes = samples.len() * 16;
        let compressed = block.compressed_size();
        let ratio = raw_bytes as f64 / compressed as f64;

        // All identical values + regular deltas: extreme compression
        assert!(
            ratio > 20.0,
            "binary sensor ratio {ratio:.1}x too low (raw={raw_bytes}, compressed={compressed})"
        );
    }

    // ── Partial decode tests ─────────────────────────────────────────

    #[test]
    fn partial_matches_full() {
        // Partial decode must produce identical results to full decode+filter
        let samples: Vec<TimeSample> = (0..200)
            .map(|i| sample(i * 1_000_000_000, 72.0 + (i as f64 * 0.03).sin()))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);

        // Middle range
        let start = 50_000_000_000i64;
        let end = 149_000_000_000i64;
        let full = block.decode_range(start, end);
        let partial = block.decode_range_partial(start, end);

        assert_eq!(full.len(), partial.len(), "length mismatch");
        for (f, p) in full.iter().zip(&partial) {
            assert_eq!(f.timestamp_nanos, p.timestamp_nanos);
            assert_eq!(
                f.value.to_bits(),
                p.value.to_bits(),
                "value mismatch at ts={}",
                f.timestamp_nanos
            );
        }
    }

    #[test]
    fn partial_first_samples() {
        // Query the first N samples (no skipping needed)
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, i as f64))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let partial = block.decode_range_partial(0, 9_000_000_000);
        assert_eq!(partial.len(), 10);
        assert_eq!(partial[0].timestamp_nanos, 0);
        assert_eq!(partial[9].timestamp_nanos, 9_000_000_000);
        for (i, s) in partial.iter().enumerate() {
            assert_eq!(s.value, i as f64);
        }
    }

    #[test]
    fn partial_last_samples() {
        // Query the last N samples (maximum skipping)
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, i as f64))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let partial = block.decode_range_partial(90_000_000_000, 99_000_000_000);
        assert_eq!(partial.len(), 10);
        assert_eq!(partial[0].timestamp_nanos, 90_000_000_000);
        assert_eq!(partial[0].value, 90.0);
        assert_eq!(partial[9].timestamp_nanos, 99_000_000_000);
        assert_eq!(partial[9].value, 99.0);
    }

    // ── RLE encoding ─────────────────────────────────────────────

    #[test]
    fn rle_binary_sensor_round_trip() {
        // Binary sensor: 100 samples of 1.0, then 100 of 0.0, then 100 of 1.0
        let mut samples = Vec::new();
        for i in 0..100 {
            samples.push(sample(i * 1_000_000_000, 1.0));
        }
        for i in 100..200 {
            samples.push(sample(i * 1_000_000_000, 0.0));
        }
        for i in 200..300 {
            samples.push(sample(i * 1_000_000_000, 1.0));
        }

        let block = TsBlock::encode(&samples, ValueEncoding::Rle);
        assert_eq!(block.encoding, ValueEncoding::Rle);
        assert_eq!(block.sample_count, 300);

        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn rle_constant_value() {
        let samples: Vec<TimeSample> = (0..1000).map(|i| sample(i * 1_000_000_000, 42.0)).collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Rle);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);

        // Single constant run: should be just 2 u64s (value bits + run length)
        assert_eq!(block.val_words.len(), 2);
    }

    #[test]
    fn rle_single_sample() {
        let samples = vec![sample(1_000_000_000, 3.15)];
        let block = TsBlock::encode(&samples, ValueEncoding::Rle);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn rle_alternating_values() {
        // Worst case for RLE: every sample is different
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, if i % 2 == 0 { 0.0 } else { 1.0 }))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Rle);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn rle_range_query() {
        let mut samples = Vec::new();
        for i in 0..100 {
            samples.push(sample(i * 1_000_000_000, 1.0));
        }
        for i in 100..200 {
            samples.push(sample(i * 1_000_000_000, 0.0));
        }

        let block = TsBlock::encode(&samples, ValueEncoding::Rle);
        let range = block.decode_range_partial(50_000_000_000, 149_000_000_000);
        assert_eq!(range.len(), 100);
        // First 50 should be 1.0, next 50 should be 0.0
        assert_eq!(range[0].value, 1.0);
        assert_eq!(range[49].value, 1.0);
        assert_eq!(range[50].value, 0.0);
        assert_eq!(range[99].value, 0.0);
    }

    // ── Dictionary encoding ──────────────────────────────────────

    #[test]
    fn dictionary_discrete_sensor_round_trip() {
        // HVAC mode sensor cycling through 4 states
        let modes = [0.0, 1.0, 2.0, 3.0];
        let samples: Vec<TimeSample> = (0..400)
            .map(|i| sample(i * 1_000_000_000, modes[i as usize % 4]))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        assert_eq!(block.encoding, ValueEncoding::Dictionary);
        assert_eq!(block.sample_count, 400);

        let decoded = block.decode_all();
        assert_eq!(decoded, samples);
    }

    #[test]
    fn dictionary_single_value() {
        let samples: Vec<TimeSample> = (0..100).map(|i| sample(i * 1_000_000_000, 5.0)).collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        let decoded = block.decode_all();
        assert_eq!(decoded, samples);

        // Codebook length should be 1
        assert_eq!(block.val_words[0], 1);
    }

    #[test]
    fn dictionary_fallback_on_too_many_distinct() {
        // 300 distinct values -> exceeds 256 limit -> falls back to Gorilla
        let samples: Vec<TimeSample> = (0..300)
            .map(|i| sample(i * 1_000_000_000, i as f64 * 0.1))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        // Should have fallen back to Gorilla
        assert_eq!(block.encoding, ValueEncoding::Gorilla);

        let decoded = block.decode_all();
        assert_eq!(decoded.len(), 300);
        for (orig, dec) in samples.iter().zip(&decoded) {
            assert_eq!(orig.value.to_bits(), dec.value.to_bits());
        }
    }

    #[test]
    fn dictionary_range_query() {
        let modes = [0.0, 1.0, 2.0, 3.0];
        let samples: Vec<TimeSample> = (0..200)
            .map(|i| sample(i * 1_000_000_000, modes[i as usize % 4]))
            .collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        let range = block.decode_range_partial(50_000_000_000, 99_000_000_000);
        assert_eq!(range.len(), 50);
        for s in &range {
            assert!(modes.contains(&s.value));
        }
    }

    // ── Compression ratio comparisons ────────────────────────────

    #[test]
    fn rle_compression_better_than_gorilla_for_binary() {
        // Binary sensor: 900 samples at 1.0, then 900 at 0.0
        let mut samples = Vec::new();
        for i in 0..900 {
            samples.push(sample(i * 1_000_000_000, 1.0));
        }
        for i in 900..1800 {
            samples.push(sample(i * 1_000_000_000, 0.0));
        }

        let gorilla = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let rle = TsBlock::encode(&samples, ValueEncoding::Rle);

        let gorilla_val_bytes = gorilla.val_words.len() * 8;
        let rle_val_bytes = rle.val_words.len() * 8;

        assert!(
            rle_val_bytes < gorilla_val_bytes,
            "RLE ({rle_val_bytes}B) should beat Gorilla ({gorilla_val_bytes}B) for binary data"
        );
    }

    #[test]
    fn dictionary_compression_better_than_gorilla_for_discrete() {
        // 4-state sensor cycling rapidly
        let modes = [0.0, 1.0, 2.0, 3.0];
        let samples: Vec<TimeSample> = (0..1800)
            .map(|i| sample(i * 1_000_000_000, modes[i as usize % 4]))
            .collect();

        let gorilla = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let dict = TsBlock::encode(&samples, ValueEncoding::Dictionary);

        let gorilla_val_bytes = gorilla.val_words.len() * 8;
        let dict_val_bytes = dict.val_words.len() * 8;

        assert!(
            dict_val_bytes < gorilla_val_bytes,
            "Dictionary ({dict_val_bytes}B) should beat Gorilla ({gorilla_val_bytes}B) for discrete data"
        );
    }

    // ── Serialization ────────────────────────────────────────────

    #[test]
    fn tsblock_serialization_round_trip() {
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, (i as f64 * 0.1).sin()))
            .collect();

        for encoding in [
            ValueEncoding::Gorilla,
            ValueEncoding::Rle,
            ValueEncoding::Dictionary,
        ] {
            let block = TsBlock::encode(&samples, encoding);
            let bytes = block.serialize();
            let restored = TsBlock::deserialize(&bytes).unwrap();
            assert_eq!(restored.sample_count, block.sample_count);
            assert_eq!(restored.encoding, block.encoding);
            let decoded = restored.decode_all();
            assert_eq!(decoded, samples);
        }
    }

    #[test]
    fn legacy_gorilla_block_deserialization() {
        let samples: Vec<TimeSample> = (0..50).map(|i| sample(i * 1_000_000_000, 72.5)).collect();

        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let legacy_bytes = block.serialize_legacy();
        let restored = TsBlock::deserialize(&legacy_bytes).unwrap();
        assert_eq!(restored.encoding, ValueEncoding::Gorilla);
        assert_eq!(restored.decode_all(), samples);
    }

    #[test]
    fn empty_data_returns_none() {
        assert!(TsBlock::deserialize(&[]).is_none());
        assert!(TsBlock::deserialize(&[0x01]).is_none()); // too short for v1
    }

    #[test]
    fn deserialize_truncated_v1_returns_none() {
        let samples: Vec<TimeSample> = (0..10).map(|i| sample(i * 1_000_000_000, 42.0)).collect();
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let bytes = block.serialize();
        for len in [0, 1, 5, 20, bytes.len() / 2] {
            assert!(TsBlock::deserialize(&bytes[..len]).is_none());
        }
    }

    #[test]
    fn deserialize_absurd_lengths_returns_none() {
        // Craft a v1 header with ts_len = u32::MAX
        let mut data = vec![0x01, 0x00]; // version + Gorilla
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&0u32.to_le_bytes());
        data.extend_from_slice(&u32::MAX.to_le_bytes()); // absurd ts_len
        assert!(TsBlock::deserialize(&data).is_none());
    }

    #[test]
    fn legacy_deserialize_realistic_timestamps() {
        // Real IoT timestamps (nanos since epoch) never have byte[0] = 0x01.
        // Timestamps 1-769 (sub-microsecond epoch) are the only collision space,
        // which is unrealistic for any sensor deployment.
        let ts_2025 = 1_735_689_600_000_000_000i64; // 2025-01-01 in nanos
        let samples = vec![sample(ts_2025, 42.0)];
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let legacy_bytes = block.serialize_legacy();
        assert_ne!(legacy_bytes[0], 0x01); // real timestamps don't collide
        let restored = TsBlock::deserialize(&legacy_bytes).unwrap();
        assert_eq!(restored.decode_all(), samples);
    }

    #[test]
    fn dictionary_256_distinct_values() {
        let samples: Vec<TimeSample> = (0..256)
            .map(|i| sample(i * 1_000_000_000, i as f64))
            .collect();
        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        assert_eq!(block.encoding, ValueEncoding::Dictionary);
        assert_eq!(block.decode_all(), samples);
    }

    #[test]
    fn dictionary_two_distinct_values() {
        let samples: Vec<TimeSample> = (0..100)
            .map(|i| sample(i * 1_000_000_000, if i % 2 == 0 { 0.0 } else { 1.0 }))
            .collect();
        let block = TsBlock::encode(&samples, ValueEncoding::Dictionary);
        assert_eq!(block.encoding, ValueEncoding::Dictionary);
        assert_eq!(block.val_words[0], 2); // codebook length = 2
        assert_eq!(block.decode_all(), samples);
    }

    #[test]
    fn empty_block_serialization_round_trip() {
        let block = TsBlock::encode(&[], ValueEncoding::Gorilla);
        let bytes = block.serialize();
        let restored = TsBlock::deserialize(&bytes).unwrap();
        assert_eq!(restored.sample_count, 0);
        assert!(restored.decode_all().is_empty());
    }
}
