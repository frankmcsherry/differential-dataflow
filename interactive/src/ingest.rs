//! Native columnar input batches, independent of the server's control protocol.
//!
//! A batch carries keys, values and signed differences; the receiving input supplies one epoch.
//! The versioned binary frame is also the local file format: concatenate frames to make a stream.
//! Leaf encoding is Corgi's column codec, so decoding copies columns, never constructs DDIR rows.

use std::io::{Read, Write};

use crate::corgi::col_times::ColTimes;
use crate::corgi::container::CorgiContainer;
use crate::ir::Diff;
pub use corgi::Value as Column;
use corgi::{Shape, Value};

const MAGIC: &[u8; 8] = b"DDIRCOL1";
const HEADER: usize = 32;

/// A native batch. Construct from well-formed Corgi columns with [`Self::new`], or decode checked
/// external bytes with [`Self::decode`]. The three columns must have equal lengths.
#[derive(Clone, Debug)]
pub struct ColumnBatch {
    keys: Value,
    vals: Value,
    diffs: Vec<Diff>,
}

impl ColumnBatch {
    pub fn new(keys: Value, vals: Value, diffs: Vec<Diff>) -> Result<Self, String> {
        let n = diffs.len();
        if keys.len() != n || vals.len() != n {
            return Err(format!(
                "column batch: {} keys, {} values, {n} differences",
                keys.len(),
                vals.len()
            ));
        }
        Ok(Self { keys, vals, diffs })
    }

    pub fn len(&self) -> usize {
        self.diffs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
    pub fn keys(&self) -> &Value {
        &self.keys
    }
    pub fn vals(&self) -> &Value {
        &self.vals
    }
    pub fn diffs(&self) -> &[Diff] {
        &self.diffs
    }
    /// Exact frame length, so a session client can write its header then stream the columns
    /// directly without first encoding the whole batch into a temporary byte buffer.
    pub fn length_in_bytes(&self) -> usize {
        HEADER
            + corgi::bytes::length_in_bytes(&self.keys)
            + corgi::bytes::length_in_bytes(&self.vals)
            + 8 * self.len()
    }
    pub fn shape(&self) -> (Shape, Shape) {
        (
            corgi::shape_of_value(&self.keys),
            corgi::shape_of_value(&self.vals),
        )
    }

    /// Round-robin input partition, before worker transport. `offset` is the position of this
    /// batch in a larger stream, allowing short file batches to spread across workers too.
    pub fn shard(&self, index: usize, peers: usize, offset: usize) -> Self {
        assert!(index < peers);
        let start = (index + peers - offset % peers) % peers;
        let rows: Vec<_> = (start..self.len()).step_by(peers).collect();
        Self {
            keys: corgi::arrange::gather(&self.keys, &rows),
            vals: corgi::arrange::gather(&self.vals, &rows),
            diffs: rows.iter().map(|&r| self.diffs[r]).collect(),
        }
    }

    pub(crate) fn into_container(self, epoch: u64) -> CorgiContainer<u64, Diff> {
        let n = self.len();
        CorgiContainer {
            keys: self.keys,
            vals: self.vals,
            times: ColTimes::from_raw_lanes(vec![vec![epoch; n]], n),
            diffs: self.diffs,
        }
    }

    /// Bound declarations nested in lists, sums and references before external data reaches
    /// operators that may materialize them. This is structural work, not a scan of the rows.
    pub fn check_declared_rows(&self, limit: u64) -> Result<(), String> {
        let n =
            corgi::bytes::declared_rows(&self.keys).max(corgi::bytes::declared_rows(&self.vals));
        if n > limit {
            return Err(format!(
                "column batch declares {n} rows/elements; limit is {limit}"
            ));
        }
        Ok(())
    }

    /// Write one frame: magic/version, key bytes, value bytes, row count (u64 LE header words),
    /// then the two Corgi columns and one little-endian i64 difference per row.
    pub fn write_to(&self, writer: &mut impl Write) -> std::io::Result<()> {
        writer.write_all(MAGIC)?;
        for n in [
            corgi::bytes::length_in_bytes(&self.keys),
            corgi::bytes::length_in_bytes(&self.vals),
            self.len(),
        ] {
            writer.write_all(&(n as u64).to_le_bytes())?;
        }
        corgi::bytes::write_to(&self.keys, writer)?;
        corgi::bytes::write_to(&self.vals, writer)?;
        for diff in &self.diffs {
            writer.write_all(&diff.to_le_bytes())?;
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.length_in_bytes());
        self.write_to(&mut bytes)
            .expect("writing a Vec cannot fail");
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let (kl, vl, rows, total) = header(bytes)?;
        if bytes.len() != total {
            return Err(format!(
                "column batch: frame has {} bytes, expected {total}",
                bytes.len()
            ));
        }
        let (keys, used) = corgi::bytes::read_from(&bytes[HEADER..HEADER + kl])?;
        if used != kl {
            return Err("column batch: trailing key bytes".into());
        }
        let (vals, used) = corgi::bytes::read_from(&bytes[HEADER + kl..HEADER + kl + vl])?;
        if used != vl {
            return Err("column batch: trailing value bytes".into());
        }
        let diffs = bytes[HEADER + kl + vl..]
            .chunks_exact(8)
            .map(|x| i64::from_le_bytes(x.try_into().unwrap()))
            .collect::<Vec<_>>();
        debug_assert_eq!(diffs.len(), rows);
        Self::new(keys, vals, diffs)
    }

    /// Read one bounded frame, or `None` at a clean frame boundary at EOF. A partial frame is an
    /// error. The limit bounds both encoded bytes and nested row/element declarations.
    pub fn read_next(reader: &mut impl Read, limit: usize) -> Result<Option<Self>, String> {
        let mut prefix = [0u8; HEADER];
        if reader.read(&mut prefix[..1]).map_err(|e| e.to_string())? == 0 {
            return Ok(None);
        }
        reader
            .read_exact(&mut prefix[1..])
            .map_err(|e| format!("column batch header: {e}"))?;
        let (_, _, _, total) = header(&prefix)?;
        if total > limit {
            return Err(format!("column batch has {total} bytes; limit is {limit}"));
        }
        let mut bytes = vec![0; total];
        bytes[..HEADER].copy_from_slice(&prefix);
        reader
            .read_exact(&mut bytes[HEADER..])
            .map_err(|e| format!("column batch payload: {e}"))?;
        let batch = Self::decode(&bytes)?;
        batch.check_declared_rows(limit as u64)?;
        Ok(Some(batch))
    }
}

fn header(bytes: &[u8]) -> Result<(usize, usize, usize, usize), String> {
    if bytes.len() < HEADER || &bytes[..8] != MAGIC {
        return Err("column batch: expected DDIRCOL1 header".into());
    }
    let word = |i| {
        usize::try_from(u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap()))
            .map_err(|_| "column batch: length does not fit usize".to_string())
    };
    let (kl, vl, rows) = (word(8)?, word(16)?, word(24)?);
    let total = rows
        .checked_mul(8)
        .and_then(|n| n.checked_add(HEADER))
        .and_then(|n| n.checked_add(kl))
        .and_then(|n| n.checked_add(vl))
        .ok_or("column batch: frame length overflow")?;
    Ok((kl, vl, rows, total))
}

// Typed process channels keep the columns as columns; serializing channels use the same checked
// frame. Work records can therefore carry a batch without constructing a row-wise serde model.
impl serde::Serialize for ColumnBatch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.encode())
    }
}
impl<'de> serde::Deserialize<'de> for ColumnBatch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::decode(&bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_frames_round_trip_and_stream() {
        let batch = ColumnBatch::new(
            Value::Prod(vec![
                Value::i64(vec![-1, 7, 7]),
                Value::f64(vec![0.5, -1.0, 2.0]),
            ]),
            Value::List(
                corgi::Bounds::offsets(vec![0, 2, 3]),
                Box::new(Value::i64(vec![10, 11, 12])),
            ),
            vec![1, -2, 3],
        )
        .unwrap();
        let bytes = batch.encode();
        assert_eq!(bytes.len(), batch.length_in_bytes());
        let decoded = ColumnBatch::decode(&bytes).unwrap();
        assert_eq!(decoded.keys(), batch.keys());
        assert_eq!(decoded.vals(), batch.vals());
        assert_eq!(decoded.diffs(), batch.diffs());
        let mut reader = std::io::Cursor::new([bytes.clone(), bytes.clone()].concat());
        for _ in 0..2 {
            assert_eq!(
                ColumnBatch::read_next(&mut reader, 4096)
                    .unwrap()
                    .unwrap()
                    .diffs(),
                &[1, -2, 3]
            );
        }
        assert!(ColumnBatch::read_next(&mut reader, 4096).unwrap().is_none());
        for end in 0..bytes.len() {
            assert!(ColumnBatch::decode(&bytes[..end]).is_err());
        }
        let mut truncated = std::io::Cursor::new(&bytes[..bytes.len() - 1]);
        assert!(ColumnBatch::read_next(&mut truncated, 4096).is_err());
        assert!(ColumnBatch::read_next(&mut std::io::Cursor::new(&bytes), 32).is_err());
        let mut corrupt = bytes;
        corrupt[24..32].copy_from_slice(&2u64.to_le_bytes());
        assert!(ColumnBatch::decode(&corrupt).is_err());
    }

    #[test]
    fn sharding_preserves_columns_and_signed_differences() {
        let batch = ColumnBatch::new(
            Value::i64((0..11).collect()),
            Value::Unit(11),
            (-5..6).collect(),
        )
        .unwrap();
        for offset in 0..5 {
            let mut rows = Vec::new();
            for peer in 0..4 {
                let shard = batch.shard(peer, 4, offset);
                for (&key, &diff) in shard
                    .keys()
                    .as_i64("test")
                    .unwrap()
                    .iter()
                    .zip(shard.diffs())
                {
                    assert_eq!((key as usize + offset) % 4, peer);
                    rows.push((key, diff));
                }
            }
            rows.sort();
            assert_eq!(rows, (0..11).map(|k| (k, k - 5)).collect::<Vec<_>>());
        }
    }

    #[test]
    fn declared_nested_rows_are_bounded_before_materialization() {
        let batch = ColumnBatch::new(
            Value::List(
                corgi::Bounds::Stride(1 << 30, 1),
                Box::new(Value::Unit(1 << 30)),
            ),
            Value::Unit(1),
            vec![1],
        )
        .unwrap();
        let decoded = ColumnBatch::decode(&batch.encode()).unwrap();
        assert!(decoded.check_declared_rows(4096).is_err());
        assert!(ColumnBatch::read_next(&mut std::io::Cursor::new(batch.encode()), 4096).is_err());
    }
}
