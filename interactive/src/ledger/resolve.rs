//! Encode transactions for the keyed `.ddp` rules, and decode their conclusions.
//! The program is compiled once; `Engine` evaluates it on resident workers.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use corgi::{Bounds, Value};

use super::{Change, Error, Failure, Frontier, Id, Payload, Rejection, Shape, Time};
use crate::runtime::container::Updates;

/// A supplied transaction key and its changes, grouped in strictly increasing ledger order.
/// Keys must be unique within a batch and order its transactions consistently with completed operations.
/// Root publication orders successful batches.
pub type Transaction = (i64, Vec<(Id, Change)>);

/// The compiled keyed `.ddp` transaction resolver.
pub(super) struct Resolver {
    pub(super) program: crate::scope_ir::Program,
    pub(super) workers: usize,
}

impl Resolver {
    pub(super) fn new(workers: usize) -> Result<Self, Error> {
        if workers == 0 { return Err(Error::Invalid("the resolver needs at least one worker")); }
        let program = std::panic::catch_unwind(|| {
            let mut program = crate::lower::lower_tree(crate::parse::pipe::parse(include_str!("../../model/transactions_keyed.ddp")));
            program.root.exports.retain(|output| matches!(output.name.as_str(), "aborted" | "registry_add" | "manifest_ordered" | "initial_delta" | "registered_delta"));
            crate::passes::optimize(&mut program);
            program
        }).map_err(|_| Error::Resolve("cannot compile transaction rules".into()))?;
        Ok(Self { program, workers })
    }
}

pub(super) fn unique_keys(transactions: &[Transaction]) -> Result<(), Error> {
    let mut keys = BTreeSet::new();
    if transactions.iter().any(|(key, _)| !keys.insert(*key)) {
        return Err(Error::Invalid("transaction keys must be unique"));
    }
    Ok(())
}

/// The widest time any transaction names or any creation declares.
pub(super) fn maximum_width(transactions: &[Transaction]) -> usize {
    let mut width = 0;
    let mut include = |frontier: &Frontier| {
        width = width.max(frontier.elements().iter().map(|time| time.len()).max().unwrap_or(0));
    };
    for (_, groups) in transactions {
        for (_, change) in groups {
            match change {
                Change::Create { start, .. } => include(start),
                Change::Append { lower, upper, .. } => { include(lower); include(upper); }
                Change::CreateAppend { start, upper, .. } => { include(start); include(upper); }
            }
        }
    }
    for (_, groups) in transactions {
        for (_, change) in groups {
            if let Change::Create { shape, .. } | Change::CreateAppend { shape, .. } = change {
                width = width.max(shape.time_width);
            }
        }
    }
    width
}

pub(super) fn bytes<'a>(values: impl IntoIterator<Item = &'a [u8]>) -> Value {
    let mut ends = Vec::new();
    let mut flat = Vec::new();
    for value in values { flat.extend_from_slice(value); ends.push(flat.len()); }
    Value::List(ends.into(), Box::new(Value::u8(flat)))
}

/// Two signed words preserve all UUID bits and their unsigned lexicographic order.
pub(super) fn ids(values: impl IntoIterator<Item = Id>) -> Value {
    let (mut high, mut low) = (Vec::new(), Vec::new());
    for id in values {
        let bytes = id.as_bytes();
        high.push(i64::from_be_bytes(bytes[..8].try_into().unwrap()) ^ i64::MIN);
        low.push(i64::from_be_bytes(bytes[8..].try_into().unwrap()) ^ i64::MIN);
    }
    Value::Prod(vec![Value::int(high), Value::int(low)])
}

/// Restore the public UUID byte columns once, after collecting resolver conclusions.
fn public_ids(value: Value) -> Result<Value, Error> {
    let value = corgi::OpLike::eval(&corgi::NumOp::Core(corgi::Op::Clone), value).map_err(Error::Corrupt)?;
    let Value::Prod(words) = value else { return Err(corrupt("expected UUID words")); };
    let [high, low]: [Value; 2] = words.try_into().map_err(|_| corrupt("expected two UUID words"))?;
    let high = high.as_i64("UUID high word").map_err(Error::Corrupt)?;
    let low = low.as_i64("UUID low word").map_err(Error::Corrupt)?;
    if high.len() != low.len() { return Err(corrupt("UUID word lengths differ")); }
    let mut bytes = Vec::with_capacity(16 * high.len());
    for (high, low) in high.iter().zip(low.iter()) {
        bytes.extend_from_slice(&(high ^ i64::MIN).to_be_bytes());
        bytes.extend_from_slice(&(low ^ i64::MIN).to_be_bytes());
    }
    Ok(Value::List(Bounds::Stride(16, high.len()), Box::new(Value::u8(bytes))))
}

pub(super) fn public_outputs(mut outputs: BTreeMap<String, Updates>) -> Result<BTreeMap<String, Updates>, Error> {
    let restore = |value: &mut Value| -> Result<(), Error> {
        *value = public_ids(std::mem::replace(value, Value::Unit(0)))?;
        Ok(())
    };
    for (name, updates) in &mut outputs {
        match name.as_str() {
            "registry_add" => {
                let Value::Prod(fields) = &mut updates.keys else { return Err(corrupt("expected UUID key")); };
                restore(&mut fields[0])?;
            }
            "manifest_ordered" => {
                let Value::Prod(fields) = &mut updates.vals else { return Err(corrupt("expected span fields")); };
                restore(&mut fields[0])?;
            }
            "aborted" => {
                let Value::Prod(fields) = &mut updates.vals else { return Err(corrupt("expected result field")); };
                let Value::Sum(_, results) = &mut fields[0] else { return Err(corrupt("expected result variants")); };
                let Value::Prod(aborted) = &mut results[1] else { return Err(corrupt("expected rejection fields")); };
                let Value::Sum(_, failures) = &mut aborted[1] else { return Err(corrupt("expected failure variants")); };
                restore(&mut failures[1])?;
                restore(&mut failures[2])?;
            }
            _ => {}
        }
    }
    Ok(outputs)
}

pub(super) fn frontiers<'a>(values: impl IntoIterator<Item = &'a Frontier>, width: usize) -> Value {
    let (mut frontier_ends, mut time_ends, mut coordinates) = (Vec::new(), Vec::new(), Vec::new());
    for value in values {
        let mut points = value.elements().to_vec();
        points.sort();
        for point in points {
            coordinates.extend((0..width).map(|index| point.get(index).copied().unwrap_or(i64::MIN)));
            time_ends.push(coordinates.len());
        }
        frontier_ends.push(time_ends.len());
    }
    Value::List(frontier_ends.into(), Box::new(Value::List(time_ends.into(), Box::new(Value::int(coordinates)))))
}

fn shapes<'a>(values: impl IntoIterator<Item = &'a Shape>) -> Result<Value, Error> {
    let encoded = values.into_iter().map(|shape| bincode::serialize(shape).map_err(|error| Error::Corrupt(error.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(bytes(encoded.iter().map(Vec::as_slice)))
}

fn payloads(values: &[&Payload]) -> Value {
    let (mut tags, mut inline, mut blobs, mut empty) = (Vec::new(), Vec::new(), Vec::new(), 0);
    for value in values {
        tags.push(match value {
            Payload::Empty => { empty += 1; 0 }
            Payload::Inline(value) => { inline.push(value.as_slice()); 1 }
            Payload::Blob(value) => { blobs.push(value.as_slice()); 2 }
        });
    }
    Value::sum(tags, vec![Value::Unit(empty), bytes(inline), bytes(blobs)])
}

pub(super) fn commands(transactions: &[Transaction], width: usize) -> Result<Updates, Error> {
    let groups = transactions.iter().flat_map(|(_, groups)| groups).collect::<Vec<_>>();
    let (mut tags, mut creates, mut appends, mut combined) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (_, change) in &groups {
        tags.push(match change {
            Change::Create { shape, start } => { creates.push((shape, start)); 0 }
            Change::Append { lower, upper, batch } => { appends.push((lower, upper, batch)); 1 }
            Change::CreateAppend { shape, start, upper, batch } => { combined.push((shape, start, upper, batch)); 2 }
        });
    }
    let changes = Value::sum(tags, vec![
        Value::Prod(vec![shapes(creates.iter().map(|(shape, _)| *shape))?, frontiers(creates.iter().map(|(_, start)| *start), width)]),
        Value::Prod(vec![frontiers(appends.iter().map(|(lower, _, _)| *lower), width), frontiers(appends.iter().map(|(_, upper, _)| *upper), width), payloads(&appends.iter().map(|(_, _, batch)| *batch).collect::<Vec<_>>())]),
        Value::Prod(vec![shapes(combined.iter().map(|(shape, _, _, _)| *shape))?, frontiers(combined.iter().map(|(_, start, _, _)| *start), width), frontiers(combined.iter().map(|(_, _, upper, _)| *upper), width), payloads(&combined.iter().map(|(_, _, _, batch)| *batch).collect::<Vec<_>>())]),
    ]);
    let mut end = 0;
    let ends = transactions.iter().map(|(_, groups)| { end += groups.len(); end }).collect::<Vec<_>>();
    let commands = Updates {
        keys: Value::Prod(vec![Value::int(transactions.iter().map(|(key, _)| *key))]),
        vals: Value::Prod(vec![Value::List(ends.into(), Box::new(Value::Prod(vec![ids(groups.iter().map(|(id, _)| *id)), changes])))]),
        diffs: vec![1; transactions.len()],
    };
    Ok(commands)
}

#[derive(Clone, Copy)]
pub(super) struct Row<'a> { pub(super) value: &'a Value, pub(super) index: usize }

impl<'a> Row<'a> {
    pub(super) fn product<const N: usize>(self) -> Result<[Self; N], Error> {
        let Value::Prod(fields) = self.value else { return Err(corrupt("expected product")); };
        if fields.len() != N { return Err(corrupt("unexpected product width")); }
        Ok(std::array::from_fn(|field| Self { value: &fields[field], index: self.index }))
    }
    pub(super) fn list(self) -> Result<(&'a Value, Range<usize>), Error> {
        match self.value {
            Value::List(bounds, values) => {
                let (start, end) = match bounds {
                    Bounds::Offsets(ends) => (if self.index == 0 { 0 } else { *ends.get(self.index - 1).ok_or_else(|| corrupt("invalid list row"))? }, *ends.get(self.index).ok_or_else(|| corrupt("invalid list row"))?),
                    Bounds::Stride(width, rows) if self.index < *rows => (self.index * width, (self.index + 1) * width),
                    _ => return Err(corrupt("invalid list row")),
                };
                if start > end || end > values.len() { return Err(corrupt("invalid list bounds")); }
                Ok((values, start..end))
            }
            Value::Ref(arena, rows) => Self { value: arena, index: *rows.get(self.index).ok_or_else(|| corrupt("invalid referenced row"))? }.list(),
            _ => Err(corrupt("expected list")),
        }
    }
    pub(super) fn integer(self) -> Result<i64, Error> {
        corgi::arrange::borrowed_i64(self.value).and_then(|values| values.get(self.index).copied())
            .ok_or_else(|| corrupt("expected integer"))
    }
    pub(super) fn bytes(self) -> Result<Vec<u8>, Error> {
        let (value, rows) = self.list()?;
        rows.map(|index| u8::try_from(Self { value, index }.integer()?).map_err(|_| corrupt("byte out of range"))).collect()
    }
    pub(super) fn id(self) -> Result<Id, Error> {
        Ok(Id::from_bytes(self.bytes()?.try_into().map_err(|_| corrupt("UUID must contain 16 bytes"))?))
    }
    pub(super) fn frontier(self) -> Result<Frontier, Error> {
        let (points, rows) = self.list()?;
        rows.map(|index| {
            let (coordinates, rows) = Self { value: points, index }.list()?;
            rows.map(|index| Self { value: coordinates, index }.integer()).collect::<Result<Vec<_>, _>>()
                .map(|point| Time::new(point.into()))
        }).collect()
    }
    pub(super) fn variant(self) -> Result<(usize, Self), Error> {
        let Value::Sum(tags, variants) = self.value else { return Err(corrupt("expected sum")); };
        if self.index >= tags.len() { return Err(corrupt("invalid sum row")); }
        let tag = tags.tag_at(self.index);
        let value = variants.get(tag).ok_or_else(|| corrupt("invalid sum tag"))?;
        let index = tags.offset_at(self.index);
        if index >= value.len() { return Err(corrupt("invalid variant offset")); }
        Ok((tag, Self { value, index }))
    }
    pub(super) fn unit(self) -> Result<(), Error> {
        match self.value {
            Value::Unit(rows) if self.index < *rows => Ok(()),
            _ => Err(corrupt("expected unit")),
        }
    }
    pub(super) fn payload(self) -> Result<Payload, Error> {
        let (tag, value) = self.variant()?;
        match tag {
            0 => { value.unit()?; Ok(Payload::Empty) }
            1 => Ok(Payload::Inline(value.bytes()?)),
            2 => Ok(Payload::Blob(value.bytes()?.try_into().map_err(|_| corrupt("blob hash must contain 32 bytes"))?)),
            _ => Err(corrupt("invalid payload tag")),
        }
    }
}

fn corrupt(message: &str) -> Error { Error::Corrupt(format!("transaction resolver: {message}")) }

pub(super) fn output<'a>(outputs: &'a BTreeMap<String, Updates>, name: &str) -> Result<&'a Updates, Error> {
    let output = outputs.get(name).ok_or_else(|| corrupt("missing output"))?;
    if output.keys.len() != output.len() || output.vals.len() != output.len() || output.diffs.iter().any(|diff| *diff != 1) {
        return Err(corrupt("outputs must be sets of rows"));
    }
    Ok(output)
}

// Widen each output leaf once, so row reads borrow integers instead of repeatedly widening a whole byte column through CORGI's public integer reader.
pub(super) fn wide(value: Value) -> Result<Value, Error> {
    Ok(match value {
        leaf @ Value::Prim(_) => Value::i64(leaf.into_i64("resolver output").map_err(Error::Corrupt)?),
        Value::Prod(fields) => Value::Prod(fields.into_iter().map(wide).collect::<Result<_, _>>()?),
        Value::Sum(tags, variants) => Value::Sum(tags, variants.into_iter().map(wide).collect::<Result<_, _>>()?),
        Value::List(bounds, values) => Value::List(bounds, Box::new(wide(*values)?)),
        Value::Ref(arena, rows) => Value::Ref(std::sync::Arc::new(wide((*arena).clone())?), rows),
        unit @ Value::Unit(_) => unit,
    })
}

/// Read the failure-only results after widening their integer leaves once; every other transaction committed.
pub(super) fn outcomes(results: &Updates, transactions: &[Transaction]) -> Result<BTreeMap<i64, Result<(), Rejection>>, Error> {
    let mut outcomes: BTreeMap<i64, Result<(), Rejection>> = transactions.iter().map(|(key, _)| (*key, Ok(()))).collect();
    let mut seen = BTreeSet::new();
    for index in 0..results.len() {
        let [key] = Row { value: &results.keys, index }.product()?;
        let [result] = Row { value: &results.vals, index }.product()?;
        let (tag, payload) = result.variant()?;
        let result = match tag {
            0 => { payload.unit()?; Ok(()) }
            1 => {
                let [group, failure] = payload.product()?;
                let group = usize::try_from(group.integer()?).map_err(|_| corrupt("negative group index"))?;
                let (tag, value) = failure.variant()?;
                let reason = match tag {
                    0 => Failure::Write(value.frontier()?),
                    1 => Failure::Exists(value.id()?),
                    2 => Failure::UnknownLedger(value.id()?),
                    3 => { value.unit()?; Failure::Invalid("transaction structure is invalid") }
                    _ => return Err(corrupt("invalid failure tag")),
                };
                Err(Rejection { group, reason })
            }
            _ => return Err(corrupt("invalid transaction result tag")),
        };
        let key = key.integer()?;
        if !seen.insert(key) { return Err(corrupt("duplicate transaction result")); }
        if result.is_ok() { return Err(corrupt("failure output contains a success")); }
        outcomes.insert(key, result);
    }
    if outcomes.len() != transactions.len() || transactions.iter().any(|(key, groups)| {
        outcomes.get(key).is_none_or(|result| result.as_ref().is_err_and(|failure| failure.group >= groups.len()))
    }) { return Err(corrupt("transaction results do not match requests")); }

    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_words_preserve_bits_and_order() {
        let mut values = vec![0, 1, (1 << 63) - 1, 1 << 63, u64::MAX as u128,
            1 << 64, (1 << 127) - 1, 1 << 127, u128::MAX];
        let mut random = 17_u128;
        for _ in 0..128 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            values.push(random);
        }
        values.sort();
        values.dedup();
        let values = values.into_iter().map(Id::from_u128).collect::<Vec<_>>();
        let encoded = ids(values.iter().copied());
        let Value::Prod(words) = &encoded else { unreachable!() };
        let high = words[0].as_i64("high").unwrap();
        let low = words[1].as_i64("low").unwrap();
        let ordered = high.iter().zip(low.iter()).collect::<Vec<_>>();
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
        let restored = wide(public_ids(encoded).unwrap()).unwrap();
        for (index, id) in values.into_iter().enumerate() {
            assert_eq!(Row { value: &restored, index }.id().unwrap(), id);
        }
        assert_eq!(public_ids(ids([])).unwrap().len(), 0);
    }
}
