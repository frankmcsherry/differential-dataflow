//! [`LaneStore`]: the flat fixed-width [`TimeStore`] for `Product<u64, PointStamp<u64>>`.
//!
//! Each distinct time is one row of `k` zero-padded `u64` lanes (`[outer, coords.., 0..]`,
//! row-major). The rung-2 audit's facts make the rank algebra pure integer loops:
//! * total order = lane-lexicographic (≡ canonical `Ord` over canonical inputs),
//! * partial order = all-lanes `<=` (`Product` ∧ coordinatewise `PointStamp`, absent = 0),
//! * join/meet = elementwise max/min (results interned; memoized per pair).
//!
//! Owned `PointStamp`s exist only at the edges: `intern` consumes one, `time` reconstructs one
//! through `PointStamp::new` (which trims — re-canonicalization is the type's own invariant
//! path, so the padded/trimmed `Eq` hazard cannot arise). `k` grows by re-laning when a deeper
//! time arrives (bounded by the program's nesting depth; rare).

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::operators::recipes::{Rank, TimeStore};
use timely::order::Product;

/// The DDIR render time: one iterative scope of dynamic depth.
pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// An identity-ish hasher for u64 keys that are already mixed (see `IdHasher` in the reduce
/// backend; duplicated here to keep the store self-contained).
#[derive(Default)]
struct U64Hasher(u64);
impl std::hash::Hasher for U64Hasher {
    #[inline]
    fn write_u64(&mut self, i: u64) { self.0 = i; }
    #[inline]
    fn write(&mut self, _: &[u8]) { unreachable!("U64Hasher keys are u64") }
    #[inline]
    fn finish(&self) -> u64 { self.0 }
}

/// Flat fixed-width time store for `Product<u64, PointStamp<u64>>`.
pub struct LaneStore {
    /// Lanes per row: 1 (outer) + inner coordinate capacity. Grows by re-laning.
    k: usize,
    /// Row-major `len * k` zero-padded lanes.
    data: Vec<u64>,
    /// Content hash of a row's lanes → candidate ranks (verified by lane equality).
    index: HashMap<u64, Vec<Rank>, BuildHasherDefault<U64Hasher>>,
    join_memo: HashMap<u64, Rank, BuildHasherDefault<U64Hasher>>,
    meet_memo: HashMap<u64, Rank, BuildHasherDefault<U64Hasher>>,
    /// Encode/compute scratch row.
    scratch: Vec<u64>,
}

impl Default for LaneStore {
    fn default() -> Self {
        LaneStore {
            k: 1,
            data: Vec::new(),
            index: HashMap::default(),
            join_memo: HashMap::default(),
            meet_memo: HashMap::default(),
            scratch: Vec::new(),
        }
    }
}

/// splitmix64 — the store's lane-content mix.
#[inline]
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

impl LaneStore {
    fn len_rows(&self) -> usize {
        if self.k == 0 { 0 } else { self.data.len() / self.k }
    }

    #[inline]
    fn row(&self, r: Rank) -> &[u64] {
        let i = r as usize * self.k;
        &self.data[i..i + self.k]
    }

    fn hash_lanes(lanes: &[u64]) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        for &l in lanes {
            h = mix(h ^ l);
        }
        h
    }

    /// Grow to `k2` lanes: every stored row gains trailing zero lanes. Hashes change (row
    /// content is k-dependent only through padding — zeros hash in), so the index rebuilds.
    fn relane(&mut self, k2: usize) {
        debug_assert!(k2 > self.k);
        let n = self.len_rows();
        let mut data = Vec::with_capacity(n * k2);
        for r in 0..n {
            data.extend_from_slice(&self.data[r * self.k..(r + 1) * self.k]);
            data.resize((r + 1) * k2, 0);
        }
        self.data = data;
        self.k = k2;
        self.index.clear();
        for r in 0..n {
            let h = Self::hash_lanes(&self.data[r * k2..(r + 1) * k2]);
            self.index.entry(h).or_default().push(r as Rank);
        }
        // memos hold ranks (stable) keyed by rank pairs — unaffected by re-laning.
    }

    /// Intern the scratch row (length == k). Clears scratch.
    fn intern_scratch(&mut self) -> Rank {
        let h = Self::hash_lanes(&self.scratch);
        if let Some(cands) = self.index.get(&h) {
            for &c in cands {
                let i = c as usize * self.k;
                if self.data[i..i + self.k] == self.scratch[..] {
                    self.scratch.clear();
                    return c;
                }
            }
        }
        let r = self.len_rows() as Rank;
        self.data.extend_from_slice(&self.scratch);
        self.index.entry(h).or_default().push(r);
        self.scratch.clear();
        r
    }
}

impl TimeStore for LaneStore {
    type Time = Time;

    fn intern(&mut self, t: Time) -> Rank {
        let coords = t.inner.into_inner();
        let need = 1 + coords.len();
        if need > self.k {
            self.relane(need);
        }
        self.scratch.clear();
        self.scratch.push(t.outer);
        self.scratch.extend_from_slice(&coords);
        self.scratch.resize(self.k, 0);
        self.intern_scratch()
    }

    fn time(&self, r: Rank) -> Time {
        let row = self.row(r);
        let mut coords: smallvec::SmallVec<[u64; 1]> = smallvec::SmallVec::new();
        coords.extend_from_slice(&row[1..]);
        Product::new(row[0], PointStamp::new(coords))
    }

    fn len(&self) -> usize {
        self.len_rows()
    }

    fn cmp_ranks(&self, a: Rank, b: Rank) -> std::cmp::Ordering {
        self.row(a).cmp(self.row(b))
    }

    fn le(&self, a: Rank, b: Rank) -> bool {
        self.row(a).iter().zip(self.row(b)).all(|(x, y)| x <= y)
    }

    fn join(&mut self, a: Rank, b: Rank) -> Rank {
        let key = mix(((a.min(b) as u64) << 32) | a.max(b) as u64);
        if let Some(&r) = self.join_memo.get(&key) {
            return r;
        }
        self.scratch.clear();
        {
            let i = a as usize * self.k;
            let j = b as usize * self.k;
            for l in 0..self.k {
                self.scratch.push(self.data[i + l].max(self.data[j + l]));
            }
        }
        let r = self.intern_scratch();
        self.join_memo.insert(key, r);
        r
    }

    fn meet(&mut self, a: Rank, b: Rank) -> Rank {
        let key = mix(((a.min(b) as u64) << 32) | a.max(b) as u64);
        if let Some(&r) = self.meet_memo.get(&key) {
            return r;
        }
        self.scratch.clear();
        {
            let i = a as usize * self.k;
            let j = b as usize * self.k;
            for l in 0..self.k {
                self.scratch.push(self.data[i + l].min(self.data[j + l]));
            }
        }
        let r = self.intern_scratch();
        self.meet_memo.insert(key, r);
        r
    }

    fn clear(&mut self) {
        self.data.clear();
        self.index.clear();
        self.join_memo.clear();
        self.meet_memo.clear();
        // k is retained: the program's depth does not shrink between retires.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::operators::recipes::OwnedStore;

    fn xs(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    fn random_time(s: &mut u64) -> Time {
        let depth = (xs(s) % 4) as usize;
        let coords: smallvec::SmallVec<[u64; 1]> = (0..depth).map(|_| xs(s) % 3).collect();
        Product::new(xs(s) % 3, PointStamp::new(coords))
    }

    /// LaneStore ≡ OwnedStore on identical op sequences: every returned rank, every
    /// materialized time, every cmp/le verdict — including re-laning (depth growth mid-run)
    /// and post-`clear` reuse.
    #[test]
    fn lane_store_matches_oracle() {
        for seed in 1u64..25 {
            let mut s = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let mut lane = LaneStore::default();
            let mut owned: OwnedStore<Time> = Default::default();
            for _epoch in 0..2 {
                let mut ranks: Vec<Rank> = Vec::new();
                for _ in 0..40 {
                    match xs(&mut s) % 4 {
                        0 | 1 => {
                            let t = random_time(&mut s);
                            let (rl, ro) = (lane.intern(t.clone()), owned.intern(t));
                            assert_eq!(rl, ro, "intern rank");
                            ranks.push(rl);
                        }
                        2 if ranks.len() >= 2 => {
                            let a = ranks[(xs(&mut s) as usize) % ranks.len()];
                            let b = ranks[(xs(&mut s) as usize) % ranks.len()];
                            let (rl, ro) = (lane.join(a, b), owned.join(a, b));
                            assert_eq!(rl, ro, "join rank");
                            ranks.push(rl);
                        }
                        _ if ranks.len() >= 2 => {
                            let a = ranks[(xs(&mut s) as usize) % ranks.len()];
                            let b = ranks[(xs(&mut s) as usize) % ranks.len()];
                            let (rl, ro) = (lane.meet(a, b), owned.meet(a, b));
                            assert_eq!(rl, ro, "meet rank");
                            ranks.push(rl);
                        }
                        _ => {}
                    }
                }
                for &a in &ranks {
                    assert_eq!(lane.time(a), owned.time(a), "time");
                    for &b in &ranks {
                        assert_eq!(lane.cmp_ranks(a, b), owned.cmp_ranks(a, b), "cmp");
                        assert_eq!(lane.le(a, b), owned.le(a, b), "le");
                    }
                }
                lane.clear();
                owned.clear();
            }
        }
    }
}
