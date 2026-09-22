# A CHUNK-framework replacement for `ord_neu`

> **Status 2026-09-21: built, measured, rebased, pushed for reading.** Branch
> **`chunk-ord-neu`** (`origin`, worktree `~/Projects/dd-chunk-ordneu`), four
> commits atop `tdf/master-next` tip `51c52cbd` ("Remove the three unbuildable
> crates", #893). The workspace builds and its 318 tests pass; `bfs` output is
> byte-identical to the `ord_neu` baseline. **How to integrate is the open
> question** — see §4. Nothing is merged and nothing upstream depends on it.

The brief (2026-09-11 evening): `ord_neu` has massive per-batch allocations, and
forces awkward `BatchContainer` implementations whose monomorphization does not
always pan out — columnar containers pay a borrow tax on each access, and
re-doing that borrow per access is expensive. The CHUNK API's control shift and
the TACTIC approach are meant to address this. The sketch to model it on: the
trie-based chunk in `columnar/`.

---

## 1. What is on the branch

**`a130a7f5` — the replacement.** New module
`differential-dataflow/src/trace/chunk/trie.rs`: `TrieChunk`, which is
`ord_neu`'s layered trie (deduplicated keys → per-key val runs → per-val
`(time, diff)` runs) cut into `TARGET`-bounded chunks and driven through the
existing `Chunk` harness. Also `TrieBuilder` (a `Vec`-input `Builder`, which
`reduce_abelian` and `arrange_from_upsert` need) and the `OrdValSpine` /
`OrdValBatcher` / `OrdValBuilder` + `OrdKey*` aliases.

The crate's default spines in `trace/implementations/mod.rs` (`ValSpine`,
`ValBatcher`, `ValBuilder`, `KeySpine`, `KeyBatcher`, `KeyBuilder`) now alias
the trie types. That six-line `pub use` block is the *only* invasive change:
cursor item types are identical to `ord_neu`'s `Vector` layout (`&K`, `&V`,
`&T`, `&R`), so every `arrange` / `reduce` / `join` call site compiled
untouched. `ord_neu.rs` itself is left in place, unchanged — only
`examples/spines.rs` names it directly, and it gained a `trie` mode alongside
`key`/`val`/`vec`/`col`.

Merge is trie-native: a survey-free three-level two-pointer with galloped runs
and bulk range copies, horizon-correct at mid-key and mid-val stops, draining
both deques within one call. (Mirroring `ColChunk`'s one-pair-per-call shape
first cost ~20%, because each call rebuilds the survivor's suffix — worth
knowing if `ColChunk::merge` ever shows up hot.)

The module is 1567 lines — 1148 of implementation (of which 201 comment and 107
blank, so ~840 lines of code) and 419 of test, against the 1169 lines of
`ord_neu.rs` it displaces and the 795 of `chunk/vec.rs` beside it.

**`5310f696` — memory.** The first cut was the *worst* mode on RSS (563MB on
`spines`, vs `ord_neu`'s 526MB): `ord_neu`'s `OffsetList` and its
singleton-update encoding store almost nothing for snapshot-shaped data, while
the trie paid two words of offsets plus a time and a diff per update. Two
compressed columns fix it, with clean semantics rather than the
empty-range-means-look-back trick:

* `Offsets` — cumulative bounds stored in O(1) while strided (one val per key,
  one update per val), spilling to an explicit `Vec` when the stride breaks. The
  open group's bound is held separately (`tail`) because it is the one the
  writers mutate, and is folded into the compressed form when the next group
  opens.
* `Column<T>` — times and diffs held as a single repeated value while every
  entry matches (a chunk of snapshot data carries one time and one diff),
  materializing to a `Vec` on the first difference.

A snapshot chunk now costs its keys alone. Compression survives merge, settle
and advance — the bulk copies fold compatible compressed ranges in O(1) — and a
test pins that. `settle` also releases excess vector capacity on committed
chunks.

**`4f523187` — the two hot spots that measurement found.** `Offsets` had the
compressed *representation* but not the compressed *copy*: `extend_shifted` ran
a `bound` / `push` pair per group and was the hottest leaf in a `spines` profile
at 16% of samples, more than the entire join. The strided prefix of a copied
range is an arithmetic progression and folds in O(1) when it continues the
destination's own; only the spill and tail remainder still copies bound by
bound. (`Column::extend_from` already had its equivalent fold; `Offsets` never
got one.) Separately, `ChunkBatchCursor::step_key` read two chunks' `bounds` and
compared keys on *every* key step, then re-sought the inner cursor before
stepping it — but a key can only spill forward if it is its chunk's last, which
stepping the inner cursor reveals for free. Together: `spines` 1M batches 4.89s
→ 3.94s, `bfs` 1M/2M 397ms → 335ms, max RSS unchanged. The cursor half is in
`chunk/cursor.rs`, shared harness code, so `vec` and `col` collect it too.

## 2. Measurements

`spines <1M keys> <batch> <mode>` — a `String`-keyed `arrange` + `join`, loaded
then queried round by round. Wall clock / max RSS:

| batch | `key` (ord_neu) | `val` (ord_neu) | `vec` | `col` | `trie` |
|------:|----------------:|----------------:|------:|------:|-------:|
| 1M    | 2.17s / 448MB   | 2.34s / 502MB   | 3.95s / 426MB | 4.11s / 309MB | **3.94s / 299MB** |
| 100k  | 3.10s / 266MB   | 3.34s / 300MB   | 5.42s / 378MB | 6.19s / 217MB | **6.43s / 222MB** |
| 10k   | 4.05s / 268MB   | 4.45s / 269MB   | 6.72s / 384MB | 8.23s / 197MB | **8.96s / 220MB** |

Flipping the six-line default and rebuilding, everything else equal:

| workload | `ord_neu` | `trie` | |
|---|---:|---:|---|
| `bfs` 100k/200k, 10 rounds | 33.0ms / 24MB | **24.9ms / 20MB** | 0.75× time, 0.83× RSS |
| `bfs` 1M/2M, 10 rounds     | 348ms / 199MB | **335ms / 160MB** | 0.96× time, 0.80× RSS |
| `scc` 100k/200k, 5 rounds  | 17.3s / 1410MB | **27.5s / 1304MB** | 1.59× time, 0.92× RSS |

Three readings, and they separate the harness from the layout:

* **The memory is the trie's, not CHUNK's.** `vec` — the same harness with row
  storage — is no better than `ord_neu` at 1M batches and *worse* at 100k and
  10k. Only `trie` and `col`, which dedupe keys and compress structure, run
  0.6–0.8×. Sampling RSS through a `spines` run, `ord_neu`'s peak is 1.46× its
  own mean against the trie's 1.19×: the per-batch allocations show up as
  transient as well as resident.
* **The bulk-arrange CPU is CHUNK's, not the trie's.** At 1M batches `vec`,
  `col` and `trie` cluster at 3.9–4.1s against `ord_neu`'s 2.3s — the chunk
  ladder pays maintenance per level where `ord_neu` builds once at seal. Only at
  smaller batches do the modes spread, the trie adding per-batch construction of
  its own.
* **On iterative work it flips, and not uniformly.** `bfs` is faster on the trie
  at both scales; `scc` — far more arrangements, much larger footprint — is
  1.59× slower while still leaner. Whatever the read side costs, it is not a
  constant factor.

## 3. How this relates to `columnar/`

Same skeleton, opposite answers to "what is a value". Both are `Chunk` impls in
the identical harness (same `ChunkBatch`, straddle cursor, batcher/builder/spine
aliases), and the algorithms are near-isomorphic — `TrieChunk::advance` is
`ColChunk::advance` transliterated, and the compression in `5310f696` is
consciously `Strides` and a two-state `Repeats`, hand-rolled.

The divergence is the representation of values, and it shows up on owned data:

| | `trace/chunk/trie` | `columnar/` |
|---|---|---|
| value repr | native Rust types in `Vec<K>` | decomposed columns, `Ref<'a, K>` |
| bound on `K` | `Ord + Clone` | `Columnar` |
| operator GATs | `&K` — `ord_neu`-identical, drop-in | columnar refs — call sites adapt |
| owned data (`String`) | per-row heap, `clone` per bulk copy | flat bytes+offsets, no per-row malloc |
| serialization / spill | none | `Stash` / `Paged`, nearly free |

`5310f696` compressed the *structure* (offsets, times, diffs); the keys are
still row-native, so on fatter owned values `col` should pull ahead again on
memory and locality. For small `Copy` types a `Vec<u64>` *is* a column and the
trie gives up nothing. After `4f523187` the top of a `spines` profile is
`memmove` / `memcmp` / `malloc` / `free` — `String` clone-and-drop churn, which
is exactly what `col` avoids.

Deliberate non-goal: pulling columnar's `Repeats` / `LookBack` into `trie.rs`.
They carry the `Columnar` bound, and plain-type genericity is this module's
entire reason to exist. The right direction is the reverse — `columnar/`'s
time/diff `Lists` adopting `Repeats` natively would win there what `Column` just
won here.

## 4. TBD: how to integrate

The convergence is **with chunks, and likely a custom tactic** (fmcsherry,
2026-09-12). Supporting observations, all checkable on the branch:

* The `BatchContainer` tax is already confined to a shim. Nothing in
  `trie.rs`'s storage or its four transducers touches `BatchContainer`; it
  appears only in the `Cursor` impl and `NavigableChunk` — the compatibility
  surface for the cursor-path operators. Same on the columnar side: `ColChunk`'s
  maintenance is trie-native, and the per-access `borrow()` lives in the
  `Cursor` / `Coltainer` machinery.
* So "deprecate cursors" need not touch storage at all. `join_with_tactic`
  already requires only `TraceReader`, never `Navigable`, and hands the tactic
  whole batches; `reduce_with_tactic` likewise. `CursorTactic` is just the one
  tactic that drags the GAT ceremony back in — and it is what `join_core` and
  `reduce_abelian` resolve to today, over a `CursorList` of straddle cursors.
  `int_proxy` is nowhere in that path.
* A custom tactic recovers monomorphism to the chunk type: a `TrieChunk` tactic
  borrows nothing, a `ColChunk` tactic calls `view()` once per chunk and
  amortizes the borrow across the whole chunk without giving up columnar refs.
  The output side sheds a compatibility artifact too — `TrieBuilder` exists
  *solely* because `reduce_abelian` demands
  `Bu: Builder<Input = Vec<((K,V),T,R)>>`; a tactic yields finished chunks and
  that bound evaporates.
* Compression-awareness is expressible only in a tactic: `times` as `Repeat`
  means the chunk is snapshot-shaped and its diff column can be processed
  wholesale; `val_offs` strided at 1 means one val per key and a whole trie
  level can be skipped. That is the CHUNK thesis extended from maintenance to
  compute.
* Caveat: seeking does not go away, only its trait surface does. A join against
  a large trace still wants `seek_key` and the cross-chunk bounds gallop; a
  tactic implements that over its native layout (galloping a `Vec<K>` is what
  `TrieCursor` already does, minus the ceremony).

The `int_proxy` shape looks like the answer to "custom tactic per storage
without an N×M explosion": one generic tactic logic over dense identifiers and
runs, with per-chunk-type *backends* minting identifiers from their native
layout. `VecReduceBackend` is the worked example, the columnar-hash backend is
the recorded keystone, and a `TrieChunk` backend is the natural third — it is
where `Offsets` / `Column` would pay on the compute path rather than only at
rest. With hash-keys folded in, the key column is a `u64` lane.

Candidate end state: `ChunkSpine<C>` as the only trace shape (already true for
trie/vec/col), `Chunk` as the maintenance contract, tactics/backends as the
compute contract, and `Navigable` + `Cursor` demoted to an optional capability
for the long tail (dogsdogsdogs-style demand-driven access, `examples/cursors.rs`).

**The measurable next step**, and the one that would settle the read side the
way §2 settled storage: a `TrieChunk` reduce backend (or a trie-native
`JoinTactic`) benchmarked against `CursorTactic` on an identical arrangement.
That number *is* the cursor tax. `scc`'s 1.59× is the standing argument that it
is worth measuring.

## 5. Gotchas for whoever picks this up

* **`Cargo.lock` is gitignored**, and the branch needs one matching
  `master-next`'s timely — currently `4eb3ee09`. A worktree holding an older
  lock (`6a0ffa8`, which this branch was first built against) fails with
  `retain_least` / `inspect_core` / `for_each_stamp` errors; `cargo update
  timely timely_communication timely_container timely_logging columnar` fixes
  it, or copy a lock from another `master-next`-based worktree.
* `tpchlike` — named in earlier drafts of this note as the run that would give a
  real verdict — was removed from the repository in #893 along with `doop` and
  `advent_of_code_2017`, all three unbuildable. `scc` and `bfs` in §2 are the
  standing substitutes.
* Other open questions if this ever goes upstream: whether `ord_neu` is
  deprecated or kept as a second layout, and whether the default flip belongs in
  the same PR as the new module or a follow-up.
* Smaller follow-ups: per-run RLE in `Column`'s explicit arm (the
  columnar-`Repeats` shape, for mixed-time chunks); `u32` spill offsets; the
  `step_key` trick of `4f523187` applied to `map_times` / `step_val` /
  `seek_val`, which still reconstruct "am I at the chunk's last key" from
  `bounds()` plus a key comparison where `TrieCursor` knows it as an integer
  compare.
