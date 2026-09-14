# Time-container tactics

This changes int-proxy's existing join and reduce tactics to use `C: TimeContainer`.
`Vec<T>` supplies ordinary lattice timestamps; Corgi's `ColTimes<T>` supplies shared
primitive lanes. Both use the same scheduler. Backends present aligned key, value-id,
time and difference columns and receive corrections in that representation.

Read in this order:

1. [TimeContainer](../differential-dataflow/src/operators/int_proxy/time_container.rs):
   range/gather/repeat operands and bulk comparison, lattice, copy and order operations.
2. [Keyed and Updates](../differential-dataflow/src/operators/int_proxy/updates.rs) and
   [history](../differential-dataflow/src/operators/int_proxy/history.rs): aligned
   key/time support, payloads, consolidation and replay.
3. [Join](../differential-dataflow/src/operators/int_proxy/join.rs) and
   [reduce](../differential-dataflow/src/operators/int_proxy/reduce.rs): the existing
   backend contracts now use containers; `Walk` and `Sweep` own time navigation.
4. [Corgi kernels](src/corgi/time_container.rs): implementations lane by lane.

Reduce selects complete affected keys, keeps raw novel time support in `Keyed<C>`
(two aligned columns, deduplicated without differences), and walks source, seed and
generated times. Callbacks reconcile visible values; corrections enter the sweep before its next evaluation. Deferred
work lives in flat [pending runs](../differential-dataflow/src/operators/int_proxy/pending.rs),
with readiness tested against the full frontier. Join crosses bounded blocks and
compacts replay buffers against the remaining history's meet.

`Sweep` drives a group of keys. It tests single-seed dominance across disjoint
history ranges before allocating replay state. General keys copy their candidate
heads into one retained container. Each tournament round compares two index gathers;
consumption compares the full head range against gathered winners. Index buffers
are retained across waves. Keys test readiness together and reduce remaining-time
summaries into shared floor columns. Corrections are copied by lane across the
whole callback wave using `copy_many`. Replay-buffer preparation and candidate
generation remain local to each key; only live keys enter subsequent waves.
Batching those phases requires shared run storage with per-key segments and
segmented suffix meets. Head collection still allocates borrowed source descriptors;
the wave/commit protocol is still checked at run time.

Corgi's edge, trace and exchange changes keep the same primitive representation
between operators. The Timely pin selects the revision compatible with this base.
There are no retained experimental engines or benchmark archives in this change.

The second commit keeps head positions, advancement floors and suspended evaluation
times in containers. In-place operations accept the same collection operands as
maps and comparisons; there is no separate `advance_by` hook. Each group retains
its control storage across evaluations;
the algebra has no scalar timestamp-reading interface or owned-time operand.
The retirement driver imports `upper` once and exports the final pending frontier
using `FromIterator`/`IntoIterator`, bounds unavailable to the internal walks.
Replay advances while copying and gathers once after consolidation; join describes
Cartesian pairings with ranges and repeated rows instead of two index arrays.
Trace partitioning retains its residual columnarly across the whole extraction
pass. `Chunk::Residual` lets each implementation own that storage; the harness
imports and exports once while continuing to settle each chunk as it goes. This
associated type requires only timestamp import/export, with no int-proxy bound.
Corgi chooses columnar storage and reuses the container antichain helper; ordinary
chunks choose `Antichain<T>`.

Validation uses the existing DD cursor-reduce comparisons and Corgi integration
tests, plus bulk-algebra, Cartesian-join, product-grid snapshot and extraction
oracles:

```sh
cargo test --locked -p differential-dataflow --test int_proxy
cargo test --locked -p differential-dataflow --lib
cargo test --locked -p interactive --tests
cargo check --locked --workspace --all-targets
```
