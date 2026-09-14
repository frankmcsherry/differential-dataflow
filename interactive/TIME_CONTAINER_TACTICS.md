# Time-container tactics

This changes int-proxy's existing join and reduce tactics to use `C: TimeContainer`.
`Vec<T>` supplies ordinary lattice timestamps; Corgi's `ColTimes<T>` supplies shared
primitive lanes. Both use the same scheduler. Backends present aligned key, value-id,
time and difference columns and receive corrections in that representation.

Read in this order:

1. [TimeContainer](../differential-dataflow/src/operators/int_proxy/time_container.rs):
   range/gather/repeat operands and bulk comparison, lattice, copy and order operations.
2. [Updates](../differential-dataflow/src/operators/int_proxy/updates.rs) and
   [history](../differential-dataflow/src/operators/int_proxy/history.rs): aligned
   payloads, consolidation and replay.
3. [Join](../differential-dataflow/src/operators/int_proxy/join.rs) and
   [reduce](../differential-dataflow/src/operators/int_proxy/reduce.rs): the existing
   backend contracts now use containers; `Walk` and `Sweep` own time navigation.
4. [Corgi kernels](src/corgi/time_container.rs): implementations lane by lane.

Reduce selects complete affected keys, keeps raw novel time support separate from
netted records, and walks source, seed and generated times. Callbacks reconcile
visible values; corrections enter the sweep before its next evaluation. Deferred
work lives in flat [pending runs](../differential-dataflow/src/operators/int_proxy/pending.rs),
with readiness tested against the full frontier. Join crosses bounded blocks and
compacts replay buffers against the remaining history's meet.

Corgi's edge, trace and exchange changes keep the same primitive representation
between operators. The Timely pin selects the revision compatible with this base.
There are no retained experimental engines or benchmark archives in this change.

Validation uses the existing DD cursor-reduce comparisons and Corgi integration
tests, plus bulk-algebra, Cartesian-join and product-grid snapshot oracles:

```sh
cargo test --locked -p differential-dataflow --test int_proxy
cargo test --locked -p differential-dataflow --lib operators::int_proxy
cargo test --locked -p interactive --tests
cargo check --locked --workspace --all-targets
```
