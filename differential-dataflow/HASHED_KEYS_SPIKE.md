# Columnar storage under the existing operators — spike (pruned)

This started as a broad investigation of a "hashed keys / type-erased operators / logic-as-program"
idea. After measuring it, the only direction worth keeping is the **multitemporal, parity-aiming**
one: run the *existing* `reduce`/`join` operators (full lattice time, work-efficient, output-trace,
compaction — all unchanged) over **columnar (`ColChunk`) storage**, for its memory benefits. The
total-order / column-native-compute / kernel-microbench explorations have been removed (see *What was
cut* below).

Two files, both validated against the built-in operators, both on the public API (**no `src/`
changes**):

| file | what it is |
|---|---|
| `examples/reduce_columnar_arr.rs` | Multitemporal `reduce` producing a maintained `ColChunk` arrangement, via the real `reduce_trace` (`reduce_abelian`) over columnar input + output spines. |
| `examples/join_columnar.rs` | Multitemporal delta-query `join` over `ColChunk` arrangements, via the real `join_traces` (`join_core`). |

```text
cargo run --release --example reduce_columnar_arr     # prints MATCH vs built-in reduce
cargo run --release --example join_columnar           # prints MATCH vs built-in join_map
```

## The point

The operators are unchanged — `reduce_trace` / `join_traces` are generic over the trace, so they run
over a `ColChunk` spine as-is. The only thing that changes is **storage**: columnar trie
(`UpdatesTyped`) instead of the flat `Vec` spine. So this is purely a *storage* swap under
identical, already-at-parity compute, aiming for the memory win of columnar layout (dedup of repeated
keys/values) while keeping full multitemporal behaviour.

## Honest performance

Since the compute is the incumbent's, the only delta vs the built-in (Vec-spine) operators is
storage. Measured (on the reduce path): **roughly bulk-parity, ~1.5× slower incremental** — the
`ColChunk` trie's per-round build/merge costs more than the flat `Vec` spine. The upside (columnar
memory savings) is not yet measured here. So: a storage trade — potential memory win, modest
incremental compute cost — under unchanged, parity compute.

## What was cut, and why

Removed as not-multitemporal and/or not-aiming-for-parity:

- **Column-native *compute*** (a `reduce_trace` fork that folds columns instead of using the cursor):
  only ever built for **total-order**, and after full optimization it merely *reached* ~parity
  (bulk) — it did not beat the incumbent, and it converged to cursor-over-`ColChunk` anyway. Not
  worth carrying.
- **Kernel microbenchmarks** (fold-vs-cursor, value-blind-diff cost, logic-as-program): measured
  isolated kernels (~2× fold-vs-cursor) that did **not** survive as an operator-level win — they
  oversold the idea relative to the end-to-end result.
- **Userland model checks and the value-blind CHUNK/Logic/OutSpace protocol prototypes**: conceptual
  validations (Vec-backed), not parity-aiming.

The durable lesson from all of it: operator engineering and access pattern dominate any column-native
kernel advantage; the realistic value of columnar here is **storage/memory under the existing
operators**, not a compute speedup.

## Open / not done

There is currently **no benchmark for the surviving (multitemporal) path** — the perf figure above
was measured on the now-removed total-order fork, and transfers because the storage cost is the same.
A proper `reduce_columnar_arr`-vs-built-in benchmark, and a memory comparison (the actual hoped-for
win), would be the next things to add if this direction is pursued.
