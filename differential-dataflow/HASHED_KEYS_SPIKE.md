# Hashed keys / type-erased operators — spike (pruned)

A broad investigation of "hashed keys / type-erased operators / logic-as-program", pruned to the
parts that are **multitemporal** (partial-order time — the whole point of DD; total order is 1980s
tech). Two threads are kept:

1. **The value-blind membrane** — `Logic` as a type that understands the input and output chunks,
   with the operator working only in `u32` proxies. This is the actual idea. Kept in its
   **multitemporal** form (synthetic interesting times, joint logic).
2. **Columnar storage under the existing operators** — the parity-aiming direction: run the
   *unchanged* multitemporal `reduce`/`join` over `ColChunk` storage for its memory benefits.

| file | what it is |
|---|---|
| `examples/chunk_protocol_mt.rs` | The multitemporal `CHUNK1`/`CHUNK2`/`Logic` + `OutSpace` membrane (Vec-backed prototype). Lattice time with synthetic interesting times; joint logic reads prior output mid-sweep; operator never compares a value. The expression of the core idea, over partial orders. |
| `examples/reduce_columnar_arr.rs` | Multitemporal `reduce` → maintained `ColChunk` arrangement, via the real `reduce_trace` over columnar input + output spines. |
| `examples/join_columnar.rs` | Multitemporal delta-query `join` over `ColChunk`, via the real `join_traces`. |

```text
cargo run --release --example chunk_protocol_mt       # prints "Multitemporal OK"
cargo run --release --example reduce_columnar_arr     # prints MATCH vs built-in reduce
cargo run --release --example join_columnar           # prints MATCH vs built-in join_map
```

## Discipline: partial order is the bar

The recurring failure mode of this spike was doing **total-order** work and stopping — easy wins that
dodge DD's actual value proposition. To avoid it:

- **Partial order is the acceptance criterion, not a follow-up.** A total-order version counts only
  as scaffolding *explicitly on the path* to the partial-order one; "works for total order" is never
  "done."
- **The test harness's timestamp must be partially ordered** (iteration / `Product<T, Iter>`), or
  the test is vacuous. **Caveat that applies right now:** `reduce_columnar_arr` / `join_columnar` use
  the *real* multitemporal operators, but their self-checks feed **total-order data**
  (`InputSession<u64>` + `advance_to(round)`) — so `MATCH` currently only proves total-order
  behaviour even there. They need to be re-validated under iteration (e.g. reachability) before
  "multitemporal" is earned. `chunk_protocol_mt.rs` does test a genuine partial order (incomparable
  input times → a synthetic time).
- **The hard part is the deliverable, not the deferral.** A column-native *compute* that is
  multitemporal (folding over interesting/synthetic times) was repeatedly deferred as "the murk."
  `chunk_protocol_mt` proves the *semantics* are reachable; the open question is whether that compute
  vectorizes over columns — that question is the work, not something to sidestep with total order.

## The columnar-storage point

The operators are unchanged — `reduce_trace` / `join_traces` are generic over the trace, so they run
over a `ColChunk` spine as-is. The only thing that changes is **storage**: columnar trie
(`UpdatesTyped`) instead of the flat `Vec` spine. So this is purely a *storage* swap under
identical, already-at-parity compute, aiming for the memory win of columnar layout (dedup of repeated
keys/values) while keeping full multitemporal behaviour.

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
