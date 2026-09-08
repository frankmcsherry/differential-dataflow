External-memory CHUNK traces need a read path that does not retain every decoded source chunk touched by a cursor. This draft checkpoints key-selected trace caching and an isolated file-backed experiment using a real DD join and count.

Based directly on `master-next` at `75fba2b9`. The final code is ported to the current Span API, updated join tactic signature, and relocated merger traits. The original measured checkpoint remains on `frankmcsherry:file-chunk-spike`; its unrelated Corgi branch commits are not included here.

### Changes

- `KeyedChunk` and `CachedTrace::span_through_keys`: materialize selected histories without advancing timestamps, reuse overlapping coverage, and read uncovered suffix spans. Empty selections preserve coverage with no batch payload.
- Example-only file-backed CHUNK wrapper with resident bounds, scoped decoding, incremental packing, and no cursor implementation.
- A static-side join tactic feeding standard DD count, with full insertion/retraction checks, an RSS watchdog, recorded experiments, and a dependency snapshot.
- Handoff and reproduction guide: `benches/file_chunk/README.md`.

### Evidence and limits

Original-checkpoint measurements: 1.53 GiB of files and a dispersed 1% hot set ran at 86 MiB peak RSS, with zero file I/O after warm-up. The same source with 64 hot keys used about 5 MiB. These numbers have not been remeasured on this port.

The large source is static; query memberships change. COUNT only receives sparse join results. Full-keyspace REDUCE hydration and large-source updates have not been demonstrated. Oversized selections can exceed the memory budget even with cache retention disabled; failed watchdog runs are retained as evidence.

### Remaining work

- Sequential REDUCE hydration with bounded input reads and spillable output, followed by cached sparse updates.
- Byte-budgeted streaming selection, continuation within large keys, and yielding/asynchronous fetch preparation.
- Evolving source traces, symmetric joins, ordinary ingestion, and background compaction.
- Read granularity, cache retirement, and disk/file-handle controls before 10–50 GB runs.
- Compression and Corgi integration.

### Validation on master-next

- 58 tests passed: 42 library, 10 cache, and six file-wrapper tests; one existing library test ignored. Both additional example test targets compile.
- A 1M-update file-backed join completed 12 checked rounds below 7 MiB peak RSS and did zero file I/O after warm-up.
- Moving clustered-key join, suffix/eviction cache example, and small resident/paged benchmark checks passed.
- New-port logs are in `benches/file_chunk/results-master-next` and `tests-master-next.log`; historical measurements remain separate.

This draft is a checkpoint for later work, not a production-ready change.
