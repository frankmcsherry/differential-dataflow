# Key cache measurements, 2026-09-06

The prototype adds 432 net implementation lines, including comments and blank
lines: 292 in `wrappers/cached.rs`, 32 in `chunk/keyed.rs`, and 108 net lines in
existing chunk implementations and module declarations. The original tests and
example add another 675 lines. The benchmark and measurements here are additional.

## Method

- Apple M4, 16 GiB RAM; rustc 1.96.0; Cargo's normal release profile (including LTO).
- Real `TraceAgent<ChunkSpine<ColChunk>>`; eight initial batches, four values per
  key, one timestamp per batch. Sizes: 65,536 keys / 2,097,152 updates, and
  262,144 keys / 8,388,608 updates. Chunk target: 8,192 updates.
- Each request selects 16 distinct keys and consumes every matching update.
  Checksums must agree for every mode and round. Hot keys are spread across the
  key space to separate temporal reuse from contiguous-key locality.
- 256 queries, two rounds with execution order reversed. Each round starts with
  an empty selection cache. Tables average the two per-round mean latencies,
  including initial cache population. Raw CSVs also include p50/p95 per round.
- `uncached`: identical selected-read API with retention disabled (capacity 0).
  `cached`: budget 65,536 unless specified, counted as updates + keys + entries.
  `cursor`: resident baseline that seeks and consumes the original batches
  without constructing a selected batch. This has a different output contract
  from materialization, but is a relevant alternative for existing operators.
- `paged` forces all source chunks into real temporary files. The files fit in
  RAM and use the OS page cache; these are **not cold-disk, remote-storage, or
  larger-than-RAM benchmarks**. Avoided reads include file open/read and columnar
  decoding. No artificial latency is injected. A paged cursor baseline is omitted
  because the current cursor pins entire decoded source chunks indefinitely.
- Only selected reading and consumption are timed. Source construction, batch
  insertion, and physical compaction outside the read are excluded. These are
  read costs, not end-to-end dataflow throughput or ingestion measurements.

## 65,536 keys: mean microseconds per request

| Workload | Resident cursor | Resident uncached | Resident cached | Paged uncached | Paged cached |
|---|---:|---:|---:|---:|---:|
| Identical 16 keys repeatedly | 15.68 | 61.34 | 2.00 | 3647.44 | 18.89 |
| Changing 16-key subsets of 64 hot keys | 16.15 | 59.98 | 22.65 | 3831.99 | 183.70 |
| Changing subsets of 4,096 keys | 28.81 | 69.74 | 79.29 | 3454.73 | 2893.55 |
| Uniform keys | 30.47 | 71.97 | 75.07 | 3487.75 | 3435.10 |
| Same 16 keys, appended updates, compaction enabled | 20.04 | 145.16 | 61.90 | 1148.14 | 81.05 |

The append workload adds 64 updates (four per requested key) before every read.
`append_compact` advances physical compaction to the previous committed upper
before each read, allowing source merges while protecting the cached cut. Logical
compaction remains at zero, preserving the full history. The raw `append` rows
instead hold physical compaction at zero; those runs accumulate 256 extra source
batches and yield much larger apparent benefits. They should not be treated as
steady-state LSM results.

For 64 hot keys, paged source fetches fall from 112.875 to 4.594 per request
(including fills and flushes). Median cached reads are about 19 us, but p95 is
1.1–1.3 ms and the mean is 184 us. Flushes and initial misses matter.

At 262,144 keys, the corresponding paged speedups are 211x for exact repeated
requests, 23x for 64 hot keys, 1.25x for 4,096 keys, and essentially 1x for uniform
requests. Resident hot64 still loses to cursors: 24.48 us versus 18.26 us.

## The policy retains overlapping histories repeatedly

For the static 64-key hot set, one combined entry would cost only
64 * (32 updates + 1 key) + 1 entry = 2,113 units. Distinct overlapping requests
instead retain separate 529-unit entries. This creates a strong budget effect:

| Budget units | Peak charge | Peak entries | Paged cached mean, us | Speedup vs uncached |
|---|---:|---:|---:|---:|
| 4,096 | 3,703 | 7 | 1564.19 | 2.46x |
| 65,536 | 65,067 | 123 | 183.70 | 20.86x |
| 262,144 | 135,424 | 256 | 83.17 | 45.91x |

The largest budget avoids flushing during these 256 requests; it still stores
roughly 64 copies' worth of the hot history. The next useful policy change is
coalescing or trimming overlapping coverage, with an admission policy for cold
requests. Changing an upper also rebuilds the selected history, so the prototype
does not yet implement an amortized base-plus-deltas strategy for large keys.

The wrapper falls back to reading the source when physical compaction passes a
cached cut. These measurements do not exercise repeated loss of those cuts.

## Reproduce

These measurements are from the original `file-chunk-spike` checkpoint at
`3fecff64`. This PR ports the benchmark to current master-next; its results
should be recorded separately when rerun. The original dependency snapshot
is available on the checkpoint branch.

From the workspace root:

```sh
cp benches/file_chunk/Cargo.lock Cargo.lock # Dependency snapshot for the master-next port.
cargo build --release -p differential-dataflow --example key_cache_bench --locked
target/release/examples/key_cache_bench 65536 256 65536
target/release/examples/key_cache_bench 262144 256 65536
target/release/examples/key_cache_bench 65536 256 4096 hot64
target/release/examples/key_cache_bench 65536 256 262144 hot64
target/release/examples/key_cache_bench 65536 256 65536 append_compact
```

The unfiltered commands now include `append_compact`, which was run separately
for the recorded results. `summary.csv` averages each raw file's two rounds;
its percentile columns average the per-round percentiles rather than pooling
individual query samples. These are short local microbenchmarks, without
confidence intervals or an end-to-end operator workload.
