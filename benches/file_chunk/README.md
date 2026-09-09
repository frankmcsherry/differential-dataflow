# File-backed CHUNK / sparse-key cache spike

The central experiment works: a 67,108,864-update relation occupies **1.53 GiB
of files**, and an actual differential join followed by `count` repeatedly
operates on its dispersed 1% key set at **86.2 MiB peak process RSS**. After
the first query, all eleven subsequent insertion/retraction rounds do **zero
file reads and zero file writes**. With only 64 hot keys, the same cold relation
needs **5.0 MiB peak RSS**.

This verifies a static backstop with changing query memberships. It does not
yet demonstrate an evolving disk-backed arrangement in a general symmetric
join, or a hard memory bound for arbitrary key histories.

## Location and reproduction

PR branch: `file-chunk-spike-master-next` in
[`frankmcsherry/differential-dataflow`](https://github.com/frankmcsherry/differential-dataflow/tree/file-chunk-spike-master-next),
based on `TimelyDataflow/differential-dataflow:master-next`, refreshed through
`7576bfe3` (cursor cleanup #866).
The local PR worktree is `/Users/mcsherry/Projects/dd-file-chunk-pr`.
This is a research checkpoint, not a proposed production change.

The original measured checkpoint remains at
[`file-chunk-spike` / `3fecff64`](https://github.com/frankmcsherry/differential-dataflow/tree/3fecff64),
based on `c9824fb8`. The large-run numbers in this report and the
`results` folder come from that checkpoint, **not a new
performance measurement of the master-next port**. The PR branch adapts the code
to spans carrying descriptions separately from optional batch payloads, updated
join tactic signatures, and relocated merger traits. The selected-read method
is now `span_through_keys`; update-free results carry `inner: None`.

The first two commits preserve the key-cache prototype and file-backed spike;
a follow-up commit ports them to the current APIs. Earlier cache measurements
are in [key_cache/README.md](../../differential-dataflow/benches/key_cache/README.md).
The wrapper's decode/selection code and the custom join tactic are example
support code, not additions to the public library API. The original source
and cache worktrees remain unchanged.

From this worktree, with Cargo on PATH:

```sh
cp benches/file_chunk/Cargo.lock Cargo.lock # Restore the dependency snapshot for the master-next port in a fresh checkout.
cargo build --release -p differential-dataflow --example file_chunk_spike --locked
cargo test --release -p differential-dataflow --example file_chunk_spike --test key_cache --locked
python3 benches/file_chunk/run.py --case all --memory-mib 128
```

The runner records stdout, exit status, kernel peak RSS, sampled RSS, and a CSV
summary in [results](results). It runs each workload in its own process and
kills that process on a sampled RSS or time overrun. This is a watchdog, not
an allocator-enforced limit: overshoot between 50 ms samples is possible.
The giant-key and medium cursor cases deliberately explore that boundary and
were stopped by the watchdog. Consult their status, rather than treating the
runner's own exit as a blanket success indication.

Direct invocation takes `keys fanout rounds hot_keys cache_capacity`, followed
by flags. Cache capacity is the existing prototype's charge in
`updates + keys + entries`, not bytes. For example:

```sh
target/release/examples/file_chunk_spike 4194304 16 12 41943 713032 --nocache
```

Flags: `--clustered`, `--moving` (shift the set after each insertion/retraction
pair), `--profile` (RSS immediately after cache selection), `--cursor-control`.
The last flag runs a read-only existing-cursor control, not the DD join.

Measurements were made on an Apple M4, 16 GiB RAM, macOS, Rust 1.96.0, release
build with the repository's LTO settings. All recorded runs requested macOS
`F_NOCACHE`, and the `fcntl` calls succeeded. Files are real, uncompressed,
unlinked temporary files, with explicit `write_all`, `seek`, and `read_exact`.
The reported I/O counts are application calls/bytes, not physical-device
telemetry. This is neither a cloud-latency benchmark nor a test with a dataset
larger than the machine's physical RAM. It tests a much smaller **process
memory budget**. For other platforms use the runner's `--buffered` option;
the current `--nocache` implementation is macOS-specific.

## What was built

- [storage.rs](../../differential-dataflow/examples/file_chunk_support/storage.rs):
  `FileChunk<C>` implements `Chunk` and `KeyedChunk`, deliberately **without
  `Navigable`**. A codec supplies serialization, decoding, and resident key
  bounds. The concrete codec uses `ColChunk<(u64,u64,u64,i64)>` and its existing
  columnar wire representation. Maintenance delegates to the inner chunk,
  loading input bodies on demand and writing settled output. Owned resident
  carries can be moved into the codec, avoiding unnecessary copies.
- The store rotates roughly 8 MiB segments. Chunk handles retain segments;
  dropping the last handle reclaims the segment. Selected resident chunks
  retain no source file handles. Full already-persisted chunks pass through
  `settle` without I/O. Smaller chunks may still be unpacked and rewritten.
- Key selection checks resident bounds, reads one intersecting source body at
  a time, keeps only requested keys, and incrementally packs the selected
  records into resident chunks. It never populates a source-side decode cache.
  The existing `CachedTrace` supplies coverage, suffix selection, physical
  merging without timestamp advancement, and eviction.
- [join.rs](../../differential-dataflow/examples/file_chunk_support/join.rs): a
  custom `JoinTactic` gathers query keys from the small resident input, requests
  the cached source selection, and emits at most 1,024 joined updates per
  iterator step. It implements a weighted semijoin projected to source keys:
  every matching source value contributes its difference at the joined time.
  Only the query side uses a cursor.
- [file_chunk_spike.rs](../../differential-dataflow/examples/file_chunk_spike.rs):
  builds the cold relation in 8,192-record chunks, persists it, exposes the
  prebuilt batch as an `Arranged`, and joins changing memberships against it.
  Standard DD `count` then produces per-key fanout counts. Every completed
  round checks the entire output against an independent expected map.
  Retractions must remove the previous counts completely. Probe advancement
  is included in the timings; initial source construction is logged separately.

At the original checkpoint, the spike added about 790 formatted Rust lines for storage, tactic, and example,
plus 261 test lines and the Python runner, on top of the copied key cache.

## Measurements

Single runs, not confidence intervals. Hot keys are evenly dispersed unless
specified. "Warm" is the median of rounds after the first. MiB are binary.
Peak RSS includes construction, selection, join, count, and verification.

| Workload | File MiB | Hot keys | Peak RSS MiB | First round ms | Warm ms | Later file reads |
|---|---:|---:|---:|---:|---:|---:|
| 1.05M updates | 24.5 | 655 (~1%) | 6.6 | 32.8 | 0.46 | 0 |
| 16.8M updates | 392.4 | 10,485 (~1%) | 25.0 | 447 | 7.56 | 0 |
| 67.1M updates | 1,569.8 | 41,943 (~1%) | 86.2 | 1,851 | 33.1 | 0 |
| 67.1M updates | 1,569.8 | 64 | 5.0 | 20.5 | 0.045 | 0 |
| 16.8M, clustered | 392.4 | 10,485 | 25.1 | 14.9 | 7.50 | 0 |
| 16.8M, cache disabled | 392.4 | 10,485 | 28.6 | 492 | 447 | 2,048 per round |

The medium steady run completed **200 alternating insertion/retraction rounds**
at 24.9 MiB kernel peak RSS. It read 392.4 MiB once, and nothing subsequently.
The 67.1M record source was initially constructed at roughly 4 MiB RSS. The
resident chunk directory grows with source size; the cold payload does not.

Locality matters even when the number of selected keys is unchanged. At 16
values/key, a full chunk contains 512 keys. A dispersed 1% set touches **every
source chunk**, reading the entire file on a cold miss. The clustered 1% set
reads only 21 chunks / 4.02 MiB. The 64-key case on the large relation reads
64 chunks / 12.26 MiB initially. Whole-chunk decoding is adequate for these
bounded chunks, but cold read amplification remains substantial.

Moving a dispersed set by one key after each pair replaces all its selected
keys in these runs. Each new set therefore rereads 392.4 MiB and takes about
448 ms, while its paired retraction hits the cache. Moving a clustered set by
one key retains almost all its keys: each new set reads **one chunk**, totaling
2.11 MiB of additional reads across eleven shifts; changed-set rounds take a
median 10.7 ms. Both use a cache capacity for just one complete selection and
remain below 33 MiB peak RSS. These cases distinguish locality-driven reuse
from a benchmark that only repeats an identical request.

## Iterations and boundaries found

An early implementation accumulated one tiny selected trie per source chunk
before repacking and passed the full key request to every chunk's selector.
Its large 1% run reached about 129 MiB peak RSS. Incremental packing and limiting
each chunk's key argument reduced the peak to 86 MiB. The intermediate debugging
logs are omitted from this PR; the retained results exercise the corrected
implementation at the original checkpoint.

The existing `ColChunk` spill cursor, using the **same file backend**, pins
decoded source chunks in its `OnceCell`. A dispersed 1% scan of the small
24.5 MiB source reaches 27.3 MiB RSS; its warm scans are very fast because it
has retained essentially the whole source. The 392.4 MiB source crosses the
128 MiB watchdog before finishing its first scan. This control only sums
cursor results, so its timings are not a join-vs-join comparison.

Selecting one of 100 keys where each key has 2,097,152 values also crosses the
128 MiB watchdog, **with cache capacity zero**. Source construction succeeds
at about 6 MiB RSS with 4.69 GiB of files. Reading that one key fetches only
48.1 MiB, but selection plus downstream processing reaches 152.6 MiB kernel
peak RSS. The first count was correct before the process was stopped. Thus
"one percent of keys" and "fits the retained-cache budget" do not establish
a bound on active memory; the API materializes the full selected batch even
when retention is disabled.

## Remaining work, in priority order

1. **Demonstrate full REDUCE hydration.** The measured dataflow is
   `cold relation -> sparse join -> count`: COUNT only receives requested keys.
   It never initializes counts for the other 99%. Add or extend a REDUCE tactic
   to scan input chunks in key order, release decoded inputs as it advances,
   carry incomplete key groups, and settle/spill output incrementally. Use
   keyed cached reads for later sparse updates. There is no automatic
   unrestricted-read bypass in `span_through_keys`: an empty key list selects
   nothing, and requesting all keys materializes all selected data. A streaming
   scan can instead consume shared source batch handles. A single huge key may
   still require spillable reducer state.
2. **Stream selected results under a byte budget.** `span_through_keys` returns
   a fully resident batch; capacity is checked after materialization. Add an
   incremental selected-read interface and a non-retaining streaming path.
   Partitioning a request by key helps many-key requests but cannot bound a
   single huge key: that needs value/history continuation or spillable selected
   output. Account for retained caches, transient copies, returned batches,
   in-flight operators, and variable-width records together.
3. **Integrate selected reads with general joins.** `join_with_tactic` provides
   batch lists, not a keyed reader callback. This tactic owns an independent
   cache over an immutable source trace and explicitly rejects cold-side
   updates. A general tactic needs both input directions, timestamp/frontier
   coverage, and coordination with shared trace compaction. File-backed real
   spine tests verify suffix reads and compaction separately; the large
   dataflow has no background cold-side merge workload.
4. **Make fetching resumable.** The tactic's `prep` currently performs the whole
   selected read synchronously, before its bounded output iterator can yield.
   The large miss occupies roughly seconds. An iterator/continuation should
   cover fetching, decoding, merging, and output, with asynchronous or batched
   remote I/O when appropriate. The one-key compaction carry inherited from
   `ColChunk::advance` can also grow with an entire `(key,value)` history; small
   correctness tests pass, but this is not a hard memory bound for maintenance.
5. **Choose read granularity and indexes.** Bounded 8,192-record chunks work
   here without cursor navigation or partial decoding. Larger external chunks
   would still require full-body allocation in this codec. Smaller indexed
   blocks, a resident key/block directory, and coalesced reads could reduce the
   cold miss cost; their memory/latency tradeoff needs measurement. The wrapper
   currently scans source chunk metadata on a miss.
6. **Connect ordinary ingestion and batch-to-stream paths.** Construction here
   creates a sorted static batch directly. Wire the wrapper into a normal
   chunker/`arrange_core` input next. Existing CHUNK batcher and fueled batch
   merger machinery already work in tests. `Arranged::as_container` offers a
   cursor-free batch conversion hook, but its unary closure drains each
   iterator in one activation; `as_collection` requires navigation. A reusable,
   yielding batch reader would serve both conversion and join tactics.
7. **Coordinate hot snapshots with LSM maintenance.** The existing cache can
   reuse a protected prefix and read only newly appended batches, but a merge
   crossing that prefix forces a refresh. Background source compaction can
   still read/write cold data even if queries hit the cache. Logical compaction
   must also bound growing update histories. The flush-on-full policy counts
   overlapping copies separately; a shared per-key/range representation and
   cost-aware retirement policy remain useful next steps.

The storage layer itself is a single-worker spike: synchronous I/O, no durable
manifest/recovery, codec versioning, checksums, or general error propagation.
Segments reclaim only when all their handles disappear. These limitations are
separate from the demonstrated query locality benefit.

Before scaling to 10–50 GB, add an explicit spill directory, disk-space preflight,
and memory enforcement. The current roughly 8 MiB segments each retain an open
file; 50 GiB would need about 6,400 descriptors. Larger segments or managed file
handles would address this. No runs that large were attempted. A fixed hot set
should isolate growth in source metadata; a fixed 1% hot fraction also grows
the resident selection and operator state.

Compression can follow the existing `columnar_spill` example's per-blob LZ4
backend, but has not been added here. Corgi already uses cursor-free CHUNKs and
tactics; integration still needs a codec, resident bounds, selected reads, and
adapted consumers. The cache's owned `Ord` keys do not directly express Corgi's
structural column ordering. Its larger chunks also need separate memory tests.

## Validation

At the original measured checkpoint, all **15 targeted tests passed**: nine copied key-cache tests plus six new file
wrapper tests. The latter cover round trips and detached selections, bounds
that avoid reads, repeated loads that do not pin sources, segment reclamation,
zero-I/O settling of full stored chunks, batcher merge/extract against a row
oracle, fueled batch merging with timestamp advancement and cancellation across
file boundaries, a single history spanning chunks, and real-spine suffix-cache
reads followed by compaction. Every successful benchmark checks the full DD
count output each round. The two watchdog stops are retained as experimental
limits, not reported as successful complete runs.

The master-next port also adds a test for update-free span progress, canonical
absent payloads, and extending a cached absence when a key first appears.
Port verification is recorded separately in `tests-master-next.log` and
`results-master-next`; the historical logs above are unchanged.

Refresh through `7576bfe3` (cursor cleanup #866): the same test command passed
58 tests with one existing library test ignored; see `tests-refresh.log`.
The refresh preserves both the cursor module and the keyed-selection module.
Historical performance measurements were not rerun.
