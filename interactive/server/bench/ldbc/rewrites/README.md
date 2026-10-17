# Explicit query alternatives

These are manual physical plans. They do not add a query optimizer or replace
the original control plans. Run from the repository root:

```sh
cargo build --release -p ddir-server
python3 interactive/server/bench/ldbc/rewrites.py \
  --server target/release/ddir_server --people 2000 --posts 16 \
  --rounds 11 --maintained --output /tmp/ddir-rewrites
```

`--people 0` uses the tiny fixture; `--snapshot` uses an existing SNB snapshot
through the panel's adapter. The generated graph has ring-offset friendships,
64 tags with a deliberately popular tag, and regular posts/comments/likes.
It is a selectivity/fanout control, not an LDBC scale factor or workload score.
`--posts 64` increases message fanout without changing the friendship graph.
`--backend vec --people 0 --workers 4` is a small alternative-backend check.

Every plan is checked against the panel's independent graph oracle on initial
and changed graphs, after releasing its request batch, and after restoring
the graph. `--maintained` also holds that batch across graph changes and checks
the changed and restored answers. Query timing includes sending the small
request batch and `tick`; it excludes installation, base loading, peeking,
decoding and oracle work. Setup and graph-change timings are separate. Reports
include plans, hashes, requests, deltas, raw phase samples and server logs.

| Variant | IC6 | BI5 |
| --- | --- | --- |
| `original` | Existing expand-posts-then-test-tag plan | Existing collect/left-fold totals |
| `rewrite` | [Probe the global tag index first](ic6.ddp) | [Weighted native segmented sums](bi5.ddp) |
| `indexed` | [Build `(author, tag) → post` once](ic6_indexed.ddp) | Not applicable |
| `bounded` | Original join plan; [bounded ranking](ic6_bounded.ddp) | Weighted totals; [bounded ranking](bi5_bounded.ddp) |

The indexed IC6 variant joins/tag-indexes the base graph before requests arrive.
It avoids generating all reachable authors' posts for every request. This is
a reusable materialized index with a construction/update cost, not free
predicate pushdown. The tag-first variant probes tagged posts across the whole
graph before intersecting authors, and can do substantially more work than
the original neighborhood-first plan.

BI5's original sum expands contribution multiplicities into a list before a
general left fold. The weighted reducer consumes distinct `(value, diff)`
pairs and uses `fold_add` for each of the three components. This equivalence
assumes valid graph snapshots with nonnegative contribution multiplicities;
`collect` truncates negative multiplicities, whereas a weighted sum uses
them algebraically. Ranking's bounded alternatives rely on the upstream
aggregate rows having unit differences. They retain the input arrangement:
`sort_limit` reduces the resulting list, not the proxy presentation work.

## Measured controls (2026-10-17)

Apple M4, release, one worker, 2,000 people, 16 posts/person, 193,622 projected
graph rows. IC6 binds 16 requests; BI5 binds four tag requests including an
absent tag. One warmup and eleven samples, median initial bind + settlement:

| Plan | Pinned Corgi | With separate scalar-comparison patch |
| --- | ---: | ---: |
| IC6 original | 3.494 ms | 2.764 ms |
| IC6 global tag first | 10.010 ms | 8.916 ms |
| IC6 author/tag index | 1.920 ms | 1.291 ms |
| BI5 original | 21.885 ms | 13.991 ms |
| BI5 weighted sums | 20.938 ms | 12.903 ms |
| BI5 weighted sums + bounded ranking | 20.936 ms | 13.044 ms |

Both columns use the same experimental DDIR join/reduction/ingress changes.
The second additionally uses a Corgi patch refreshing the existing unmerged
[`corgi-scalar-compare` work](https://github.com/frankmcsherry/WIP/commit/2e4acfa2ed4134be5baf4dd583163c9bc0a5b8d0)
for current types; the repository dependency pin has not changed. These
whole-query binaries contain an early gathered-equality prototype. It was
subsequently removed: with allocation-free scalar comparison, direct borrowed
comparisons beat the operand gathers. Thus the table compares query writings
under matched runtimes, but does not time the final recommended reduction code.
The reduction performance change is proposed separately from these query controls.

The first controls before/after the initial DDIR reduction patch showed no
clear whole-query improvement. Bounded ranking also provides no established
binding latency gain here. Retain these negative results when choosing work.

At 64 posts/person with the local Corgi patch, IC6 original/indexed are
8.847/2.421 ms, and BI5 original/weighted are 49.375/45.040 ms. The index's
advantage grows with posts per reachable author. Setup wire times are recorded,
but short single startup samples do not establish an index-build cost or memory
advantage. RSS is sampled at one-second intervals; `null` means no sample.

The displayed binding table omits `--maintained`. The separate maintained report
also validates graph changes with requests held; changing that schedule changes
the amount of history work in later phases, even at a restored graph snapshot.

Raw historical results are in [query-controls.json.gz](../../../../benchmarks/query-controls.json.gz).
The join, reduction, ingress and scalar-comparison changes are proposed separately;
this branch changes the benchmark plans and harness only.

Read the complete historical reports with:

```sh
gzip -dc interactive/benchmarks/query-controls.json.gz > /tmp/query-controls.json
```

The measured binaries predate this branch split. Re-run the harness to compare
these writings on a chosen runtime; the checked-in times are not measurements
of unmodified master-next or of a merged final stack.
