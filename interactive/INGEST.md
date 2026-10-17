# Native columnar ingress: initial implementation

`ingest::ColumnBatch` is the common boundary for in-process inputs, session
uploads, and local files. It owns Corgi **key and value columns** plus signed
`i64` differences. One server epoch applies to the batch; timestamps are not
repeated in the external payload. Lists, products, sums, references, integers
and floats use the existing Corgi column representation and checked byte codec.

The Corgi backend takes the columns directly. The Vec reference backend
converts them to rows at ingress. A positional input must have a shape
ascription, for example `input 0 : ((int, int) ; ())`. That shape is checked
at the batch boundary before any of it is staged, including on workers with an empty
shard. There is no per-row `Value::has_shape` or rows-to-columns transpose on
the native path.

## In-process input

```rust
use interactive::ingest::{Column, ColumnBatch};

let batch = ColumnBatch::new(
    Column::Prod(vec![Column::i64(vec![1, 2]), Column::i64(vec![2, 3])]),
    Column::Unit(2),
    vec![1, -1],
)?;
server.feed_columns("graph", 0, batch)?;
server.tick(worker);
```

`new` takes well-formed Corgi columns and checks equal outer lengths. Native
Rust callers retain Corgi's structural invariants when constructing columns;
`decode` checks those invariants for external bytes. `feed_columns` validates
the destination and shape, supplies the current epoch, and takes ownership.
Like `feed_batch`, it stages input without advancing time. In a multiworker
embedding, introduce each update once; `batch.shard(index, peers, offset)`
provides round-robin partitions. `offset` preserves the partition across a
stream of short batches.

## Streaming from sessions

TCP and stdin accept a UTF-8 header followed by exactly the stated number of
binary bytes:

```text
r7 feed graph 0 columns <N>\n
<N raw bytes from ColumnBatch::write_to / encode>
```

The next command starts immediately after those bytes; no payload terminator
or extra newline is required. A WebSocket upload sends the whole header and
payload as one binary message. Existing text commands remain text messages.
`length_in_bytes()` lets a client write the header and then `write_to` directly
to its socket without a temporary encoded buffer. Payloads can be sent in
successive batches. They use the ordinary request IDs
and `ok`/`err` responses; an acknowledgement means admission, while a following
`tick` establishes completion.

The session thread decodes complete frames before admission. Worker 0
partitions columns before worker transport; workers receive their shards,
not a replicated full input. Typed channels retain native columns; serialized
channels use the same checked batch codec. Keyed dataflow exchange still
places records with the workers owning their keys.

`DDIR_MAX_FEED_BYTES` (default 16 MiB) bounds each session payload and file
frame. The same numeric bound limits nested row/element declarations that a
small payload could otherwise name without storing leaf bytes. An invalid
complete frame admits no data. An oversized TCP/stdin frame or truncated body
ends that session after reporting the error, so binary bytes cannot be
misinterpreted as subsequent commands.

This first implementation keeps batches in the existing ordered command
stream. It removes the row representation and full-batch replication, but is
not an independent asynchronous data plane or an autonomous continual task.
Those require their own progress/admission design rather than a different
payload representation.

## Local loading

Concatenate `ColumnBatch::write_to` frames into a local file, then:

```text
feed graph 0 columns from /path/to/edges.col
tick
```

`Server::load_columns(worker, program, input, path, max_batch_bytes)` is the
corresponding in-process API. Every worker reads the same immutable file and
introduces its shard exactly once. Memory is bounded by a frame, apart from
data retained by the running computation. It does not first read the complete
file into nested rows.

A file is a **stream of batches**: each complete batch is validated before
staging. If a later frame is invalid, the error reports the complete prefix
already staged. That prefix is not rolled back; a subsequent tick can complete
it. The older text-file loader's whole-file behavior is unchanged.

## Frame format

| Offset | Contents |
| --- | --- |
| 0 | Eight-byte magic/version `DDIRCOL1` |
| 8 | Key column byte length, u64 little endian |
| 16 | Value column byte length, u64 little endian |
| 24 | Difference/outer row count, u64 little endian |
| 32 | Corgi key bytes, then Corgi value bytes, then one little-endian i64 difference per row |

The outer format is versioned and uses the repository's pinned Corgi codec.
Decoding preserves column types/widths and checks exact lengths, including
agreement of the key, value and difference columns. Corgi decoding copies
leaf buffers; this is columnar ingestion, not a zero-copy mapping API.
Arrow/Parquet/CSV adapters can target `ColumnBatch` without changing DDIR's
renderer. They are not included in this first implementation.

## A control that isolates the representation

The [Q6 control](server/bench/batch/README.md) creates a four-integer projection
and equivalent DDP plans. On that projection:

```sh
cargo run --release -p interactive --example q6_native_bench -- \
  /tmp/ddir-q6/lineitem.txt /tmp/ddir-q6/weighted.ddp 7 /tmp/lineitem.col
```

The example prepares 64K-row native batches outside the timer, writes the
column file, and checks the exact Q6 answer after each run. It reports native
feed/settlement, column-file load/settlement, and text-file load/settlement
under the **same process, allocator, query and data**. Prebuilt difference
vectors are cloned outside the native timer; timestamp construction is
inside. File read/decoding is inside the file timers. Install, answer reading,
and teardown are outside all three. Runs include one warmup, so file results
are warm-cache controls, not cold-storage throughput.

Use `collect.ddp`, `reduce.ddp`, `segmented.ddp`, and `weighted.ddp` as explicit
alternative query writings. `segmented` uses Corgi's native `fold_add`, while
`collect`/`reduce` exercise a general scalar fold. No query optimizer is involved.

## Initial measurement and review questions

On the audit's Apple M4, the weighted Q6 plan on all 6,001,215 SF1 lineitem
rows took median 15.426 ms for prebuilt native feed plus settlement, 31.426 ms
for column-file load plus settlement, and 626.054 ms for the text-file path.
All three used the same process and System allocator, one warmup and seven
samples, and checked the same exact answer. [Raw samples and hashes](benchmarks/columnar-ingress.json)
retain the measurement boundaries. These historical controls used the combined
audit runtime; they isolate ingestion representations, rather than establish
an engine ranking or time this independent branch exactly.

The intended review topics are the owning batch API and required shape
ascriptions, the versioned framing tied to Corgi's codec, admission versus tick
completion, and local files' complete-prefix failure semantics. Multiworker
local loading currently repeats file reading/decoding on each worker before
partitioning. A partitioned file source, adapters for external formats, and an
independent data plane remain follow-up work.
