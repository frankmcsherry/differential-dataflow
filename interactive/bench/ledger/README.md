# Ledger durability diagnostic handoff

This branch builds ledger durability beneath DDIR computations.
Start with [`../../model.md`](../../model.md), especially “Operations and cohorts”.
The earlier “Ledger evolution” text is retained discussion, not the implemented transition specification.

The implementation starts at `bb9f4c088f89777de20768506721be1224d8ea7a`, the `master-next` tip when this handoff was prepared.
The comparison branch is `ddir-durable-names` at `640ee77dcffe49e8349c0f773409634a66a32826`.
The implementation and recorded measurements through `3650895b2` do not depend on other local branches or the untracked model sketches.

## Implemented state

- Cohorts publish through immutable SHA-256 blobs and a versioned CAS root.
- `MemoryStore` and Unix `FileStore` implement those primitives.
- Transactions group at most one create, append, or combined create-and-append per UUID, in UUID order.
- Appends require `lower < upper` and `lower` equal to the deciding write frontier.
- Frontiers are antichains of signed product times, with arbitrary dimension and empty top.
- `Writer` retains workers and a frontier catalog, and installs a fresh keyed resolver per batch.
- Each batch resolves and absorbs its frontier delta with two server ticks.
- One publication can run while later batches resolve; queued batches can share a publication.
- Receipts become available after root CAS; a lost CAS reloads and re-resolves the unacknowledged suffix.
- An unknown CAS outcome stops the writer without automatic retry.
- Recovery replays conclusion logs; transaction keys are not durable idempotency keys.
- Independent ledger sources can feed DDIR computations through the Rust server API.

The resolver's UUID columns use two sign-biased integer words, preserving all 128 bits and UUID order.
Public results and durable logs retain UUID bytes.
Frontier comparisons borrow column slices; command frontiers normalize once.
Only failures leave the recursive reducer; it carries existence and write frontier, while starts, shapes, and payloads remain outside.

The server's ledger-source API is described in [`../../server/README.md`](../../server/README.md).
This is not yet a durable-workflow server or a complete ledger command protocol.
Compaction, retention holds, garbage collection, and durable naming remain future work.

## Commit map

The ten implementation commits were kept in dependency order.
The foundation commit is large but coherent; it contains the model, memory backing, frontier kernels, serial rules, and oracle tests.

| Commit | Contents |
|---|---|
| `fadadb7b9` | Model, grouped transactions, memory backing, shared kernels, oracle |
| `d6430d86c` | Independent DDIR ledger sources |
| `6ebe1aee9` | Filesystem blobs and CAS roots |
| `9bcf5b072` | Publish DDP conclusions through roots |
| `efbd252d0` | Keyed resolution |
| `b9f19e145` | Widen frontier coordinates once per kernel call |
| `d2d6361b6` | Resident catalog, conclusion log, pipelined writer |
| `cb841f281` | Full UUID words and columnar output conversion |
| `191047fd1` | Borrowed frontier comparisons and normalization once |
| `3650895b2` | Failure-only resolution and bounded start lookups |

A subsequent compatibility commit constrains UUID to the 1.26 series because 1.27 requires Rust 1.89 and the workspace declares 1.88.
The historical profiles used Rust 1.95 and UUID 1.29.1.
`dependencies.lock` records the compatible dependency resolution used to verify this handoff.
It is copied explicitly because the repository intentionally ignores root `Cargo.lock`.

## Recorded measurements

These are metadata-only appends with empty payloads, one ledger per transaction, one million ledgers, 25,600 attempted transactions per batch, 40 batches, five warmup batches, four workers, and mimalloc on `mini-00` (M4).
These are not multi-ledger transaction throughput figures.
Both implementations support multi-ledger transactions, which the correctness tests exercise separately.

The reference was reproduced at 1.44M committed appends/s with full filesystem durability.
Running the reference model on the current DDIR server reproduced that result, so shared runtime changes alone did not explain the gap.
The current implementation reached 586k/s with filesystem durability and 597k/s with memory storage after the three representation/processing commits.
Three-run medians and the product-antichain workload are in [`results/throughput-2026-11-09.json.gz`](results/throughput-2026-11-09.json.gz).

The matched stream has 1,024,000 attempts and 1,011,005 commits in total.
The measured 35 batches have 896,000 attempts and 884,623 commits.
The recorded Python 3.9 fixture SHA-256 is `7f21926736e87e985d4e86960aae203218d1e9f6197b8859e9166720dd457ade`.
Use the same Python interpreter to export requests and run the reference; Python's random sampling implementation can affect the stream.
The exporter prints the actual digest and commit counts.

Earlier restricted controls reached about 1.41M/s through the current writer by using integer identifiers, scalar frontiers, simpler output handling, preparation/checks outside timing, and append-only rules.
Those controls narrowed the supported inputs and changed several processing paths; they did not establish that the remaining production cost is intrinsically required by UUIDs or lattices.
They are not production implementations.

The CPU profile measured about 139 CPU-ms per current batch versus 69 CPU-ms for the reference, summed across threads.
Current memory and filesystem runs took about 43 ms elapsed per batch.
Workers were balanced at about 19 scheduled ms each.

| Scheduled ms/batch, summed across workers | Current | Reference |
|---|---:|---:|
| Joins | 28.1 | 12.7 |
| Arrangement maintenance | 20.5 | 5.5 |
| Reductions | 16.6 | 8.9 |
| Columnar maps | 7.1 | 2.2 |

Scheduled durations are not OS CPU counters and omit work outside Timely operator scheduling.
In particular, compacting `catalog::current` took 38.5 CPU-ms per batch outside operators.
Resolver-export compaction was negligible.
That 38.5 ms is a current cost, not a measured cross-version compaction difference.
The Rust caller used about 8.3 CPU-ms, including 4.9 ms checking accepted effects and ledger widths.
Its command encoding and checks occur inside its clock, whereas the reference prepares frames and checks results outside its clock.
The resolver inputs themselves are CORGI columns.

`bases`, `starts`, and `old_initial` separately probe the resident catalog and total about 24.7 scheduled ms.
`bases` and `starts` probe the same touched identifiers.
Compound keys take the backend's hash-plus-full-key path; integer keys have an identity-key path.
The stacks prominently contain join range searches, structural sorting/grouping, column gathers, list-offset construction, copying, and trace merging.
Frontier kernels are a small part of the sample.

Do not assume that the remaining gap is an inherent cost of product lattices or UUIDs.
Sequence numbers and external frontier interning have not been demonstrated to remove it.
Useful diagnostic targets are sharing catalog probes, arrangement construction, catalog compaction, and column gathering/copying.
Preserve full UUIDs, product antichains, mixed transactions, error precedence, atomic rejection, and pre-publication checks while investigating.

The detailed counters and caveats are in [`results/cpu-2026-11-09.json.gz`](results/cpu-2026-11-09.json.gz).
The raw stack sample is preserved as [`results/current-cpu-2026-11-09.sample.txt.gz`](results/current-cpu-2026-11-09.sample.txt.gz).
Sample counts overlap and include waiting threads; they are not CPU percentages.
A previous rewrite projected starts from the whole catalog and regressed to 429k/s.
Joining `touched` before projecting corrected the full-cohort scan.

## Reproduce the comparison

Use a fresh worktree and run these commands from its repository root.
The request exporter imports only the reference's Python generator and serial oracle.
The reference adapter changes DDP syntax and trace namespaces; it keeps the reference's workload, frame format, resolver, durability code, and checks.
It runs that model on the current server so the runtime is held constant.

```sh
cp interactive/bench/ledger/dependencies.lock Cargo.lock
cargo build --release --locked -p interactive --example ledger_replay -p ddir-server

git fetch https://github.com/frankmcsherry/differential-dataflow.git ddir-durable-names
LEDGER_REFERENCE_DIR=$(mktemp -d)
git worktree add --detach "$LEDGER_REFERENCE_DIR" 640ee77dcffe49e8349c0f773409634a66a32826
LEDGER_RUN_DIR=$(mktemp -d)

python3 interactive/bench/ledger/export_reference.py "$LEDGER_RUN_DIR/requests.bin" \
  --checkout "$LEDGER_REFERENCE_DIR"

target/release/examples/ledger_replay "$LEDGER_RUN_DIR/requests.bin" memory pipeline 4 5
target/release/examples/ledger_replay "$LEDGER_RUN_DIR/requests.bin" \
  "file:$LEDGER_RUN_DIR/current-store" pipeline 4 5

python3 interactive/bench/ledger/reference.py --source "$LEDGER_REFERENCE_DIR" \
  --binary "$PWD/target/release/ddir_server" --dir "$LEDGER_RUN_DIR/reference-store" \
  --ids 1000000 --appends 25600 --width 1 --ticks 40 --warmup 5 \
  --workers 4 --sync full --modes pipelined
```

On Linux, use `--sync fsync` for the reference; its `full` option is macOS-specific.
The Rust `FileStore` uses `File::sync_all`, which uses `F_FULLFSYNC` on the tested macOS toolchain and fsync on Linux.
Use fresh store directories for each repetition and run competing variants sequentially on the same machine.
The replayer independently checks every outcome, conflict frontier, registration, recovered span chain, and final upper.
The simpler [`../../examples/ledger_resolve.rs`](../../examples/ledger_resolve.rs) measures disjoint transactions and is a different workload; it also uses the default allocator.

## Reproduce operator and CPU profiles

The opt-in patches target macOS and Linux and contain instrumentation only; they are not applied in the normal branch.
`profile.patch` records stage durations, worker CPU time, cumulative live and retired operator time, and direct compaction CPU time.
Its markers are fixed to batches 5 and 40, matching the commands below.
It explicitly flushes and processes logging at both boundaries, including the last catalog tick.

```sh
git apply interactive/bench/ledger/profile.patch
cargo build --release --locked -p interactive --example ledger_replay
DDIR_ATTR=1 DDIR_PROFILE_OPS=1 DDIR_COMPACT_PROFILE=1 \
  target/release/examples/ledger_replay "$LEDGER_RUN_DIR/requests.bin" memory pipeline 4 5 \
  > "$LEDGER_RUN_DIR/current.out" 2> "$LEDGER_RUN_DIR/current.log"
python3 interactive/bench/ledger/summarize.py "$LEDGER_RUN_DIR/current.log" \
  --json "$LEDGER_RUN_DIR/current-operators.json"
git apply -R interactive/bench/ledger/profile.patch
```

The replayer prints process/main CPU totals in addition to elapsed time.
Stage rows report nanoseconds; the summarizer converts them to milliseconds per measured batch.
Publication overlaps resolution, so its durations must not be added to the foreground wall time.
Rebuild after reverting the patch before collecting ordinary throughput figures.

For the reference's operator profile, apply `reference-profile.patch` in its worktree and add `--diag` to the reference command.
That patch adds stage/CPU counters and drains diagnostics at both boundaries; it does not change the requests or resolver rules.
It supports macOS and Linux CPU counters.
macOS `proc_pid_rusage` values are Mach ticks; the patch converts them using the machine's timebase.
The diagnostics query itself adds overhead, so use the uninstrumented runs for throughput comparisons.

```sh
LEDGER_CHECKOUT_DIR=$PWD
(cd "$LEDGER_REFERENCE_DIR" && git apply "$LEDGER_CHECKOUT_DIR/interactive/bench/ledger/reference-profile.patch")
python3 interactive/bench/ledger/reference.py --source "$LEDGER_REFERENCE_DIR" \
  --binary "$PWD/target/release/ddir_server" --dir "$LEDGER_RUN_DIR/reference-profile-store" \
  --ids 1000000 --appends 25600 --width 1 --ticks 40 --warmup 5 \
  --workers 4 --sync full --modes pipelined --diag "$LEDGER_RUN_DIR/reference-operators.json"
(cd "$LEDGER_REFERENCE_DIR" && git apply -R "$LEDGER_CHECKOUT_DIR/interactive/bench/ledger/reference-profile.patch")
```

On macOS, `DDIR_SAMPLE_GATE=PATH` lets `sample` attach after warmup.
The replayer writes its PID to `PATH.ready` and waits for `PATH.go` before starting the clock.
Use a fresh path without an extension, launch `sample PID 1 1 -file OUTPUT`, then create the go file.
The recorded sample included about 150 ms of the initial gate wait; do not count those waits as CPU work.
On Linux, use the same gate with a system profiler such as `perf`.

## Verification

The final implementation passed 254 tests, including full-width UUID order/roundtrips, arbitrary-dimensional frontier comparisons against `Antichain`, multi-ledger rejection recovery, width changes, CAS loss, unknown commits, process-exit recovery, and DDIR source replay.
The Rust 1.88 toolchain is also checked during handoff preparation.

```sh
cargo +1.88 test --locked -p interactive --lib \
  --test ledger_file --test ledger_keyed --test ledger_model \
  --test ledger_resolve --test ledger_writer --test server_ledgers
python3 interactive/model/check_verbs.py target/release/ddir_server
```

The process-exit tests are not power-failure tests.
The benchmark does not establish compaction or retention correctness, which remain outside the implemented model.
