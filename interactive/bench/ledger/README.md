# Matched ledger benchmark

This compares the ledger `Writer` with the older catalog prototype on `ddir-durable-names`, on the same requests and the same `ddir_server`.
It is a diagnostic, not a target: see [`../../LEDGERS.md`](../../LEDGERS.md) for where the work stands.

## Workload

Metadata-only appends with empty payloads, one ledger per transaction, one million ledgers.
25,600 attempted transactions per batch, 40 batches, the first five as warmup, four workers, pipelined publication.
The stream has 1,024,000 attempts and 1,011,005 commits; the 35 measured batches have 896,000 attempts and 884,623 commits.
The request fixture exported with Python 3.9 has SHA-256 `7f21926736e87e985d4e86960aae203218d1e9f6197b8859e9166720dd457ade`.
Use the same Python interpreter to export requests and run the reference, since Python's random sampling can change the stream.

`ledger_replay` registers every ledger through the writer before the clock starts, then reopens the writer, so the measured writer begins from recovery.
Its timed submissions include encoding, resolution, the host checks, publication and outcomes.
It checks every outcome, conflict frontier, registration, recovered span chain and final upper after the clock stops.
The reference prepares its frames and checks its results outside its clock.

## Recorded results

Apple M4 Mac mini (mini-03), `file:` store, 2026-11-09, three runs each.

| Revision | Committed appends/s | ms per batch |
| --- | ---: | ---: |
| reference, `ddir-durable-names` 640ee77d | 1.42–1.44M | 17.6 |
| 666cf003, before review | 562–568k | 44.8 |
| this branch | 659–665k | 38.1 |

The handoff profile at 666cf003, and the per-trace merge cost measured for both systems, are in [`REVIEW.md`](REVIEW.md).

## Reproduce

Run from a fresh worktree of this branch, from the repository root.

```sh
cargo build --release -p interactive --example ledger_replay
cargo build --release -p ddir-server

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

The two `cargo build` commands are separate because `--example` limits a build to examples, so a combined command does not build `ddir_server`.
On Linux, use `--sync fsync` for the reference; its `full` option is macOS-specific.
Use fresh store directories for each run, and run competing variants one after another on the same machine.

The profiling patches and CPU samples used for the review are at 666cf003 under `interactive/bench/ledger/`.
To measure resident-trace merges in both systems, apply only the `interactive/src/server/progress.rs` hunk of that `profile.patch` and set `DDIR_COMPACT_PROFILE=1`.
The reference's server discards stderr, so pass `reference.py` a `--wrap` script that redirects it, such as `exec "$@" 2>>compact.log`.
