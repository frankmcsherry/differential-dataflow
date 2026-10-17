# Batch controls

`q6.py` runs TPC-H Q6 against an existing `lineitem.tbl`, on one worker,
with four equivalent DDIR plans: `collect`/`fold`, a weighted `reduce`
expression, native segmented `fold_add`, and revenue carried as the update
difference through
`join_function`/`count`. Each result is checked against an independent Python
integer calculation. The optional DuckDB CLI queries the identical projection
and must return the same answer. No data or dependencies are downloaded.

From the repository root:

```sh
cargo build --release -p ddir-server
python3 interactive/server/bench/batch/q6.py \
  --server target/release/ddir_server --lineitem /path/to/lineitem.tbl \
  --rows 100000 --rounds 5 --output /tmp/ddir-q6
```

Use `--rows 0` for the whole file, `--backend vec` for the row reference,
and `--duckdb /path/to/duckdb` to add the DuckDB control. The output directory
must be new. Start with a prefix: the server file loader materializes its
input as rows before feeding it. Input and answer must be nonempty for this
control. Quantities and prices are parsed exactly as hundredths; revenue is
reported in units of 1/10000, with no floating point or per-row rounding.

DDIR reports **file ingestion plus initial settlement**, including file I/O,
integer parsing, row validation, conversion to columns, and execution. Install,
answer retrieval, and teardown are outside this timer. Each round installs a
fresh plan in the same private server process; the first round is warmup.
DuckDB reports query-only times on a loaded projection, and separately reports
the load process time (including CLI startup and table creation). Both use one
worker/thread. These boundaries differ: **do not divide these times to claim
an engine speedup**. The native control below adds a separate matched
ingestion comparison.

`report.json` records raw DDIR samples, projected-data and executable hashes,
source revision/status, harness/client hashes, and the available Cargo lockfile.
Generated plans, the integer projection, and server logs remain beside it.
Optional DuckDB SQL and raw CLI output are saved too. A report with status
`running` is incomplete. This is one query control, not a TPC-H benchmark score;
the prefix option is not a TPC-H scale factor.

For a comparison under the same in-process timing and allocator, use the
[native ingress control](../../../INGEST.md):

```sh
cargo run --release -p interactive --example q6_native_bench -- \
  /tmp/ddir-q6/lineitem.txt /tmp/ddir-q6/weighted.ddp 7 /tmp/lineitem.col
```

This reports prebuilt column feed plus settlement, local column-file loading
plus settlement, and the existing text-file loader plus settlement. Change the
DDP path to compare explicit `collect`, `reduce`, `segmented`, and `weighted`
query formulations. Columns are prepared outside the native timer; both file
controls include their decoding. All answers are checked.

The `weighted` writing carries each positive qualifying Q6 revenue in the
update difference and uses `count` to sum those weights. This changes the
aggregate's input values to unit, avoiding a retained set/list of revenues.
It is a Q6 control, not a blanket SUM rewrite: `count` emits no row when the
total is zero, so a general signed sum needs separate group-presence handling.
Numeric range/overflow must also agree. The generated control checks a nonzero
exact answer on valid positive-price/discount data.
