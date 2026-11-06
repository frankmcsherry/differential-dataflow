# A durable catalog, tested

[`SPEC.md`](SPEC.md) describes the design: the state, the transactions, the invariants, the reader's chain, and the durable log.
This page is what [`run.py`](run.py) checks and what it measured.

## What is checked

Each epoch, appenders (width 1–4, 10% reading last epoch's uppers) and three competing mergers (views 0–2 epochs old, 30% adding a rollup without retiring its inputs) submit transactions in a random order.
After each epoch, `run.py` checks:

- the rollbacks and the conclusions (`delta`, `adds`, `removes`) against a serial reference;
- `records` and `batches` against the reference;
- `uncovered` is empty;
- every id's chain runs from 0 to its upper, uses batches with `since <= R`, and is a fewest such chain;
- `retirable` is exactly the batches off the chain;
- 50 reads per epoch, each accumulating a chain's blobs from disk at a random time `t >= R`, against every update ever appended.

Crashes kill the server and discard everything but the disk, before or after the root advances.
Recovery rebuilds from the root, and the checks continue.

Results, all passing, release build of master-next `379f0564`, 2026-11-03:

| Run | Epochs | Crashes | Reads checked |
|---|---:|---:|---:|
| 4 seeds, 1–3 workers, crash chance 0.15, each log policy | 600 | 89 | 29,884 |
| grace 0, sweep every epoch | 100 | 10 | 4,952 |

With grace 0, 314 merges found an input blob already swept and abandoned their plans; nothing incorrect was committed.
With `--unguarded`, the test fails by epoch 4 in each of three seeds.

## End to end, durable

Each epoch's outcomes are reported only after its commit returns, with `--sync full`: every file fsync'd, and the drive's cache flushed twice per commit.
Apple M4 Mac mini, 4 workers, 12 measured epochs after 3 warmups, one transaction per append unless noted.

| Shards | Appends per epoch | Committed per epoch | Commit p50 (p95) | Epoch p50 | Committed appends/s |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 1,000 | 995 | 21 ms (43) | 24 ms | 34k |
| 100,000 | 10,000 | 9,509 | 48 ms (62) | 66 ms | 133k |
| 100,000 | 50,000 | 39,323 | 97 ms (268) | 281 ms | 133k |
| 1,000,000 | 10,000 | 9,953 | 51 ms (75) | 66 ms | 145k |
| 100,000 | 2,500 transactions of 1–4 shards | 5,693 | 39 ms (49) | 51 ms | 103k |

The commit is from sending the transactions to the root's second flush.
The epoch adds the catalog absorbing the conclusions and updating its views, before the next epoch can start.
Not counted: the writers writing their data blobs (in Python, one file each, about 150 µs per file here), the background log compaction and sweep (12–161 ms per epoch, run between epochs by this driver), and encoding and checking.

At 100,000 shards and 10,000 appends per epoch, the flushes are most of the commit: 25 ms for the first (it flushes the epoch's 10,000 data blobs too) and 12.5 ms for the root's.
Without them (`--sync none`) the commit is 14 ms and the epoch 36 ms, 261k appends/s; with fsync but no flush (`--sync fsync`), 15 ms and 37 ms.
At 50,000 appends, the catalog's own update (179 ms) is most of the epoch.

## Overlapping flushes with resolution

[`pipeline.py`](pipeline.py) hands each tick's conclusions to a committer thread and starts the next tick at once; ticks that queue during a commit are committed together.
Outcomes are still reported only after their commit.
1M shards, single-shard appends, empty data token, no maintained views, `--sync full`, 35 measured ticks.

| Appends per tick | Serial appends/s | Pipelined appends/s | Pipelined ms per tick | Pipelined latency p50 (p95) |
|---:|---:|---:|---:|---:|
| 3,200 | — | 620k | 5.2 | 15.6 ms (20.3) |
| 6,400 | 427k | 923k | 6.9 | 18.3 ms (23.3) |
| 12,800 | 618k | 1.11M | 11.4 | 17.4 ms (31.7) |
| 25,600 | 905k | 1.37M | 18.5 | 24.5 ms (58.5) |
| 51,200 | 1.16M | 1.54M | 32.3 | 38.9 ms (74.8) |

At 25,600 appends, the same run with no flushes at all reaches 1.40M/s: once overlapped, durability costs about 2%.
The 51,200 row ran before group commit was added; at that size every commit holds one tick, so grouping does not apply.

## Bounded durable state

300 ids, history 30, 100 appends and up to 60 merges per epoch, 2 workers, `--sync full`, 800 epochs.
The live catalog is about 100 KiB; each epoch's conclusions are about 20 KiB.

| Log policy | Log blobs at epoch 800 | Log size, epochs 100–800 | Written per epoch | Recovery at epoch 800 |
|---|---:|---:|---:|---:|
| geometric | 2 | 99–243 KiB | 136 KiB | 26 ms |
| full merge every 64 epochs | 33 | 208–1,539 KiB | 75 KiB | 35 ms |
| never | 801 | grows 20 KiB per epoch, 15.4 MiB at 800 | 73 KiB | 165 ms |

Written per epoch includes the data blobs.
Geometric compaction rewrites about 63 KiB per epoch, about 3x the conclusions.
Data blobs stay at 1.7–3.3 MiB under the compacting policies: rollups advance times to `R`, so an id's data is bounded by its keys and its history window.

## Reproduction

```console
cargo build --release -p ddir-server
python3 interactive/bench/txn/catalog/run.py --dir /tmp/cat --epochs 200
python3 interactive/bench/txn/catalog/run.py --dir /tmp/cat-crash --crash 0.15 --epochs 150 --ids 200
python3 interactive/bench/txn/catalog/run.py --dir /tmp/cat-never --policy never --crash 0 --epochs 800 --report-every 100
```

`--dir` must not exist; it becomes the durable store.
