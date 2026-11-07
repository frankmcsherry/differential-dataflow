# Multi-ledger compare-and-append

A sketch of Materialize Persist's compare-and-append, extended to several ledgers in one atomic action, using the transaction resolver of [`resolve_by_key.ddp`](../../bench/txn/resolve_by_key.ddp).

A transaction appends one batch to each of several ledgers.
For each ledger it reads the upper it expects and writes the new upper.
It commits if and only if every expected upper is the ledger's upper just before it, in id order.
Uppers only advance, so equal uppers mean no append came between.

- [`ledgers.ddp`](ledgers.ddp) holds `uppers`, ((ledger), (txn, upper)), and `batches`, ((ledger), (lower, upper, blob)).
- [`resolve.ddp`](resolve.ddp) is installed for one epoch. Input 0 is the intents, input 1 is each written ledger's blob. It exports `rollback`, `delta` (the change to `uppers`), `append` (the batches [expected, new) to add), and `garbage` (blobs of rolled-back transactions, for the writer to delete).
- [`bench_storage.py`](bench_storage.py) drives it closed loop: load the resolver, feed intents and blobs as frames, tick, read `rollback` and `garbage`, feed `delta` and `append` into `ledgers`, drop the resolver.

Every cycle's rollbacks and garbage are checked against a serial reference.
At the end every ledger's upper and batch list are compared in full, and each ledger's batches are checked to chain (each lower is the previous upper).

## Results

Measured 2026-11-03 on an Apple M4 Mac mini, master-next `379f0564`, 4 workers, five cycles after two warmups.
Each cycle attempts 25,600 ledger appends, so a transaction of width `W` is one of `25,600 / W` per cycle.
Expected uppers are those at the start of the cycle.

Disjoint ledgers (no two transactions in a cycle share a ledger), 1M ledgers:

| Width | Txns/cycle | Commit | Txns/s | Appends/s | Cycle p50 |
|---:|---:|---:|---:|---:|---:|
| 1 | 25,600 | 100% | 2.36M | 2.36M | 10.6 ms |
| 4 | 6,400 | 100% | 588k | 2.35M | 10.6 ms |
| 16 | 1,600 | 100% | 147k | 2.36M | 10.6 ms |
| 64 | 400 | 100% | 36.7k | 2.35M | 10.4 ms |
| 256 | 100 | 100% | 9.3k | 2.37M | 10.6 ms |

The cost is per ledger append; the width of a transaction does not change it.

Random ledgers, committed ledger appends per second:

| Width | 1M ledgers | 100k ledgers | 10k ledgers |
|---:|---:|---:|---:|
| 1 | 1.88M (99%) | 1.50M (88%) | 617k (36%) |
| 4 | 1.78M (95%) | 1.18M (68%) | 282k (20%) |
| 16 | 1.58M (84%) | 695k (39%) | 102k (8.5%) |
| 64 | 1.08M (59%) | 283k (18%) | 42k (3.1%) |
| 256 | 504k (29%) | 129k (7%) | 28k (1.2%) |

Percentages are the transactions that commit.
These fall with width because every transaction here reads the cycle's starting uppers, so any ledger it shares with an earlier committed transaction in the cycle rolls it back.
That is a property of this workload, not of the resolver.
Attempts cost 13–24 ms per cycle against 10.6 ms disjoint; rollbacks add rounds to the recursion.

Cycle size, disjoint, 1M ledgers (width 1, 16, and 256 within 3% of each other):

| Appends/cycle | Cycle p50 | Appends/s |
|---:|---:|---:|
| 256 | 2.0 ms | 128k |
| 2,560 | 3.0 ms | 843k |
| 25,600 | 10.6 ms | 2.36M |
| 256,000 | 100 ms | 2.39M |

At 256 appends per cycle, installing the resolver is about 0.9 ms of 2.0.
At 25,600, the resolution tick is 7.8 ms of 10.6, admission 1.0, install 1.1, applying the store change 0.6.

## Reproduction

```console
cargo build --release -p ddir-server
W=1,2,4,8,16,32,64,128,256
python3 interactive/storage/ledgers/bench_storage.py --pattern disjoint --ledgers 1000000 --widths $W --appends 25600
python3 interactive/storage/ledgers/bench_storage.py --ledgers 1000000,100000,10000 --widths $W --appends 25600
```
