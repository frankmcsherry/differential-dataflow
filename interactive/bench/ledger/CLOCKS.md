# Clocks: advancing many ledgers at once

A spike showing that, with clocks, a round's cost follows the appends that carry data, not the number of ledgers whose frontiers advance.
The design is item P1 of the design notes; this file records the approach and the measurement.

## The approach

A clock is an ordinary ledger, usually one that holds no data.
A ledger may follow a clock, and clocks may follow clocks.
A ledger's write frontier is the join of its own last upper and the write frontiers of the clocks above it, as they stood when the round began.
Advancing a bundle of ledgers is then one empty append to their clock.
An append to a follower in the same round is judged against the clock's earlier frontier, so the advance lands after it and does not contend with it.

The rules are [`../../model/transactions_clocks.ddp`](../../model/transactions_clocks.ddp): the keyed rules plus about twelve lines.

- Input 3, `follows`, maps a ledger to the clock it follows.
- The recursion `up` climbs the clock tree, starting from only the ledgers a round touches.
- Each clock above a touched ledger contributes a `Floor` row to that ledger's group, and the per-ledger reduce joins it into the write frontier before judging proposals.

Nothing is maintained per follower, so an idle ledger costs nothing when its clock advances.
In this spike `follows` is fed straight into the catalog; it is not yet part of creating a ledger.

## Run it

Both demos run on the stock server, with no code beyond the rules:

```sh
cargo build --release -p ddir-server
./target/release/ddir_server < interactive/server/demo/ledger_rounds.txt   # rounds without clocks
./target/release/ddir_server < interactive/server/demo/ledger_clocks.txt   # three rounds, a two-level clock tree
python3 interactive/bench/ledger/clock_bench.py flat
python3 interactive/bench/ledger/clock_bench.py tree
```

## Recorded results

Apple M4 Mac mini (mini-03), 2026-11-10, one million resident ledgers, four workers, medians of ten rounds.
Every append commits; in the clock rows the clock also advances every round.
The figures are the server's own time for the resolve tick, in ms.

| Appends with data per round | 0 | 256 | 2,560 | 25,600 |
| --- | ---: | ---: | ---: | ---: |
| Rules without clocks | | 0.53 | 1.79 | 13.9 |
| Clock rules, nothing follows a clock | | 0.56 | 1.81 | 14.1 |
| All ledgers follow one clock | 0.32 | 0.75 | 2.70 | 21.0 |
| 1,000 clocks under one root clock | 0.34 | 0.97 | 3.71 | 28.6 |

The tick that absorbs conclusions into the catalog adds 0.03 ms at 0 appends and about 2 ms at 25,600.

- Advancing all one million ledgers by a clock takes 0.35 ms, resolve and absorb together.
  As one million appends without clocks it is 39 rounds of 25,600 at 15.8 ms each, about 620 ms.
- Clock support costs nothing when no ledger follows a clock.
- An append to a follower costs about 50% more for each level of clock above it.
  With one clock every touched ledger joins on the same key, so part of that may be skew; the two were not separated.

These are uncontended appends, timed inside the server.
They are not comparable to the whole-round figures in [`README.md`](README.md), whose workload has conflicts.
