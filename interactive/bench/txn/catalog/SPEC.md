# A durable catalog

A store is a map from ids to timestamped changelogs.
Each changelog is a set of immutable batches, and the catalog says which batches exist.
Appends, merges, and retirements are transactions on the catalog, decided by the read-set/write-set resolver.
Each epoch's decisions are made durable as one batch of catalog changes, named by one root register updated by compare-and-swap.

[`catalog.ddp`](catalog.ddp) holds the catalog and its views, [`resolve.ddp`](resolve.ddp) decides one epoch, and [`run.py`](run.py) drives and checks both, with the durable layer in Python.

## State

- **Records.** Compare-and-swap cells, `key -> (txn, value)`. Each id has two: its `upper`, and its `gen`, a counter that every merge advances.
- **Batches.** A set of rows `(id, lower, upper, since, blob)`. The blob holds updates `(time, key, diff)` with times in `[lower, upper)`, each advanced to at least `since`.
- **Blobs.** Immutable, named by their content hash. They are written before any transaction that names them.
- **Retention.** Each id keeps readable history at times at or after `R = max(0, upper - HISTORY)`.

## Transactions

A transaction has reads `(key, expected)`, writes `(key, value)`, and effects: batch rows to add and to remove.
Transactions are ordered by id.
A transaction commits if every read matches the record at its turn, and only then do its writes and effects apply.

- **Append** to ids `i1..ik`. Reads `upper(i) = u`, writes `upper(i) = u'`, adds `(i, u, u', 0, blob)`, for each id. All ids or none.
- **Merge** on id `i`. Reads `upper(i) = u` and `gen(i) = g`, writes `gen(i) = g + 1`. Its effects are any of: add a rollup, remove the rollup's inputs, remove batches the merger saw as retirable.

A rollup replaces a contiguous run of chain batches `[a, c)`.
Its contents are the inputs' updates with times advanced to `s = min(R, c)`, consolidated.
Adding a rollup alone is always correct, since its contents are a function of immutable inputs.
Removing batches is what needs a guard.

**Why the merge reads `upper` and `gen`.**
Appends only raise `upper`, and only merges change `gen`.
So if both still match, the id's batches are exactly the set the merger planned against, and its plan is valid unchanged.
Without the guard, two mergers can each remove a batch the other's chain relies on.
For batches `[0,1) [1,2) [2,3)`, a merger rolling up `[0,2)` and another rolling up `[1,3)` both remove `[1,2)`, and no chain from 0 to 3 survives.
With `--unguarded`, the randomized test fails by epoch 4 in each of three seeds: two mergers add the same rollup.

The guard is coarse: a merge rolls back if any append to its id commits ahead of it.
A merge planned on the previous epoch's catalog still commits if it is ordered before that epoch's appends to its id.

## Invariants

1. **Coverage.** For each id with `upper > 0`, some chain of batches with `since <= R` runs from 0 to `upper`.
2. **Rollups.** A rollup's contents equal its inputs' updates, advanced to its since and consolidated.
3. **Agreement.** Any two such chains give the same accumulation at every time `t >= R`. This follows from 2.

Appends preserve coverage by extending the chain.
A guarded merge preserves it: its plan swaps a run of the chain for a rollup with `since <= R`, and removes only batches off the chain.
`R` only advances, so a rollup's `since` stays at or below it.

## Readers

A reader at time `t >= R` sums every update with time `<= t` in the chain of fewest batches.
`catalog.ddp` maintains that chain: a shortest-path recursion from 0 over usable batches, then a walk back from `upper`.
It also maintains `uncovered`, the ids whose upper no chain reaches, which must stay empty.
It also maintains `retirable`, the batches off the chain, which mergers may remove.

## Durability

An epoch's conclusions are the resolver's `delta` (record changes), `adds`, and `removes`.
They are written as one blob: the frames the server returns when its exports are read, which are exactly what it accepts as input.
Then the root advances from version `v` to `v + 1` by compare-and-swap, to a manifest that lists the log of conclusion blobs.
Clients hear their outcomes only after that.

With `--sync full` (the default), every file is fsync'd, and the drive's cache is flushed (`F_FULLFSYNC`) twice per commit.
The first flush follows the conclusions blob, and covers it and every data blob written before it.
The second follows the root's rename.
On macOS, fsync alone hands data to the drive without making the drive store it: 0.07 ms against 3.9 ms for `F_FULLFSYNC`, for a 4 KiB file on this machine.

- A crash before the root advances loses the epoch, which was never acknowledged.
- A crash after it loses nothing.
- Recovery reads the root, feeds the log's frames to a fresh `catalog.ddp` in order, and ticks.
- The log holds conclusions, not intents, so recovery never re-runs the resolver, and decisions cannot change if the resolver does.
- A failed compare-and-swap means another process holds the root, and this one must stop.

**Keeping it bounded.**
The log grows by one blob per epoch.
In the background, a geometric policy merges the last two log blobs while the older is at most twice the newer, consolidating their rows.
A merged blob holds at most the live catalog, because adds and removes cancel.
The merge itself is done by the server: a scratch program ([`logmerge.ddp`](logmerge.ddp)) is fed the blobs' frames, and its arrangements are read back.
So the log stays within a small multiple of the live catalog, recovery reads about that much, and each row is rewritten about `log2(live / delta)` times.
The merge is a root swap like any other.
With several committers, the merger would hand its blob to the committer, who swaps it in with the next commit.

**Garbage.**
A sweep marks every blob the root reaches: the log's blobs, and the blob of every batch the log holds.
A blob is deleted once sweeps have found it unreachable for `grace` epochs, and only if it was written more than `grace` epochs ago, as it may belong to an undecided transaction.
A merger or reader whose view is older than that may find a blob gone, and starts over from a fresh view.
This rule relies on time: a transaction undecided for longer than `grace` epochs could name a deleted blob.
Here every transaction is decided in the epoch that writes its blobs.

## Open

- Merges read `upper`, so a merger loses to appends on hot ids. A finer guard would read only the presence of the batches its plan touches, at the cost of a larger read set.
- The catalog's views are maintained before the next epoch starts, but only `records` must be. The views could trail behind.
- Reads before `R` are not offered. Keeping them means a policy for `HISTORY` per id, or holds recorded in the catalog.
