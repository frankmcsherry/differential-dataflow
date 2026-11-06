# Durability for DDIR: where it stands

This note records the durability work of 2026-11-03 to 2026-11-06, so it can be picked up again with its context.
It says what exists, what was decided and why, what was measured, and which design questions are open.

## Where everything is

Everything is on branch `ddir-durable-names`, on master-next.

- Steps 1 and 2 below, in Rust: `interactive/src/storage.rs`, `interactive/server/src/durable.rs`, and the `save`, `unsave`, `restore` and `ledger` commands, with tests `saves_resume_on_one_worker` and `saves_resume_on_four_workers`.
- The Python and DDIR prototypes behind everything else, under `interactive/bench/txn/`.
  - `interactive/bench/txn/storage/`: multi-shard compare-and-append on the transaction resolver.
  - `interactive/bench/txn/catalog/`: `SPEC.md`, the catalog and resolver programs, `run.py` (crash tests against a serial reference) and `pipeline.py` (pipelined ticks), and `sketches/` of directories and the ledger schema.
  - `interactive/bench/txn/control/`: the store and control services, provisioner, runner and client; etcd and S3 backends; `deploy/` notes for the minis.
- Shared write-ups:
  - [Multi-shard compare-and-append in DDIR](https://claude.ai/artifact/48f4cy3LbqiFRoWFruaVrW)
  - [A durable catalog on DDIR](https://claude.ai/artifact/9HmVSdNG23Zd3Nqcs16phn), with the programs annotated
  - [Durable ticks for DDIR](https://claude.ai/artifact/2cWGgagg9o1Kbr14MxDqft)
  - [Names, hashes and roots in durable DDIR](https://claude.ai/artifact/LKC13KFe27GsHb21mtmqX3)

## The sequence

Each step is usable on its own, and each later step is built from the earlier ones.

1. **The storage interface.** Done, with one backend.
   Two traits: blobs, immutable and named by SHA-256, and named roots, replaced only by compare-and-swap on a version.
   `FileStore` keeps both in a directory, syncs every write (`F_FULLFSYNC` on macOS), and serializes swaps across processes with `flock`.
   Still to do: S3 and etcd implementations in Rust (prototyped in Python in `control/backends.py`).
2. **One durable name: a ledger.** Done.
   A ledger is a root holding a manifest, and one blob per batch, each a frame of updates.
   `save <trace> to <ledger>` appends each tick's changes before `tick` answers, and resumes a ledger rather than repeating it.
   `restore <prog> <in#> from <ledger>` feeds a ledger back into an input.
   Still to do: batches should carry domain time, not the ledger's own counter (see Lifecycle); the write should move off worker 0's operator onto a background writer with group commit; `restore` may become `feed … from ledger:<name>`.
3. **The catalog.** Prototyped, not in Rust.
   Many names under one root; transactions decided by the resolver, including atomic appends across names; the durable log of each tick's conclusions, with geometric compaction; pipelined, group-committed ticks; garbage collection.
   Merges are to change as agreed: a rollup is added unconditionally, and a deletion reads the presence of every batch in the chain it keeps (this replaces the generation counter).
4. **Programs as durable state.** Prototyped in `control/`: specs in the catalog, and an instance reconciling to them.
5. **Deployment.** Prototyped in `control/`: a provisioner starting a runner per instance; runners fence predecessors by incarnation; outputs resume from their ledgers.
   It ran across the four minis, through killed runners, moved instances, duplicate runners and a crashed control service, without losing or repeating an update.

## Decisions, and why

- **One root register with compare-and-swap on a version**, not write-once numbered slots: one root, no slots to clean up. Slots remain the fallback for stores with only put-if-absent.
- **Only the root needs compare-and-swap.** Blobs are immutable and named by their content, so writing one twice is harmless; any store can hold them.
- **The log holds conclusions, not transactions.** Recovery never re-runs the resolver, so a reported decision cannot change.
- **Writers make their own data durable, before they submit.** A tick then commits only catalog changes. When the tick also stored the writers' blobs, its cost was the number of files, not the bytes: 4.7 MB in 25,601 files flushed at about 100 MB/s, against 420 MB/s in one file.
- **Overlap each tick's flush with the next tick's work**, reporting outcomes only once their commit returns, and commit queued ticks together.
- **Names bind to ids, and ids own batches**, so renames never touch data.
- **Read views are queried on demand** (`snapshot.ddp`) instead of maintained, which took 34 ms off each 25,600-append tick.
- **"Ledger", not "shard"**: entries are (account, date, amount), corrections are reversing entries, balances are sums up to a date, and closing the books is compaction.

## Measured

Apple M4 Mac minis, 4 workers unless noted.

| What | Result |
|---|---|
| Resolution alone, 25,600 appends over 1M names | 1.90M appends/s, 12.9 ms per tick |
| Durable, serial, writers' data off the tick, no maintained views | 814k–841k appends/s, 29 ms ticks |
| Durable, pipelined, 25,600 appends per tick | 1.37M appends/s; 1.43M after splitting uppers from generations |
| Durable, pipelined, 12,800 appends per tick | 1.11M appends/s, a tick every 11.4 ms, 17.4 ms to an outcome |
| Upper bound if conclusions never leave the server | 1.70M appends/s |
| Where a pipelined tick's CPU goes | about 55% operators (the two store joins most), 29% end-of-tick arrangement merges, the rest install, feed, peek |
| etcd compare-and-swap, three minis over Thunderbolt | 7 ms p50 |
| Garage put, three copies over Thunderbolt, 1 KiB / 64 KiB / 2 MiB | 2.6 / 20 / 53 ms; small objects are not fully durable (SQLite WAL with `synchronous=NORMAL`) |
| SeaweedFS put, one node, fsync rule on, 1 KiB / 64 KiB / 2 MiB | 11.9 / 14.1 / 45.5 ms |
| A durable control transaction, local disk store / Garage + etcd | 22.5 / 28.4 ms p50 |

## Open design threads

These were discussed and are not settled; they shape steps 2 to 5.

- **Lifecycle.**
  Every durable thing is a ledger; kinds differ only in who writes it (clients through transactions, one external ingester, one program instance) and when its upper advances.
  One clock per domain: an upper moves only when its domain commits a tick, and "autonomous" means a writer chooses which ticks to append in.
  Tables have no upper of their own: theirs is the domain's.
  A view's upper is at most its inputs' least upper, and advances when its program catches up.
  States: created, writing, sealed, dropped.
- **Names and writers.**
  A program imports names and exports names; inputs become imports of tables; durability and arrangement become properties of a name (durable or not, arranged here or not), not kinds of thing.
  Saving should not require an arrangement: a ledger needs each tick's consolidated changes, not an index.
  `bind`'s one-tick delay survives only for feedback.
- **Definitions and instances.**
  A `.ddp` file should be a definition with parameters, and the catalog should bind instances to names, so text never names deployment.
  The idiom inside a file, lexical scope with results read by path (`scope::name`), extends upward: functions (`def f(x) { … }`, called and inlined at lowering), files as modules, and the namespace tree as the outermost scopes, so a deployment is itself a DDIR program.
  Parameters should be explicit, defaulting to the same-named thing in scope.
- **Time arity.**
  `lift` and `delay` address time coordinates absolutely (the epoch, then each loop outermost first), so a function's meaning would depend on where it is called.
  Proposed: a function sees only coordinates it creates; `delay` addresses loops by name or relative position; `lift` gives `(outer, own)` with `outer` opaque; a function needing more says so in its signature.
  Then only loops (blocks with `set`) create a coordinate, and grouping blocks are free: no optimization needed to remove them. Today the passes treat every scope as a barrier, correctly, because of absolute addressing.
  To check first: `examples/programs/scc.ddp` and `tests/programs/lambda_slots.ddp`, which use `delay`, and whether lowering gives a block without `set` a full iteration scope.
- **Retention across domains.**
  A ledger's since must not pass the upper of any durable output that reads it; within a domain that is a view over the catalog, and across domains it needs a published hold or acceptance that a slow reader rebuilds.
  Garbage collection deletes a blob only after sweeps have found it unreachable for a grace period, which relies on transactions being decided within it.

## Infrastructure findings

- **macOS Local Network privacy** refuses outgoing connections to LAN and Thunderbolt addresses, with "No route to host", from a process whose SSH session has ended, even under `screen`; Tailscale addresses are not affected.
  Daemons run under SSH sessions held open from another machine until the binaries have Local Network permission or run as launchd daemons, which needs someone at each mini or `sudo`.
- **The minis' network.** Each Thunderbolt link is its own /30 network, so no address reaches every peer; the default route is Wi-Fi, and LAN round trips are 5–70 ms.
  A single routable address per mini over Thunderbolt (static routes, with `sudo`) would let SeaweedFS replicate and simplify etcd and Garage.
- **Store durability.** Garage's `data_fsync` and `metadata_fsync` are off by default, and on its SQLite metadata `metadata_fsync` still does not sync each commit. SeaweedFS's fsync rule makes acknowledged writes durable from 4.49 (fix merged 2026-10-22), including replicas. etcd syncs each commit. Garage is AGPL, the others Apache 2.0.

## Next steps

1. Write the lifecycle, names, and definitions threads into one design note and settle them, before more server code.
2. Give ledger batches domain time, and move the write onto a background writer with group commit.
3. S3 and etcd implementations of the storage traits, against SeaweedFS and etcd on the minis.
4. A spike of `def` and calls within one file, with relative `delay` and the split `lift`, checked against `scc.ddp` and `lambda_slots.ddp`.
5. Land the catalog (step 3) in the server, starting from `catalog/` and the agreed merge guard.
