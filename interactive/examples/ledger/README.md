# Ledgers: a stack of durable ledgers around DDIR

This directory is a prototype of a system built around DDIR, as Materialize is built around differential dataflow.
One ledger, the root, is made durable by brute force: a log of records in a blob store, behind one compare-and-swap.
Every other ledger is rows in it, whose spans name batches of updates in the blob store.
Its decisions are DDIR programs over one collection of state, and the Python around them moves bytes, swaps the register, and starts processes.

It is a prototype: the programs are real and tested, and the processes around them are Python standing in for Rust.
[`DEMO.md`](DEMO.md) says how to run it, across four machines.
[`BACKLOG.md`](BACKLOG.md) says what it lacks, and what it does that we mean to change.

## The layers and their roles

| Layer | Role | Process | What it does |
| --- | --- | --- | --- |
| Durability | Keeper | [`keeper.py`](keeper.py) | Judges every change, puts it in order, and records it in the root ledger. It keeps every entry but spans. |
| Configuration | Followers | [`follower.py`](follower.py) | Each reads the root ledger and keeps a copy of the state. By role: serve instances, advise compaction, audit every rule. |
| Execution | Instances | [`runner.py`](runner.py) | Each is told its part of the state, runs the computations it names, and appends their outputs. A launcher starts and stops them. |
| Operation | Compactor, hold steward, collector | [`steward.py`](steward.py) | Merge spans, move read holds, and delete blobs that an audited state no longer names. |

[`plumbing.py`](plumbing.py) is shared by all: the register, the blob store, frames, and the DDIR server.

## The programs, by who runs them

| Program | Who runs it | What it is for |
| --- | --- | --- |
| [`system.ddp`](system.ddp) | the keeper, a follower that audits | the state's views, and the rules a state should keep |
| [`index.ddp`](index.ddp) | the keeper, followers, instances, the tests | each ledger's row from the entries: its version and own upper |
| [`stand.ddp`](stand.ddp) | the keeper, followers, instances | where each durable ledger stands, from the rows |
| [`needs.ddp`](needs.ddp) | the keeper, followers | what each computation reads and writes, and what each ledger is needed from |
| [`step.ddp`](step.ddp) | the keeper | the rules of a change: what may not be undone |
| [`commands.ddp`](commands.ddp) | the keeper | what each command removes and adds |
| [`admit.ddp`](admit.ddp) | the keeper | the short check of a transaction of appends |
| [`round.ddp`](round.ddp) | the keeper | the same check for many transactions at once |
| [`shift.ddp`](shift.ddp) | the keeper | the row a transaction gives a ledger it registers, or takes from one it drops |
| [`gone.ddp`](gone.ddp) | the keeper | what leaves the keeper's tables with a dropped ledger |
| [`own.ddp`](own.ddp) | the keeper, the policy follower, the hold steward | each ledger's own read hold |
| [`readers.ddp`](readers.ddp) | the keeper | a new reader that would begin before its ledger's read hold |
| [`pass.ddp`](pass.ddp) | the keeper | a pass of merges: the generation, the cover, and the merge's bounds |
| [`bundle.ddp`](bundle.ddp) | a follower that serves | what each instance is told, and the instances each machine has |
| [`instance.ddp`](instance.ddp) | each instance | what to run, where to begin, and which spans to read next |
| [`compact.ddp`](compact.ddp) | the policy follower | which spans of each ledger to merge next |

## Checks

| Check | What it does |
| --- | --- |
| [`../../tests/ledger_system.rs`](../../tests/ledger_system.rs) | Builds a system by commands, breaks each rule once, and holds admission and rounds to the full rules. |
| [`live.py`](live.py) | The whole system on this machine: keeper, followers, instances, compactor, collector; restarts from the register and the blobs alone. |
| [`from_empty.py`](from_empty.py) | A system built up from nothing, one transaction at a time, with what each caused. |
| [`fleet.py`](fleet.py) | The system of `live.py` across four machines. |

    cargo build --release -p ddir-server
    cargo test --release -p interactive --test ledger_system
    python3 interactive/examples/ledger/live.py

`live.py` needs an etcd at `127.0.0.1:2379` and SeaweedFS at `127.0.0.1:8888`.

## The demo

[`demo.py`](demo.py) is a walk in eight acts across four machines, [`steady.py`](steady.py) runs the system with data and configuration churning until stopped, and [`watch.py`](watch.py) is a page of live views beside either.
[`DEMO.md`](DEMO.md) says what each needs and shows.

## Earlier work

An earlier generation of this directory, with resolvers in place of a keeper (`ledger.ddp`, `resolve.ddp`, `fabric.py` and their tests), is on the branch `ddir-ledgers`.
The round harness (`rounds.py`, `flat.ddp`) that measured round admission alone is in this branch's history, before the commit that removed it.
