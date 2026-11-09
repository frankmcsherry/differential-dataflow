# Ledgers: state, decisions, next steps

Ledgers that clients commit to work, through one write path, and committed ledgers can feed DDIR computations.
The next milestone is the contract between programs and ledgers, shown by a computation that reads ledger A, writes ledger B, and survives a restart.

This note records the review of 2026-11-09 and what was decided.
The review itself is [`bench/ledger/REVIEW.md`](bench/ledger/REVIEW.md); the model is [`model.md`](model.md).

## Where things are

- `src/ledger.rs`: the types, the `Store` trait and `MemoryStore`, `validate`, `State::transact`, and `Cohort`, which reads committed state.
- `src/ledger/file.rs`: `FileStore`, blobs and roots on a local filesystem.
- `src/ledger/writer.rs` and `src/ledger/engine.rs`: `Writer`, the one way to change a cohort, and the resident workers it resolves on.
- `src/ledger/resolve.rs`: encoding transactions for the rules, and decoding their conclusions.
- `src/ledger/log.rs`: conclusion records, linked from the root, and recovery by replaying them.
- `model/transactions_keyed.ddp`: the rules the writer runs.
- `model/transactions.ddp`: the same rules written serially, kept as a test oracle.
- `src/server/ledgers.rs`: `LedgerSnapshot` and `publish_ledger`, which turn committed ledgers into DDIR sources.
- `bench/ledger/`: the matched benchmark against the older prototype, and the review.

## Decisions

Taken 2026-11-09, with the branch author's agreement.

1. **One production write path.** `Writer` on resident workers is the only way to change a cohort.
   Removed: `Cohort::transact`, `create` and `append`, which published whole-state snapshots; `Resolver::resolve` and `Cohort::transact_batch`, which started workers per submission; the snapshot form of the log.
   All are at 666cf003 if needed.
2. **Oracles stay, as tests.** `State::transact` is the serial rules in Rust, and `model/transactions.ddp` is the serial rules in DDIR.
   `tests/ledger_model.rs` checks the writer against both, and `tests/ledger_keyed.rs` checks it against `State::transact`.
   `model/verbs.ddp` and `check_verbs.py` were removed; they are at 666cf003.
3. **The resident catalog is two derived indexes.** Write frontiers, which every append changes, are one trace; registration starts, which only creations add, are another.
   The rules probe the write frontiers once per submission.
4. **Host checks stay where they are.** `Writer::submit` still checks widths, registrations and payload presence before it returns, so a refusal is reported from `submit`.
   Moving the checks to the publication thread was measured and not adopted, because it changes when errors are delivered.
5. **Throughput work stops here.** The writer commits 659–665k appends/s on the matched replay with the file store, from 562–568k; the benchmark remains as a diagnostic.
6. **Converting between Rust types and columns is an interface choice**, not part of the ledger model.
7. **A submission whose transactions are all rejected still publishes.** It links an empty conclusion record and advances the root.
   The successful CAS is what confirms its rejections against the current root.
   Without it: writers W1 and W2 open at version v, with ledger L at 0; W2 commits [0, 1) to L, reaching v+1; a client then asks W1 to append [1, 2); W1's resident index still says 0 and rejects; reporting that rejection would contradict a commit that had already completed.
   With it, W1's CAS fails at v+1, so W1 reloads, re-resolves and commits the append.
   Confirming by a root read instead of a write is possible later; this choice closes no doors.

## Gaps before landing

1. **Server verbs.** Clients cannot yet create, append or transact through the DDIR server; `Writer` is a Rust API only.
   Until clients can say these to the server, ledgers are not a DDIR feature, and this is the main thing that prevents landing.
2. **Maintained tests for every claim.** Missing: several writers or sessions committing to overlapping ledgers, and the stale-rejection history in decision 7.
3. **The manifest as a ledger.** The root and its chain of conclusion records are the manifest, with the root version as a one-coordinate time.
   Its records are not yet spans with lower, upper and since, and it has no compaction, so recovery replays the whole chain.
   Backing a ledger by durable mechanisms needs both; the manifest would then be one such ledger, holding the metadata of the others.
4. **Name the writer's unit of work.** `Writer` calls a group of submitted transactions a "batch", which collides with a span's batch of updates; rename it, for example to "submission".

## Open questions

- **Width checks in the rules.** The author prefers the rules to check widths, leaving only payload presence to the host.
  Frontiers are padded with `i64::MIN`, which `PointStamp` already treats as an omitted trailing coordinate, so padding loses nothing: a time is too wide exactly when a coordinate at or past the ledger's width is not `i64::MIN`.
  It still needs a place in the rejection order.
  In an instrumented run the host check took about 4.5 ms of a 38 ms submission, nearly all of it looking up widths in a one-million-entry `BTreeMap`.
- **Resident catalog cost.** Merging the catalog costs about 3.6x per row what the older prototype's index did, and a catalog built through 40 submissions of creations ran about 3% slower (647k against 666k appends/s) than one loaded at open.
  Whether the row types force this, or it can be optimized, is unexplored; the cost should also fall with fewer ledgers, which is unmeasured.
- **Consumption is a Rust API that polls.** `LedgerSnapshot::capture` replays the whole cohort for each capture, and `refresh_ledger` takes a later capture; there is no live tail and no DDIR syntax for importing a ledger.
- **`model.md` is unchanged, pending discussion.** It is the aligning document, so it changes only after discussion; the proposal is [`MODEL_PROPOSAL.md`](MODEL_PROPOSAL.md).
  Topics: its verbs as rules any batched implementation must respect; multi-ledger transactions and their rejection order; the register backing reduced to the guarantees it illustrates; the manifest as a ledger; and "Proximal target", checked against what was built.
- **Not started:** compaction, read holds, garbage collection, and names bound to ledger ids.

## Next milestone

1. Write the program–ledger contract in `model.md`.
   It must say how a restarted program finds where to resume, what an output ledger's committed frontier means, who may append to an output ledger, and what a program holds back on its inputs.
2. Build one computation that reads ledger A and writes ledger B, and test it through a killed and restarted process.

It is done when the contract has been reviewed and the test shows B with nothing lost and nothing repeated.

## Working arrangement

- One lead session integrates and hands out bounded tasks: reviewing a change, consolidating code, or drafting the contract.
  Other sessions do not redesign the foundation on their own.
- Each task works in its own worktree, starting from a named revision.
- Benchmarks run on mini-03 with nothing else running, against the revisions named in `bench/ledger/README.md`.

## Validation

```sh
cargo test --release -p interactive --lib \
  --test ledger_file --test ledger_keyed --test ledger_model \
  --test ledger_writer --test server_ledgers
```

For throughput, follow [`bench/ledger/README.md`](bench/ledger/README.md).
