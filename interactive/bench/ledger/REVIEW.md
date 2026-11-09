# Review of `ddir-ledger-model`, against `ddir-durable-names`

This is the review as it was sent to the branch's author on 2026-11-09, kept as a record.
It describes 666cf003; [`../../LEDGERS.md`](../../LEDGERS.md) says what was adopted, including the author's answers.
The `one-probe` and `split-start` spikes are commits on this branch; `off-path-checks` was not adopted.

This is a review of `ddir-ledger-model` at 666cf003, written for its authoring agent.
It asks for your reactions before the work is handed on, especially on the notes in the last section.
Where I state a number, I measured it or it comes from your handoff, and I say which.
Where I state a judgment, I mark it as one, and I expect you to know things about the branch that I do not.

## Bottom line

1. Your handoff reproduces exactly on mini-03: the reference runs at 1.42–1.44M committed appends/s and the branch at 562–568k/s, on the same request stream and the same `ddir_server`.
2. Three small spikes, each keeping UUIDs, antichain frontiers, error precedence and pre-publication checks, take the branch to 731–736k/s.
3. My judgment: stop throughput work at that point, trim to one production path, and turn to the half of the DDIR surface not yet built, which is programs that produce ledgers.

## What I measured

All runs: mini-03 (M4), the matched replay from `interactive/bench/ledger/README.md`, `file:` store, pipelined, 4 workers, 5 warmup batches, three runs each.
Every run committed 1,011,005 appends, and the replayer checked every outcome.

### Catalog merges, measured in both

I applied only the `interactive/src/server/progress.rs` hunk of `profile.patch` and set `DDIR_COMPACT_PROFILE=1` for both systems.
The reference's server stderr is discarded by `bench_txn.py`, so I passed `--wrap` a script that redirects it.
Totals over all 40 batches, CPU in `set_logical_compaction` plus `set_physical_compaction`:

| Trace | CPU ms, 40 batches |
| --- | ---: |
| reference `cat::records_up` | 376 |
| reference `cat::batches` | 161 |
| branch `catalog::current` | 1,363 |

So the reference's merge cost was not zero, as the handoff left open; it is about 13 CPU-ms per batch against the branch's 34.
Both catalogs hold 1M rows, and the reference also holds every batch, so the branch pays about 3.6x per resident row.

### Attribution of the 27 ms gap per batch

| Cost | Wall per batch | Source |
| --- | ---: | --- |
| Resolver operators (75.6 vs about 30 scheduled ms, summed over workers) | ~11.5 ms | your profile |
| Catalog merges (34 vs 13.4 CPU-ms) | ~5 ms | measured above |
| Main-thread encode, validate and outcomes, inside the clock | ~6.6 ms | your profile |

That accounts for about 23 of 27 ms; I have not attributed the rest.

### Spikes

Each was a single commit on 666cf003, measured alone.

| Spike | Change | Committed/s | ms per batch |
| --- | --- | ---: | ---: |
| baseline (re-run) | none | 562–564k | 44.8 |
| `one-probe` | `bases`, `starts` and `old_initial` read one `probed` join of the catalog | 602–607k | 41.6 |
| `split-start` | catalog input 0 holds write frontiers only; starts move to input 2, a trace `catalog::registered` fed by a new output `registered_delta` | 650–654k | 38.7 |
| `off-path-checks` | the checks in `Writer::resolve` move to the publication thread, still before the root CAS | 610–614k | 41.4 |
| `all-three` | all three together | 731–736k | 34.4 |

Tests: the six ledger test targets plus `--lib` pass on every spike (254; 255 with the test `off-path-checks` adds).
I did not run `check_verbs.py`; the spikes do not change `transactions.ddp` or `verbs.ddp`.

Things to check in the spikes:

- `split-start` changes the keyed resolver's inputs and both execution paths: `Engine` (two catalog traces) and `Resolver::resolve` (three inputs).
- `off-path-checks` changes behaviour: a refused batch reports its error from `wait`, not `submit`.
  Earlier batches in its publication group still publish, and the writer stops.
  The new test `refused_effects_publish_earlier_batches_in_their_group_and_stop_the_writer` covers that; the baseline had no writer-level refusal test.
  The `Published` struct carries the batches back from the thread, so a spawn failure or a panic now reports by batch number.
- Together they save 10.4 ms, not the 12.6 ms sum, because after the split the shared probe no longer covers `starts`.

## Complexity: inherent and incidental

Lines added against each branch's base:

| | `ddir-durable-names` | `ddir-ledger-model` |
| --- | ---: | ---: |
| Rust, excluding tests | 658 | 2,951 |
| Rust tests | 100 | 2,319 |
| DDP | 357 | 340 |
| Python | 2,289 | 349 |

The counts mislead in both directions.
The reference's fast path is Python and two DDP files; its Rust is the unrelated `save`/`restore` feature.
It is small partly because its resolver is general (client-encoded cells) and partly because it trusts clients for well-formedness that your resolver checks.
That checking is inherent cost, not sprawl.

My classification of your branch, for you to correct:

- **Inherent:** `lambda/frontiers.rs` (323), `transactions_keyed.ddp` (145), `engine.rs` plus `writer.rs` (441), `ledger/file.rs`, the log and its recovery, and `server/ledgers.rs` as the consumption bridge.
- **Incidental, or at least duplicated:**
  - four statements of the transition rules: native Rust in `Cohort::transact`, `verbs.ddp`, `transactions.ddp`, `transactions_keyed.ddp`;
  - two execution paths: a fresh worker group per batch (`Resolver::resolve`) and the resident `Engine`;
  - two write-side log forms: whole-state snapshots from `Cohort::publish`, and linked conclusions that treat those snapshots as legacy checkpoints;
  - conversion between Rust types and columns and back, most of `resolve.rs` (498), plus host re-checks of what the rules concluded;
  - in-tree benchmark material: `dependencies.lock` (1,137 lines), two patches, gzipped results.

## Against the DDIR surface

My read of where the branch stands on the intended surface: ledgers you commit to, and programs that consume and produce ledgers.

- **Ledgers you commit to:** done, and faithful to `model.md`.
- **Programs consume ledgers:** half done.
  `server/ledgers.rs` is a Rust API, and `LedgerSnapshot::capture` replays the whole cohort for each capture; there is no live tail and no `.ddp` or server command.
- **Programs produce ledgers:** not started.
  Nothing commits a program's output as spans whose bounds come from its output frontier.
- **Names, `since`, read holds, compaction:** described in `model.md`, not implemented; recovery grows with history.
- **`model.md` itself:** "Computation" and "Timelines" are a paragraph each, so the model does not yet specify how programs relate to ledgers.

From the older branch, these decisions seem worth carrying over: writers make data durable before submitting; the log holds conclusions; names bind to ids; retention across domains needs published holds.
Its general resolver and integer frontiers do not seem worth carrying over.

Proposed next steps, as judgment:

1. Land the three spikes, then trim to one production path, keeping the other rule statements as test oracles.
2. Extend `model.md` with the program–ledger contract: what an output ledger's upper means, who may append, how a restarted instance resumes, what holds it places on inputs.
3. Build one loop end to end: a program reads ledger A and writes ledger B; kill and restart it, and check B has nothing lost or repeated; then add a second program that consumes B.

## Notes to react to

Please answer each, briefly; disagreement is the useful outcome.

1. **Four statements of the rules.** Which of native `Cohort::transact`, `verbs.ddp` and `transactions.ddp` are oracles by design, and could `Cohort::transact` stop publishing whole-state snapshots, leaving one write path?
2. **Two execution paths.** Does anything need `Resolver::resolve` with a fresh worker group, other than tests and the `Cohort::transact_batch` path?
3. **Legacy checkpoints.** Is the snapshot form in `log.rs` needed for anything beyond reading histories written by the native path?
4. **Host re-checks.** The checks in `Writer::resolve` re-verify the resolver's conclusions (widths, registrations, counts). Should width checking move into the rules, so the host only checks payload durability?
5. **`split-start`.** Is a second catalog input acceptable to you, or would you rather keep one input and drop `start` from it some other way, such as only for ledgers whose start equals the cohort default?
6. **`off-path-checks`.** Is reporting a refusal at `wait` instead of `submit` acceptable for the `Writer` contract?
7. **The classification above.** Which items I called incidental are load-bearing, and what did I call inherent that you would cut?
8. **The unattributed ~4 ms.** Do you know where it goes?
9. **Next step.** Do you agree that output ledgers should come before further throughput work, and is `model.md` the right place to settle the program–ledger contract first?
