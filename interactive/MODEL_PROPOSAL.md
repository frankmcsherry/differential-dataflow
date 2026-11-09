# Proposed changes to `model.md`

This is a proposal for discussion; `model.md` itself is unchanged.
It goes section by section, says what to keep, change, move or decide, and gives draft text where a change is substantive.
Each item is numbered so a reply can name it.

The principle: `model.md` states the rules any implementation must respect, and the guarantees it must give.
How this implementation meets them belongs in code comments and `LEDGERS.md`.

## Summary

| Section | Proposal |
| --- | --- |
| Ledgers | keep; two small precision fixes (M1) |
| Properties of Ledgers | keep (M2) |
| Ledger evolution | keep the lifecycle; move its compaction text to Compaction (M3) |
| Operations and cohorts | add the transaction verb, moved from Proximal target, with its rejection order (M4) |
| Manifests | rework: the manifest is itself a ledger (M5) |
| Register backing, Creation, Append | replace with the guarantees they illustrate (M6) |
| Ordering and recovery | keep, folded into M6 |
| Compaction | keep as design, marked not built (M7) |
| Timelines | move out of the model, as policy (M8) |
| Computation | replace with the server's read and write bindings (M9) |
| Proximal target | replace with a checklist of what was built and dropped (M10) |
| new: Server verbs | add (M11) |

## M1. Ledgers: keep, with two fixes

The definitions hold as written: updates, the read and write frontiers, spans `(id, lower, upper, since, batch)`, the initial time, and antichains of product times.

- "Write frontiers" says the write frontier is "the largest recorded `upper`".
  For antichains, "largest" is defined only because a ledger's spans form a chain, each span's lower equal to the previous upper.
  Proposed: "the upper of the last span in the ledger's chain from its initial time, or the initial time if there are none."
- Typos: "shoud", "taxing the max", "must accumulated", "commiting", "withs".

## M2. Properties of Ledgers: keep

Well-formedness, the write frontier, readability, maximal intervals and the read frontier are rules, written as DDIR.
They use single-integer times; the multitemporal details box already says how they generalize.
Nothing implemented contradicts them.
Readability and the read frontier are not computed anywhere yet, which is fine for a model.

## M3. Ledger evolution: keep the lifecycle, move compaction

Keep: creation by registration, advancing the write frontier by appending spans, sealing by advancing to the empty frontier, and dropping by removing every span.
Move the paragraphs on compaction and its guards into "Compaction", so each concept is described once.

## M4. Operations and cohorts: add the transaction verb

Today this section has `create` and `append`, each on one ledger.
Multi-ledger transactions are in `model.md`, but only in "Proximal target".
They are the verb clients use, so they belong here.
Draft text, to follow the two existing verbs:

> 3. `transact(groups) -> RESULT<(), REJECTION>`: apply changes to several ledgers atomically.
>    Each group names one ledger and one change: a creation, an append, or a creation followed by an append from the new ledger's start.
>    Groups name strictly increasing ledger identifiers, so a transaction changes each ledger at most once.
>    The transaction commits only if every group succeeds; otherwise the cohort is unchanged.
>    A rejection names the first group that fails and why: its ledger's actual write frontier, an existing ledger, an unknown ledger, or an invalid request.
>    Structural invalidity, meaning groups out of order or an append whose lower is not strictly before its upper, is checked without consulting the cohort, and takes precedence over any failure that depends on the cohort.
>    An empty transaction commits.
>
> `create` and `append` are transactions with one group.

The existing requirements, atomic in a serial order, respecting real-time completion order, and durable when they return, then apply to `transact` unchanged.

Question for discussion: a combined creation and append uses the start as the span's lower and its since.
Should `append` always commit `since = start`, as now, or may a writer supply a since?

## M5. Manifests: the manifest is a ledger

Proposed rewrite of the section's core:

> A cohort's state is its registry and its spans.
> The cohort's manifest is a ledger whose updates are changes to those two collections: a registration `(id, shape, start)`, or a span `(id, lower, upper, since, batch)`, each with a diff.
> Its time has one coordinate, which counts commits.
> Each commit appends one span `[v, v+1)` to the manifest, whose batch holds that commit's changes.
> The registry and spans at version `v` are the manifest's accumulation up to `v`.
> Compacting the manifest is ordinary ledger compaction: a rollup of spans `[a, c)` consolidates their changes, and additions cancel removals.
>
> The manifest is the one ledger not described by another manifest.
> Its write frontier is held by a durable register that only compare-and-swap changes, which is what makes its commits ordered and atomic.

Today's implementation is close to this: each commit links one record of conclusions from the root, and the root version counts commits.
What is missing is that records are not written as spans, and there is no compaction, so recovery replays every record.

Questions for discussion:

- Does a manifest's span use the same `Payload` choices, so that small commits are inline and large ones are blobs?
- Once the manifest is a ledger, does "cohort" remain a separate concept, or is a cohort simply the ledgers one manifest describes?

## M6. Register backing, Creation, Append: replace with guarantees

These sections describe one backing: read the root, write a whole-state blob, swap the root.
As Frank noted, they were meant as one way to describe the expected behavior.
Proposed replacement, a section called "Durability":

> A durable cohort uses immutable blobs named by their contents, and one register replaced only by compare-and-swap on its version.
> Any implementation must give these guarantees:
>
> - An operation's outcome is reported only after it is durable.
> - A committed operation survives a crash of any process; an operation not committed leaves no visible effect, even if its blobs were written.
> - Every blob a commit references is durable before the commit.
> - Commits are ordered by the register's versions, and this order respects real-time completion.
> - A rejection is decided at a read of the committed state that is still current when the rejection is confirmed.
>   A writer working from an older copy must confirm, for example by its compare-and-swap succeeding, before it reports a rejection.
> - A storage failure, and an outcome that may or may not have committed, are reported as such, and never as rejections.

The "Ordering and recovery" text is mostly these guarantees already, and would fold in.
The `put`, `get`, `read_root` and `cas_root` signatures could stay as a short illustration, labelled as one backing.

## M7. Compaction: keep as design, not built

Keep the definition, the viability guard (`[0,1) [1,2) [2,3)` rolled up as `[0,2)` and `[1,3)`), and read holds.
Mark the section as not implemented.
One addition from the older branch: retiring spans must be guarded, while adding a rollup never needs a guard, because its contents are a function of immutable inputs.

## M8. Timelines: move out of the model

Which ledgers share a meaning of time, and how far apart they may drift, is policy layered over the mechanism, as Frank suggested.
Proposal: delete the section from `model.md`, and keep the open question in `LEDGERS.md`.

## M9. Computation: replace with read and write bindings

Under Frank's model, programs do not act on ledgers.
The server offers a ledger's contents as an arrangement, and can make a program's output arrangement durable as a ledger.
Proposed replacement:

> A server can read a ledger as a source: an arrangement holding the ledger's committed updates, whose frontier is the ledger's write frontier, advancing as spans commit.
>
> A server can write a ledger from an output arrangement.
> At each step, it appends one span from the ledger's write frontier to the arrangement's new frontier, holding the output's updates at those times.
> The output must be a deterministic function of its inputs.
> Then:
>
> - After a restart, the server recomputes the output and appends only updates at or beyond the ledger's write frontier, so nothing is lost or repeated.
>   This requires the inputs to remain readable from far enough back to recompute the output.
> - If two servers write the same output, the append rule lets exactly one of them commit each span; the other holds identical updates and skips ahead.

The old "continual tasks" paragraph, `(input, state) -> (input', state')`, could stay as an open direction, or move to `LEDGERS.md`.

Questions for discussion:

- Is "far enough back" for inputs a hold the writing server places, or a policy someone else sets?
- Should a write binding be allowed for any output, or only one whose time width matches the ledger's?

## M10. Proximal target: replace with a checklist

| Target in `model.md` | State |
| --- | --- |
| Grouped transactions, increasing ids, combined creation and append | built |
| First checkpoint on a memory backing | built: `MemoryStore` |
| Ledgers created and appended independently of computations | built, as the Rust `Writer` |
| A DDIR computation consumes committed updates and frontier progress | built, as a Rust API that polls: `publish_ledger`, `refresh_ledger` |
| Rust and `.ddp` rules checked against the same histories | built: `State::transact` and `transactions.ddp`, against the writer |
| Durable backing, recovery after restart | built: `FileStore`; process-exit tests, not power-failure tests |
| `.ddp` resolver commits through a ledger holding the registry and manifest | partly: through linked records, not yet a ledger (M5) |
| Compaction, reclamation, deployment metadata | not started |

Dropped along the way: per-operation publication of whole-state snapshots, and the single-verb rules in `verbs.ddp`.

## M11. Server verbs: add

A new section listing what clients can say to the server, as the client's view of M4 and M9.
To be designed; a starting list:

- `create`, `append` and `transact`, with the outcomes of M4.
- Reading a ledger's write frontier and its committed spans.
- Binding a ledger as a program's input, and an output as a ledger (M9).

This is the main gap before landing, so its section may be best written alongside the implementation.
