# Backlog

Three lists.
The first is what the DDIR server lacks that a good design here needs, and what the spike does in its place.
The second is what is not yet done in the spike itself.
The third is debt: what the spike does in a way we mean to change, and can change later without touching what is decided.

## What blocks a good design

### An input should take a span

This is built, on this branch, and the spike runs on it.

A ledger's span is some updates, each with its time, and an upper.
An input now takes exactly that: `feed <prog> <in> frame <nbytes>` takes a frame with times, as a binary tail gives it, and `advance <prog> <in> <time>` moves that input alone.
A program loaded `piloted` is moved this way, and `tick` leaves it be.

With this an instance is a pipe.
It feeds each span's frames to a local ledger as they are, and what a tail of an output sends is the next span to append.

What is left of it:

- A frame holds one time lane, so times of more than one coordinate need a wider frame.
- A reader that begins from a time moves earlier times up to it, by writing over the frame's last words. A feed could do that itself: "feed this, as of the time the input stands at".

### A feed that moves times up to a frontier

A reader that begins from a time, and a compactor that merges to a since, both want a span's updates with their earlier times moved up.
Today Python does this, by writing over the frame's last words (`plumbing.raised`).
A feed that takes a frontier and moves the times itself would take this out of Python.

This replaces an earlier item, which asked for a read of a trace's history added up.
That read is not needed.
A program loaded for the one merge, fed the parts with their times moved up and then advanced past every time, sends the merged span on a tail, with equal updates added together.
This was tried with what the server has, on one worker and on two.
What it lacks is a way to say "this input is closed": the trial advanced to a very large time.

### A tail at a key

A reader wants the rows of a view at one key, and then each change to them.
Today a follower loads a small filter program for each reader, and tails that.
This is fine to about 100 readers a follower, and each one is a dataflow.
`tail <name> <key>`, read from the view's index, would serve many readers from one arrangement.
A reader could then also connect to the server itself, and the follower's Python would not pass frames on.

### A frame's fixed size

A frame of `Entry` rows spends 2,472 bytes describing 108 columns, however many rows it holds.
35 of those columns are empty leaves.
One span entry is 88 bytes of values in a frame of 2,568 bytes, and 224 bytes as text.
At 1,000 span entries a frame is 56 bytes an entry, against 226 as text.
So a log record of one entry is several times larger as a frame than as text.
A round of many appends does not have this problem.

### The meet of two frontiers is there, and not used

This item said DDIR has no meet. It has one: the points of the frontiers together, put in normal form by `frontier_normalize`.
`compact.ddp` (`limit`) still takes the least of its readers' frontiers to be one of them.
An instance takes the least of the times its outputs stand at, in Python.
Both are right for times of one coordinate, and not in general, and both can now be made exact.

### A text tail does not mark the end of a tick

A binary tail sends a `progress` frame when its frontier moves, and a text tail sends nothing.
The spike uses binary tails, so this matters only to someone who reads a tail as text.

## What is not done in the spike

- **A computation whose durable outputs stand at different times.** An instance begins a computation from one time. It stops if the ledgers the computation writes stand apart, or if a derived ledger it reads is behind them.
- **A derived ledger is read only on the instance that writes it.** The model says so too, in the rule `starved`. A durable ledger may now be written from a derived one.
- **A computation that reads the state takes its durable inputs with no times.** The state's times are the server's ticks, and a ledger's are its own. The model lets such a computation write no durable ledger.
- **The compaction policy counts appends, not bytes.** The state does not say how large a span's batch is.
- **A ledger that nothing needs, and that was once compacted,** is not merged further: the policy keeps its times as written, and cannot read its compacted spans that way.
- **Blobs are deleted on time, not on what readers have read.** A blob that spans named and no longer name is deleted at once. One never seen named is left for 30 s (`COLLECT_LINGER`; `live.py` sets 2 s), since a blob is written before the span that names it. Nothing keeps a blob for a slow reader. This rests on time, and it should not.
- **Whether the keeper retries a transaction that waited** is not decided. Today it does.
- **Where an instance runs** is the first number of its id, and who starts the launchers is outside the state.
- **The actor of a transaction is claimed, not proven.**
- **A clock that follows a clock** is not tested in a round.
- **Text is still read** by the keeper, of the keys a round leaves waiting or refused and of whether a rule is empty, and by the collector, of the blobs that spans name. Each is a few small rows.
- **The rules over spans are kept where there are no spans.** There is one copy of the rules, and the keeper keeps all of them. Those over spans read nothing in the keeper's state, which has none, so they cost little there and say nothing. The ones that matter are kept by a follower that audits, which has the spans. The rules of a step (`step.ddp`) are not audited: a round and a pass change spans without them, and only the keeper's transactions and `tests/ledger_system.rs` are judged by them.
- **An own hold only moves forward, and the model does not say so.** The keeper refuses a transaction that would move a ledger's own hold back, or take it away while the ledger stays registered (`receded`, in `readers.ddp`), because it cannot tell which spans merges have taken out. The model judges such a move by the spans, in `forgotten`, and would let one by when the spans a reader needs are still there. Whether the model should have the rule too is not decided.
- **A round refuses a transaction that appends to one ledger twice.** A round takes appends as frames it does not open, so it cannot order two appends to one ledger within a transaction. The rules alone would make such a transaction. `round.ddp` says so, at `twice`.

## What a scale probe found

Measured on one machine, with 1,000 source ledgers, 1,000 computations on 8 instances, and 1,000 output ledgers, and every source appended to each tick.
The whole system did 1.4 ticks a second, and slowed as spans piled up. With the rules off the path of a round, and the instance's walk changed, it does 2.7, and 10.7 with 200 computations on 4 instances.
The probes and their output are in `ledger-fabric-sweeps-2026-11-10/scale`.

- **Three programs walked a ledger's spans from a place that moves every tick.** `forgotten` in `system.ddp` walks them once for each need, `compact.ddp` does the same for its policy, and `leads` in `instance.ddp` walked back from a ledger's last append. Each walk is redone whole when the need, or the last append, moves, so each costs as many rows as the ledger has spans, every tick. `instance.ddp` is changed: it finds the numbers that are dead ends, which an append does not change, and it asks a reader's need only of spans that a merge made. On random ledgers, readers and changes it says what the old one said (`check_instance.py`, with the probes). `compact.ddp` is changed the same way, and checked the same way (`check_compact.py`): with 200 ledgers of 120 spans each it costs 13 ms a tick, where it cost 88. What still grows in it is `heavy`, `first` and `version`, which read every span of the chain when the version moves; the chain is short when merges keep up. `forgotten` is changed too: it carries a floor forward from 0, the join of the sinces of the merged spans on the way, and a need is met when some floor at the version is at or before it. On random ledgers and holds of one-coordinate times it says what the old rule said (`check_forgotten.py`). For times of several coordinates it is stricter: it takes a merged span to need `since <= need`, where the old rule asked `since <= join(need, lower)`. It would be exact with a frontier verb DDIR lacks: the least frontier `x` with `since <= join(x, lower)`.
- **A ledger's version was found by a reduce over all its spans,** in `system.ddp`, `index.ddp`, `instance.ddp` and `compact.ddp`. Each append read them all again. This is changed: it is found from the ledger's tips, the numbers no span begins at, which an append changes two rows of, once in `index.ddp` for every role, and again in `compact.ddp`.
- **Every rule was kept up to date on the path of every round, in three places.** This is changed. Each follower now keeps a second copy of the entries for the rules (the keeper then did too; it now holds no spans, and its rules read only the configuration), and bring it up to date only when a rule or a view is read, or after 64 frames. At 1,000 computations the keeper's work a tick went from about 405 ms to about 150 ms, and a follower's from 317 ms to 79 ms. A keeper that keeps no rule at all does a tick in 55 ms. What is left: the catching up is slow and comes all at once, and a tick that meets it takes over a second.
- **The parts are in a line.** A tick goes keeper, follower, instances, keeper, follower, and each of the first two does one thing at a time. With 10 cores, fewer than 3 are busy.

## Debt

Each of these works, and is not how we mean it to be.
None of them is in the programs, and none is in anything stored: no program reads the server's epoch, and a log record is numbered by its place in the log and holds frames with no times.
So each can be changed later by editing the places named, with no change to the model or to what is on disk.

- **The server has two kinds of program.** One is moved by `tick` and one, loaded `piloted`, by its client. We mean to have one kind, moved by its client, with `tick` as something a client does for a demo. To get there, what `tick` also does needs a home: delivering `bind`, moving the built-in clock and the churning sources, and entering diagnostics. The spike says `piloted` in two places (`runner.py`, where a computation is loaded, and `steward.py`, where a merge is made).
- **`advance` waited for outputs.** This is changed: `advance` moves one input and nothing more, and `spin` waits until outputs reach the least of their inputs. A step of 1,000 small computations in one server took 2.1 s with the wait in each `advance`, and takes 85 ms with one `spin`. Still wanted: a command that says where a trace stands and which input holds it back.
- **The state is moved by `tick`.** The keeper, each follower and each instance feed the state, and the programs they load beside it, and tick: 33 lines of `keeper.py`, `follower.py` and `runner.py` name `tick`. We mean to feed each log record at its number in the log, and advance to the next number. With that, a computation that reads the state is moved like any other, and the rule that it writes no durable ledger can go.
- **Python writes a frame's times.** `plumbing.raised` (three callers) moves times up, and `plumbing.untimed` (two callers) cuts them off. Neither opens a column. Both know how a frame is laid out, which is more than a pipe should know. The feed above removes the first, and feeding the state at record numbers removes the second. The rule was no new place where Python reads or writes inside a frame of someone's data, and two have come since: `plumbing.shifted` and `plumbing.latest`, for the compactor's `Merger`.
- **The compactor merges in a server of its own.** This is changed: the one-off program above is what `Merger` in `steward.py` does, so equal updates are added together. It puts frames one after another only when a shape does not load there.
