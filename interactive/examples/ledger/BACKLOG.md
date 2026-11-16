# Backlog

Two lists.
The first is what the DDIR server lacks that a good design here needs, and what the spike does in its place.
The second is what is not yet done in the spike itself.

## What blocks a good design

### An input should take a span

A ledger's span is some updates, each with its time, and an upper.
An input should take exactly that: the updates at their times, and then advance to the upper.
Different inputs should stand at different times, and no input should have to move because another did.

Today `tick` moves every input of every program one epoch.
A row that is fed takes the time of the tick, and a frame that is fed may hold no times.
So an instance opens each batch, splits it by time, and feeds one ledger time per tick.
It also turns the server's epochs back into ledger times before it appends what came out.

The server library already has the parts: `advance_input`, `feed_at` and `feed_columns_at` in `interactive/src/server/`.
The server's commands do not reach them.
A frame holds at most one time lane, so times of more than one coordinate also need a wider frame.

With this, an instance would be a pipe.
It would feed each span's frames as they are, and what a tail of an output sends would be the next span to append.
A compaction written in DDIR would also read times as times, and not as a column of the rows.

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

### A text tail does not mark the end of a tick

A binary tail sends a `progress` frame when its frontier moves, and a text tail sends nothing.
The spike now uses binary tails, so this matters only to someone who reads a tail as text.

## What is not done in the spike

- **Batches are JSON.** An instance and the compactor open them in Python. They should be frames, as a tail of an output gives them.
- **An instance works things out in Python that DDIR could.** Where a ledger stands, which spans to read, and what to run should be views in the instance's own server. Python would then read a few small rows: a program's text to load, a blob's hash to fetch, the version to append at.
- **Compaction has a mechanism and no policy.** The model has `Merge` and `Discard`, and the rule `forgotten` refuses a merge that a reader or a holder needs otherwise. The compactor merges a whole ledger into one span once it has four, and builds the merged batch in Python. The merge should be a DDIR program.
- **Old blobs are deleted after a wait of two seconds.** Nothing else keeps a blob for a slow reader. This rests on time, and it should not.
- **A derived ledger is read only on the instance that writes it.** The model says so too, in the rule `starved`. A computation that reads a derived ledger and writes a durable one is allowed by the model, and the instance does not do it yet.
- **The keeper keeps every rule up to date after each round.** A round reads no rule, so the rules can come off its path.
- **Whether the keeper retries a transaction that waited** is not decided. Today it does.
- **Where an instance runs** is the first number of its id, and who starts the launchers is outside the state.
- **A keeper that starts reads the whole log.** There is no snapshot of the state.
- **The actor of a transaction is claimed, not proven.**
- **A clock that follows a clock** is not tested in a round.
