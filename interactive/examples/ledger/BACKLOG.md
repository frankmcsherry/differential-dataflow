# Backlog

Two lists.
The first is what the DDIR server lacks that a good design here needs, and what the spike does in its place.
The second is what is not yet done in the spike itself.

## What blocks a good design

### An input should take a span

This is built, on the branch `ddir-feed-spans`, and the spike runs on it.

A ledger's span is some updates, each with its time, and an upper.
An input now takes exactly that: `feed <prog> <in> frame <nbytes>` takes a frame with times, as a binary tail gives it, and `advance <prog> <in> <time>` moves that input alone.
A program loaded `piloted` is moved this way, and `tick` leaves it be.

With this an instance is a pipe.
It feeds each span's frames to a local ledger as they are, and what a tail of an output sends is the next span to append.

What is left of it:

- A frame holds one time lane, so times of more than one coordinate need a wider frame.
- A reader that begins from a time moves earlier times up to it, by writing over the frame's last words. A feed could do that itself: "feed this, as of the time the input stands at".

### A trace's history, with its times, added up

A compactor wants the updates of some spans, with their times moved up to a since, and equal ones added together.
That is what a trace holds once it has compacted to that since and merged its batches.
A `peek` gives a trace's contents added up, and without times.
A `tail` gives its history with times, and does not promise that equal updates were added together.

So the compactor merges by putting the frames of the parts one after another, with their times moved up.
The spans get fewer, and the batches do not get smaller: an update and its retraction are both kept.
A read of a trace's history as it is stored, at its times, would let the server do the merge.

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

### The meet of two frontiers

DDIR has `frontier_join`, `frontier_le` and `frontier_lt`, and no meet.
`compact.ddp` wants the least frontier a ledger is needed from, and takes it to be one of the frontiers asked.
An instance takes the least of the times its outputs stand at, in Python.
Both are right for times of one coordinate, and not in general.

### A text tail does not mark the end of a tick

A binary tail sends a `progress` frame when its frontier moves, and a text tail sends nothing.
The spike uses binary tails, so this matters only to someone who reads a tail as text.

## What is not done in the spike

- **A computation whose durable outputs stand at different times.** An instance begins a computation from one time. It stops if the ledgers the computation writes stand apart, or if a derived ledger it reads is behind them.
- **A derived ledger is read only on the instance that writes it.** The model says so too, in the rule `starved`. A durable ledger may now be written from a derived one.
- **A computation that reads the state takes its durable inputs with no times.** The state's times are the server's ticks, and a ledger's are its own. The model lets such a computation write no durable ledger.
- **The compaction policy counts appends, not bytes.** The state does not say how large a span's batch is.
- **A ledger that nothing needs, and that was once compacted,** is not merged further: the policy keeps its times as written, and cannot read its compacted spans that way.
- **Old blobs are deleted after a wait of two seconds.** Nothing else keeps a blob for a slow reader. This rests on time, and it should not.
- **The keeper keeps every rule up to date after each round.** A round reads no rule, so the rules can come off its path.
- **Whether the keeper retries a transaction that waited** is not decided. Today it does.
- **Where an instance runs** is the first number of its id, and who starts the launchers is outside the state.
- **A keeper that starts reads the whole log.** There is no snapshot of the state.
- **The actor of a transaction is claimed, not proven.**
- **A clock that follows a clock** is not tested in a round.
- **Text is still read** by the keeper, of the keys a round leaves waiting or refused and of whether a rule is empty, and by the collector, of the blobs that spans name. Each is a few small rows.
