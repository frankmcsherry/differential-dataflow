# The demo

A walk through the system of ledgers, in eight acts, with a page of live views beside it.
It is a first version.
It shows what the design makes possible, and it is not a product.

## What must be up first

- One etcd of three members, on machines 0, 1 and 2, reachable at their addresses on the links from this machine.
- SeaweedFS on this machine, at `127.0.0.1:8888`.
- An etcd on this machine, at `127.0.0.1:2379`, for the last act, which runs on this machine alone.
- The stock server, built: `cargo build --release -p ddir-server`.
- Key-based SSH to the three machines.

These are what `fleet.py` needs, and `fleet.py` is the check that they are there: if it passes, the demo will run.
Every port is bound to `127.0.0.1` on every machine.
The keeper's port and the blob store's reach the other machines inside the SSH sessions that start their launchers.

## How to run it

From the repository root, in two terminals.

    python3 interactive/examples/ledger/watch.py 100=A 101=B 102=C
    python3 interactive/examples/ledger/demo.py

The first prints the address of the page: open it in a browser on this machine.
The second stops before each act and goes on at Enter.

    python3 interactive/examples/ledger/demo.py --auto            the whole of it without stopping, to try it
    python3 interactive/examples/ledger/demo.py --no-load         the walk without the last act
    python3 interactive/examples/ledger/demo.py scale 200 4 10 20 the last act alone: computations, instances, ticks a second, seconds

Without stopping, the walk takes about 25 seconds and the last act about 30.
The walk copies the programs and the server to `~/ledger-spike-rows` on each machine before it starts.

## What is on the page

Everything on the page is a view of the state, read from the follower on this machine, or the keeper's own counts.

- **What the state asks to run, and where.** Machines, their instances, and the computations each is asked to run.
- **Where each ledger stands.** Its name, how many appends it has, its frontier, how many spans hold it, and who may write it.
- **Computations.** What each reads and writes, and where it runs.
- **The rules.** That all 29 hold, or the names of those that do not, and any computation that no instance runs. They are read from a follower that has the spans: the one on this machine in the walk, and the audit follower in the running mode.
- **At the top.** The keeper's rounds, the transactions it judged alone by the rules, and the records in its log.

A cell that changes is marked for a moment.
With more than 40 ledgers the page shows counts in place of rows.

## What the three computations do

A line is some words, and a word is an int.
Line `t` holds the words `t % 5`, `t % 7` and `100 + t`, so a few words are in many lines and most are in one.

- **A, `counts`:** how many lines each word is in.
- **B, `often`:** the words that are in two lines or more.
- **C, `summary`:** how many words are in each number of lines.

After each batch of lines the demo works the summary out again in a few lines of Python, and holds the ledger to it.

## The acts

### Act 0. A system with nothing in it

Say: there is a keeper, which decides what is made and writes it down, and on each machine a follower, which holds a copy of the state, and a launcher, which starts what the state asks its machine to run.
Nothing has been asked for yet.

On the page: no ledgers, no computations, nothing asked to run.

### Act 1. One transaction sets up a pipeline across three machines

Say: this one transaction is the whole configuration.
It makes four ledgers and names them, publishes three programs, defines three computations, and says which machine runs each.
Nobody is told to start anything: each launcher sees that the state now asks for an instance on its machine.

On the page: three instances appear, on machines 0, 1 and 2, and the four ledgers begin to move.

The demo then says how many of the state's entries each instance was told.
Say: an instance is told what it needs and no more, by one tail of a view.

### Act 2. The rules refuse what would break the state

Say: a rule is a query over the state that should be empty.
A transaction that would make one non-empty is refused, and the reply is the rule's name.

- Dropping the ledger that A reads comes back with `misnamed` and `unregistered`: a name would point at nothing, and a computation would read a ledger that is not there.
- A user's append to a ledger that B writes comes back with `barred`.
- Starting a computation that is not defined comes back with `undefined`.

On the page: nothing changes, and the rules still hold.

### Act 3. A computation is moved to another machine

Say: to move a computation is one transaction, a Stop and a Start.
The instance that takes it over begins from where the ledger stands, and keeps nothing of its own.

On the page: B moves from machine 1 to the instance on machine 2, beside C.

One line may be printed here: `instance: computation 101 was refused (stale), and must start over`.
Say, if it is: for a moment two instances ran B.
The keeper took the append of one and refused the other, and the one that was refused began again from the ledgers.
Nothing coordinated them.
This came in three of four runs.

### Act 4. A machine is lost

Say: machine 2 goes away, with its follower, its launcher, and the instance that ran B and C.
Nothing notices on its own, and nothing is wrong: A goes on, and the ledgers that B and C write stand where they stood.
One transaction gives B and C new homes, and they catch up from where their ledgers stand.

On the page: `counts` goes on, and `often` and `summary` stand still, until B appears on machine 1 and C on machine 3.

### Act 5. The keeper is stopped, and another is started

Say: the keeper holds nothing that is not in the log.
A new one reads the state back from the register and the blobs, and no other machine is told.

On the page: "the keeper is away" at the top, and then its counts again.
The rounds begin again from nothing, and the records in the log go on from where they were.

### Act 6. Compaction is one more client

Say: which spans to merge is a view of the state.
A compactor reads it, makes the merged batch, and asks by a transaction, which the rules judge like any other.

On the page: the spans of each ledger fall to one or two, and stay few as lines come in.

### Act 7. Load

Say: this is a fresh system on this machine alone: 200 ledgers in, 200 computations on 4 instances, 200 ledgers out, and every source ledger appended to ten times a second.
A tick is done when every output ledger has it, and the delay is from when the tick was due.

On the page: 400 ledgers, the appends made, and the least and greatest frontier, a tick or two apart.

## Numbers worth quoting

Each is from this machine or these four machines, in the runs named.
Single runs differ from one another by tens of milliseconds.

From the demo's own runs:

- **The set-up transaction, 17 commands:** made in 44 to 76 ms, over four runs.
- **One line, through three computations on three machines:** about 160 ms from the request to the summary having it.
- **A new keeper:** the same state of 100 entries in 0.7 s.
- **A lost machine's two computations, on two other machines:** caught up 6 lines in 0.27 to 0.28 s.
- **Load, 200 computations at 10 ticks a second for 20 s:** every tick done, median delay 129 to 145 ms, nine in ten under 218 to 238 ms, with the page up, over three runs.

From the scale probes of 2026-11-16 (`ledger-fabric-sweeps-2026-11-10/scale`), on the keeper of then, which held every span and kept every rule over them.
The keeper here holds no spans, and these were not measured again on it:

- **One server, 1,000 small computations, one step of all of them:** 85 ms, where it was 2.1 s before `advance` and `spin` were parted.
- **A keeper that only admits, with no rules installed:** a round of 1,000 appends in 13 ms, and a tick of 2,000 appends in 55 ms.
- **A transaction judged by every rule, with 200 ledgers of 200 spans:** a create in 9 ms, and a merge with its 200 discards in 41 to 50 ms.
- **The keeper as it is, 200 computations at 10 ticks a second for 800 ticks:** median delay by quarter 140, 175, 180 and 251 ms.
- **The same keeper, 500 computations at 10 ticks a second:** 8.9 ticks done a second.
- **The same keeper, 1,000 computations at 10 ticks a second:** 5.7 to 6.7 done a second, and the delay climbs. This is past what it holds.
- **With the rules in a second server, 1,000 computations at 10 ticks a second:** 8.6 to 9.5 done a second, with a delay of 0.7 to 0.9 s. That keeper is saved, and is not the one in the demo.

The sizes of things:

- **The programs, as DDIR:** 16 programs, 1,912 lines.
- **The keeper, follower, instance, stewards and what they share, as Python:** 3,229 lines, in five files.

## What is not shown, and why

- **1,000 computations at 10 ticks a second.** The walk is sized for the page. The numbers above are of an earlier keeper, and were not measured again on this one.
- **Two instances of one computation, on purpose.** Act 3 shows it for a moment, by accident of timing. It is not tested as a way to run.
- **Derived ledgers, and computations over the state.** They work, and `fleet.py` checks them. They are left out to keep the walk short.
- **Clocks.** The model has them, and the round admits appends under them. No demo ticks one.
- **Merges at a high rate.** The keeper judges a pass of many merges at once (`pass.ddp`), and the walk has four ledgers, so few are made.
- **Blob clean-up that can be trusted.** The collector deletes a blob as soon as no span names it, and one never seen named after a wait. That rests on time, and it should not.
- **Who may transact.** The actor of a transaction is claimed, and nothing proves it.
- **Times of more than one coordinate.** The model allows them. Every ledger here has times of one int.
