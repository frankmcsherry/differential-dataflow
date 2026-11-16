#!/usr/bin/env python3
"""An instance: it runs the computations the state asks it to, and nothing else tells it what to do.

  python3 interactive/examples/ledger/runner.py run <instance> <keeper url> <follower>         one instance
  python3 interactive/examples/ledger/runner.py launch <keeper url> <follower> [machine]      start and stop instances as the state says
  python3 interactive/examples/ledger/runner.py ask <instance> <query>                         ask an instance's server on this machine

An instance is named by two numbers, written `machine.number`. The first says which machine it runs on:
a launcher that is given a machine starts only that machine's instances. Which machine has which number
is not in the state; whoever starts the launchers decides it.

An instance reads the state from a follower, and writes to the keeper. It holds one tail, of the entries it
needs (see bundle.ddp), and does nothing until they change: it asks no one whether they have. What the tail
sends is frames, which the instance feeds to its own server unopened, and so has a copy of those entries there.
It then reads that copy, and decodes it, to see what it is asked to do. For each computation
it is to run, it loads the program's text into a DDIR server of its own, reads the input ledgers' spans and their
blobs, and feeds them in order of time. What comes out is written as blobs and appended to the output ledgers,
as a runner of the computation: this is all it asks of the keeper.
It keeps nothing of its own: it starts from where its output ledgers stand, so it can be stopped at any moment,
and another instance can take the computation over. A launcher holds one tail too, of its machine's instances.

What an instance is told may be behind the state, and is never ahead of it. So after an append of its own it
waits to be told of that append before it goes on, and an append that the keeper refuses ends the instance:
the launcher starts another, which begins from where the ledgers stand.

A derived ledger is not appended to. It is the output of its computation, inside this instance's server, and a
computation here that reads it is bound to that output. An input that is the state is bound to the entries, which
the instance keeps a copy of. Anyone may ask this instance's server what a derived ledger holds: `peek c<id>::out<n>`.
What this does not do: a computation that writes a durable ledger must read only durable ledgers.

A ledger's times are one int here. A batch is a blob of JSON: {"updates": [[key, val, time, diff], ..]}.
A program's outputs are named out0, out1, and so on, in the order the program gives them.
"""
import json, os, signal, subprocess, sys, time
import keeper as model
import plumbing
from plumbing import SPAN, PROGRAM, COMPUTATION, RUN, number, instant

EMPTY = "inject(0,tuple())"


def split(line):
    """A row of `peek`, as ((key, val), count)."""
    body, _, diff = line.partition(" diff=")
    key, _, val = body.partition(" val=")
    return (key, val or "tuple()"), int(diff) if diff else 1


class Computation:
    """One computation, loaded into the instance's server."""
    def __init__(self, server, ident, text, inputs, outputs, bound):
        """`bound` is, for each input read inside the server, the output it reads: {position: "program::output"}."""
        self.server, self.ident, self.name, self.inputs, self.outputs, self.bound = server, ident, f"c{ident}", inputs, outputs, bound
        binds = "".join(f"input{position}={source} " for position, source in bound.items())
        server.ask(f"load {self.name} {binds}begin\n{text.rstrip()}\n{self.name} end-load")
        server.ask("tick")
        self.fed = None                     # the time below which the inputs have been fed
        self.consumed = {}                  # for each input, the number of its appends that have been read
        self.pending = {}                   # for each input, the updates that have been read and not yet fed
        self.mine = {}                      # for each input, the rows that have been fed, each with its count
        self.awaited = {}                   # for each output ledger, the version this instance's last append brings it to
        self.seen = [{} for _ in outputs]   # each output's rows, as of the last time fed

    def feed(self, updates):
        """Feed some updates, and return what each output changed by."""
        self.server.many([f"feed {self.name} {position} {key} val={val} diff={diff}" for position, key, val, diff in updates] + ["tick"])
        for position, key, val, diff in updates:
            rows = self.mine.setdefault(position, {})
            rows[(key, val)] = rows.get((key, val), 0) + diff
            if not rows[(key, val)]: del rows[(key, val)]
        changes = []
        for position, seen in enumerate(self.seen):
            now = dict(split(line) for line in self.server.ask(f"peek {self.name}::out{position}"))
            changes.append([(row, now.get(row, 0) - seen.get(row, 0)) for row in set(now) | set(seen) if now.get(row, 0) != seen.get(row, 0)])
            self.seen[position] = now
        return changes

    def take(self, wanted):
        """The pending updates whose time is wanted, as updates to feed. They are no longer pending."""
        taken = []
        for position, updates in self.pending.items():
            taken += [(position, key, val, diff) for key, val, at, diff in updates if wanted(at)]
            self.pending[position] = [update for update in updates if not wanted(update[2])]
        return taken

    def read(self, position, spans, blobs, cache, need=None):
        """Read what one input ledger has that was not yet read, and make it pending.
        The spans that follow the appends that were read are read. If none follow them, the appends that were read
        have been merged with ones that were not, and the two cannot be told apart. The ledger is then read whole,
        and what was fed of it is taken away. What is left of the times up to `need` is pending at `need`:
        a span held from `need` does not say which of those times each of its updates had, and all of them
        but the last, `need` itself, were fed. With no need, times do not matter, and all of it is as one."""
        found = history(spans, blobs, cache, self.consumed.get(position, 0), need)
        if found is not None:
            self.pending.setdefault(position, []).extend(found[0])
        else:
            found = history(spans, blobs, cache, 0, need)
            if found is None: raise SystemExit(f"instance: computation {self.ident} cannot read input {position} from {need}")
            rest, later = {row: -count for row, count in self.mine.get(position, {}).items()}, []
            for key, val, at, diff in found[0]:
                if need is None or at <= need: rest[(key, val)] = rest.get((key, val), 0) + diff
                else: later.append([key, val, at, diff])
            self.pending[position] = [[key, val, need, diff] for (key, val), diff in rest.items() if diff] + later
            print(f"instance: computation {self.ident} read input {position} whole again, after a merge", flush=True)
        self.consumed[position] = found[1]

    def drop(self):
        self.server.ask(f"drop {self.name}")
        self.server.ask("tick")


def path(spans, start, end, need=None):
    """Spans that cover a ledger's appends from number `start` up to number `end`, one after another, or None.
    With a need, only the spans a reader from that time can use: those held from the need or before, and
    those that are not compacted, whose since is their own lower (see `forgotten` in system.ddp)."""
    by_start = {}
    for key, val in spans:
        since, lower = instant(key[3]), instant(val[0])
        if key[1] < key[2] and (need is None or since <= max(need, lower)): by_start.setdefault(key[1], []).append((key, val))
    # The numbers from which `end` can be reached. A span leads to a greater number, so the greatest are settled first.
    reach = {end}
    for at in sorted(by_start, reverse=True):
        if any(key[2] in reach for key, _ in by_start[at]): reach.add(at)
    if start not in reach: return None
    found, at = [], start
    while at != end:
        found.append(max((span for span in by_start[at] if span[0][2] in reach), key=lambda span: span[0][2]))
        at = found[-1][0][2]
    return found


def history(spans, blobs, cache, start=0, need=None):
    """A ledger's updates from append `start` on, and the append they reach: ([(key, val, time, diff)], number).
    None if no spans lead from `start` to the last append (see `path`)."""
    end = max([key[2] for key, _ in spans], default=start)
    found = path(spans, start, end, need)
    if found is None: return None
    updates = []
    for _, (_, _, batch) in found:
        if batch[1] == 2:
            name = plumbing.hash_name(batch[2])
            if name not in cache: cache[name] = json.loads(blobs.get(name))["updates"]
            updates += cache[name]
    return updates, end


def port(instance): return 21000 + 2 * (instance[1] % 1000)


def run(instance, url, follower):
    keeper, blobs, cache = plumbing.Client(url), plumbing.Blobs(), {}
    server = plumbing.Server(port(instance))
    # What the instance is told is rows of `bundle`, each an entry under this instance's id. `entries` is the entries alone.
    server.ask("load state begin\n" + model.declarations() + 'let told = input 0 : ((Id), (Entry));\noutput "told" = told;\n'
               'output "entries" = told | map((_, (entry)) -> ((entry), ()));\nstate end-load')
    told = plumbing.Tail(follower, "bundle", instance)
    running, order = {}, []
    try:
        while True:
            # Nothing is done until the tail has sent something. Its frames are fed to the server as they are,
            # less their times, so the instance's copy of the entries follows what it is told.
            feeds = []
            for afresh, frames in told.take():
                if afresh:
                    # The follower was asked again, and has sent every row. What was told before is taken back out.
                    if feeds: server.many(feeds + ["tick"])
                    feeds = ["feed state 0 from state::told diff=-1"]
                feeds += [(f"feed state 0 frame {len(frame)}", frame) for frame in map(plumbing.untimed, frames)]
            server.many(feeds + ["tick"])
            # The entries this instance is told, all of one state. They are decoded here, for the Python below to read.
            entries = plumbing.kinds(server.ask("peek state::entries as corgi"))
            standing = plumbing.standing_of(entries)
            # The plan: each computation this instance is asked to run.
            asked = {number(computation) for (runner, computation), _ in entries.get(RUN, []) if runner == instance}
            plan = {number(key[0]): val for key, val in entries.get(COMPUTATION, []) if number(key[0]) in asked}
            # Readers are dropped before what they read, and loaded after it.
            for ident in [ident for ident in reversed(order) if ident not in plan]:
                running.pop(ident).drop(); order.remove(ident)
            writers = {number(output[2]): (ident, position) for ident, (_, _, outputs) in plan.items()
                       for position, output in enumerate(outputs) if output[1] == 1}
            for _ in plan:
                for ident, (program, inputs, outputs) in plan.items():
                    if ident in running: continue
                    bound = {}
                    for position, source in enumerate(inputs):
                        if source[1] == 0: bound[position] = "state::entries"
                        elif number(source[2]) not in standing:
                            writer = writers.get(number(source[2]))
                            bound[position] = f"c{writer[0]}::out{writer[1]}" if writer and writer[0] in running else None
                    if None in bound.values(): continue
                    text = next(val[2] for key, val in entries.get(PROGRAM, []) if key[0] == program)
                    running[ident] = Computation(server, ident, plumbing.ints_text(text), inputs, outputs, bound)
                    order.append(ident)
            for ident in order: step(keeper, blobs, cache, running, ident, entries, standing)
    finally:
        server.close()


def step(keeper, blobs, cache, running, ident, entries, standing):
    """Take one computation as far as its inputs allow, and append what it made."""
    this = running[ident]
    reads = [(position, number(source[2])) for position, source in enumerate(this.inputs) if position not in this.bound]
    writes = [(position, number(output[2])) for position, output in enumerate(this.outputs) if output[1] == 1]
    writes = [(position, ledger) for position, ledger in writes if ledger in standing]   # the durable ones
    if not reads: return                                                        # all it reads is inside the server
    spans = lambda ledger: [(key, val) for key, val in entries.get(SPAN, []) if number(key[0]) == ledger]
    if not writes:
        # Nothing it writes is recorded, so there is nowhere to start from, and no time to keep to:
        # its durable inputs are fed as they come.
        for position, ledger in reads: this.read(position, spans(ledger), blobs, cache)
        updates = this.take(lambda at: True)
        if updates: this.feed(updates)
        return
    if this.bound: return                                                       # see the note at the top
    # What this was told may be from before its own last append. It waits until it is told of that append.
    if any(standing[ledger][0] < version for ledger, version in this.awaited.items() if ledger in standing): return
    this.awaited = {}
    start = min(standing[ledger][1] for _, ledger in writes)                    # where the outputs stand
    until = min(standing[ledger][1] for _, ledger in reads)                     # where the inputs stand
    if this.fed is not None and this.fed > start:
        # The outputs stand before what was fed, so what was made in between was not appended, and is lost.
        # The instance stops: the launcher starts another, which begins from where the outputs stand.
        raise SystemExit(f"instance: computation {ident} must start over")
    if until <= start: return
    for position, ledger in reads: this.read(position, spans(ledger), blobs, cache, start if this.fed is None else this.fed)
    made = [[] for _ in this.outputs]
    if this.fed is None:
        # A start. Everything up to where the outputs stand is fed at once: this is what the outputs should hold there.
        # What they do hold is read back from the output ledgers, and the difference is made at that time.
        # A span compacted to that time cannot say which of its updates were before it, and this does not need to know.
        this.feed(this.take(lambda at: at <= start))
        for position, ledger in writes:
            held = {}
            for key, val, _, diff in history(spans(ledger), blobs, cache)[0]: held[(key, val)] = held.get((key, val), 0) + diff
            should = this.seen[position]
            made[position] = [[row[0], row[1], start, should.get(row, 0) - held.get(row, 0)]
                              for row in set(should) | set(held) if should.get(row, 0) != held.get(row, 0)]
    # The times before where the inputs stand are fed in order. What is pending at later times stays so.
    # What comes out at a time before where the outputs stand is there already, written by another instance.
    for at in sorted({at for updates in this.pending.values() for _, _, at, _ in updates if at < until}):
        changes = this.feed(this.take(lambda moment: moment == at))
        for position, change in enumerate(changes):
            if at >= start: made[position] += [[key, val, at, diff] for (key, val), diff in change]
    this.fed = until
    commands = []
    for position, ledger in writes:
        batch = EMPTY
        if made[position]:
            name = blobs.put(json.dumps({"updates": made[position]}).encode())
            batch = f"inject(2,list({','.join(map(str, plumbing.hash_ints(name)))}))"
        version = standing[ledger][0]
        commands.append(f"inject(1,tuple(tuple(0,{ledger}),{version},list(list({start})),list(list({until})),{batch}))")
    while True:
        # While the keeper is away, the instance waits, and asks the same again when it is back.
        # If the keeper made the append before it went, asking again is refused, and the instance starts over.
        try: refused = keeper.transact(f"inject(1,tuple(0,{ident}))", commands); break
        except OSError: time.sleep(0.2)
    # An append that is refused was made from a state that is not the keeper's: what it held is lost.
    if refused: raise SystemExit(f"instance: computation {ident} was refused ({', '.join(refused)}), and must start over")
    this.awaited = {ledger: standing[ledger][0] + 1 for _, ledger in writes}


def launch(url, follower, machine=None):
    """Start an instance for each one the state asks for, and stop each one it no longer asks for.
    With a machine, this is that machine's launcher, and the other machines' instances are not its to start."""
    roster, live, wanted = plumbing.Rows(plumbing.Tail(follower, "roster", machine)), {}, set()
    try:
        while True:
            # The wait ends when the roster changes, or after a moment, to see whether an instance here has ended.
            # The launcher has no server, so it decodes the roster's frames: each row's value is an instance's id.
            rows = roster.wait(timeout=0.1)
            if rows is not None: wanted = {val[0] for val in rows}
            for instance in [i for i in live if i not in wanted or live[i].poll() is not None]:
                live.pop(instance).terminate()
            for instance in wanted - set(live):
                live[instance] = subprocess.Popen([sys.executable, os.path.abspath(__file__), "run", "%d.%d" % instance, url, follower])
                print(f"launcher: started instance {instance[0]}.{instance[1]}", flush=True)
    finally:
        for process in live.values(): process.terminate()


if __name__ == "__main__":
    for sign in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sign, lambda *_: sys.exit(0))            # so that being stopped also stops the server
    instance = lambda text: tuple(map(int, text.split(".")))
    if sys.argv[1] == "run": run(instance(sys.argv[2]), sys.argv[3], sys.argv[4])
    elif sys.argv[1] == "ask":
        session = plumbing.Server(port(instance(sys.argv[2])), start=False)
        print(json.dumps(session.ask(sys.argv[3])))
    else: launch(sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 4 else None)
