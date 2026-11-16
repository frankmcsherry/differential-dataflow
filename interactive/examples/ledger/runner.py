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
Views of that copy, in the same server, say what the instance is to run, where each ledger stands, where each
computation begins from, and which spans to read next (see instance.ddp). The instance reads the views, and
works none of that out itself.

The instance is a pipe. A ledger's batch is frames of updates, each at its own time, as a tail of the server gives
them, and the instance does not open them. For each computation it loads three kinds of program into its server:
  a local ledger   before each input that reads a durable ledger: it holds what the instance has read of that ledger
  the computation  the program's text, as the state has it, bound to its local ledgers
  what is owed     after each output that writes a durable ledger: what the computation makes, less what the
                   ledger held when the instance began
It feeds each span's frames to the local ledger as they are, and moves that input to where the ledger stands.
The server needs no tick for this: each input is moved alone. A tail of what is owed then sends frames, and how far
they are complete, and those frames are the next span of the output ledger. That append is all the instance asks
of the keeper, and it asks as a runner of the computation.

It keeps nothing of its own, so it can be stopped at any moment, and another instance can take a computation over.
A computation begins from where its outputs stand. What its input ledgers held before that is fed with its times
moved up to there, and what its output ledgers held is fed, the same way, to what is owed. So what is owed at
the first is what the outputs should hold there, less what they do hold, and after that it is what changes.
A reader whose place no spans lead on from has had what it read merged with what it had not (`stuck`, in instance.ddp).
It takes back all that its local ledger holds, and reads the ledger again whole, from where it stands.

What an instance is told may be behind the state, and is never ahead of it. So after an append of its own it
waits to be told of that append before it goes on, and an append that the keeper refuses ends the instance:
the launcher starts another, which begins from where the ledgers stand. A launcher holds one tail too, of its
machine's instances.

A derived ledger is not appended to. It is the output of its computation, inside this instance's server, and a
computation here that reads it is bound to that output, and has its times. So a durable ledger may be written
from a derived one. Anyone may ask this instance's server what a derived ledger holds: `peek c<id>::out<n>`.
An input that is the state is bound to the instance's copy of the entries. The state has times of its own, which
the server's `tick` moves, so a computation that reads it, or that reads what such a computation writes, is ticked
too: it takes what it reads of durable ledgers as it comes, with no times, and the model lets it write none.

A ledger's times are one int here. A program's outputs are named out0, out1, and so on, in the order it gives them.
What this does not do: a computation whose durable outputs stand at different times.
"""
import json, os, signal, struct, subprocess, sys, time
import keeper as model
import plumbing
from plumbing import number, instant

DIR = "interactive/examples/ledger"
EMPTY = "inject(0,tuple())"
FAR = 1 << 62       # a time after every time a ledger has: an input that is moved here takes no more


def frames_of(found, blobs, cache):
    """The frames of some spans' batches, in order. Each span is (from, to, batch). The frames are not opened."""
    read = []
    for _, _, batch in found:
        if batch[1] == 2:
            name = plumbing.hash_name(batch[2])
            if name not in cache: cache[name] = plumbing.unpack(blobs.get(name))[1]
            read += cache[name]
    return read


class Computation:
    """One computation in the instance's server: its local ledgers, its program, and what is owed to the ledgers it writes.
    It is piloted if no time of the state reaches it. Its inputs are then moved one at a time, to where their ledgers
    stand, and its outputs have the ledgers' times. Otherwise it is ticked (see the note at the top)."""
    def __init__(self, server, ident, text, takes, gives, reads, writes, bound, sources, piloted):
        """`reads` is each input that reads a durable ledger, and `writes` each output that writes one: [(position, ledger)].
        `bound` is each other input, and the output it reads inside the server: {position: "program::output"}.
        `sources` is the computations here whose derived ledgers this reads."""
        self.server, self.ident, self.name = server, ident, f"c{ident}"
        self.reads, self.writes, self.sources, self.piloted = reads, writes, sources, piloted
        assert piloted or not writes, "a ticked computation writes no durable ledger"
        word = " piloted" if piloted else ""
        for position, _ in reads:
            name = f"i{ident}_{position}"
            server.ask(f'load {name}{word} begin\nlet rows = input 0 : {takes[position]};\noutput "rows" = rows;\n{name} end-load')
        sources_of = {**bound, **{position: f"i{ident}_{position}::rows" for position, _ in reads}}
        binds = "".join(f"input{position}={source} " for position, source in sorted(sources_of.items()))
        server.ask(f"load {self.name} {binds}begin\n{text.rstrip()}\n{self.name} end-load")
        self.tails, self.owed = {}, {}      # for each durable output: the tail of what is owed to it, and its frames not yet appended
        for position, _ in writes:
            name = f"w{ident}_{position}"
            server.ask(f'load {name} input0={self.name}::out{position} begin\nlet made = input 0 : {gives[position]};\n'
                       f'let held = input 1 : {gives[position]};\noutput "owed" = made - held;\n{name} end-load')
            self.owed[position] = server.ask(f"tail {name}::owed as corgi")
            self.tails[position] = server.last
            server.tails[server.last] = []
        if not piloted: server.ask("tick")
        self.consumed = {position: 0 for position, _ in reads}     # for each local ledger, the number of the ledger's appends it has read
        self.at = {position: 0 for position, _ in reads}           # for each local ledger, the time its input stands at
        self.reached = {position: 0 for position, _ in writes}     # for each durable output, the time before which what is owed is complete
        self.begun, self.start = False, None                       # whether the ledgers' histories are loaded, and from what time
        self.stood = None                                          # where the durable outputs stand, as far as this has appended
        self.awaited = {}                                          # for each output ledger, the version this instance's last append brings it to

    def floor(self, running):
        """A time before which what this makes will not change again."""
        own = max(self.start or 0, min(self.at.values())) if self.reads else FAR
        return min([own] + [running[writer].floor(running) for writer in self.sources])

    def drop(self):
        for position, _ in self.writes:
            self.server.ask(f"stop {self.tails[position]}")
            self.server.tails.pop(self.tails[position], None)
            self.server.ask(f"drop w{self.ident}_{position}")
        self.server.ask(f"drop {self.name}")
        for position, _ in self.reads: self.server.ask(f"drop i{self.ident}_{position}")
        self.server.ask("tick")


class Local:
    """The views an instance keeps of the entries it is told, in its own server (see instance.ddp).
    They say what to run, where each ledger stands, and which spans to read. What they say is decoded here."""
    def __init__(self, server, instance):
        self.server, self.readers = server, {}      # each reader this has told the views of, and the row it told them
        server.ask(f"load local from {DIR}/instance.ddp input0=state::entries")
        server.many([f"feed local 1 tuple(tuple({instance[0]},{instance[1]})) val=tuple()", "tick"])

    def rows(self, view):
        return [(key, val) for frame in self.server.ask(f"peek local::{view} as corgi") for key, val, _, _ in plumbing.decode(frame)]

    def plan(self):
        """Each computation this instance is asked to run: {computation: (inputs, outputs, takes, gives, text)}."""
        return {number(key[0]): val for key, val in self.rows("plan")}

    def begins(self):
        """Where each computation begins from: the least of the times that the durable ledgers its outputs reach stand at."""
        found = {}
        for key, (_, frontier) in self.rows("resumes"):
            found[number(key[0])] = min(found.get(number(key[0]), instant(frontier)), instant(frontier))
        return found

    def binds(self):
        """Each input that reads a derived ledger, and the output that is the ledger: {(computation, position): (writer, place)}."""
        return {(number(key[0]), key[1]): (number(val[0]), val[1]) for key, val in self.rows("binds")}

    def standing(self):
        """Where each durable ledger stands: {ledger: (version, frontier)}, with a frontier of one time as that time."""
        return {number(key[0]): (val[0], instant(val[1])) for key, val in self.rows("standing")}

    def spans(self, reader, ledger, read, need=None):
        """The spans that lead a reader of a ledger on from its append `read` to its last, for one that reads from `need`:
        [(from, to, batch)] in order, or None if no spans lead on from there. With no need, any span will do.
        A reader is (computation, side, position), and the views are told where it has read to."""
        frontier = "list()" if need is None else f"list(list({need}))"
        row = f"tuple(tuple(tuple(0,{reader[0]}),{reader[1]},{reader[2]})) val=tuple(tuple(0,{ledger}),{read},{frontier})"
        if self.readers.get(reader) != row:
            feeds = [f"feed local 2 {self.readers[reader]} diff=-1"] if reader in self.readers else []
            self.readers[reader] = row
            self.server.many(feeds + [f"feed local 2 {row}", "tick"])
        key = (((0, reader[0]), reader[1], reader[2]),)
        if any(found == key for found, _ in self.rows("stuck")): return None
        return sorted(val for found, val in self.rows("fetch") if found == key)

    def forget(self, computation):
        """Take back the readers of a computation that is no longer run here."""
        gone = [reader for reader in self.readers if reader[0] == computation]
        if gone: self.server.many([f"feed local 2 {self.readers.pop(reader)} diff=-1" for reader in gone] + ["tick"])


def port(instance): return 21000 + 2 * (instance[1] % 1000)


def run(instance, url, follower):
    keeper, blobs, cache = plumbing.Client(url), plumbing.Blobs(), {}
    server = plumbing.Server(port(instance))
    # What the instance is told is rows of `bundle`, each an entry under this instance's id. `entries` is the entries alone.
    server.ask("load state begin\n" + model.declarations() + 'let told = input 0 : ((Id), (Entry));\noutput "told" = told;\n'
               'output "entries" = told | map((_, (entry)) -> ((entry), ()));\nstate end-load')
    local = Local(server, instance)
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
            # What the views say of the entries, all of one state.
            plan, binds, standing, begins = local.plan(), local.binds(), local.standing(), local.begins()
            # Readers are dropped before what they read, and loaded after it.
            for ident in [ident for ident in reversed(order) if ident not in plan]:
                running.pop(ident).drop(); order.remove(ident); local.forget(ident)
            for _ in plan:
                for ident, (inputs, outputs, takes, gives, text) in plan.items():
                    if ident in running: continue
                    bound, sources, piloted = {}, [], True
                    for position, source in enumerate(inputs):
                        if source[1] == 0: bound[position], piloted = "state::entries", False
                        elif (ident, position) in binds:
                            writer, place = binds[(ident, position)]
                            bound[position] = f"c{writer}::out{place}" if writer in running else None
                            if writer in running: sources.append(writer); piloted = piloted and running[writer].piloted
                    if None in bound.values(): continue
                    reads = [(position, number(source[2])) for position, source in enumerate(inputs) if position not in bound]
                    writes = [(position, number(output[2])) for position, output in enumerate(outputs) if output[1] == 1 and number(output[2]) in standing]
                    shapes = lambda found: [plumbing.ints_text(shape) for shape in found]
                    running[ident] = Computation(server, ident, plumbing.ints_text(text), shapes(takes), shapes(gives), reads, writes, bound, sources, piloted)
                    order.append(ident)
            for ident in order: step(keeper, blobs, cache, local, running, ident, standing, begins)
    finally:
        server.close()


def step(keeper, blobs, cache, local, running, ident, standing, begins):
    """Take one computation as far as its inputs allow, and append what it made."""
    this = running[ident]
    server = this.server

    def read(position, ledger, need, afresh):
        """The requests that feed a local ledger what its ledger has that it has not read. `afresh` says to move the times
        of what is read up to `need`, as is done when the local ledger has nothing of the ledger yet.
        If no spans lead on from what was read, it was merged with what was not: all that the local ledger holds
        is taken back, and the ledger is read again whole, from `need`."""
        name, reader = f"i{ident}_{position}", (ident, 0, position)
        requests, found = [], local.spans(reader, ledger, this.consumed[position], need)
        if found is None:
            requests, found, afresh = [f"feed {name} 0 from {name}::rows diff=-1"], local.spans(reader, ledger, 0, need), True
            if found is None: raise SystemExit(f"instance: computation {ident} cannot read input {position} from {need}")
            print(f"instance: computation {ident} read input {position} whole again, after a merge", flush=True)
        for frame in frames_of(found, blobs, cache):
            # A piloted input takes each update at its time. A ticked one takes them with no times, at the tick.
            if not this.piloted: frame = plumbing.untimed(frame)
            elif afresh and need is not None: frame = plumbing.raised(frame, need)
            requests.append((f"feed {name} 0 frame {len(frame)}", frame))
        if found: this.consumed[position] = found[-1][1]
        return requests

    if not this.piloted:
        requests = [request for position, ledger in this.reads for request in read(position, ledger, None, False)]
        if requests: server.many(requests + ["tick"])
        return

    # What this was told may be from before its own last append. It waits until it is told of that append.
    if any(standing[ledger][0] < version for ledger, version in this.awaited.items() if ledger in standing): return
    this.awaited, requests = {}, []
    if not this.begun:
        # A start. The computation begins from where its outputs stand, or, if no durable ledger follows from it, from the first.
        this.start = this.stood = begins.get(ident)
        if any(standing[ledger][1] != this.start for _, ledger in this.writes):
            raise SystemExit(f"instance: computation {ident} writes ledgers that stand at different times, which this does not do")
        if this.start is not None and any(running[writer].floor(running) < this.start for writer in this.sources):
            raise SystemExit(f"instance: computation {ident} reads a derived ledger that is behind where its outputs stand, which this does not do")
        # What the output ledgers hold is fed to what is owed, as of the start, and that input is then done with.
        for position, ledger in this.writes:
            held = local.spans((ident, 1, position), ledger, 0, this.start)
            if held is None: raise SystemExit(f"instance: computation {ident} cannot read what output {position} holds")
            requests += [(f"feed w{ident}_{position} 1 frame {len(frame)}", frame)
                         for frame in (plumbing.raised(frame, this.start) for frame in frames_of(held, blobs, cache))]
            requests.append(f"advance w{ident}_{position} 1 {FAR}")
    for position, ledger in this.reads:
        requests += read(position, ledger, this.at[position] if this.begun else this.start, not this.begun)
        # The local ledger is moved to where the ledger stands. Nothing else moves with it.
        if standing[ledger][1] > this.at[position]:
            requests.append(f"advance i{ident}_{position} 0 {standing[ledger][1]}")
            this.at[position] = standing[ledger][1]
    this.begun = True
    if requests: server.many(requests)
    if not this.writes: return

    # Each tail of what is owed has sent its frames, and how far they are complete. Those frames are the next span.
    for position, _ in this.writes:
        sent, server.tails[this.tails[position]] = server.tails[this.tails[position]], []
        for kind, found in sent:
            if kind == "data": this.owed[position].append(found)
            else: this.reached[position] = struct.unpack("<Q", found)[0]
    reach = min(this.reached.values())
    if reach <= this.stood: return
    commands = []
    for position, ledger in this.writes:
        version, stands = standing[ledger]
        # The outputs stand elsewhere than this left them, so another has written them, and what this holds is not what follows.
        if stands != this.stood: raise SystemExit(f"instance: computation {ident} must start over")
        batch = EMPTY
        if this.owed[position]:
            name = blobs.put(plumbing.pack({}, this.owed[position]))
            batch = f"inject(2,list({','.join(map(str, plumbing.hash_ints(name)))}))"
        commands.append(f"inject(1,tuple(tuple(0,{ledger}),{version},list(list({this.stood})),list(list({reach})),{batch}))")
    while True:
        # While the keeper is away, the instance waits, and asks the same again when it is back.
        # If the keeper made the append before it went, asking again is refused, and the instance starts over.
        try: refused = keeper.transact(f"inject(1,tuple(0,{ident}))", commands); break
        except OSError: time.sleep(0.2)
    # An append that is refused was made from a state that is not the keeper's: what it held is lost.
    if refused: raise SystemExit(f"instance: computation {ident} was refused ({', '.join(refused)}), and must start over")
    this.awaited = {ledger: standing[ledger][0] + 1 for _, ledger in this.writes}
    this.stood, this.owed = reach, {position: [] for position in this.owed}


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
