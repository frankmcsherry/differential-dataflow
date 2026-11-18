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
The server needs no tick for this: each input is moved alone (`advance`), and then the server is asked to run until
its outputs have caught up with their inputs (`spin`). A tail of what is owed then sends frames, and how far
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
import concurrent.futures, json, os, signal, struct, subprocess, sys, time
import keeper as model
import plumbing
from plumbing import number, instant

DIR = "interactive/examples/ledger"
EMPTY = "inject(0,tuple())"
FAR = 1 << 62       # a time after every time a ledger has: an input that is moved here takes no more
KEPT = 4096         # how many blobs an instance keeps of those it has read
OVERLAP = os.environ.get("RUNNER_OVERLAP", "1") == "1"      # whether an instance feeds its copy while it writes and asks
WAITS = concurrent.futures.ThreadPoolExecutor(1)            # the one thread that waits on the blob store or the keeper
STORE = {"puts": 0, "gets": 0, "inline": 0, "blob": 0}      # what this instance asked of the blob store, and how its batches were kept


def frames_of(found, blobs, cache):
    """The frames of some spans' batches, in order. Each span is (from, to, batch). The frames are not opened.
    A batch names parts of blobs (or, in the short forms, a blob or a part of one). A part that is not in `cache` is
    read from `blobs` by range, held to its own hash, and kept there: what is read is no more than what is named."""
    read = []
    for _, _, batch in found: read += plumbing.frames_of_batch(blobs, batch, cache)
    return read


def wanted(found, cache):
    """What the batches of some spans need read that `cache` does not have: parts, as (blob, offset, length, digest), and
    blobs whole for a name of the short forms. Each as the key it is kept under in `cache`, and how to read it."""
    need = {}
    for spans in found:
        for _, _, batch in spans or []:
            if batch[1] != 2: continue
            pieces = plumbing.pieces_of(batch[2])
            for piece in pieces:
                if piece[:2] not in cache: need[piece[:2]] = piece
            if not pieces and plumbing.blob_of(batch[2]) not in cache: need[plumbing.blob_of(batch[2])] = None
    return need


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
        self.seen = {}                              # each view's frames as last read, and their rows
        # Each ledger's row, and where each durable ledger stands, as a follower works them out (index.ddp, stand.ddp).
        server.ask(f"load index from {DIR}/index.ddp input0=state::entries")
        server.ask(f"load stand from {DIR}/stand.ddp input0=index::own")
        server.ask(f"load local from {DIR}/instance.ddp input0=state::entries input3=index::own input4=stand::standing")
        server.many([f"feed local 1 tuple(tuple({instance[0]},{instance[1]})) val=tuple()", "tick"])

    def rows(self, view):
        """The rows of a view: of `local`, unless another program is named. They are decoded only if the view's frames
        are not the ones it gave last."""
        frames = self.server.ask(f"peek {view if '::' in view else 'local::' + view} as corgi")
        if self.seen.get(view, (None,))[0] != frames:
            self.seen[view] = (frames, [(key, val) for frame in frames for key, val, _, _ in plumbing.decode(frame)])
        return self.seen[view][1]

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
        return {number(key[0]): (val[0], instant(val[1])) for key, val in self.rows("stand::standing")}

    def spans(self, asks):
        """For each (reader, ledger, read, need): the spans that lead the reader of the ledger on from its append `read`
        to its last, for one that reads from `need`: [(from, to, batch)] in order, or None if no spans lead on from there.
        With no need, any span will do. A reader is (computation, side, position), and the views are told where it has read to."""
        feeds = []
        for reader, ledger, read, need in asks:
            frontier = "list()" if need is None else f"list(list({need}))"
            row = f"tuple(tuple(tuple(0,{reader[0]}),{reader[1]},{reader[2]})) val=tuple(tuple(0,{ledger}),{read},{frontier})"
            if self.readers.get(reader) != row:
                if reader in self.readers: feeds.append(f"feed local 2 {self.readers[reader]} diff=-1")
                self.readers[reader] = row
                feeds.append(f"feed local 2 {row}")
        if feeds: self.server.many(feeds + ["tick"])
        stuck, fetch = {key for key, _ in self.rows("stuck")}, {}
        for key, val in self.rows("fetch"): fetch.setdefault(key, []).append(val)
        key = lambda reader: (((0, reader[0]), reader[1], reader[2]),)
        found = [None if key(reader) in stuck else sorted(fetch.get(key(reader), [])) for reader, _, _, _ in asks]
        # A reader of side 1 reads what an output ledger holds, once, when its computation begins. It is taken back
        # here: left in place, the views would go on saying what it would read next, for every span the ledger gains.
        once = [reader for reader, _, _, _ in asks if reader[1] == 1 and reader in self.readers]
        if once: self.server.many([f"feed local 2 {self.readers.pop(reader)} diff=-1" for reader in once] + ["tick"])
        return found

    def forget(self, computation):
        """Take back the readers of a computation that is no longer run here."""
        gone = [reader for reader in self.readers if reader[0] == computation]
        if gone: self.server.many([f"feed local 2 {self.readers.pop(reader)} diff=-1" for reader in gone] + ["tick"])


def port(instance): return 21000 + 2 * (instance[1] % 1000)


# With RUNNER_TIMES=1 an instance says, every ten seconds, where its time went: waiting to be told, bringing its own
# copy up to date and reading its views, and each kind of thing its computations ask for together.
TIMES, SPENT = os.environ.get("RUNNER_TIMES") == "1", {"said": time.perf_counter(), "passes": 0}
def spent(what, begin):
    if TIMES: SPENT[what] = SPENT.get(what, 0.0) + time.perf_counter() - begin
    return time.perf_counter()


def run(instance, url, follower):
    keeper, blobs, cache = plumbing.Client(url), plumbing.Blobs(), {}
    server = plumbing.Server(port(instance))
    # What the instance is told is rows of `bundle`, each an entry under this instance's id. `entries` is the entries alone.
    server.ask("load state begin\n" + model.declarations() + 'let told = input 0 : ((Id), (Entry));\noutput "told" = told;\n'
               'output "entries" = told | map((_, (entry)) -> ((entry), ()));\nstate end-load')
    local = Local(server, instance)
    told = plumbing.Tail(follower, "bundle", instance)
    running, order = {}, []
    def feed(sent):
        # What the tail sent is fed to the server as it is, less its times, so the instance's copy of the entries
        # follows what it is told.
        feeds = []
        for afresh, frames in sent:
            if afresh:
                # The follower was asked again, and has sent every row. What was told before is taken back out.
                if feeds: server.many(feeds + ["tick"])
                feeds = ["feed state 0 from state::told diff=-1"]
            feeds += [(f"feed state 0 frame {len(frame)}", frame) for frame in map(plumbing.untimed, frames)]
        if feeds: server.many(feeds + ["tick"])
    # A PASS, IN ORDER: wait to be told; feed the copy and read the views; find the spans to read and read their
    # blobs; feed the computations and run them; write what they made, as one blob; ask the keeper to append.
    # The blob is written before the append is asked, and a computation goes no further until it is told of its
    # own append. While the blob is written and while the keeper is asked the instance has nothing to compute, so
    # it takes what the tail sends then and feeds its copy (`meanwhile`), and does not wait for that again after.
    early = [False]
    def meanwhile(asked):
        begin, fed = time.perf_counter(), 0.0
        while not asked.done():
            sent = told.take(timeout=0.004)
            if sent:
                at = time.perf_counter()
                feed(sent)
                early[0], fed = True, fed + time.perf_counter() - at
        if TIMES: SPENT["(its copy fed while it wrote or asked)"] = SPENT.get("(its copy fed while it wrote or asked)", 0.0) + fed
    try:
        while True:
            # Nothing is done until the tail has sent something, unless it sent something during the last pass.
            begin = time.perf_counter()
            sent = told.take(timeout=0) if early[0] else told.take()
            early[0] = False
            begin = spent("waiting to be told", begin)
            feed(sent)
            # What the views say of the entries, all of one state.
            plan, binds, standing, begins = local.plan(), local.binds(), local.standing(), local.begins()
            spent("its copy and its views", begin)
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
            together(server, keeper, blobs, cache, local, [step(running, ident, standing, begins, cache) for ident in order],
                     meanwhile if OVERLAP else None)
            SPENT["passes"] += 1
            if os.environ.get("RUNNER_STORE") == "1" and time.perf_counter() - STORE.get("said", 0) >= 10:
                # What this instance has asked of the blob store, and how its batches were kept, so far.
                print(f"instance {instance} store: puts {STORE['puts']} gets {STORE['gets']} inline {STORE['inline']} blob {STORE['blob']} "
                      f"bytes put {plumbing.STORED['put']} got {plumbing.STORED['got']}", flush=True)
                STORE["said"] = time.perf_counter()
            if TIMES and time.perf_counter() - SPENT["said"] >= 10:
                print(f"instance {instance}: {SPENT.pop('passes')} passes in {time.perf_counter() - SPENT.pop('said'):.0f} s; ms: "
                      + "; ".join(f"{what} {1000 * seconds:.0f}" for what, seconds in SPENT.items()), flush=True)
                SPENT.clear(); SPENT.update(said=time.perf_counter(), passes=0)
    finally:
        server.close()


def step(running, ident, standing, begins, cache):
    """One computation's part of a pass: take it as far as its inputs allow, and append what it made.
    This is a generator, so that what every computation here wants of the views, the server, the blob store and the
    keeper is asked for together (see `together`). It yields what it wants, and is sent the answer:
      ("spans", [(reader, ledger, read, need)])   the spans that lead each reader on (see `Local.spans`), with their blobs in `cache`
      ("feed", requests)                          requests to the server: it has made them, and caught up, when this goes on
      ("put", [frames])                           frames to keep, a group for each batch: the name of each batch, as ints.
                                                  The groups of every computation here go in one blob, each a part of it
      ("append", computation, appends)            a transaction of appends for the keeper, as this computation's runner:
                                                  each (ledger, number, lower, upper, batch). What refused it, or none"""
    this = running[ident]

    def read(need, afresh):
        """The requests that feed each local ledger what its ledger has that it has not read. `afresh` says to move the times
        of what is read up to the need, as is done when the local ledger has nothing of the ledger yet.
        If no spans lead on from what was read, it was merged with what was not: all that the local ledger holds
        is taken back, and the ledger is read again whole, from the need."""
        asks = [((ident, 0, position), ledger, this.consumed[position], need(position)) for position, ledger in this.reads]
        found = yield ("spans", asks)
        stuck = [at for at, spans in enumerate(found) if spans is None]
        if stuck:
            again = yield ("spans", [(asks[at][0], asks[at][1], 0, asks[at][3]) for at in stuck])
            for at, spans in zip(stuck, again):
                position = asks[at][0][2]
                if spans is None: raise SystemExit(f"instance: computation {ident} cannot read input {position} from {asks[at][3]}")
                print(f"instance: computation {ident} read input {position} whole again, after a merge", flush=True)
                found[at] = spans
        requests = []
        for at, ((position, _), spans) in enumerate(zip(this.reads, found)):
            name = f"i{ident}_{position}"
            if at in stuck: requests.append(f"feed {name} 0 from {name}::rows diff=-1")
            for frame in frames_of(spans, None, cache):
                # A piloted input takes each update at its time. A ticked one takes them with no times, at the tick.
                if not this.piloted: frame = plumbing.untimed(frame)
                elif (afresh or at in stuck) and need(position) is not None: frame = plumbing.raised(frame, need(position))
                requests.append((f"feed {name} 0 frame {len(frame)}", frame))
            if spans: this.consumed[position] = spans[-1][1]
        return requests

    if not this.piloted:
        requests = yield from read(lambda position: None, False)
        if requests: yield ("feed", requests + ["tick"])
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
        held = yield ("spans", [((ident, 1, position), ledger, 0, this.start) for position, ledger in this.writes])
        for (position, _), spans in zip(this.writes, held):
            if spans is None: raise SystemExit(f"instance: computation {ident} cannot read what output {position} holds")
            requests += [(f"feed w{ident}_{position} 1 frame {len(frame)}", frame)
                         for frame in (plumbing.raised(frame, this.start) for frame in frames_of(spans, None, cache))]
            requests.append(f"advance w{ident}_{position} 1 {FAR}")
    begun = this.begun
    requests += yield from read(lambda position: this.at[position] if begun else this.start, not begun)
    for position, ledger in this.reads:
        # The local ledger is moved to where the ledger stands. Nothing else moves with it.
        if standing[ledger][1] > this.at[position]:
            requests.append(f"advance i{ident}_{position} 0 {standing[ledger][1]}")
            this.at[position] = standing[ledger][1]
    this.begun = True
    # Every computation here waits at this point, with or without requests of its own: what one is fed may change
    # what another owes, when the other reads a derived ledger that the one writes.
    yield ("feed", requests)
    if not this.writes: return

    # Each tail of what is owed has sent its frames, and how far they are complete. Those frames are the next span.
    for position, _ in this.writes:
        sent, this.server.tails[this.tails[position]] = this.server.tails[this.tails[position]], []
        for kind, found in sent:
            if kind == "data": this.owed[position].append(found)
            else: this.reached[position] = struct.unpack("<Q", found)[0]
    reach = min(this.reached.values())
    if reach <= this.stood: return
    full = [position for position, _ in this.writes if this.owed[position]]
    names = dict(zip(full, (yield ("put", [this.owed[position] for position in full])) if full else []))
    appends = []
    for position, ledger in this.writes:
        version, stands = standing[ledger]
        # The outputs stand elsewhere than this left them, so another has written them, and what this holds is not what follows.
        if stands != this.stood: raise SystemExit(f"instance: computation {ident} must start over")
        batch = names.get(position, plumbing.EMPTY)
        appends.append(((0, ledger), version, [[this.stood]], [[reach]], batch))
    refused = yield ("append", ident, appends)
    # An append that is refused was made from a state that is not the keeper's: what it held is lost.
    if refused: raise SystemExit(f"instance: computation {ident} was refused ({refused}), and must start over")
    this.awaited = {ledger: standing[ledger][0] + 1 for _, ledger in this.writes}
    this.stood, this.owed = reach, {position: [] for position in this.owed}


def together(server, keeper, blobs, cache, local, steps, meanwhile=None):
    """Run every computation's part of a pass, each as far as it goes, and ask for what they want together:
    one question of the views, one run of requests to the server, one blob written, and one request to the keeper.
    All of one kind is asked before any of the next, in the order a computation asks: spans, then requests, then
    blobs, then appends. So every computation's requests are made before any reads what its outputs owe."""
    wants = []
    def aside(work):
        # What only waits, on the blob store or on the keeper, is asked on another thread, and `meanwhile` runs here until it is answered.
        if meanwhile is None: return work()
        asked = WAITS.submit(work)
        meanwhile(asked)
        return asked.result()
    def go(step, answer=None, first=False):
        try: wants.append((step, next(step) if first else step.send(answer)))
        except StopIteration: pass
    for each in steps: go(each, first=True)
    while wants:
        kind = next(kind for kind in ("spans", "feed", "put", "append") if any(want[0] == kind for _, want in wants))
        now, wants[:] = [(each, want) for each, want in wants if want[0] == kind], [(each, want) for each, want in wants if want[0] != kind]
        begin = time.perf_counter()
        if kind == "spans":
            found = local.spans([ask for _, want in now for ask in want[1]])
            # The blobs of the spans that were found are read at once, and kept until the pass is over.
            # Each part is read by range: an instance's blob holds the parts of every output of a pass, and a
            # reader of one output reads its own part only.
            need = wanted(found, cache)
            wholes = sorted(key for key, piece in need.items() if piece is None)
            STORE["gets"] += plumbing.fetch(blobs, [piece for piece in need.values() if piece is not None], cache) + len(wholes)
            cache.update(zip(wholes, blobs.many(blobs.get, wholes)))
            answers = []
            for _, want in now: answers.append(found[:len(want[1])]); del found[:len(want[1])]
        elif kind == "feed":
            requests = [request for _, want in now for request in want[1]]
            if requests: server.many(requests + ["spin"])
            answers = [None] * len(now)
        elif kind == "put":
            # What every computation here made in this pass: a batch that is small is kept in its entry, inline, and
            # the rest are written as one blob, of which each batch names its part. A pass whose batches are all
            # small writes no blob.
            groups = [frames for _, want in now for frames in want[1]]
            # The bound is on the batch as it would be kept: compressed, unless that is off.
            kept = [plumbing.inline(frames) for frames in groups]
            small = [len(each[2]) < plumbing.INLINE for each in kept]
            data, places = plumbing.several([frames for frames, inside in zip(groups, small) if not inside])
            name = aside(lambda: blobs.put(data, stored=True)) if not all(small) else None
            places, names = iter(places), []
            for each, inside in zip(kept, small):
                names.append(each if inside else ("inject", 2, plumbing.piece_ints(name, next(places))))
            STORE["puts"] += 0 if all(small) else 1
            STORE["inline"] += sum(small)
            STORE["blob"] += len(small) - sum(small)
            answers = []
            for _, want in now: answers.append(names[:len(want[1])]); del names[:len(want[1])]
        else:
            def ask():
                while True:
                    # While the keeper is away, the instance waits, and asks the same again when it is back.
                    # If the keeper made an append before it went, asking again is refused, and the instance starts over.
                    # The appends of every computation go in one request, as frames: each computation's are one transaction,
                    # at its own place in the request, and the keeper is told who its runner is. The keeper opens none of it.
                    try:
                        return keeper.append([(place, *each) for place, (_, want) in enumerate(now) for each in want[2]],
                                             [(place, (0, want[1])) for place, (_, want) in enumerate(now)])
                    except OSError: time.sleep(0.2)
            refused = aside(ask)
            answers = [refused.get(place) for place in range(len(now))]
        begin = spent({"spans": "spans found and their blobs read", "feed": "its server fed and run", "put": "blobs written", "append": "the keeper asked to append"}[kind], begin)
        for (each, _), answer in zip(now, answers): go(each, answer)
        spent("the computations' own steps", begin)
    # What was read of the blob store is kept for a reader that must read again, up to a bound.
    for name in list(cache)[:max(0, len(cache) - KEPT)]: del cache[name]


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
