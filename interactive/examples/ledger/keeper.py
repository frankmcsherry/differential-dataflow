#!/usr/bin/env python3
"""A keeper of a system of ledgers: the model's programs, on the stock server.

This is the plainest thing that obeys the model (system.ddp, step.ddp, commands.ddp, admit.ddp, round.ddp).
Nothing here has data: every batch is empty or a few bytes. The point is that the state is kept to its rules.

Standing programs on one server:
  committed  the state: the entries of every transaction that was made
  trial      the state a transaction would make, while it is judged
  intake     the commands of the one transaction being judged, and who makes it
  before     system.ddp of `committed`: the views that admission and the step rules read
  after      system.ddp of `trial`: the rules of a state
  change     commands.ddp: the entries the commands would remove and add
  step       step.ddp: the rules of a change, from `committed` to `trial`
  admit      admit.ddp: the short check of appends
  derive     index.ddp of `committed`: what round.ddp reads of each ledger
  delta      a change as one collection: the entries it adds, less those it removes

One transaction at a time (`transact`):
A transaction of appends that admission covers is made or refused on admission alone, and no rule is read.
Any other transaction is put into `trial`, judged by every rule, and then made or taken back out.

Many transactions of appends at a time (`round`):
round.ddp is loaded for the round, and judges them together. Those it makes are made as one change of the state.
It leaves some waiting, which are for a later round, and refuses some. The round is its made transactions, one
after another in the order of their keys, so a keeper that reads the round back makes them one at a time.
Every rule is still kept up after each round, in `before` and `after`. No rule is read to decide a round.

Each change that is made is written to a log, and a keeper that starts again reads the log back.
A record of the log is what was asked, and what it changed: the entries it added and removed.
What was asked is one transaction, or the transactions of a round, as text: it is what a user said.
What it changed is frames, as the server gave them (`peek .. as corgi`). The keeper does not open them,
and neither does a follower, which feeds them to its own server as they are.
The log is a file, or it is a chain of blobs whose head is in a register (see plumbing.py):
the register changes only by compare-and-swap, so of two keepers that write, one finds out and stops.

A keeper that serves is for those who write. Those who read are meant to ask a follower (see follower.py), which
holds a copy of the state. The keeper sends each follower the records as it makes them, and the log is there for
whatever a follower missed.

Run from the repository root, after `cargo build --release -p ddir-server`:
  python3 interactive/examples/ledger/keeper.py                         a test of the keeper, with a file for its log
  python3 interactive/examples/ledger/keeper.py serve <register> <port> a keeper that others reach over HTTP
"""
import collections, json, os, re, shutil, signal, sys, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import plumbing

HERE = os.path.dirname(os.path.abspath(__file__))
DIR = "interactive/examples/ledger"
PORT = int(os.environ.get("KEEPER_PORT", "7971"))

STATE_RULES = ["repeated", "reregistered", "doubled", "ambiguous", "reprogrammed", "redefined", "contested", "reheld", "forked",
               "orphaned", "unclocked", "misnamed", "unprogrammed", "unregistered", "misheld", "undefined", "mismatched",
               "malformed", "early", "skipped", "forgotten", "spanned", "hollow", "starved", "belated",
               "misbound", "misplaced", "misshapen", "unfounded"]
STEP_RULES = ["reshaped", "rewritten", "reworded", "rebound", "regressed", "revised", "detached", "usurped"]
ADMISSION = ["absent", "stale", "empty", "barred"]
# An append that a round may take: its frontiers are each one time of one coordinate, or no time, and so are in canonical form.
APPEND = re.compile(r"inject\(1,tuple\((tuple\(\d+,\d+\)),\d+,list\((list\(-?\d+\))?\),list\((list\(-?\d+\))?\),inject\(.*\)\)\)")


def plain(commands):
    """Whether a transaction is one for a round: all appends as above, each to a different ledger."""
    found = [APPEND.fullmatch(command) for command in commands]
    return bool(found) and all(found) and len({match.group(1) for match in found}) == len(found)


def declarations():
    """The types the model's programs share, as commands.ddp declares them, and who may act."""
    lines = open(os.path.join(HERE, "commands.ddp")).read().split("\n")
    first = next(i for i, line in enumerate(lines) if line.startswith("type Id"))
    last = next(i for i, line in enumerate(lines) if line.startswith("             | Release"))
    return "\n".join(lines[first:last + 1]) + "\nenum Actor = User | Runner Id;\n"


class FileLog:
    """The changes that were made, one after another in a file, each synced as it is written."""
    def __init__(self, path): self.path = path
    def read(self):
        """What was asked, of each change that was made."""
        records = []
        if os.path.exists(self.path):
            with open(self.path, "rb") as file:
                try:
                    while True: records.append(plumbing.receive(file)[0])
                except EOFError: pass
        return records
    def append(self, asked, frames):
        with open(self.path, "ab") as file:
            file.write(plumbing.pack(asked, frames))
            file.flush(); os.fsync(file.fileno())


class RegisterLog:
    """The changes that were made, each a blob that names the one before it, with the last named by a register."""
    def __init__(self, name):
        self.register, self.blobs = plumbing.Register(name), plumbing.Blobs("log")
        self.state, self.revision = None, 0
    def read(self):
        """What was asked, of each change that was made."""
        self.state, self.revision = self.register.read()
        if self.state is None: self.state = {"count": 0, "head": None}
        records, at = [], self.state["head"]
        while at:
            record, _ = plumbing.unpack(self.blobs.get(at))
            at = record.pop("prev")
            records.append(record)
        return records[::-1]
    def append(self, asked, frames):
        head = self.blobs.put(plumbing.pack(dict(asked, prev=self.state["head"]), frames))
        state = {"count": self.state["count"] + 1, "head": head}
        revision = self.register.swap(self.revision, state)
        if revision is None: raise RuntimeError("the register has moved: another keeper is writing")
        self.state, self.revision = state, revision


class Keeper:
    """The server, one connection to it, and the log of what was made."""
    def __init__(self, log, workers=1):
        if isinstance(log, str): log = FileLog(log)
        self.server = plumbing.Server(PORT, workers)
        self.many, self.ask = self.server.many, self.server.ask
        self.fast, self.slow = 0, 0
        types = declarations()
        entries = types + 'let entries = input 0 : ((Entry), ());\noutput "entries" = entries;\n'
        intake = types + ('let commands = input 0 : ((int), (Command));\nlet actor = input 1 : ((), (Actor));\n'
                          'output "commands" = commands;\noutput "actor" = actor;\n')
        for name, text in (("committed", entries), ("trial", entries), ("intake", intake)):
            self.ask(f"load {name} begin\n{text}{name} end-load")
        self.ask(f"load before from {DIR}/system.ddp input0=committed::entries")
        self.ask(f"load after from {DIR}/system.ddp input0=trial::entries")
        self.ask(f"load change from {DIR}/commands.ddp input0=intake::commands input1=committed::entries")
        self.ask(f"load step from {DIR}/step.ddp input0=committed::entries input1=trial::entries "
                 "input2=before::standing input3=after::standing input4=intake::actor")
        self.ask(f"load admit from {DIR}/admit.ddp input0=intake::commands input1=before::standing "
                 "input2=before::bound input3=intake::actor")
        self.ask(f"load derive from {DIR}/index.ddp input0=committed::entries")
        delta = types + 'let adds = input 0 : ((Entry), ());\nlet removes = input 1 : ((Entry), ());\noutput "delta" = adds - removes;\n'
        self.ask(f"load delta input0=change::adds input1=change::removes begin\n{delta}delta end-load")
        self.ask("tick")
        # What was made before is made again, in order, and then the log is added to.
        # A round is its transactions, one after another in the order of their keys, which is the order they are written in.
        self.log, self.number = None, 0                     # the number of records in the log
        for record in log.read():
            for asked in record.get("round") or [record]:
                assert self.transact(asked["actor"], asked["commands"]) == [], "the log holds a transaction that is refused"
            self.number += 1
        self.log = log
        self.made = collections.deque(maxlen=4096)          # the last changes this keeper wrote, for its followers: (number, frames)
        self.fast, self.slow, self.rounds = 0, 0, 0

    def write(self, asked, frames):
        """Write a change down, and keep it for the followers: what was asked, and the frames of what it changed."""
        self.log.append(asked, frames)
        self.made.append((self.number, frames))
        self.number += 1

    def move(self, target, sign):
        """Put the change into `target`, or with sign -1 take it back out."""
        self.ask(f"feed {target} 0 from change::adds diff={sign}")
        self.ask(f"feed {target} 0 from change::removes diff={-sign}")

    def transact(self, actor, commands):
        """One transaction. Returns the checks or rules that refused it, which is none if it was made."""
        for index, command in enumerate(commands):
            self.ask(f"feed intake 0 {index} val=tuple({command})")
        self.ask(f"feed intake 1 tuple() val=tuple({actor})")
        self.ask("tick")
        admitted = not self.ask("peek admit::deferred")
        if admitted:
            # Admission covers it: the rules are not read.
            refused = [check for check in ADMISSION if self.ask(f"peek admit::{check}")]
            self.fast += 1
        else:
            self.move("trial", 1)
            self.ask("tick")
            refused = [rule for rule in STATE_RULES if self.ask(f"peek after::{rule}")]
            refused += [rule for rule in STEP_RULES if self.ask(f"peek step::{rule}")]
            if self.ask("peek change::unmet"): refused.append("unmet")
            self.slow += 1
        # It is written down before it is made: if the log cannot take it, the state does not have it.
        if not refused and self.log: self.write({"actor": actor, "commands": commands}, self.ask("peek delta::delta as corgi"))
        if not refused:
            self.move("committed", 1)
            if admitted: self.move("trial", 1)
        elif not admitted:
            self.move("trial", -1)
        self.ask("feed intake 0 from intake::commands diff=-1")
        self.ask("feed intake 1 from intake::actor diff=-1")
        self.ask("tick")
        return refused

    def round(self, asked):
        """Many transactions of appends, judged together: [(key, actor, commands)], each `plain`, with keys that differ.
        Returns the keys of those that were made, of those that were refused, and of those left waiting (see round.ddp).
        Those that are made are one change of the state, and one record of the log."""
        feeds = []
        for key, actor, commands in asked:
            feeds += [f"feed adm 0 tuple({key}) val={command[len('inject(1,'):-1]}" for command in commands]
            if actor != USER: feeds.append(f"feed adm 1 tuple({key}) val=tuple({actor[len('inject(1,'):-1]})")
        self.ask(f"load adm from {DIR}/round.ddp input2=derive::own input3=before::bound")
        self.many(feeds + ["tick"])
        waiting = {int(row[len("tuple("):-1]) for row in self.ask("peek adm::waiting")}
        refused = {int(row[len("tuple("):-1]) for row in self.ask("peek adm::refused")}
        made = [(key, actor, commands) for key, actor, commands in sorted(asked) if key not in waiting | refused]
        # They are written down before they are made: if the log cannot take them, the state does not have them.
        if made and self.log:
            self.write({"round": [{"actor": actor, "commands": commands} for _, actor, commands in made]}, self.ask("peek adm::spans as corgi"))
        self.many((["feed committed 0 from adm::spans", "feed trial 0 from adm::spans"] if made else []) + ["drop adm", "tick"])
        self.rounds += 1
        return {key for key, _, _ in made}, refused, waiting

    def audit(self):
        """The rules of the state that is committed: each should be empty."""
        return [rule for rule in STATE_RULES if self.ask(f"peek before::{rule}")]

    def close(self): self.server.close()


def serve(register, port):
    """A keeper that others reach over HTTP. It does one thing at a time.
    POST /transact with {"actor": .., "commands": [..]} answers {"refused": [..]}.
    The transactions that have come while the keeper was busy are taken together. Those of plain appends are a round.
    One that the round leaves waiting is in the next round, with a lesser key than any that comes later.
    One that the round refuses is then judged alone, as is any that is not of plain appends: that says why.
    GET /follow/<number> answers with each change from that number on, as it is made, and does not end:
    its number and its frames (see `pack` in plumbing.py).
    GET /peek/<program>::<output> answers the rows of a view, or of `committed::entries`.
    GET /peek/<one>,<another> answers the rows of each, all of one state."""
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))     # so that being stopped also stops the server
    keeper = Keeper(RegisterLog(register))
    turn = threading.Condition()                              # held by whoever asks the keeper; told of each record
    come, keys = [], iter(range(1 << 62))                     # the transactions that are not yet judged, each with a key

    def judge():
        carried = []
        while True:
            with turn:
                turn.wait_for(lambda: come or carried)
                fresh, come[:] = list(come), []
                fast, slow = carried + [asked for asked in fresh if plain(asked[2])], [asked for asked in fresh if not plain(asked[2])]
                carried = []
                try:
                    if fast:
                        made, refused, waiting = keeper.round([asked[:3] for asked in fast])
                        for asked in fast:
                            if asked[0] in made: asked[3].append([])
                            elif asked[0] in refused: slow.append(asked)
                            else: carried.append(asked)
                    for key, actor, commands, answer in sorted(slow, key=lambda asked: asked[0]):
                        answer.append(keeper.transact(actor, commands))
                except Exception as error:
                    # The log did not take a change, or something else is wrong. This keeper no longer knows the state, and stops.
                    print(f"keeper: {error}; stopping", flush=True)
                    keeper.close(); os._exit(1)
                turn.notify_all()
    threading.Thread(target=judge, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def answer(self, value):
            body = json.dumps(value).encode()
            self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
            self.wfile.write(body)
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            answer = []
            with turn:
                come.append((next(keys), request["actor"], request["commands"], answer))
                turn.notify_all()
                turn.wait_for(lambda: answer)
            self.answer({"refused": answer[0]})
        def do_GET(self):
            if self.path.startswith("/follow/"): return self.follow(int(self.path.split("/follow/", 1)[1]))
            if self.path == "/counts":
                with turn: return self.answer({"rounds": keeper.rounds, "alone": keeper.fast + keeper.slow, "records": keeper.number})
            # Several views named together are read between two transactions, and so are of one state.
            names = self.path.split("/peek/", 1)[1].split(",")
            with turn: rows = [keeper.ask("peek " + name) for name in names]
            self.answer(rows[0] if len(names) == 1 else rows)
        def follow(self, at):
            # A record is sent once it is in the log. One that this keeper no longer has is not sent: whoever follows
            # sees the numbers skip, and reads the log. A line with nothing on it says only that the keeper is here.
            self.send_response(200); self.end_headers()
            try:
                while True:
                    with turn:
                        turn.wait_for(lambda: keeper.number > at, timeout=0.5)
                        fresh = [(number, frames) for number, frames in keeper.made if number >= at]
                    for number, frames in fresh: self.wfile.write(plumbing.pack({"number": number}, frames))
                    if fresh: at = fresh[-1][0] + 1
                    else: self.wfile.write(b"\n")
            except OSError: pass
        def log_message(self, *args): pass
    try:
        ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
    finally:
        keeper.close()


# The commands, as text.
USER = "inject(0,tuple())"
def runner(computation): return f"inject(1,tuple(0,{computation}))"
def ident(n): return f"tuple(0,{n})"
def frontier(t): return f"list(list({t}))"
def create(ledger, shape=7, start=0, clock=None, keep=0):
    following = f"inject(1,{ident(clock)})" if clock else "inject(0,tuple())"
    return f"inject(0,tuple({ident(ledger)},list({shape}),1,{frontier(start)},{following},inject({keep},tuple())))"
def append(ledger, number, lower, upper):
    return f"inject(1,tuple({ident(ledger)},{number},{frontier(lower)},{frontier(upper)},inject(0,tuple())))"
def merge(ledger, first, last, since):
    return f"inject(2,tuple({ident(ledger)},{first},{last},{frontier(since)},inject(1,list(1,2,3))))"
def discard(ledger, first, last, since): return f"inject(3,tuple({ident(ledger)},{first},{last},{frontier(since)}))"
def drop(ledger): return f"inject(4,{ident(ledger)})"
def alias(name, ledger): return f"inject(5,tuple(list({name}),{ident(ledger)}))"
def publish(program, takes, gives):
    shapes = lambda count: ",".join("list(7)" for _ in range(count))
    return f"inject(7,tuple(list({program}),list({shapes(takes)}),list({shapes(gives)}),list({program})))"
def define(computation, program, inputs, outputs):
    reads = ",".join("inject(0,tuple())" if i == "state" else f"inject(1,{ident(i)})" for i in inputs)
    writes = ",".join("inject(0,tuple())" if o is None else f"inject(1,{ident(o)})" for o in outputs)
    return f"inject(8,tuple({ident(computation)},list({program}),list({reads}),list({writes})))"
def start(instance, computation): return f"inject(10,tuple({ident(instance)},{ident(computation)}))"
def hold(holder, ledger, since): return f"inject(12,tuple({ident(holder)},{ident(ledger)},{frontier(since)}))"


def check(label, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + label + (f"  [{detail}]" if detail else ""), flush=True)
    if not ok: sys.exit(1)


def main():
    folder = tempfile.mkdtemp(prefix="keeper-")
    log = os.path.join(folder, "made.log")
    k = Keeper(log)
    try:
        # A small system: ledger 1 feeds computation 100, which writes ledger 2; a clock, 9, and its follower, 8.
        # Ledger 3 is derived, written from the state by computation 101. Instance 500 runs both computations.
        setup = [create(1), create(2), create(9), create(3, keep=1), publish(71, 1, 1),
                 define(100, 71, [1], [2]), define(101, 71, ["state"], [3]), start(500, 100), start(500, 101), alias(1, 1)]
        check("a system is set up in one transaction", k.transact(USER, setup) == [])
        check("a ledger follows the clock", k.transact(USER, [create(8, clock=9)]) == [])

        # Appends, on admission alone.
        steps = 200
        begin = time.perf_counter()
        for t in range(steps):
            assert k.transact(USER, [append(1, t, t, t + 1), append(9, t, t, t + 1)]) == []
            assert k.transact(runner(100), [append(2, t, t, t + 1)]) == []
        elapsed = time.perf_counter() - begin
        check(f"{2 * steps} transactions of appends are admitted", k.fast == 2 * steps,
              f"{1000 * elapsed / (2 * steps):.2f} ms each, on admission alone")
        standing = dict(row.split(" val=") for row in k.ask("peek before::standing"))
        at = f"tuple({steps},{frontier(steps)})"
        check("the three ledgers stand where they were appended to", all(standing[f"tuple({ident(l)})"] == at for l in (1, 2, 9)))
        check("the follower stands at its clock", standing[f"tuple({ident(8)})"] == f"tuple(0,{frontier(steps)})")
        # An instance keeps its own view of where ledgers stand (instance.ddp). It is held here to the model's.
        k.ask(f"load local from {DIR}/instance.ddp input0=committed::entries")
        k.ask("tick")
        check("where an instance's views say the ledgers stand is where the model says", sorted(k.ask("peek local::standing")) == sorted(k.ask("peek before::standing")))
        k.many(["drop local", "tick"])

        # Appends that admission turns away.
        check("a user is barred from a bound ledger", k.transact(USER, [append(2, steps, steps, steps + 1)]) == ["barred"])
        check("an append at an old number is stale", k.transact(USER, [append(1, 0, steps, steps + 1)]) == ["stale"])
        check("an append to a derived ledger is absent", k.transact(runner(101), [append(3, 0, 0, 1)]) == ["absent"])

        # The rest goes by the rules.
        begin, before = time.perf_counter(), k.slow
        check("a durable ledger written from the state is belated", k.transact(USER, [define(102, 71, ["state"], [8])]) == ["belated"])
        check("a ledger that is read cannot be dropped", k.transact(USER, [drop(1)]) == ["misnamed", "unregistered"])
        # Computation 100 writes ledger 2, which stands at `steps`, so ledger 1 is needed from there.
        check("ledger 1 is merged into one span", k.transact(USER, [merge(1, 0, steps, 0)]) == [])
        late = [merge(1, 0, steps, steps + 1), discard(1, 0, steps, 0)] + [discard(1, t, t + 1, t) for t in range(steps)]
        check("it cannot be kept only from after where its reader resumes", k.transact(USER, late) == ["forgotten"])
        kept = [merge(1, 0, steps, steps), discard(1, 0, steps, 0)] + [discard(1, t, t + 1, t) for t in range(steps)]
        check("it can be kept from where its reader resumes", k.transact(USER, kept) == [])
        check("a hold from before that is refused", k.transact(USER, [hold(900, 1, steps - 1)]) == ["forgotten"])
        check("a hold from there is made", k.transact(USER, [hold(900, 1, steps)]) == [])
        judged = k.slow - before
        check(f"{judged} transactions are judged by every rule", judged == 7,
              f"{1000 * (time.perf_counter() - begin) / judged:.0f} ms each")

        check("the committed state keeps every rule", k.audit() == [])
        entries = sorted(k.ask("peek committed::entries"))
        check("the trial state is the committed state", sorted(k.ask("peek trial::entries")) == entries)
        spans = [row for row in entries if row.startswith("tuple(inject(1,")]
        check("ledger 1 is one span, and ledgers 2 and 9 are as they were appended", len(spans) == 1 + 2 * steps)

        # Rounds: transactions of appends that are judged together. Each has a key, and the least goes first.
        t = steps
        check("a round makes what it can, and leaves the rest waiting or refused", k.round([
            (1, USER, [append(1, t, t, t + 1)]),
            (2, runner(100), [append(2, t, t, t + 1)]),         # a runner of the computation that writes ledger 2
            (3, USER, [append(1, t, t, t + 1)]),                # waits: transaction 1 appends to ledger 1
            (4, USER, [append(2, t, t, t + 1)]),                # waits: transaction 2 appends to ledger 2
            (5, USER, [append(8, 0, t, t + 1)]),                # the follower, before its clock
            (6, USER, [append(9, t, t, t + 1)]),                # the clock, after its follower
            (7, USER, [append(3, 0, 0, 1)]),                    # refused: ledger 3 is derived
        ]) == ({1, 2, 5, 6}, {7}, {3, 4}))
        check("what waited is judged in the next round, against the state the first one left",
              k.round([(3, USER, [append(1, t, t, t + 1)]), (4, USER, [append(2, t, t, t + 1)])]) == (set(), {3, 4}, set()))
        check("a follower waits for an append to its clock that has a lesser key",
              k.round([(1, USER, [append(9, t + 1, t + 1, t + 2)]), (2, USER, [append(8, 1, t + 1, t + 2)])]) == ({1}, set(), {2}))
        standing = plumbing.stands(k.ask("peek before::standing"))
        check("the ledgers stand where the rounds left them",
              standing == {1: (t + 1, t + 1), 2: (t + 1, t + 1), 9: (t + 2, t + 2), 8: (1, t + 2)}, str(standing))
        check("the committed state keeps every rule after the rounds", k.audit() == [])
        entries = sorted(k.ask("peek committed::entries"))
        check("the trial state is the committed state", sorted(k.ask("peek trial::entries")) == entries)
        k.close()

        # A keeper that starts again reads the log, and arrives at the same state.
        begin = time.perf_counter()
        k = Keeper(log)
        check("a keeper that starts again has the same state", sorted(k.ask("peek committed::entries")) == entries,
              f"{len(FileLog(log).read())} records read back in {time.perf_counter() - begin:.1f} s, the rounds one transaction at a time")
        check("and takes the next append", k.transact(runner(100), [append(2, steps + 1, steps + 1, steps + 2)]) == [])
    finally:
        k.close()
        shutil.rmtree(folder)


if __name__ == "__main__":
    if sys.argv[1:2] == ["serve"]: serve(sys.argv[2], int(sys.argv[3]))
    else: main()
