#!/usr/bin/env python3
"""A keeper of a system of ledgers: the model's programs, on the stock server.

This is the plainest thing that obeys the model (system.ddp, step.ddp, commands.ddp, admit.ddp, round.ddp).
Nothing here has data: every batch is empty or a few bytes. The point is that the state is kept to its rules.

THIS IS A SCRATCH VARIANT: a keeper that holds no spans, and keeps no rule over them.
The keeper holds one row for each ledger, which a round moves; each ledger's generation; and the configuration:
ledgers, names, programs, computations, runs and holds. A round's spans go to the log, and a follower has them.
Each ledger has an own hold (own.ddp): a `Hold` whose holder is (0, 0), the frontier from which every reader of the
ledger is promised it can read it in full, whatever spans there are. A new reader must begin at or after it, and it
moves only by a transaction that the rules refuse if some reader needs the ledger from before (readers.ddp). A merge
that takes spans out must be held from at or before it; a merge that takes nothing out needs no such check, and may
stand beside the spans it covers: a ledger may hold more than one version of its appends.

Standing programs on one server. There is one copy of the configuration, and one copy of the rules:
  rows       one row for each ledger: its version, its own upper, the clock it follows, and whether it is durable.
             It is what round.ddp reads as `own`. Each round moves it with its own output, `moved`.
  stand      stand.ddp: where each durable ledger stands, from the rows
  shift      shift.ddp: the row a transaction in hand gives a ledger it registers, or takes from one it drops
  gone       gone.ddp: what leaves the keeper's tables with a ledger the transaction in hand drops: its generation
  gens       each ledger's generation: the log's length at its last pass of merges or drop
  own        own.ddp: each ledger's own hold, as the transaction in hand would leave it
  readers    readers.ddp: a need, of the transaction in hand or of the state, that is before its ledger's own hold
  state      the entries of every transaction that was made, less the spans: a round's spans go to the log only
  intake     the commands of the one transaction in hand, and who makes it
  change     commands.ddp: the entries those commands would remove and add
  trial      the state the transaction in hand would make: `state`, with what `change` adds, less what it removes.
             It is not fed. With no transaction in hand it is the state. It also gives the change as one collection, `delta`.
  views      system.ddp of `trial`: the model's views and its rules. Between transactions these are of the state,
             and with a transaction in hand they are of the state it would make
  step       step.ddp: the rules of a change, of `state` and what `change` takes out of it and puts in
  admit      admit.ddp: the short check of appends

One transaction at a time (`transact`):
Its commands are fed, and with that `trial` is the state it would make, and the rules are of that state.
A transaction of appends, each to a different ledger, is a round of one. If the round refuses it, admission says why.
Any other is made if no rule complains, and may not add or remove a span. A reader it brings, a holder or a computation,
must begin at or after the own hold of what it reads: with no spans, that is how this keeper keeps the rule `forgotten`.
One that drops a ledger takes the ledger's own hold with it. Its spans are in no table here: the drop's record says
how many ledgers it drops, and a follower takes their spans out of its copy itself (see follower.py).
To make a transaction, what it changes is fed to `state` and its commands are taken back, in one tick. The state then holds what `trial` already held, so `trial` does not change, and the rules do
no work a second time. To refuse it, its commands are taken back, and `trial` is the state again.

Many transactions of appends at a time (`judge`, and `round` for those that come as text):
round.ddp is loaded for the round, and judges them together. Those it makes are made as one change of the state.
It leaves some waiting, which are for a later round, and refuses some. The round is its made transactions, one
after another in the order of their keys, so a keeper that reads the round back makes them one at a time.
No rule is read to decide a round, and none hears of it: what it makes moves the rows, and its spans go to the log.
The appends come as frames, which the one who asks made (see `Client.append` in plumbing.py), and the keeper feeds them
to the round as they are: it opens none. So it does not number the transactions. Each has a key that its asker made
from a ticket the keeper gave: tickets are given in order, and each is used once, so the keys of two requests differ,
and a request's are greater than those of every request whose ticket was given before its own.
What the round leaves waiting it gives back as frames, which are fed to the next round, with the keys they had.
An append that comes as text, alone or beside others, is made into a frame here, and goes the same way.

A pass of merges (`merges`):
A compactor hands over what a follower's policy proposes, and the entries those merges take out, as the frames the
follower gave it, and says how long the log was as its follower had it: one line. It opens none of the frames, and
neither does the keeper, which feeds them to pass.ddp as they are. pass.ddp is loaded for the pass. A merge is made
if its ledger's generation is no later than what the compactor read, if it covers appends the ledger has, and if
every entry it takes out is a span of that ledger within what it covers, once, with the merge held from at or before
the ledger's own hold. So a pass takes nothing out of the state but spans of the ledgers it merges, under a cover a
reader may use. The keeper cannot say whether those spans are there, or whether the ledger's spans still join up:
a follower keeps those rules.
The merges of a pass that are made are one record of the log.

Each change that is made is written to a log, and a keeper that starts again reads the log back.
A record of the log is what it changed, and what was asked. Both are frames, but for a transaction the rules judged,
whose commands are text: they are few, and a person wrote them.
What a round changed is the spans it made; what it was asked is not kept. What a pass changed is the entries it put in
and took out, and beside them is its change to the generations; what was proposed is not kept.
A record's first line says how many of its frames are entries: those are what a follower feeds to its own server,
as they are. The keeper opens none of them. Beside a round's spans the record keeps what the round changed in the
keeper's own tables, and beside a pass's entries its changes to them. When the keeper starts again it feeds those:
it judges no round again, so what an old round made does not depend on what round.ddp says now.
Every so many records the keeper writes its tables to the log whole, as a record of their own, a snapshot. A keeper
that starts reads the log back from its head only as far as the latest snapshot, and takes its tables from it.
The log is a file, or it is a chain of blobs whose head is in a register (see plumbing.py):
the register changes only by compare-and-swap, so of two keepers that write, one finds out and stops.

A keeper that serves is for those who write. Those who read are meant to ask a follower (see follower.py), which
holds a copy of the state. The keeper does nothing for followers: each watches the register, and reads the records
from the store. The keeper does not know who follows, and cuts its log behind snapshots alone (see `write` in `serve`).

A keeper that serves writes its log beside its judging. A round, a pass or a transaction is judged, and what it
changes is made in the keeper's own tables at once, and its record is handed to a writer. The keeper goes on to the
next round, and does not wait for the record to be written. The writer takes every record that is waiting, writes
their blobs, and moves the register once, past all of them.
Nothing is seen before it is in the log. A request is answered, whether it was made or refused, only when every record
judged before the answer is in the log; a follower can read a record only then; and one who reads the keeper waits until
all that was judged is in the log, while the keeper judges nothing more. So if the register has moved, and the keeper
stops, what it had made and not written was seen by no one, and is lost with it: nothing it had answered is.

Run from the repository root, after `cargo build --release -p ddir-server`:
  python3 interactive/examples/ledger/keeper.py                         a test of the keeper, with a file for its log
  python3 interactive/examples/ledger/keeper.py serve <register> <port> a keeper that others reach over HTTP
"""
import collections, hashlib, json, os, re, shutil, signal, socket, sys, tempfile, threading, time, urllib.error
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
MOST = int(os.environ.get("DDIR_MAX_FEED_BYTES", str(1 << 24)))    # the longest frame a server takes
# ONE CHUNK SIZE, which every producer keeps to: a request of appends, a round, a pass of merges (see plumbing.CHUNK).
# What a round or a pass makes goes to the log as one frame, which every follower's server must take, and it is about
# as long as what it was asked. The keeper refuses anything longer, and splits nothing.
ROUND = plumbing.CHUNK
SNAP = int(os.environ.get("KEEPER_SNAP", "500"))     # how many records there are between one snapshot of the keeper's tables and the next
# The most bytes of one frame of a snapshot, and of one blob of it: each table is read in as many pieces as keep a frame
# under this, and the frames are written to blobs of about this size, which the snapshot's record names in order.
PIECE = int(os.environ.get("KEEPER_SNAP_PIECE", str(4 << 20)))
CUT = os.environ.get("KEEPER_CUT", "1") == "1"       # whether the log is cut back, behind the older of the two latest snapshots
# A follower more than this many records behind is not waited for: the log may be cut ahead of it, and it starts again.
LAG = int(os.environ.get("KEEPER_LAG", str(4 * SNAP)))
SHAPES = {"rows": "((Id), (int, Frontier, int, Id, int))", "gens": "((Id), (int))", "state": "((Entry), ())"}      # the shape of each table's rows
TABLES = [("rows", "rows::own"), ("gens", "gens::rows"), ("state", "state::entries")]     # what a snapshot holds: each table, and where its rows are read
SPANNED = ("inject(1,", "inject(2,", "inject(3,")     # the commands that add or remove a span: Append, Merge, Discard
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
    def __init__(self, path): self.path, self.base = path, 0
    def read(self, whole=False):
        """Each change that was made: (its first line, its frames). Unless `whole`, those from the latest snapshot on:
        `base` is then how many records came before the first that is given."""
        records = []
        if os.path.exists(self.path):
            with open(self.path, "rb") as file:
                try:
                    while True: records.append(plumbing.receive(file))
                except EOFError: pass
        self.base = 0 if whole else max([at for at, (record, _) in enumerate(records) if "snapshot" in record], default=0)
        return records[self.base:]
    def append(self, asked, frames):
        with open(self.path, "ab") as file:
            file.write(plumbing.pack(asked, frames))
            file.flush(); os.fsync(file.fileno())


class RegisterLog:
    """The changes that were made, each a blob that names the one before it, with the last named by a register.
    The register says how many records there are, which is the last, and from which number on they are all still in
    the log (`from`): the log is cut back, behind a snapshot, and what is behind the cut is deleted (see `cut`)."""
    def __init__(self, name):
        self.register, self.blobs = plumbing.Register(name), plumbing.Blobs("log")
        self.state, self.revision, self.base = None, 0, 0
        self.names = {}                 # for each record this knows: its blob's name, and the names of a snapshot's blobs
        self.snaps = []                 # the numbers of the snapshots this knows, in order
        self.older = None               # the blob before the first record this read: those before it are not in `names`
    def read(self, whole=False):
        """Each change that was made: (its first line, its frames). Unless `whole`, those from the latest snapshot on:
        the chain of blobs is read back from its head, and no further than that snapshot. `whole` reads back as far as
        the log still goes, which is to its cut. `base` is then how many records came before the first that is given."""
        self.state, self.revision = self.register.read()
        if self.state is None: self.state = {"count": 0, "head": None}
        records, at, number = [], self.state["head"], self.state["count"]
        while at and number > self.state.get("from", 0):
            record, frames = plumbing.unpack(self.blobs.get(at))
            number -= 1
            self.names[number] = (at, [name for name, _ in record.get("parts", [])])
            if "snapshot" in record: self.snaps.insert(0, number)
            at = record.pop("prev")
            records.append((record, plumbing.expand(record, frames, self.blobs)))
            if "snapshot" in record and not whole: break
        self.older = at
        self.base = self.state["count"] - len(records)
        return records[::-1]
    def stored(self, asked, frames):
        """A record as its blobs: [bytes], the record's own last. A snapshot's frames go to blobs of their own, each about
        `PIECE` bytes, and its record names them; any other record holds its frames."""
        if "snapshot" not in asked: return [], asked, frames
        pieces, groups = [], plumbing.split(frames, PIECE)
        for group in groups: pieces.append(plumbing.squeeze(plumbing.pack({}, group)))
        names = [hashlib.sha256(piece).hexdigest() for piece in pieces]
        return pieces, dict(asked, parts=[[name, len(group)] for name, group in zip(names, groups)]), []
    def entries_at(self):
        """The log's length at the followers' latest snapshot of the entries (see follower.py), or 0 if there is none.
        The log is not cut past it: a follower that starts reads from there on."""
        found, _ = plumbing.Register(self.register.name + ":entries").read()
        return found["at"] if found else 0
    def append(self, asked, frames): self.append_many([(asked, frames)])
    def append_many(self, records, cut=None):
        """Several changes, in order: [(asked, frames)]. Each is a blob that names the one before it, and the register
        is moved once, past them all. A blob's name is the hash of what it holds, so each can name the one before it
        before that one is written, and the blobs are written together. With `cut`, the register says too that the log
        now goes back only to that record: what is behind it is deleted once the register says so (see `cut`)."""
        head, blobs, number = self.state["head"], [], self.state["count"]
        for asked, frames in records:
            pieces, asked, frames = self.stored(asked, frames)
            blobs += pieces
            # A record is compressed whole, and is named by the hash of what is stored (see plumbing.py).
            blobs.append(plumbing.squeeze(plumbing.pack(dict(asked, prev=head), frames)))
            head = hashlib.sha256(blobs[-1]).hexdigest()
            self.names[number] = (head, [name for name, _ in asked.get("parts", [])])
            if "snapshot" in asked: self.snaps.append(number)
            number += 1
        self.blobs.many(lambda data: self.blobs.put(data, stored=True), blobs)
        state = {"count": number, "head": head, "from": max(self.state.get("from", 0), cut or 0)}
        try: revision = self.register.swap(self.revision, state)
        except (urllib.error.URLError, ConnectionError, socket.timeout) as error: revision = self.settle(state, error)
        if revision is None: raise RuntimeError("the register has moved: another keeper is writing")
        self.state, self.revision = state, revision
    def settle(self, state, error, tries=30):
        """A swap that was not answered may have been made or not. The register is read to say which: if it holds
        `state`, the swap was made; if it is still at the revision the swap named, it was not, and is asked again; if
        it is at another revision, another keeper moved it, and None is returned. Returns the new revision."""
        begin = time.perf_counter()
        for _ in range(tries):
            try:
                found, revision = self.register.read()
                if found == state: outcome = "it had been made"
                elif revision == self.revision:
                    outcome = "it had not been made, and was asked again"
                    revision = self.register.swap(self.revision, state)
                else: outcome, revision = "another keeper had moved the register", None
                print(f"keeper: the register did not answer a swap ({error}); {outcome}; {time.perf_counter() - begin:.1f} s", flush=True)
                return revision
            except (urllib.error.URLError, ConnectionError, socket.timeout) as again: error = again
            time.sleep(1.0)
        raise RuntimeError(f"the register did not answer for {time.perf_counter() - begin:.0f} s: {error}")
    def cut(self):
        """Delete the blobs of the records behind the cut the register now says, and of their snapshots. Each is deleted
        by its exact name. Returns how many records were deleted. Only the keeper that writes the log calls this."""
        gone, behind = [], self.state.get("from", 0)
        for number in sorted(n for n in self.names if n < behind):
            name, parts = self.names.pop(number)
            gone += parts + [name]
        self.snaps = [n for n in self.snaps if n >= behind]
        count = len([n for n in gone])
        # The records from before this keeper read the log are found by following each one's `prev`, as far as they go.
        while behind and self.older:
            try: data = self.blobs.get(self.older)
            except (urllib.error.HTTPError, AssertionError): break
            record = json.loads(data.split(b"\n", 1)[0])
            gone += [name for name, _ in record.get("parts", [])] + [self.older]
            self.older = record.get("prev")
        self.blobs.many(lambda name: plumbing.http("DELETE", self.blobs.base + name), gone)
        return len(gone)


class Keeper:
    """The server, one connection to it, and the log of what was made."""
    def __init__(self, log, workers=1):
        if isinstance(log, str): log = FileLog(log)
        self.server = plumbing.Server(PORT, workers)
        self.many, self.ask = self.server.many, self.server.ask
        self.fast, self.slow, self.rounds, self.passes, self.left = 0, 0, 0, 0, 0
        types = declarations()
        state = types + 'let entries = input 0 : ((Entry), ());\noutput "entries" = entries;\n'
        intake = types + ('let commands = input 0 : ((int), (Command));\nlet actor = input 1 : ((), (Actor));\n'
                          'output "commands" = commands;\noutput "actor" = actor;\n')
        trial = types + ('let entries = input 0 : ((Entry), ());\nlet adds = input 1 : ((Entry), ());\n'
                         'let removes = input 2 : ((Entry), ());\n'
                         'output "entries" = entries + adds - removes;\noutput "delta" = adds - removes;\n')
        rows = types + 'let own = input 0 : ((Id), (int, Frontier, int, Id, int));\noutput "own" = own;\n'
        gens = types + 'let rows = input 0 : ((Id), (int));\noutput "rows" = rows;\n'
        for name, text in (("state", state), ("intake", intake), ("rows", rows), ("gens", gens)):
            self.ask(f"load {name} begin\n{text}{name} end-load")
        self.ask(f"load change from {DIR}/commands.ddp input0=intake::commands input1=state::entries")
        self.ask(f"load trial input0=state::entries input1=change::adds input2=change::removes begin\n{trial}trial end-load")
        self.ask(f"load views from {DIR}/system.ddp input0=trial::entries")
        self.ask(f"load stand from {DIR}/stand.ddp input0=rows::own")
        self.ask(f"load shift from {DIR}/shift.ddp input0=change::adds input1=change::removes input2=rows::own")
        self.ask(f"load gone from {DIR}/gone.ddp input0=shift::dropped input1=gens::rows")
        # Each ledger's own hold, as the transaction in hand would leave it. Every need of a ledger, new or old, must be at
        # or after it: so a new use from before it is refused, and so is a move of it past a use (readers.ddp).
        self.ask(f"load own from {DIR}/own.ddp input0=trial::entries")
        self.ask(f"load readers from {DIR}/readers.ddp input0=trial::entries input1=stand::standing input2=shift::moved input3=own::holds")
        # Where ledgers stand is the rows' to say, before and after: a transaction the rules judge moves no ledger.
        self.ask(f"load step from {DIR}/step.ddp input0=state::entries input1=change::removes input2=change::adds "
                 "input3=stand::standing input4=stand::standing input5=intake::actor")
        self.ask(f"load admit from {DIR}/admit.ddp input0=intake::commands input1=stand::standing "
                 "input2=views::bound input3=intake::actor")
        self.ask("tick")
        # What was made before is made again, in order, and then the log is added to.
        # A round is its transactions, one after another in the order of their keys, which is the order they are written in.
        self.log, self.number = None, 0                     # the number of records that were judged: the log's length, once all are written
        self.snapped = 0                                    # the log's length when the keeper's tables were last written to it whole
        self.beside, self.unwritten = False, []             # whether a writer writes the log beside the judging, and the records it has yet to write
        # The log is read from its latest snapshot on. The tables are made from the snapshot, and then each record
        # after it is fed what it changed: nothing is judged again.
        records = log.read(whole=os.environ.get("KEEPER_WHOLE") == "1")
        self.number = self.snapped = log.base
        # What records changed is fed many records at a time, and the server is told to go on once for them all: the
        # tables are then as they were after the last of them, and no one reads them in between.
        pending = []
        def flush():
            if pending: self.many(pending + ["tick"])
            pending.clear()
        for first, (record, frames) in ((at == 0, each) for at, each in enumerate(records)):
            entries = record.get("entries", len(frames))
            if "snapshot" in record:
                # The keeper's tables, whole, as they were after the records before this one. Only the first record
                # read is taken this way: the tables are then empty. One met later, in a log read whole, says nothing new.
                if first:
                    feeds, at = [], entries
                    for name, _ in TABLES:
                        feeds += [(f"feed {name} 0 frame {len(frame)}", frame) for frame in frames[at:at + record["snapshot"][name]]]
                        at += record["snapshot"][name]
                    pending.extend(feeds)
            elif "round" in record and "moved" in record["round"]:
                # What the round changed in the rows is in the record, and its spans are the chain's. They are fed, and the
                # round is not judged again: so a round.ddp that has changed since does not change what an old round made.
                one = entries + record["round"]["appends"] + record["round"]["runners"]
                pending.extend([(f"feed rows 0 frame {len(frame)}", frame) for frame in frames[one:one + record["round"]["moved"]]])
            elif "round" in record:
                # A record of before the rows' change was kept in it: the round is judged again from what it was asked.
                flush()
                asked = frames[entries:]
                _, _, made, _ = self.judge(asked[:record["round"]["appends"]], asked[record["round"]["appends"]:])
                assert made == sum(map(plumbing.rows_of, frames[:entries])), "the log holds a round that is not made again as it was"
            elif "pass" in record:
                # A pass changed no row. Its ledgers' generations are this record, as they were.
                marks = frames[entries:entries + record["pass"]["marked"]]
                pending.extend([(f"feed gens 0 frame {len(frame)}", frame) for frame in marks])
            elif "moved" in record:
                # A transaction the rules judged. Its entries, and what it changed in the rows, are fed: it is not judged again.
                # After its change to the rows come the generations of the ledgers it dropped, which leave.
                at = entries + record["moved"]
                unmarked = record.get("unmarked", 0)
                pending.extend([(f"feed state 0 frame {len(frame)}", frame) for frame in frames[:entries]]
                          + [(f"feed rows 0 frame {len(frame)}", frame) for frame in frames[entries:at]]
                          + [(f"feed gens 0 frame {len(frame)}", frame) for frame in frames[at:at + unmarked]])
            else:
                # A record of before the rows' change was kept in it: the transaction is judged again.
                flush()
                assert self.transact(record["actor"], record["commands"]) == [], "the log holds a transaction that is refused"
            self.number += 1
            if len(pending) >= 512: flush()
        flush()
        self.log = log
        self.durable = self.number                          # the number of records that are in the log
        self.fast, self.slow, self.rounds = 0, 0, 0

    def write(self, said, frames, beside=()):
        """Write a change down, and keep it for the followers: its first line, the frames of the entries it changed, and
        after them the frames that are not entries (what was asked of a round; a pass's marks). A follower has the entries.
        With a writer beside, the record is handed to it, and is not in the log until `durable` has passed its number."""
        said, whole = dict(said, entries=len(frames)), list(frames) + list(beside)
        if self.beside: self.unwritten.append((self.number, said, whole, frames))
        else:
            self.log.append(said, whole)
            self.durable = self.number + 1
        self.number += 1

    def read(self, name):
        """The rows of a view, or of the entries. Where ledgers stand is asked of the rows: the rules see no span."""
        return self.ask("peek " + ("stand::standing" if name == "views::standing" else name))

    def name(self, actor, commands):
        """The checks of admission that refuse a transaction of plain appends. It is not made here, whatever they say."""
        self.many([f"feed intake 0 {index} val=tuple({command})" for index, command in enumerate(commands)]
                  + [f"feed intake 1 tuple() val=tuple({actor})", "tick"])
        refused = [check for check in ADMISSION if self.ask(f"peek admit::{check}")]
        self.many(["feed intake 0 from intake::commands diff=-1", "feed intake 1 from intake::actor diff=-1", "tick"])
        return refused

    def transact(self, actor, commands):
        """One transaction. Returns the checks or rules that refused it, which is none if it was made."""
        if plain(commands):
            # Appends are made by rounds only: a round moves the rows, and its spans go to the log and not to the state.
            made, _, _ = self.round([(0, actor, commands)])
            self.rounds -= 1
            self.fast += 1
            return [] if made else self.name(actor, commands)
        # A span is not the rules' to judge here. An append beside other commands, or two to one ledger, is turned away.
        if any(command.startswith(SPANNED) for command in commands): return ["spans"]
        # With its commands in hand, `trial` is the state it would make, and `views` and `step` are the rules of that.
        self.many([f"feed intake 0 {index} val=tuple({command})" for index, command in enumerate(commands)]
                  + [f"feed intake 1 tuple() val=tuple({actor})", "tick"])
        # `forgotten` is asked of the own holds: the rules' own answer is of a state with no spans, and is always none.
        refused = [rule for rule in STATE_RULES if self.ask("peek readers::forgotten" if rule == "forgotten" else f"peek views::{rule}")]
        refused += [rule for rule in STEP_RULES if self.ask(f"peek step::{rule}")]
        if self.ask("peek change::unmet"): refused.append("unmet")
        self.slow += 1
        # It is written down, or handed to the writer, as it is made. If the log does not take it, the keeper stops,
        # and no one has seen what it made.
        # Beside the entries it changes, the record keeps what it changes in the rows: a restart feeds both. A ledger it
        # drops leaves with its own hold (see commands.ddp); its spans are in no table here, and a follower takes them out
        # of its copy itself (see follower.py): the record says how many ledgers it drops. After the change to the rows
        # come the dropped ledgers' generations, which leave.
        if not refused and self.log:
            some = lambda frames: [frame for frame in frames if plumbing.rows_of(frame)]
            moved = some(self.ask("peek shift::moved as corgi"))
            dropped = len(self.ask("peek shift::dropped"))
            unmarked = some(self.ask("peek gone::unmarked as corgi"))
            self.write({"actor": actor, "commands": commands, "moved": len(moved), "drops": dropped, "unmarked": len(unmarked)},
                       some(self.ask("peek trial::delta as corgi")), moved + unmarked)
        # To make it, the state is fed what it changes as its commands are taken back: `trial` is then as it was.
        # A ledger it registers is given its row, and one it drops loses its row and its generation.
        # To refuse it, its commands are taken back alone.
        self.many(([] if refused else ["feed state 0 from trial::delta", "feed rows 0 from shift::moved", "feed gens 0 from gone::unmarked"])
                  + ["feed intake 0 from intake::commands diff=-1", "feed intake 1 from intake::actor diff=-1", "tick"])
        self.checkpoint()
        return refused

    def checkpoint(self):
        """Every `SNAP` records, write the keeper's tables to the log, whole, as a record of their own: a snapshot.
        A keeper that starts reads the log back from its head no further than the latest one, makes its tables from
        it, and feeds what each record after it changed. A snapshot has no entries: a follower takes nothing from it.
        This is called between two changes, when the tables hold all that was judged."""
        if not self.log or self.number - self.snapped < SNAP: return
        parts, beside = {}, []
        for name, where in TABLES:
            frames = self.pieces(name, where)
            parts[name] = len(frames)
            beside += frames
        self.write({"snapshot": parts}, [], beside)
        self.snapped = self.number

    def pieces(self, name, where):
        """A table's rows, as frames each under `PIECE` bytes: the server gives a peek as one frame, so the table is read
        in pieces, by a hash of each row's key, through a program loaded for it. How many pieces is guessed from how long
        the table was the last time, and doubled until every frame is under the bound."""
        count = getattr(self, "spread", {}).get(name, 1)
        while True:
            if count == 1: frames = self.ask(f"peek {where} as corgi")
            else:
                text = declarations() + f"let rows = input 0 : {SHAPES[name]};\n" + "".join(
                    f'output "p{at}" = rows | filter((key, _) -> ((key hash shr_b64 1, {count}) rem, {at}) eq);\n' for at in range(count))
                self.ask(f"load snap input0={where} begin\n{text}snap end-load")
                frames = [frame for at in range(count) for frame in self.ask(f"peek snap::p{at} as corgi")]
                self.ask("drop snap")
            frames = [frame for frame in frames if plumbing.rows_of(frame)]
            longest = max(map(len, frames), default=0)
            if longest <= PIECE or count >= 1 << 12: break
            count = max(count * 2, -(-longest * count // PIECE) * 2)
        # The next time, as many pieces as would have kept this one's longest under half the bound.
        self.spread = dict(getattr(self, "spread", {}), **{name: max(1, -(-sum(map(len, frames)) * 2 // PIECE))})
        return frames

    def round(self, asked):
        """Many transactions of appends that come as text, judged together: [(key, actor, commands)], each `plain`, with
        keys that differ. They are made into frames, and judged as any round is (see `judge`).
        Returns the keys of those that were made, of those that were refused, and of those left waiting (see round.ddp)."""
        appends = [((key,), plumbing.parse(command)[2], 1) for key, _, commands in asked for command in commands]
        runners = [((key,), (plumbing.parse(actor)[2],), 1) for key, actor, _ in asked if actor != USER]
        waiting, refused, _, _ = self.judge([plumbing.encode(plumbing.APPENDS, appends, timed=False)],
                                            [plumbing.encode(plumbing.RUNNERS, runners, timed=False)] if runners else [])
        return {key for key, _, _ in asked} - waiting - set(refused), set(refused), waiting

    def judge(self, appends, runners):
        """A round: frames of appends, ((tx), (ledger, number, lower, upper, batch)), and of runners, ((tx), (computation)),
        as round.ddp takes them. They are fed as they are. Returns the keys of the transactions left waiting, those refused,
        each with why ("barred", or "refused": absent, stale, empty or twice), how many appends were made, and the frames
        of what waits: (its appends, its runners), to feed to the next round.
        Those that are made are one change of the rows, and one record of the log.
        If the server will not take a frame, nothing is judged, and `spoiled` is the places, in `appends + runners`, of
        the frames it turned away: whoever sent them is told, and the round is asked again without them."""
        self.spoiled = []
        self.ask(f"load adm from {DIR}/round.ddp input2=rows::own input3=views::bound")
        # The frames are fed, and the round is judged, in one run of requests. If a frame was turned away, what the
        # round then says is of the rest alone, and is not read.
        turned = self.server.tried([(f"feed adm 0 frame {len(frame)}", frame) for frame in appends]
                                   + [(f"feed adm 1 frame {len(frame)}", frame) for frame in runners] + ["tick"])
        if turned:
            self.spoiled = sorted(turned)
            self.many(["drop adm", "tick"])
            return set(), {}, 0, ([], [])
        key = lambda row: int(row[len("tuple("):-1])
        waiting = set(map(key, self.ask("peek adm::waiting")))
        refused = {key(row): "refused" for row in self.ask("peek adm::refused")}
        if refused: refused.update((key(row), "barred") for row in self.ask("peek adm::barred") if key(row) in refused)
        some = lambda frames: [frame for frame in frames if plumbing.rows_of(frame)]
        held = (some(self.ask("peek adm::held as corgi")), some(self.ask("peek adm::heldby as corgi"))) if waiting else ([], [])
        # They are written down, or handed to the writer, as they are made (see `write`).
        frames = some(self.ask("peek adm::spans as corgi"))
        if max(map(len, frames), default=0) > MOST:
            # What the round makes is a frame no follower's server would take. None of it is made. (A round's frames
            # are held to `ROUND`, so that this does not come up: it is checked all the same.)
            self.spoiled = list(range(len(appends) + len(runners)))
            self.many(["drop adm", "tick"])
            return set(), {}, 0, ([], [])
        made = sum(map(plumbing.rows_of, frames))
        if made and self.log:
            # Beside its spans, the record keeps what the round changed in the rows: a restart feeds that. What the round
            # was asked is not kept: nothing that reads the log needs it, now that a round is not judged again.
            moved = some(self.ask("peek adm::moved as corgi"))
            self.write({"round": {"appends": 0, "runners": 0, "moved": len(moved)}}, frames, moved)
        # The round moves the rows of the ledgers it appended to. Its spans are in the log, and are not kept here.
        self.many((["feed rows 0 from adm::moved"] if made else []) + ["drop adm", "tick"])
        self.rounds += 1
        self.checkpoint()
        return waiting, refused, made, held

    def merges(self, at, proposed, leaving, batches=()):
        """A pass of merges. `proposed` is frames of ((shard), (ledger, from, to, since, lower, upper)), what a follower's
        policy proposes, and `leaving` frames of ((shard), (ledger, entry)), the entries those merges take out: both as the
        follower gave them. `batches` is frames of ((ledger), (batch)), for the merges whose batch is not empty.
        They are fed as they are. `at` is the log's length as the compactor's follower had it.
        Returns the ledgers whose merges were refused: those whose generation is later than what the compactor read (kept
        in `late`), those that take spans out and are held from after the ledger's own hold (kept in `beyond`), and those
        that do not cover their ledger's appends, or take out what they do not cover (kept in `forged`: see pass.ddp). `fused` is how many merges were made, and
        `left` how many entries they took out. If the server will not take a frame, nothing is made, and `spoiled` says so."""
        length = self.number + 1                            # the log's length once this pass is written
        self.late, self.beyond, self.forged, self.fused, self.left, self.spoiled = [], [], [], 0, 0, []
        self.ask(f"load mrg from {DIR}/pass.ddp input2=gens::rows input4=state::entries input9=rows::own")
        turned = self.server.tried([f"feed mrg 3 tuple() val=tuple({length})", f"feed mrg 7 tuple() val=tuple({int(at)})"]
                                   + [(f"feed mrg 0 frame {len(frame)}", frame) for frame in proposed]
                                   + [(f"feed mrg 6 frame {len(frame)}", frame) for frame in leaving]
                                   + [(f"feed mrg 1 frame {len(frame)}", frame) for frame in batches] + ["tick"])
        if turned:
            self.spoiled = sorted(turned.values())
            self.many(["drop mrg", "tick"])
            return []
        self.late = sorted(row[len("tuple("):-1] for row in self.ask("peek mrg::refused"))
        self.beyond = sorted(row[len("tuple("):-1] for row in self.ask("peek mrg::beyond"))
        self.forged = sorted(row[len("tuple("):-1] for row in self.ask("peek mrg::forged"))
        count = lambda rows: int(rows[0].split(" val=")[1].split(" ")[0]) if rows else 0
        self.fused, self.left = count(self.ask("peek mrg::made")), count(self.ask("peek mrg::left"))
        # They are written down, or handed to the writer, as they are made, as one record: what the pass changes, and
        # beside it its change to the marks, and what was proposed.
        if self.fused:
            some = lambda frames: [frame for frame in frames if plumbing.rows_of(frame)]
            frames, marks = some(self.ask("peek mrg::change as corgi")), some(self.ask("peek mrg::marked as corgi"))
            if max(map(len, frames), default=0) > MOST:
                # What the pass changes is a frame no follower's server would take. None of it is made.
                self.spoiled, self.fused, self.left = ["what this pass changes is longer than a follower's server takes"], 0, 0
                self.many(["drop mrg", "tick"])
                return []
            if self.log:
                # What was proposed is not kept: the entries the pass put in and took out say all it did.
                self.write({"pass": {"marked": len(marks), "proposed": 0, "at": int(at)},
                            "arrive": self.fused, "leave": self.left}, frames, marks)
        self.many((["feed gens 0 from mrg::marked"] if self.fused else []) + ["drop mrg", "tick"])
        self.passes += 1
        self.checkpoint()
        return sorted(set(self.late) | set(self.beyond) | set(self.forged))

    def audit(self):
        """The rules of the state: each should be empty. `forgotten` is asked of the own holds."""
        return [rule for rule in STATE_RULES if self.ask("peek readers::forgotten" if rule == "forgotten" else f"peek views::{rule}")]

    def close(self): self.server.close()


def serve(register, port):
    """A keeper that others reach over HTTP. It judges one thing at a time, and writes its log beside that (see the top).
    GET /ticket answers {"ticket": n}: a number to make a request's keys from (see `Client.append` in plumbing.py).
    POST /append with a first line {"ticket": .., "appends": how many frames, "runners": how many} and then those frames
    (see `pack` in plumbing.py) asks the transactions of appends in them, and answers {"refused": {place: why}, "ticket": the next}.
    A ticket is good once. One this keeper did not give, or has had used, is answered {"again": true, "ticket": another}.
    POST /transact with {"actor": .., "commands": [..]} answers {"refused": [..]}.
    POST /transact with {"many": [{"actor": .., "commands": [..]}, ..]} asks each of them, and answers {"refused": [[..], ..]}.
    POST /merges with a first line {"at": .., "proposed": how many frames, "leaving": how many, "batches": how many} and
    then those frames asks a pass of merges (see `merges`), and answers {"refused": [ledger, ..], "record": the log's length
    after it, "made": how many merges, "left": how many entries they took out}.
    The transactions that have come while the keeper was busy are taken together. Those of appends are a round.
    One that the round leaves waiting is in the next round, with the key it had: that is less than the key of any
    request whose ticket was given after its own, so one that waits is not passed by those that come later.
    One that the round refuses is told so. If it came as text it is then judged alone, as is any that is not of plain
    appends: that says why. One that came as frames is told "barred" or "refused", and may ask why as text.
    Followers are not served here: they watch the register, and read the records from the store (see follower.py).
    GET /peek/<program>::<output> answers the rows of a view, or of `state::entries`.
    GET /peek/<one>,<another> answers the rows of each, all of one state."""
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))     # so that being stopped also stops the server
    keeper = Keeper(RegisterLog(register), int(os.environ.get("KEEPER_WORKERS", "1")))     # the server's workers
    turn = threading.Condition()                              # held to read or change what is below; told of each change
    busy = threading.Lock()                                   # held by whoever judges, or reads the keeper: one at a time
    come = []                   # the requests that are not yet judged
    tickets = iter(range(1, 1 << 42))                         # given in order, to requests of frames and to transactions of text
    good = set()                # the tickets that were given to askers and not yet used
    keeper.beside = True
    held = []                   # answers that wait for the log: (the log's length each needs, where it goes, the answer)
    wrote = {"swaps": 0, "seconds": 0.0, "cut": 0, "deleted": 0, "compacted": 0}
    # For a page that watches: the appends made, and how long each request took, from coming in to its answer.
    heard = {"appends": 0, "asked": collections.deque(maxlen=50000)}
    def took(began):
        with turn: heard["asked"].append((time.time(), 1000 * (time.perf_counter() - began)))
    def lately(seconds=60):
        """The requests of the last minute: how many, and the median and nine-in-ten of how long they took, in ms."""
        since = time.time() - seconds
        with turn: ms = sorted(m for at, m in heard["asked"] if at >= since)
        return {"requests": len(ms), "median": round(ms[len(ms) // 2], 1) if ms else None,
                "nine": round(ms[min(len(ms) - 1, int(0.9 * len(ms)))], 1) if ms else None}
    # `busy` is taken before `turn`, and never the other way: whoever holds `turn` asks nothing of the keeper's server.

    def release():
        """Give each answer that the log now covers. Whoever calls this holds `turn`."""
        ready = [each for each in held if each[0] <= keeper.durable]
        held[:] = [each for each in held if each[0] > keeper.durable]
        for _, answer, value in ready: answer.append(value)
        turn.notify_all()

    def write():
        # The writer. It takes every record that waits, writes them, and moves the register once past them all.
        # Only then are they the followers' to have, and only then are the answers that needed them given.
        while True:
            with turn:
                turn.wait_for(lambda: keeper.unwritten)
                records, keeper.unwritten = keeper.unwritten, []
            begin = time.perf_counter()
            # The log is cut back behind the older of the two latest snapshots of the keeper's tables, and no further than
            # the latest snapshot of the entries a follower wrote (see follower.py): a keeper that starts reads from its
            # own latest snapshot on, and a follower from the followers' latest. The keeper does not know its followers:
            # one that is behind the cut starts again, from the followers' snapshot.
            cut = None
            with turn:
                snaps = [n for n in keeper.log.snaps if n < keeper.durable]
                if CUT and len(snaps) >= 2 and snaps[-2] > keeper.log.state.get("from", 0):
                    cut = min(snaps[-2], keeper.log.entries_at())
                    if cut <= keeper.log.state.get("from", 0): cut = None
            try: keeper.log.append_many([(said, whole) for _, said, whole, _ in records], cut)
            except Exception as error:
                # The log did not take them. What this keeper made of them was seen by no one, and it stops.
                print(f"keeper: {error}; stopping", flush=True)
                os._exit(1)
            if cut:
                # The register now says the log begins at `cut`: no reader is sent behind it, and its blobs can go.
                try: wrote["deleted"] += keeper.log.cut()
                except Exception as error: print(f"keeper: the log's blobs behind the cut were not all deleted: {error}", flush=True)
                wrote["cut"] = cut
            # The register keeps every revision unless it is compacted. It is, some way behind the keeper's own swaps.
            try: wrote["compacted"] = keeper.log.register.compact_behind(keeper.log.revision) or wrote["compacted"]
            except Exception as error: print(f"keeper: the register was not compacted: {error}", flush=True)
            with turn:
                keeper.durable += len(records)
                wrote["swaps"] += 1
                wrote["seconds"] += time.perf_counter() - begin
                release()
    threading.Thread(target=write, daemon=True).start()

    def judge():
        # A request of appends that is in hand: {"appends": frames, "runners": frames, "answer": where its answer goes,
        # "refused": {place: why}, "text": (actor, commands) if it came as text}, by its ticket.
        # `waits` is the tickets whose requests have a transaction waiting, and `back` the frames of what waits.
        hand, waits, back = {}, set(), ([], [])
        while True:
            with turn:
                turn.wait_for(lambda: come or waits)
                fresh, come[:] = list(come), []
            slow, answers = [], []
            for asked in fresh:
                if "frames" in asked: hand[asked["ticket"]] = asked
                elif plain(asked["commands"]):
                    # An append that came as text is made into a frame here, under a ticket of its own.
                    ticket = next(tickets)
                    appends = [((ticket * plumbing.SLOTS,), plumbing.parse(command)[2], 1) for command in asked["commands"]]
                    runners = [((ticket * plumbing.SLOTS,), (plumbing.parse(asked["actor"])[2],), 1)] if asked["actor"] != USER else []
                    hand[ticket] = dict(asked, text=True, refused={}, appends=[plumbing.encode(plumbing.APPENDS, appends, timed=False)],
                                        runners=[plumbing.encode(plumbing.RUNNERS, runners, timed=False)] if runners else [])
                else: slow.append(asked)
            with busy:
                try:
                    # Each is answered once the log holds all that was judged before the answer: a refusal too,
                    # since it may be of something made and not yet written.
                    while hand:
                        # Those that waited are in the round as what was handed back. Of the rest, the requests of
                        # least ticket go first, as many as keep the round's frames within `ROUND`.
                        was, order, size = set(waits), [], sum(map(len, back[0]))
                        for ticket in sorted(ticket for ticket in hand if ticket not in was):
                            size += sum(map(len, hand[ticket]["appends"]))
                            if order and size > ROUND: break
                            order.append(ticket)
                        appends = back[0] + [frame for ticket in order for frame in hand[ticket]["appends"]]
                        runners = back[1] + [frame for ticket in order for frame in hand[ticket]["runners"]]
                        waiting, refused, _, kept = keeper.judge(appends, runners)
                        if keeper.spoiled:
                            # The server would not take a frame. Whoever sent it is told, and the rest are judged again.
                            # (What waits came from the server itself, and is not what it would not take.)
                            owners = ([None] * len(back[0]) + [ticket for ticket in order for _ in hand[ticket]["appends"]]
                                      + [None] * len(back[1]) + [ticket for ticket in order for _ in hand[ticket]["runners"]])
                            spoilers = {owners[at] for at in keeper.spoiled} - {None}
                            if not spoilers: raise RuntimeError("the server did not take back what a round left waiting")
                            for ticket in spoilers:
                                answers.append((keeper.number, hand.pop(ticket)["answer"], {"error": "the server did not take a frame of this request"}))
                            continue
                        for key, why in refused.items():
                            if key // plumbing.SLOTS in hand: hand[key // plumbing.SLOTS]["refused"][key % plumbing.SLOTS] = why
                        waits, back = {key // plumbing.SLOTS for key in waiting} & set(hand), kept
                        for ticket in [ticket for ticket in sorted(was) + order if ticket in hand and ticket not in waits]:
                            done = hand.pop(ticket)
                            if "text" in done: answers.append((keeper.number, done["answer"], keeper.name(done["actor"], done["commands"]) if done["refused"] else []))
                            else: answers.append((keeper.number, done["answer"], {"refused": done["refused"]}))
                        # A round that left requests out, for its length, is followed at once by another.
                        if not any(ticket not in waits for ticket in hand): break
                    for asked in slow:
                        refused = keeper.transact(asked["actor"], asked["commands"])
                        answers.append((keeper.number, asked["answer"], refused))
                except Exception as error:
                    # Something is wrong. This keeper no longer knows the state, and stops.
                    print(f"keeper: {error}; stopping", flush=True)
                    keeper.close(); os._exit(1)
            with turn:
                held.extend(answers)
                release()
    threading.Thread(target=judge, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def answer(self, value):
            body = json.dumps(value).encode()
            self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
            self.wfile.write(body)
        def do_POST(self):
            began = time.perf_counter()
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if self.path == "/append":
                # A request of appends, as frames. They are not opened: its first line says which are which.
                request, frames = plumbing.unpack(body)
                answer = []
                with turn:
                    again = request["ticket"] not in good
                    if not again:
                        good.discard(request["ticket"])
                        if sum(map(len, frames)) > ROUND: answer.append({"error": "the frames of this request are longer than a round takes"})
                        else:
                            come.append({"frames": True, "ticket": request["ticket"], "appends": frames[:request["appends"]],
                                         "runners": frames[request["appends"]:], "answer": answer, "refused": {}})
                            turn.notify_all()
                            turn.wait_for(lambda: answer)
                    # The ticket for the asker's next request is given as this one is answered: it is after every
                    # ticket of a request that was in hand with this one.
                    ticket = next(tickets)
                    good.add(ticket)
                if again: return self.answer({"again": True, "ticket": ticket})
                found = dict({"refused": {}}, **answer[0], ticket=ticket)
                if "error" not in found:
                    with turn: heard["appends"] += sum(map(plumbing.rows_of, frames[:request["appends"]])) - len(found["refused"])
                took(began)
                return self.answer(found)
            if self.path == "/merges":
                # A pass of merges is taken between two rounds, whole. Its frames are passed on unopened.
                request, frames = plumbing.unpack(body)
                if sum(map(len, frames)) > ROUND:
                    return self.answer({"error": "the frames of this pass are longer than a chunk", "refused": [], "record": 0, "made": 0, "left": 0})
                one, two = request["proposed"], request["proposed"] + request["leaving"]
                with busy:
                    try: refused = keeper.merges(request["at"], frames[:one], frames[one:two], frames[two:])
                    except Exception as error:
                        print(f"keeper: {error}; stopping", flush=True)
                        keeper.close(); os._exit(1)
                    record, made, left, spoiled, beyond, forged = keeper.number, keeper.fused, keeper.left, keeper.spoiled, keeper.beyond, keeper.forged
                with turn:
                    turn.notify_all()
                    turn.wait_for(lambda: keeper.durable >= record)     # the pass is answered once it is in the log
                found = {"refused": refused, "record": record, "made": made, "left": left, "beyond": beyond, "forged": forged}
                return self.answer(dict(found, error="this pass was not made: " + spoiled[0]) if spoiled else found)
            request = json.loads(body)
            asked = request.get("many", [request])
            answers = [[] for _ in asked]
            with turn:
                for one, answer in zip(asked, answers): come.append({"actor": one["actor"], "commands": one["commands"], "answer": answer})
                turn.notify_all()
                turn.wait_for(lambda: all(answers))
            took(began)
            self.answer({"refused": [answer[0] for answer in answers] if "many" in request else answers[0][0]})
        def do_GET(self):
            if self.path == "/ticket":
                with turn:
                    ticket = next(tickets)
                    good.add(ticket)
                return self.answer({"ticket": ticket})
            # One who reads waits until all that was judged is in the log, and nothing more is judged while it waits:
            # what it reads could otherwise still be lost.
            with busy:
                with turn: turn.wait_for(lambda: keeper.durable == keeper.number)
                if self.path == "/counts":
                    found = {"rounds": keeper.rounds, "alone": keeper.fast + keeper.slow, "records": keeper.number,
                             "swaps": wrote["swaps"], "writing": wrote["seconds"], "cut": wrote["cut"], "deleted": wrote["deleted"],
                             "compacted": wrote["compacted"], "passes": keeper.passes, "appends": heard["appends"], "lately": lately()}
                else:
                    # Several views named together are read between two transactions, and so are of one state.
                    names = self.path.split("/peek/", 1)[1].split(",")
                    rows = [keeper.read(name) for name in names]
                    found = rows[0] if len(names) == 1 else rows
            self.answer(found)
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
        check(f"{2 * steps} transactions of appends are made, each a round of one", k.fast == 2 * steps,
              f"{1000 * elapsed / (2 * steps):.2f} ms each")
        standing = dict(row.split(" val=") for row in k.read("views::standing"))
        at = f"tuple({steps},{frontier(steps)})"
        check("the three ledgers stand where they were appended to", all(standing[f"tuple({ident(l)})"] == at for l in (1, 2, 9)))
        check("the follower stands at its clock", standing[f"tuple({ident(8)})"] == f"tuple(0,{frontier(steps)})")
        check("the keeper holds no span", not [row for row in k.ask("peek state::entries") if row.startswith("tuple(inject(1,")])
        rows = {row.split(" val=")[0]: row.split(" val=")[1] for row in k.ask("peek rows::own")}
        check("and one row for each of its five ledgers", len(rows) == 5 and rows[f"tuple({ident(1)})"].startswith(f"tuple({steps},{frontier(steps)},"), str(sorted(rows)))

        # Appends that admission turns away.
        check("a user is barred from a bound ledger", k.transact(USER, [append(2, steps, steps, steps + 1)]) == ["barred"])
        check("an append at an old number is stale", k.transact(USER, [append(1, 0, steps, steps + 1)]) == ["stale"])
        check("an append to a derived ledger is absent", k.transact(runner(101), [append(3, 0, 0, 1)]) == ["absent"])

        # The rest goes by the rules.
        begin, before = time.perf_counter(), k.slow
        check("a durable ledger written from the state is belated", k.transact(USER, [define(102, 71, ["state"], [8])]) == ["belated"])
        check("a ledger that is read, and named, cannot be dropped", k.transact(USER, [drop(1)]) == ["misnamed", "unregistered"])
        # A span is not the rules' to judge in this keeper: a merge is asked for another way (see `merges`).
        check("a merge is not taken as a transaction", k.transact(USER, [merge(1, 0, steps, 0)]) == ["spans"])
        check("nor an append beside another command", k.transact(USER, [append(1, steps, steps, steps + 1), alias(2, 2)]) == ["spans"])
        check("a hold is made", k.transact(USER, [hold(900, 1, steps)]) == [])
        # A reader and a merge, in either order. Ledger 2 is read by no computation. A holder comes first, from 3.
        check("a hold of a ledger no merge has compacted is made, from anywhere", k.transact(USER, [hold(902, 2, 3)]) == [])
        # A pass of merges: what a follower's policy proposes, and the entries the merges take out, as frames, and the
        # log's length as the compactor's follower had it. The frames are made here by a server, as a follower's are.
        span = lambda ledger, a, b, since: (f"inject(1,tuple(tuple({ident(ledger)},{a},{b},{frontier(since)}),"
                                            f"tuple({frontier(a)},{frontier(b)},inject(0,tuple()))))")
        k.ask(f'load handed begin\n{declarations()}let proposed = input 0 : ((int), (Id, int, int, Frontier, Frontier, Frontier));\n'
              'let leaving = input 1 : ((int), (Id, Entry));\noutput "proposed" = proposed;\noutput "leaving" = leaving;\nhanded end-load')
        def frames(proposed, leaving):
            """Frames of some merges, each (ledger, from, to, since), and of some entries that leave, each (ledger, entry)."""
            fed = ([f"feed handed 0 tuple(0) val=tuple({ident(l)},{a},{b},{frontier(s)},{frontier(a)},{frontier(b)})" for l, a, b, s in proposed]
                   + [f"feed handed 1 tuple(0) val=tuple({ident(l)},{entry})" for l, entry in leaving])
            k.many(fed + ["tick"])
            found = k.ask("peek handed::proposed as corgi"), k.ask("peek handed::leaving as corgi")
            k.many(["feed handed 0 from handed::proposed diff=-1", "feed handed 1 from handed::leaving diff=-1", "tick"])
            return found
        whole = frames([(1, 0, steps, 0)], [(1, span(1, t, t + 1, t)) for t in range(steps)] + [(2, span(2, 0, 1, 0))])
        read = k.number
        check("a merge is made, of a ledger not merged since its compactor read, and takes out its own ledger's entries and no other's",
              k.merges(read, *whole) == [] and (k.fused, k.left) == (1, steps), f"{k.fused} merge, {k.left} entries left")
        check("one asked for by a compactor that read before that merge is refused", k.merges(read, *whole) == [ident(1)] and k.late == [ident(1)])
        both = frames([(1, 0, steps, 0), (2, 0, steps, 0)], [(2, span(2, t, t + 1, t)) for t in range(steps)])
        check("and does not hold back another ledger's in the same pass", k.merges(read, *both) == [ident(1)] and (k.fused, k.left) == (1, steps))
        check("one that read after it is made", k.merges(k.number, *frames([(1, 0, steps, 0)], [(1, span(1, 0, steps, 0))])) == [])
        # A merge that takes nothing out removes nothing: it is made whatever it is held from, beside what it covers.
        # Ledger 1 has no own hold yet: it is read from its start, 0.
        check("a merge that takes nothing out is made, held from later than the ledger's own hold: it stands beside what it covers",
              k.merges(k.number, *frames([(1, 0, steps, 5)], [])) == [] and k.left == 0)
        check("one that takes a span out, held from after the own hold, is refused",
              k.merges(k.number, *frames([(1, 0, steps, steps)], [(1, span(1, 0, steps, 0))])) == [ident(1)] and k.beyond == [ident(1)] and k.late == [] and k.forged == [])
        check("one that takes out what it does not cover is refused as forged",
              k.merges(k.number, *frames([(1, 0, steps // 2, 0)], [(1, span(1, 0, steps, 0))])) == [ident(1)] and k.forged == [ident(1)])
        check("and one that takes out an entry of another ledger", k.merges(k.number, *frames([(1, 0, steps, 0)], [(1, span(2, 0, steps, 0))])) == [ident(1)] and k.forged == [ident(1)])
        # The own hold of ledger 1 (holder 0). Computation 100 reads ledger 1 from where ledger 2 stands, `steps`; holder 900 holds it from `steps`.
        check("its own hold cannot move past where a reader needs it", k.transact(USER, [hold(0, 1, steps + 1)]) == ["forgotten"])
        check("and moves to where its readers need it", k.transact(USER, [hold(0, 1, steps)]) == [])
        check("a new hold from before the own hold is refused", k.transact(USER, [hold(901, 1, steps - 1)]) == ["forgotten"])
        check("and one from it is made", k.transact(USER, [hold(901, 1, steps)]) == [])
        check("a computation whose output would stand before the own hold of what it reads is refused",
              k.transact(USER, [create(11), define(103, 71, [1], [11])]) == ["forgotten"])
        check("one whose output stands at it is made", k.transact(USER, [create(11, start=steps), define(103, 71, [1], [11])]) == [])
        check("one whose new output follows a clock that stands past it is made too",
              k.transact(USER, [create(12, clock=9), define(104, 71, [1], [12])]) == [])
        check("a merge held from the own hold may now take out what it covers: both versions there, the one as written and the one held from 5",
              k.merges(k.number, *frames([(1, 0, steps, steps)], [(1, span(1, 0, steps, 0)), (1, span(1, 0, steps, 5))])) == [] and k.left == 2)
        # Two compactors that read the same state: the second is refused by the ledger's generation.
        read = k.number
        check("two compactors read one state, and the first's merge is made", k.merges(read, *frames([(2, 0, steps, 0)], [(2, span(2, t, t + 1, t)) for t in range(steps)])) == [])
        check("the second's is refused: the generation it read has moved", k.merges(read, *frames([(2, 0, steps, 0)], [(2, span(2, t, t + 1, t)) for t in range(steps)])) == [ident(2)] and k.late == [ident(2)])
        gen = lambda ledger: [row.split(" val=")[1] for row in k.ask("peek gens::rows") if row.startswith(f"tuple({ident(ledger)})")]
        check("a ledger's generation is the log's length at its last pass", gen(2) == [f"tuple({read + 1})"], str(gen(2)))
        # A compactor that made a batch for a merge says so, in a frame it writes itself: the entry that arrives names it.
        blob = list(range(32))
        named = plumbing.encode(plumbing.BATCHES, [(((0, 1),), (("inject", 2, blob),), 1)], timed=False)
        check("a merge whose batch the compactor made is made", k.merges(k.number, *frames([(1, 0, steps, steps)], [(1, span(1, 0, steps, steps))]), [named]) == [] and k.fused == 1)
        arrived = [key[0] for record, held in FileLog(log).read()[-1:] for frame in held[:record["entries"]] for key, _, diff, _ in plumbing.decode(frame) if diff > 0]
        check("and the entry it puts in names that batch", arrived == [("inject", 1, (((0, 1), 0, steps, [[steps]]), ([[0]], [[steps]], ("inject", 2, blob))))], str(arrived)[:200])
        check("a pass with a frame the server will not take is turned away, and the keeper goes on",
              k.merges(k.number, [b"not a frame"], []) == [] and k.spoiled != [] and k.transact(USER, [alias(3, 2)]) == [], str(k.spoiled)[:120])
        check("the keeper still holds no span", not [row for row in k.ask("peek state::entries") if row.startswith("tuple(inject(1,")])
        check("a ledger is registered, and has a row", k.transact(USER, [create(7, start=5)]) == []
              and [row for row in k.ask("peek rows::own") if row.startswith(f"tuple({ident(7)}) val=tuple(0,{frontier(5)},")] != [])
        check("and takes an append", k.transact(USER, [append(7, 0, 5, 6)]) == [])
        # A ledger with appends is dropped with its spans. Ledger 13 has two appends, which a merge makes one span.
        check("a ledger with appends, merged, and with an own hold, is set up", k.transact(USER, [create(13)]) == [] and k.transact(USER, [append(13, 0, 0, 1)]) == []
              and k.transact(USER, [append(13, 1, 1, 2)]) == [] and k.merges(k.number, *frames([(13, 0, 2, 0)], [(13, span(13, 0, 1, 0)), (13, span(13, 1, 2, 1))])) == []
              and k.transact(USER, [hold(0, 13, 1)]) == [])
        check("a ledger with appends is dropped", k.transact(USER, [drop(7)]) == []
              and not [row for row in k.ask("peek rows::own") if row.startswith(f"tuple({ident(7)})")])
        check("and one that was merged, and held by itself, loses its row, its generation and its own hold", k.transact(USER, [drop(13)]) == [] and gen(13) == []
              and not [row for row in k.ask("peek rows::own") if row.startswith(f"tuple({ident(13)})")]
              and not [row for row in k.ask("peek own::holds") if row.startswith(f"tuple({ident(13)})")])
        last = [each for each in FileLog(log).read(whole=True) if "actor" in each[0]][-1]
        left = [(key[0], diff) for frame in last[1][:last[0]["entries"]] for key, _, diff, _ in plumbing.decode(frame)]
        check("the drop's record says the ledger and its own hold leave, and that it drops one: its spans are the followers' to take out",
              sorted((entry[1], diff) for entry, diff in left) == [(0, -1), (6, -1)] and last[0].get("drops") == 1, str(left)[:300])
        said = k.transact(USER, [drop(2)])
        check("nor one that a computation writes, a name names, and a holder holds", said == ["misnamed", "unregistered", "misheld"], str(said))
        k.many(["drop handed", "tick"])
        check("a ledger with no appends is dropped, and its row is gone", k.transact(USER, [create(6)]) == [] and k.transact(USER, [drop(6)]) == []
              and not [row for row in k.ask("peek rows::own") if row.startswith(f"tuple({ident(6)})")])
        check("an append to it is then absent", k.transact(USER, [append(6, 0, 0, 1)]) == ["absent"])
        judged = k.slow - before
        check(f"{judged} transactions are judged by every rule", judged == 20,
              f"{1000 * (time.perf_counter() - begin) / judged:.0f} ms each")

        check("the state keeps every rule", k.audit() == [])
        entries = sorted(k.ask("peek state::entries"))
        check("with no transaction in hand, the trial state is the state", sorted(k.ask("peek trial::entries")) == entries)

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
        standing = {ledger: at for ledger, at in plumbing.stands(k.read("views::standing")).items() if ledger in (1, 2, 8, 9)}
        check("the ledgers stand where the rounds left them",
              standing == {1: (t + 1, t + 1), 2: (t + 1, t + 1), 9: (t + 2, t + 2), 8: (1, t + 2)}, str(standing))
        # A round of frames, as askers make them (see `Client.append`): each transaction's key is its request's ticket
        # times SLOTS, plus its place. Ticket 7 asks two transactions, and ticket 8 three.
        S, EMPTY = plumbing.SLOTS, plumbing.EMPTY
        one = lambda ticket, place, ledger, number, lower, upper: ((ticket * S + place,), ((0, ledger), number, [[lower]], [[upper]], EMPTY), 1)
        asks = [plumbing.encode(plumbing.APPENDS, [one(7, 0, 1, t + 1, t + 1, t + 2), one(7, 1, 2, t + 1, t + 1, t + 2)], timed=False),
                plumbing.encode(plumbing.APPENDS, [one(8, 0, 1, t + 1, t + 1, t + 2), one(8, 1, 3, 0, 0, 1),
                                                   one(8, 2, 9, t + 2, t + 2, t + 3), one(8, 2, 9, t + 3, t + 3, t + 4)], timed=False)]
        waiting, refused, made, kept = k.judge(asks, [])
        check("a round of frames makes what it can: one append is made, two are barred (one of them to a derived ledger), one waits, and one that appends twice is refused",
              (waiting, refused, made) == ({8 * S}, {7 * S + 1: "barred", 8 * S + 1: "barred", 8 * S + 2: "refused"}, 1), str((waiting, refused, made)))
        check("what waits is handed back as frames, and the next round refuses it: its ledger has moved",
              k.judge(*kept)[:3] == (set(), {8 * S: "refused"}, 0))
        by = plumbing.encode(plumbing.RUNNERS, [((9 * S,), ((0, 100),), 1)], timed=False)
        check("a runner's append comes with a frame that says who it is", k.judge([plumbing.encode(plumbing.APPENDS, [one(9, 0, 2, t + 1, t + 1, t + 2)], timed=False)], [by])[:3] == (set(), {}, 1))
        check("a round with a frame the server will not take judges nothing, and says which", k.judge([asks[0], b"not a frame"], [])[2] == 0 and k.spoiled == [1])
        standing = {ledger: at for ledger, at in plumbing.stands(k.read("views::standing")).items() if ledger in (1, 2, 8, 9)}
        check("the ledgers stand where those rounds left them", standing == {1: (t + 2, t + 2), 2: (t + 2, t + 2), 9: (t + 2, t + 2), 8: (1, t + 2)}, str(standing))
        check("the state keeps every rule after the rounds", k.audit() == [])
        entries = sorted(k.ask("peek state::entries"))
        check("with no transaction in hand, the trial state is the state", sorted(k.ask("peek trial::entries")) == entries)
        rows, gens = sorted(k.ask("peek rows::own")), sorted(k.ask("peek gens::rows"))
        k.close()

        # A keeper that starts again reads the log, and arrives at the same state.
        begin = time.perf_counter()
        k = Keeper(log)
        check("a keeper that starts again has the same state", sorted(k.ask("peek state::entries")) == entries,
              f"{len(FileLog(log).read())} records read back in {time.perf_counter() - begin:.1f} s, each round's frames judged again whole")
        check("and the same rows, made again by the rounds", sorted(k.ask("peek rows::own")) == rows)
        check("and the same generations, from the marks the log kept", sorted(k.ask("peek gens::rows")) == gens and len(gens) == 2, str(gens))
        check("and takes the next append", k.transact(runner(100), [append(2, steps + 2, steps + 2, steps + 3)]) == [])
    finally:
        k.close()
        shutil.rmtree(folder)


if __name__ == "__main__":
    if sys.argv[1:2] == ["serve"]: serve(sys.argv[2], int(sys.argv[3]))
    else: main()
