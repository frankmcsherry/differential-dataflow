#!/usr/bin/env python3
"""A follower of the state: a copy of the entries, the views of them, and what each instance is told.

  python3 interactive/examples/ledger/follower.py serve <register> <keeper url> <port> [role]
  python3 interactive/examples/ledger/follower.py peek <follower> <view> ..             ask a follower, and print what it says

A keeper decides what is made, and writes it down. A follower decides nothing. It reads what was written, keeps
a copy of the state that results, and answers those who only read, so that reading asks nothing of the keeper.
There may be several followers of one keeper. Each is some way behind it, and none is ever ahead.

Where the records come from: the log. The register names the latest record, and the blobs hold every record.
A follower watches the register (etcd's watch: a request that is answered when the cell changes), and reads each new
record from the store. The keeper does nothing for followers, and does not know them. If the watch ends, the follower
reads the register, takes what is new, and watches again from there; it also reads the register now and then, in case.
What a record changed is frames (see plumbing.py). The follower feeds them to its server as they are.

A record that drops a ledger says so. The keeper holds no spans, so the record does not hold the dropped ledger's
spans: each follower takes them out of its copy itself, as the spans no registered ledger has (`orphans`, below).
Every follower finds the same ones at the same record.

A follower that serves writes, now and then, a snapshot of its entries to the store, and names it in a cell of the
register beside the keeper's (`<register>:entries`): the log's length then, and the snapshot's blobs. A follower that
starts takes the latest such snapshot, and reads the log from there on. The keeper cuts its log no further back than
that (see keeper.py). The keeper's own snapshots hold no spans, and are not a follower's to start from.

Standing programs on the follower's own server, for a follower that does everything (the role `all`):
  state    the entries, as the records leave them
  bundle   bundle.ddp of them: what each instance is told, and the instances each machine has
  checked  the entries again, for the two programs below: it is brought up to `state` only when one of them is read
  views    system.ddp of `checked`: the model's views, and the keeper's copy of its rules
  tidy     compact.ddp of `checked`: which spans of each ledger a compactor should merge next
So what an instance is told of a record does not wait for the rules to be worked out for it.
A follower may instead be started with a role, and then keeps only that role's programs (see `Follower`):
`serve` has `state` and `bundle`; `tidy` has `checked`, needs.ddp and `tidy`; `audit` has `checked` and the rules in full.

What a reader may ask, as one line of JSON to the port:
  {"peek": [names]}                          the rows of each view named, all of one state, as text
  {"frames": [names], "after": at}           the same, as frames, once the follower has `at` records of the log
  {"where": 1}                               how many records it has, and how many frames its views have not been fed
  {"tail": view, "key": key, "after": at}    the rows of `bundle` or `roster` at a key, and then each change to them
A tail is answered with {"at": number} and frames (see `pack` in plumbing.py): first every row, then, after each
record that changes a row, the changes. What is sent up to any point is of one state, the one after `at` records.
The frames are the ones the server's own tail gave (`tail .. as corgi`), and the follower does not open them.
A tail is not state. It is there while its reader's connection is, and what it took to keep it is dropped after.
Each tail is a small dataflow here: a filter of the view to the one key.
A peek is answered with a line of JSON that holds the rows as text: it is for people, and for the stewards.
"""
import http.client, json, os, queue, signal, socket, sys, threading, time, urllib.error, urllib.parse
import keeper as model
import plumbing

DIR = "interactive/examples/ledger"
# Whether a follower of the compaction policy feeds its views as records come, and how long it gathers records for each feed.
EAGER, GATHER = os.environ.get("FOLLOWER_EAGER", "1") == "1", float(os.environ.get("FOLLOWER_GATHER", "0.05"))
# The rules a follower that audits keeps: the model's, in full, unless another file is named; if it does not load,
# the keeper's copy (system.ddp).
FULL = os.environ.get("FOLLOWER_RULES", f"{DIR}/system-full.ddp")
MOST = 1 << 14      # how many frames a follower that is only read now and then keeps unfed, at most
BEHIND = 10000      # a reader that has this many lines waiting for it is let go, and may ask again
LATE = 64           # how many frames of records the views' copy of the state may be owed before it is given them
SNAP = int(os.environ.get("FOLLOWER_SNAP", "500"))     # how many records there are between one snapshot of the entries and the next
POLL = 2.0          # how often a follower reads the register, beside its watch, in case the watch missed a change

# Each view that may be tailed at a key: the shape of its rows, and how a row is tested for a key.
KEYED = {
    "bundle": ("((Id), (Entry))", lambda key: f"(((machine, number)), _) -> ((machine, {int(key[0])}) eq, (number, {int(key[1])}) eq) all"),
    "roster": ("((int), (Id))", lambda key: f"((machine), _) -> (machine, {int(key)}) eq"),
}


class Reader:
    """A connection that holds a tail: what there is to send it, and a thread that sends it."""
    def __init__(self, sock):
        self.sock, self.lines = sock, queue.Queue()
        threading.Thread(target=self.run, daemon=True).start()

    def send(self, message):
        # A reader that does not keep up is not waited for, and what is kept for it is bounded.
        if self.lines.qsize() < BEHIND: self.lines.put(message)
        else: self.close()

    def close(self):
        """End the connection, which ends the tail, and the thread."""
        self.lines.put(None)
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass

    def run(self):
        try:
            while (message := self.lines.get()) is not None: self.sock.sendall(message)
        except OSError: pass


class Follower:
    """A follower has a role, and does only that role's work:
      all     everything below, as before: what instances are told, the rules, and the compaction policy
      serve   the entries, and bundle.ddp: what each instance is told, and the instances each machine has
      tidy    the entries, what each ledger is needed from (needs.ddp), and compact.ddp: what a compactor reads
      audit   the entries, and the rules: every rule over the state, read after the fact
    A follower that serves feeds each record as it comes, since tails wait on it. One that keeps only the policy feeds
    what has come when it is asked, all at once. One that audits feeds every `LATE` frames, and when it is asked."""
    def __init__(self, register, url, port, role="all"):
        self.role = role
        self.register, self.blobs = plumbing.Register(register), plumbing.Blobs("log")
        self.keeper = urllib.parse.urlparse(url)
        self.at = 0                         # the number of records this copy of the state has had
        self.turn = threading.Condition()   # held by whoever asks the server; told of each record
        self.readers, self.count = {}, 0    # each tail's reader, by the tail's request id
        self.server = plumbing.Server(port + 1, int(os.environ.get("FOLLOWER_WORKERS", "1")))     # the server's workers
        types = model.declarations()
        # ORPHANS: the spans of a ledger that is not registered, which a follower takes out itself after a drop.
        orphans = ('let table = entries | distinct;\n'
                   'let placed = table((Span((key, val))), ()) | map((((id, from, to, since), (lower, upper, batch)), ()) -> ((id), (from, to, since, lower, upper, batch)));\n'
                   'let registered = table((Ledger((key, val))), ()) | map(((key, _), ()) -> (key, ())) | distinct;\n'
                   'let lost = placed - (placed | join(registered, (id, rest, ()) -> (id, rest)));\n'
                   'output "orphans" = lost | map(((id), (from, to, since, lower, upper, batch)) -> ((((id, from, to, since), (lower, upper, batch)) inject Span), ()));\n')
        entries = lambda name: f'load {name} begin\n{types}let entries = input 0 : ((Entry), ());\noutput "entries" = entries;\n{orphans}{name} end-load'
        self.serves = role in ("all", "serve")      # whether `state` is fed, record by record, for those who tail
        if self.serves:
            self.server.ask(entries("state"))
            self.server.ask(f"load bundle from {DIR}/bundle.ddp input0=state::entries")
        if role != "serve": self.server.ask(entries("checked"))
        if role == "all":
            self.server.ask(f"load views from {DIR}/system.ddp input0=checked::entries")
            self.server.ask(f"load tidy from {DIR}/compact.ddp input0=checked::entries input1=views::needs")
        if role in ("tidy", "all"):
            # Each ledger's own hold, for the steward that moves them (see steward.py and own.ddp).
            self.server.ask(f"load own from {DIR}/own.ddp input0=checked::entries")
        if role == "tidy":
            self.server.ask(f"load needs from {DIR}/needs.ddp input0=checked::entries")
            self.server.ask(f"load tidy from {DIR}/compact.ddp input0=checked::entries input1=needs::needs")
        if role == "audit":
            try: self.server.ask(f"load views from {FULL} input0=checked::entries")
            except RuntimeError as error:
                print(f"follower: the rules at {FULL} did not load ({str(error)[:200]}); this tree's are kept", flush=True)
                self.server.ask(f"load views from {DIR}/system.ddp input0=checked::entries")
        self.owed = []                      # the frames of records that `checked` does not yet have
        self.dropping = False               # whether a record among those owed dropped a ledger
        self.snapped = 0                    # the log's length at the last snapshot of the entries this follower wrote
        self.cell = plumbing.Register(register + ":entries")
        self.server.ask("tick")
        self.types = types
        while True:
            # Readers are not answered until this copy has all that the log has. The log may not be there yet.
            try: self.apply(self.written()); break
            except OSError: time.sleep(0.2)
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port)); listener.listen(256)
        threading.Thread(target=self.accept, args=(listener,), daemon=True).start()
        if role == "tidy" and EAGER: threading.Thread(target=self.feeder, daemon=True).start()

    def feeder(self):
        """Feed the views as records come, a moment's worth at a time, for as long as this runs.
        A compactor asks, then reads and writes blobs and asks the keeper, and asks again: the views are brought up
        while it does that, so that its next answer waits only for what has come since, and not for a whole pass's."""
        while True:
            with self.turn: self.turn.wait_for(lambda: self.owed)
            time.sleep(GATHER)
            with self.turn: self.flush()

    def written(self):
        """The records of the log that this copy has not had, in order: of each, the frames that are entries, and
        whether it dropped a ledger. A record's first line says how many of its frames are entries: they come first,
        and the rest are the keeper's. A follower that starts takes the followers' latest snapshot of the entries first."""
        state, self.revision = self.register.read()
        # The log may have been cut back behind what this copy has: it then starts again, from the latest snapshot.
        if self.at and state and state.get("from", 0) > self.at: self.again(f"the log now begins at record {state['from']}, and this copy has {self.at}")
        if self.at == 0 and os.environ.get("FOLLOWER_WHOLE") != "1":
            snap, _ = self.cell.read()
            if snap and state and snap["at"] <= state["count"]:
                frames = []
                for name, _ in snap["parts"]: frames += plumbing.unpack(self.blobs.get(name))[1]
                self.at = self.snapped = snap["at"]
                rest = self.written()
                # The snapshot is taken as one record, which brings the copy to where it was written.
                self.at -= 1
                return [(frames, True)] + rest
        records, name = [], state and state["head"]
        for _ in range(state["count"] - self.at if state else 0):
            try: record, frames = plumbing.unpack(self.blobs.get(name))
            except urllib.error.HTTPError as error:
                if error.code == 404 and self.at: self.again(f"a record this copy needs was cut from the log")
                raise
            frames = plumbing.expand(record, frames, self.blobs)
            name = record["prev"]
            entries = record.get("entries", len(frames))
            records.append((frames[:entries], bool(record.get("drops"))))
        return records[::-1]

    def again(self, why):
        """Start this follower again, as a new process with the same arguments: it then reads the log from the latest
        snapshot on. Its server is stopped first, so that the new one can have its port. Those who tail it ask again."""
        print(f"follower ({self.role}): {why}; it starts again, from the latest snapshot", flush=True)
        self.server.close()
        os.execv(sys.executable, [sys.executable] + sys.argv)

    def apply(self, records):
        """Make the changes of some records, the next in order, as one change of the copy. Each tail hears of its part.
        A record here is its frames."""
        if not records: return
        feeds = [(f"feed state 0 frame {len(frame)}", frame) for frames, _ in records for frame in frames]
        drops = any(dropping for _, dropping in records)
        with self.turn:
            if self.serves:
                self.server.many(feeds + ["tick"])
                # A record that dropped a ledger: its spans leave too, now that it is not registered.
                if drops: self.server.many(["feed state 0 from state::orphans diff=-1", "tick"])
            self.at += len(records)
            # The views are owed these records. They are not left too far behind: how far depends on who reads them.
            if self.role != "serve":
                self.owed += [frame for frames, _ in records for frame in frames]
                self.dropping = self.dropping or drops
            if len(self.owed) > (MOST if self.role == "tidy" else LATE): self.flush()
            # The tails are on the same connection as the tick, so each has sent its frames of the tick by now.
            for ident, reader in self.readers.items():
                sent, self.server.tails[ident] = self.server.tails[ident], []
                frames = [frame for kind, frame in sent if kind == "data"]
                if frames: reader.send(plumbing.pack({"at": self.at}, frames))
            self.turn.notify_all()

    def flush(self):
        """Bring the copy of the state that the views read up to this one. Whoever calls holds `turn`."""
        if self.owed:
            self.server.many([(f"feed checked 0 frame {len(frame)}", frame) for frame in self.owed] + ["tick"])
            if self.dropping: self.server.many(["feed checked 0 from checked::orphans diff=-1", "tick"])
            self.owed, self.dropping = [], False

    def snapshot(self):
        """Write this copy's entries to the store, and name them in the register's cell for followers, if this copy is
        later than the one named there. Whoever calls holds `turn`. Only a follower that serves writes them: its copy
        is fed as records come. The snapshot two back is deleted: one that starts may still be reading the last."""
        if not self.serves or self.at - self.snapped < SNAP: return
        frames = [frame for frame in self.server.ask("peek state::entries as corgi") if plumbing.rows_of(frame)]
        groups = plumbing.split(frames, plumbing.CHUNK)
        pieces = [plumbing.squeeze(plumbing.pack({}, group)) for group in groups]
        names = self.blobs.many(lambda data: self.blobs.put(data, stored=True), pieces)
        found, revision = self.cell.read()
        if found and found["at"] >= self.at: return
        mine = {"at": self.at, "parts": [[name, len(group)] for name, group in zip(names, groups)],
                "older": found["parts"] if found else []}
        if self.cell.swap(revision, mine) is None: return
        self.snapped = self.at
        for name, _ in (found or {}).get("older", []):
            try: plumbing.http("DELETE", self.blobs.base + name)
            except urllib.error.URLError: pass

    def follow(self):
        """Take records for as long as this runs. A thread watches the register, and wakes this when it changes, or
        every `POLL` seconds in any case: each time, what is new in the log is read and fed."""
        woken = threading.Event()
        def watch():
            while True:
                try: self.register.changes(self.revision, woken.set)
                except Exception: pass
                woken.set()
                time.sleep(0.2)
        # FOLLOWER_WATCH=0 stands for a watch that is down: the follower then hears only by reading the register.
        if os.environ.get("FOLLOWER_WATCH", "1") == "1": threading.Thread(target=watch, daemon=True).start()
        while True:
            woken.wait(POLL)
            woken.clear()
            try:
                self.apply(self.written())
                with self.turn: self.snapshot()
            except (OSError, ValueError, EOFError, RuntimeError) as error:
                print(f"follower ({self.role}): {str(error)[:200]}", flush=True)
                time.sleep(0.2)

    def accept(self, listener):
        while True:
            sock, _ = listener.accept()
            threading.Thread(target=self.answer, args=(sock,), daemon=True).start()

    def answer(self, sock):
        ident = program = None
        try:
            request = json.loads(sock.makefile("rb").readline())
            # A follower that does not serve keeps the entries once, for its views: they are what is asked for as the state.
            named = lambda name: name if self.serves else name.replace("state::", "checked::")
            if "where" in request:
                # How many records this copy has, and how many frames of them its views have not been fed. Nothing is fed for this.
                with self.turn: sock.sendall(json.dumps({"at": self.at, "owed": len(self.owed)}).encode() + b"\n")
                return
            if "peek" in request:
                with self.turn:
                    self.flush()
                    rows = [self.server.ask("peek " + named(name)) for name in request["peek"]]
                sock.sendall(json.dumps({"at": self.at, "rows": rows}).encode() + b"\n")
                return
            if "frames" in request:
                with self.turn:
                    # One who has heard of a later state than this copy is of waits until the copy has reached it.
                    self.turn.wait_for(lambda: self.at >= request.get("after", 0))
                    self.flush()
                    found = [self.server.ask(f"peek {named(name)} as corgi") for name in request["frames"]]
                sock.sendall(plumbing.pack({"at": self.at, "counts": [len(frames) for frames in found]}, [frame for frames in found for frame in frames]))
                return
            view, key = request["tail"], request.get("key")
            with self.turn:
                # A reader that has heard of a later state than this copy is of waits until the copy has reached it.
                self.turn.wait_for(lambda: self.at >= request.get("after", 0))
                source = f"bundle::{view}"
                if key is not None:
                    self.count += 1
                    name, (shape, test) = f"reader{self.count}", KEYED[view]
                    text = f'{self.types}let rows = input 0 : {shape};\noutput "mine" = rows | filter({test(key)});\n'
                    self.server.ask(f"load {name} input0={source} begin\n{text}{name} end-load")
                    program, source = name, f"{name}::mine"
                    self.server.ask("tick")
                frames = self.server.ask(f"tail {source} as corgi")
                ident = self.server.last
                self.server.tails[ident] = []
                self.readers[ident] = Reader(sock)
                self.readers[ident].send(plumbing.pack({"at": self.at}, frames))
            while sock.recv(4096): pass     # the tail is there until its reader's connection ends
        except (OSError, ValueError, KeyError): pass
        except RuntimeError as error: print(f"follower: {error}", file=sys.stderr, flush=True)
        finally:
            with self.turn:
                if ident in self.readers: self.readers.pop(ident).close()
                try:
                    if ident: self.server.ask(f"stop {ident}")
                    if program: self.server.many([f"drop {program}", "tick"])
                except OSError: pass        # the follower is ending, and its server with it
                self.server.tails.pop(ident, None)
            sock.close()


if __name__ == "__main__":
    if sys.argv[1] == "peek":
        print(json.dumps(plumbing.Follower(sys.argv[2]).peek(*sys.argv[3:])))
        sys.exit(0)
    for sign in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sign, lambda *_: sys.exit(0))            # so that being stopped also stops the server
    follower = Follower(sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5] if len(sys.argv) > 5 else "all")
    try: follower.follow()
    finally: follower.server.close()
