#!/usr/bin/env python3
"""A follower of the state: a copy of the entries, the views of them, and what each instance is told.

  python3 interactive/examples/ledger/follower.py serve <register> <keeper url> <port>
  python3 interactive/examples/ledger/follower.py peek <follower> <view> ..             ask a follower, and print what it says

A keeper decides what is made, and writes it down. A follower decides nothing. It reads what was written, keeps
a copy of the state that results, and answers those who only read, so that reading asks nothing of the keeper.
There may be several followers of one keeper. Each is some way behind it, and none is ever ahead.

Where the records come from:
  the keeper   sends each record as it makes it (GET /follow/<number> of keeper.py), which is quick
  the log      the register and the blobs hold every record, and are right whatever the keeper sent
A follower that starts reads the log. After that it takes what the keeper sends, and reads the log again
whenever the keeper is away or the numbers it sends skip.
What a record changed is frames (see plumbing.py), from either place. The follower feeds them to its server as they are.

Standing programs on the follower's own server:
  state    the entries, as the records leave them
  views    system.ddp of them: the model's views, and its rules, which are read here and not by the keeper
  bundle   bundle.ddp of them: what each instance is told, and the instances each machine has
  tidy     compact.ddp of them: which spans of each ledger a compactor should merge next

What a reader may ask, as one line of JSON to the port:
  {"peek": [names]}                          the rows of each view named, all of one state, as text
  {"frames": [names]}                        the same, as frames
  {"tail": view, "key": key, "after": at}    the rows of `bundle` or `roster` at a key, and then each change to them
A tail is answered with {"at": number} and frames (see `pack` in plumbing.py): first every row, then, after each
record that changes a row, the changes. What is sent up to any point is of one state, the one after `at` records.
The frames are the ones the server's own tail gave (`tail .. as corgi`), and the follower does not open them.
A tail is not state. It is there while its reader's connection is, and what it took to keep it is dropped after.
Each tail is a small dataflow here: a filter of the view to the one key.
A peek is answered with a line of JSON that holds the rows as text: it is for people, and for the stewards.
"""
import http.client, json, os, queue, signal, socket, sys, threading, time, urllib.parse
import keeper as model
import plumbing

DIR = "interactive/examples/ledger"
BEHIND = 10000      # a reader that has this many lines waiting for it is let go, and may ask again

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
    def __init__(self, register, url, port):
        self.register, self.blobs = plumbing.Register(register), plumbing.Blobs("log")
        self.keeper = urllib.parse.urlparse(url)
        self.at = 0                         # the number of records this copy of the state has had
        self.turn = threading.Condition()   # held by whoever asks the server; told of each record
        self.readers, self.count = {}, 0    # each tail's reader, by the tail's request id
        self.server = plumbing.Server(port + 1)
        types = model.declarations()
        self.server.ask(f'load state begin\n{types}let entries = input 0 : ((Entry), ());\noutput "entries" = entries;\nstate end-load')
        self.server.ask(f"load views from {DIR}/system.ddp input0=state::entries")
        self.server.ask(f"load bundle from {DIR}/bundle.ddp input0=state::entries")
        self.server.ask(f"load tidy from {DIR}/compact.ddp input0=state::entries input1=views::needs")
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

    def written(self):
        """The records of the log that this copy has not had, in order: the frames of each."""
        state, _ = self.register.read()
        records, name = [], state and state["head"]
        for _ in range(state["count"] - self.at if state else 0):
            record, frames = plumbing.unpack(self.blobs.get(name))
            name = record["prev"]
            records.append(frames)
        return records[::-1]

    def apply(self, records):
        """Make the changes of some records, the next in order, as one change of the copy. Each tail hears of its part.
        A record here is its frames."""
        if not records: return
        feeds = [(f"feed state 0 frame {len(frame)}", frame) for frames in records for frame in frames]
        with self.turn:
            self.server.many(feeds + ["tick"])
            self.at += len(records)
            # The tails are on the same connection as the tick, so each has sent its frames of the tick by now.
            for ident, reader in self.readers.items():
                sent, self.server.tails[ident] = self.server.tails[ident], []
                frames = [frame for kind, frame in sent if kind == "data"]
                if frames: reader.send(plumbing.pack({"at": self.at}, frames))
            self.turn.notify_all()

    def follow(self):
        """Take records for as long as this runs: from the keeper while it sends them, and from the log otherwise."""
        while True:
            try:
                self.apply(self.written())
                keeper = http.client.HTTPConnection(self.keeper.hostname, self.keeper.port, timeout=2)
                keeper.request("GET", f"/follow/{self.at}")
                sent = keeper.getresponse()
                while True:
                    record, frames = plumbing.receive(sent)
                    if record is None: continue                     # the keeper is here, and has made nothing
                    if record["number"] > self.at: break            # some were not sent: they are in the log
                    if record["number"] == self.at: self.apply([frames])
            except (OSError, http.client.HTTPException, ValueError, EOFError): pass
            time.sleep(0.2)

    def accept(self, listener):
        while True:
            sock, _ = listener.accept()
            threading.Thread(target=self.answer, args=(sock,), daemon=True).start()

    def answer(self, sock):
        ident = program = None
        try:
            request = json.loads(sock.makefile("rb").readline())
            if "peek" in request:
                with self.turn: rows = [self.server.ask("peek " + name) for name in request["peek"]]
                sock.sendall(json.dumps({"at": self.at, "rows": rows}).encode() + b"\n")
                return
            if "frames" in request:
                with self.turn: found = [self.server.ask(f"peek {name} as corgi") for name in request["frames"]]
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
    follower = Follower(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    try: follower.follow()
    finally: follower.server.close()
