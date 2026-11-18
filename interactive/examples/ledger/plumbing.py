"""What the keeper, the followers, the runners and their callers share:
a server session, readers of rows and of frames, a register, a blob store, and the callers of a keeper and of a follower.

Rows go between the parts as frames: the binary form the server gives with `peek .. as corgi` and `tail .. as corgi`,
and takes with `feed .. frame`. A part that only passes rows on does not open their frames.

The register is one cell that changes only by compare-and-swap, kept in etcd.
The blob store keeps bytes under their hash, in SeaweedFS.
Both are reached over HTTP with the standard library. LEDGER_ETCD may name several members of one etcd, with commas between.
"""
import base64, concurrent.futures, hashlib, json, os, socket, struct, subprocess, threading, time, urllib.error, urllib.parse, urllib.request, zlib
from http.client import HTTPConnection, HTTPException

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "release", "ddir_server")
ETCD = os.environ.get("LEDGER_ETCD", "http://127.0.0.1:2379").split(",")
FILER = os.environ.get("LEDGER_FILER", "http://127.0.0.1:8888")
# What is stored is compressed, with zlib at level 1, unless LEDGER_COMPRESS=0. The frames are not opened: they are
# compressed as bytes. A blob's name, and a part's hash, are of the bytes as stored, so the store's bytes are what is
# checked, and zlib's own check holds the bytes that come out. Bytes as `pack` makes them begin with `{`, and zlib's
# with 0x78, so a reader tells the one from the other, and reads what was stored either way.
COMPRESS = os.environ.get("LEDGER_COMPRESS", "1") == "1"
def squeeze(data): return zlib.compress(data, 1) if COMPRESS else data
def unsqueeze(data): return zlib.decompress(data) if data[:1] == b"\x78" else data
# The store's two folders: the log's records, and the batches of data. A run may name folders of its own.
SPACES = {"log": os.environ.get("LEDGER_LOG_SPACE", "log"), "blobs": os.environ.get("LEDGER_BLOB_SPACE", "blobs")}


# The server is started under a shell that stops it when this process is gone, however this process ends.
GUARD = '"$0" & pid=$!; trap "kill $pid; exit 0" TERM INT; while kill -0 $PPID 2>/dev/null; do sleep 0.2; done; kill $pid'


class Server:
    """A stock DDIR server of our own, and one connection to it."""
    def __init__(self, port, workers=1, start=True):
        """With `start` false, this is a connection to a server that another process started."""
        env = dict(os.environ, DDIR_BIND=f"127.0.0.1:{port}", DDIR_WS_BIND=f"127.0.0.1:{port + 1}", DDIR_WORKERS=str(workers))
        self.process = start and subprocess.Popen(["sh", "-c", GUARD, BIN], cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(200):
            try:
                self.sock = socket.create_connection(("127.0.0.1", port), timeout=120)
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("server never came up")
        self.count, self.buf = 0, b""
        self.tails, self.last = {}, None    # what each tail has sent and no one has taken; the last request's id

    def line(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(1 << 20)
            if not chunk: raise RuntimeError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode()

    def take(self, size):
        while len(self.buf) < size:
            chunk = self.sock.recv(1 << 20)
            if not chunk: raise RuntimeError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:size], self.buf[size:]
        return data

    def many(self, requests):
        """Send every request, then wait for every answer. A request is a line, or a line and the bytes that follow it.
        Returns what the last was answered with: its rows, each a line of text, or its frames, each as bytes.
        A tail on this connection sends its changes of a tick before the tick is answered. They are kept in `tails`:
        lines of text, or for a binary tail (kind, bytes), where the kind is "data" or "progress"."""
        ids, sent = [], []
        for request in requests:
            self.count += 1
            ids.append(f"q{self.count}")
            line, body = request if isinstance(request, tuple) else (request, b"")
            sent.append(f"{ids[-1]} {line}\n".encode() + body)
        self.last = ids[-1]
        self.sock.sendall(b"".join(sent))
        want, data = set(ids), []
        while want:
            line = self.line()
            toks = line.split(" ", 2)
            if toks[1] == "frame":
                kind, size = toks[2].split(" ")
                found = self.take(int(size))
                if toks[0] in self.tails:
                    if kind != "schema": self.tails[toks[0]].append((kind, found))
                elif toks[0] == ids[-1] and kind == "data": data.append(found)
            elif toks[1] == "data":
                if toks[0] in self.tails: self.tails[toks[0]].append(toks[2])
                elif toks[0] == ids[-1]: data.append(toks[2])
            elif toks[0] not in want: continue
            elif toks[1] == "ok": want.discard(toks[0])
            else:
                request = requests[ids.index(toks[0])]
                raise RuntimeError(f"{(request[0] if isinstance(request, tuple) else request)[:120]!r}: {line[:300]}")
        return data

    def ask(self, line): return self.many([line])

    def tried(self, requests):
        """Send every request and wait for every answer, as `many` does, and return what the server refused:
        {the request's place in `requests`: what the server said}. Nothing is raised, and every answer is read,
        so the connection is good for the next request whatever was refused."""
        ids, sent = [], []
        for request in requests:
            self.count += 1
            ids.append(f"q{self.count}")
            line, body = request if isinstance(request, tuple) else (request, b"")
            sent.append(f"{ids[-1]} {line}\n".encode() + body)
        self.last = ids[-1]
        self.sock.sendall(b"".join(sent))
        want, refused = set(ids), {}
        while want:
            line = self.line()
            toks = line.split(" ", 2)
            if toks[1] == "frame":
                kind, size = toks[2].split(" ")
                found = self.take(int(size))
                if toks[0] in self.tails and kind != "schema": self.tails[toks[0]].append((kind, found))
            elif toks[1] == "data":
                if toks[0] in self.tails: self.tails[toks[0]].append(toks[2])
            elif toks[0] not in want: continue
            else:
                want.discard(toks[0])
                if toks[1] != "ok": refused[ids.index(toks[0])] = line[:300]
        return refused

    def close(self):
        self.sock.close()
        if self.process: self.process.terminate(); self.process.wait()


def parse(text):
    """A value as the server writes it: ints, tuple(..) as a tuple, list(..) as a list, inject(i,x) as ("inject", i, x)."""
    def value(at):
        for word in ("tuple", "list", "inject"):
            if text.startswith(word + "(", at):
                at += len(word) + 1
                items = []
                while text[at] != ")":
                    item, at = value(at)
                    items.append(item)
                    if text[at] == ",": at += 1
                at += 1
                if word == "tuple": return tuple(items), at
                if word == "list": return items, at
                return ("inject", items[0], items[1]), at
        end = at
        while end < len(text) and (text[end].isdigit() or text[end] == "-"): end += 1
        return int(text[at:end]), end
    result, at = value(0)
    assert at == len(text), text
    return result


def row(line):
    """A row as `peek` gives it: its key and its value, parsed."""
    body = line.split(" diff=")[0]
    key, _, val = body.partition(" val=")
    return parse(key), parse(val) if val else ()


def show(value):
    """The inverse of `parse`."""
    if isinstance(value, tuple) and len(value) == 3 and value[0] == "inject": return f"inject({value[1]},{show(value[2])})"
    if isinstance(value, tuple): return "tuple(" + ",".join(show(v) for v in value) + ")"
    if isinstance(value, list): return "list(" + ",".join(show(v) for v in value) + ")"
    return str(value)


class Words:
    """A place in a frame, which is a run of 64-bit words, the low byte first.
    The columns are in corgi's encoding of itself; this follows the reader in interactive/bench/ldbc/client.py."""
    def __init__(self, data): self.data, self.at = data, 0

    def words(self, count, kind="Q"):
        values = struct.unpack_from(f"<{count}{kind}", self.data, self.at)
        self.at += 8 * count
        return values

    def word(self): return self.words(1)[0]

    def leaf(self):
        storage, count = self.word(), self.word()
        if storage == 8:
            values = list(self.data[self.at:self.at + count])
            self.at += (count + 7) & ~7
            return values
        if storage == 64: return list(self.words(count, "q"))
        raise ValueError(f"a frame has a leaf stored as {storage}, which this does not read")

    def column(self):
        """One column, as a function from a row's number to its value. A value is as `parse` gives it:
        an int, a tuple for a tuple, a list for a list, and ("inject", i, x) for the i-th case of an enum."""
        kind = self.word()
        if kind == 0: return self.leaf().__getitem__
        if kind == 1:
            fields = [self.column() for _ in range(self.word())]
            return lambda row: tuple(field(row) for field in fields)
        if kind == 2:
            if self.word() == 0:
                tags = self.leaf()
                offsets = self.words(self.word())
            else:
                tag, rows = self.word(), self.word()
                tags, offsets = [tag] * rows, range(rows)
            cases = [self.column() for _ in range(self.word())]
            return lambda row: ("inject", tags[row], cases[tags[row]](offsets[row]))
        if kind == 3:
            if self.word() == 0: ends = self.words(self.word())
            else:
                stride, rows = self.word(), self.word()
                ends = [stride * (row + 1) for row in range(rows)]
            values = self.column()
            return lambda row: [values(at) for at in range(ends[row - 1] if row else 0, ends[row])]
        if kind == 4:
            self.word()
            return lambda row: ()
        raise ValueError(f"a frame has a column of kind {kind}, which this does not read")


def decode(frame):
    """The updates of a frame: [(key, val, diff, time)]. The time is None if the frame has no times, as a peek's has none."""
    words = Words(frame)
    rows, lanes = words.word(), words.word()
    keys, vals = words.column(), words.column()
    diffs = words.words(rows, "q")
    times = words.words(rows) if lanes else [None] * rows
    return [(keys(row), vals(row), diffs[row], times[row]) for row in range(rows)]


def untimed(frame):
    """A frame without its times, which is what a feed takes: a tail's frame says when each change was, and a feed's rows
    take the time they are fed at. The times are the frame's last words, so this opens no column."""
    rows, lanes = struct.unpack_from("<2Q", frame)
    return struct.pack("<2Q", rows, 0) + frame[16:len(frame) - 8 * rows * lanes]


def raised(frame, time):
    """A frame with each of its times that is before `time` moved up to it: what one who reads from `time` on sees.
    The times are the frame's last words, so this opens no column."""
    rows, lanes = struct.unpack_from("<2Q", frame)
    assert lanes == 1, "a frame to raise has times"
    at = len(frame) - 8 * rows
    return frame[:at] + struct.pack(f"<{rows}Q", *(max(moment, time) for moment in struct.unpack_from(f"<{rows}Q", frame, at)))


def shifted(frame, delta):
    """A frame with `delta` added to each of its times. The times are the frame's last words, so this opens no column."""
    rows, lanes = struct.unpack_from("<2Q", frame)
    assert lanes == 1, "a frame to shift has times"
    at = len(frame) - 8 * rows
    return frame[:at] + struct.pack(f"<{rows}Q", *(moment + delta for moment in struct.unpack_from(f"<{rows}Q", frame, at)))


def latest(frame):
    """The greatest time in a frame, and the least, read from its last words: (least, greatest), or None if it has no rows."""
    rows, lanes = struct.unpack_from("<2Q", frame)
    if not rows: return None
    assert lanes == 1, "a frame to read times of has times"
    times = struct.unpack_from(f"<{rows}Q", frame, len(frame) - 8 * rows)
    return min(times), max(times)


def shape(text):
    """A shape of ints and tuples, as DDIR writes it: "int" for an int, and a tuple of shapes for a tuple."""
    tokens = text.replace("(", " ( ").replace(")", " ) ").replace(",", " ").split()
    def read(at):
        if tokens[at] == "int": return "int", at + 1
        assert tokens[at] == "(", text
        fields, at = [], at + 1
        while tokens[at] != ")":
            field, at = read(at)
            fields.append(field)
        return tuple(fields), at + 1
    return read(0)[0]


def leaf(values):
    """A run of ints, as a column's leaf holds them: a byte each if all fit, and otherwise a word each."""
    count = len(values)
    if all(0 <= value < 256 for value in values): return struct.pack("<2Q", 8, count) + bytes(values) + bytes(-count % 8)
    return struct.pack(f"<2Q{count}q", 64, count, *values)


# The forms of rows this writes: "int"; a tuple of forms, for a tuple; ("list", form), for a list;
# and ("enum", [the form of each case]), for an enum, whose value is ("inject", case, payload), as `parse` gives it.
ID = ("int", "int")
FRONTIER = ("list", ("list", "int"))
BATCH = ("enum", [(), ("list", "int"), ("list", "int")])        # Empty | Inline Bytes | Blob Hash
APPENDS = (("int",), (ID, "int", FRONTIER, FRONTIER, BATCH))     # round.ddp's appends: ((tx), (ledger, number, lower, upper, batch))
RUNNERS = (("int",), (ID,))                                      # round.ddp's runners: ((tx), (computation))
BATCHES = ((ID,), (BATCH,))                                      # pass.ddp's batches: ((ledger), (batch))
BYTES = ("list", "int")
ENTRY = ("enum", [((ID,), (BYTES, "int", FRONTIER, ("enum", [(), ID]), ("enum", [(), ()]))),                 # Ledger
                  ((ID, "int", "int", FRONTIER), (FRONTIER, FRONTIER, BATCH)),                               # Span
                  ((BYTES,), (ID,)),                                                                         # Name
                  ((BYTES,), (("list", BYTES), ("list", BYTES), BYTES)),                                     # Program
                  ((ID,), (BYTES, ("list", ("enum", [(), ID])), ("list", ("enum", [(), ID])))),              # Computation
                  ((ID, ID), ()),                                                                            # Run
                  ((ID, ID), (FRONTIER,))])                                                                  # Hold
PROPOSED = (("int",), (ID, "int", "int", FRONTIER, FRONTIER, FRONTIER))   # pass.ddp's proposed: ((shard), (ledger, from, to, since, lower, upper))
LEAVING = (("int",), (ID, ENTRY))                                         # pass.ddp's leaving: ((shard), (ledger, entry))


def column(form, values):
    """A column of values of one form, in corgi's encoding: what `Words.column` reads."""
    count = len(values)
    if form == "int": return struct.pack("<Q", 0) + leaf(values)
    if form == (): return struct.pack("<2Q", 4, count)
    if form[0] == "list":
        # Where each row's list ends, and then all their items, one after another, as one column.
        ends, items = [], []
        for value in values:
            items += value
            ends.append(len(items))
        return struct.pack(f"<3Q{count}Q", 3, 0, count, *ends) + column(form[1], items)
    if form[0] == "enum":
        # Each row's case, and its place among the rows of that case; and then a column for each case's payloads.
        cases, tags, places = [[] for _ in form[1]], [], []
        for _, tag, payload in values:
            tags.append(tag)
            places.append(len(cases[tag]))
            cases[tag].append(payload)
        return (struct.pack("<2Q", 2, 0) + leaf(tags) + struct.pack(f"<Q{count}Q", count, *places) + struct.pack("<Q", len(cases))
                + b"".join(column(each, payloads) for each, payloads in zip(form[1], cases)))
    return struct.pack("<2Q", 1, len(form)) + b"".join(column(field, [value[at] for value in values]) for at, field in enumerate(form))


def encode(rows, updates, timed=True):
    """A frame of updates, [(key, val, diff, time)], for rows of the form `rows`: DDIR text of ints and tuples, such as
    "((int), (int))", or a form as above. This is for one who has rows and no server to make frames, as a user who
    appends does. Without `timed` the frame has no times, and each update is (key, val, diff): a feed gives it the time."""
    key, val = shape(rows) if isinstance(rows, str) else rows
    count = len(updates)
    return (struct.pack("<2Q", count, 1 if timed else 0) + column(key, [update[0] for update in updates]) + column(val, [update[1] for update in updates])
            + struct.pack(f"<{count}q", *[update[2] for update in updates])
            + (struct.pack(f"<{count}Q", *[update[3] for update in updates]) if timed else b""))


def expand(record, frames, blobs):
    """The frames of a record of the log. A snapshot's frames are not in its record: they are in blobs of their own, each
    under a bound, which the record names in order, with how many frames each holds ("parts"). They are read here."""
    if "parts" not in record: return frames
    found = []
    for name, _ in record["parts"]: found += unpack(blobs.get(name))[1]
    return found


def round_uppers(record, frames):
    """Of a round's record: each ledger it appended to, and the time it now stands at, from what the round changed in the
    keeper's rows ((id), (version, upper, following, clock, durable)). Only a test or a probe reads this."""
    one = record["entries"] + record["round"]["appends"] + record["round"]["runners"]
    return [(key[0][1], val[1][0][0]) for frame in frames[one:one + record["round"]["moved"]]
            for key, val, diff, _ in decode(frame) if diff > 0 and val[1]]


def rows_of(frame):
    """How many updates a frame holds: its first word. This opens no column."""
    return struct.unpack_from("<Q", frame)[0]


def pack(header, frames):
    """Some frames, to send or to keep: a line of JSON that says how long each is, and then the frames."""
    return json.dumps(dict(header, sizes=[len(frame) for frame in frames])).encode() + b"\n" + b"".join(frames)


def fresh(frames):
    """The bytes of a blob of data, to write and then name: the frames, under a header that no other blob has.
    A blob is kept under the hash of its bytes, so two writers of the same frames would otherwise write one blob, and
    one of them could name it after a collector had seen the other's span go and deleted it. With this, a blob has
    one writer, who names it in the request it makes next, or in that request asked again, and nowhere else."""
    return pack({"fresh": os.urandom(8).hex()}, frames)


def receive(stream):
    """What `pack` made, read from a stream: (header, frames). The header is None for a line with nothing on it.
    At the end of the stream this raises EOFError."""
    line = stream.readline()
    if not line: raise EOFError
    if not line.strip(): return None, []
    header = json.loads(line)
    sizes = header.pop("sizes")
    frames = [stream.read(size) for size in sizes]
    if any(len(frame) != size for frame, size in zip(frames, sizes)): raise EOFError
    return header, frames


def unpack(data):
    """What `pack` made, read from bytes: (header, frames)."""
    import io
    return receive(io.BytesIO(data))


KEPT = threading.local()     # each thread's open connections, by where they go and how long a request may take


def http(method, url, body=None, timeout=60):
    """One request, on a connection this thread keeps open to that place, as the blob store's are (see `Blobs.ask`).
    A connection made for each request is closed after it, and each closed one holds a local port for a while: at a
    few hundred requests a second (the register, the collector's deletes) that took every local port this machine
    has, and a request then could not connect. A kept connection that the other end has since closed is tried once
    more, on a new one: only when it failed before any answer came, as a connection the other end closed while idle
    does. A request that took too long is not asked again. Errors are as urllib's: URLError, or HTTPError for a status."""
    place = urllib.parse.urlsplit(url)
    key, path = (place.hostname, place.port, timeout), place.path + ("?" + place.query if place.query else "")
    kept = getattr(KEPT, "conns", None)
    if kept is None: kept = KEPT.conns = {}
    for again in (True, False):
        conn = kept.pop(key, None)
        reused = conn is not None
        conn = conn or HTTPConnection(place.hostname, place.port, timeout=timeout)
        try:
            conn.request(method, path, body=body)
            response = conn.getresponse()
            found = response.read()
        except (ConnectionResetError, BrokenPipeError, HTTPException) as error:
            conn.close()
            if again and reused and not isinstance(error, socket.timeout): continue
            raise urllib.error.URLError(error)
        except OSError as error:
            conn.close()
            raise urllib.error.URLError(error)
        if response.will_close: conn.close()
        else: kept[key] = conn
        if response.status >= 400: raise urllib.error.HTTPError(url, response.status, response.reason, response.headers, None)
        return found


def etcd(path, body):
    """Ask etcd, by the first of its members that answers."""
    for member in ETCD:
        try: return json.loads(http("POST", member + path, json.dumps(body).encode(), timeout=5))
        except (urllib.error.URLError, ConnectionError, socket.timeout) as error: failure = error
    raise failure


# ONE CHUNK SIZE. Every producer of frames keeps what it sends in one request to this: a client's request of appends,
# a round, a compactor's pass. The keeper refuses anything longer, and splits nothing. It is a quarter of what a
# server takes in one feed, since what a round or a pass makes is about as long as what it was asked, and goes to the
# log as one frame that every follower's server must take.
CHUNK = int(os.environ.get("LEDGER_CHUNK", str(4 << 20)))
# The register keeps every revision of every cell unless it is compacted. The keeper compacts it this many revisions
# behind its own latest swap, once every `COMPACT_EVERY` revisions.
KEEP_REVISIONS, COMPACT_EVERY = 10000, 1000


class Register:
    """One cell in etcd. `read` gives its value and a revision; `swap` writes only if the revision still stands."""
    def __init__(self, name):
        self.name, self.key = name, base64.b64encode(name.encode()).decode()
        self.compacted = 0

    def compact_behind(self, revision):
        """Compact etcd's history `KEEP_REVISIONS` behind `revision`, once every `COMPACT_EVERY` revisions. Compaction
        is of the whole store, not of one cell: every keeper and follower of it only ever reads a cell's latest value.
        Returns the revision compacted to, or None if it was not yet time."""
        target = revision - KEEP_REVISIONS
        if target <= self.compacted + COMPACT_EVERY: return None
        try: etcd("/v3/kv/compaction", {"revision": str(target)})
        except urllib.error.HTTPError as error:
            # Another compacted past this already: that is as good.
            if error.code != 400: raise
        self.compacted = target
        return target

    def watch(self, after):
        """Wait for the cell to change after revision `after`, by etcd's watch: a request that streams an answer for
        each change, and here returns at the first. Returns the revision it changed at, or None if the watch ended
        without one (the member went away, or the history it asked from was compacted): whoever asks then reads the
        cell, and watches again from what it read."""
        body = json.dumps({"create_request": {"key": self.key, "start_revision": str(after + 1)}}).encode()
        for member in ETCD:
            place = urllib.parse.urlsplit(member)
            conn = HTTPConnection(place.hostname, place.port, timeout=30)
            try:
                conn.request("POST", "/v3/watch", body=body)
                response = conn.getresponse()
                while True:
                    line = response.readline()
                    if not line: return None
                    if not line.strip(): continue
                    result = json.loads(line).get("result", {})
                    if result.get("compact_revision") or result.get("canceled"): return None
                    events = result.get("events") or []
                    if events: return max(int(event["kv"]["mod_revision"]) for event in events)
            except (OSError, HTTPException, ValueError): continue
            finally: conn.close()
        return None

    def changes(self, after, each):
        """Like `watch`, but the watch is kept open, and `each()` is called at every change after revision `after`.
        etcd answers a watch made from a revision it has already passed only at its next pass over such watches, which
        it makes every 100 ms; a watch kept open is answered as each change is made. Returns when the watch ends: the
        member went away, the history it asked from was compacted, or nothing changed for 30 s."""
        body = json.dumps({"create_request": {"key": self.key, "start_revision": str(after + 1)}}).encode()
        for member in ETCD:
            place = urllib.parse.urlsplit(member)
            conn = HTTPConnection(place.hostname, place.port, timeout=30)
            try:
                conn.request("POST", "/v3/watch", body=body)
                response = conn.getresponse()
                while True:
                    line = response.readline()
                    if not line: return
                    if not line.strip(): continue
                    result = json.loads(line).get("result", {})
                    if result.get("compact_revision") or result.get("canceled"): return
                    if result.get("events"): each()
            except (OSError, HTTPException, ValueError): continue
            finally: conn.close()

    def read(self):
        """(value, revision), or (None, 0) if the cell was never written."""
        answer = etcd("/v3/kv/range", {"key": self.key})
        if not answer.get("kvs"): return None, 0
        found = answer["kvs"][0]
        return json.loads(base64.b64decode(found["value"])), int(found["mod_revision"])

    def swap(self, revision, value):
        """Write `value` if the cell is still at `revision`. Returns the new revision, or None if it has moved."""
        put = {"request_put": {"key": self.key, "value": base64.b64encode(json.dumps(value).encode()).decode()}}
        compare = {"key": self.key, "target": "MOD", "result": "EQUAL", "mod_revision": str(revision)}
        answer = etcd("/v3/kv/txn", {"compare": [compare], "success": [put]})
        return int(answer["header"]["revision"]) if answer.get("succeeded") else None


STORED = {"put": 0, "got": 0}       # the bytes this process has written to the store and read from it, as stored


class Blobs:
    """Bytes kept under their hash. A blob is written before anything names it."""
    def __init__(self, space="blobs"):
        self.base, self.pool, self.kept = f"{FILER}/{SPACES.get(space, space)}/", None, threading.local()

    def ask(self, method, name, data=None, headers=None):
        """One request of the store, on a connection that this thread keeps open."""
        url = urllib.parse.urlsplit(self.base + name)
        for again in (True, False):
            conn = getattr(self.kept, "conn", None) or HTTPConnection(url.hostname, url.port, timeout=60)
            self.kept.conn = conn
            try:
                conn.request(method, url.path, body=data, headers=headers or {})
                response = conn.getresponse()
                found = response.read()
                break
            except (HTTPException, OSError) as error:
                # The store may have ended a connection that was kept. It is asked once more, on a new one.
                conn.close()
                self.kept.conn = None
                if not again: raise ConnectionError(str(error))
        if response.status >= 400: raise urllib.error.HTTPError(self.base + name, response.status, response.reason, response.headers, None)
        return found

    def put(self, data, stored=False):
        """Keep some bytes, compressed unless they are `stored` already, under the hash of what is stored."""
        data = data if stored else squeeze(data)
        name = hashlib.sha256(data).hexdigest()
        self.ask("PUT", name, data)
        STORED["put"] += len(data)
        return name

    def get(self, name):
        data = self.ask("GET", name)
        assert hashlib.sha256(data).hexdigest() == name, "a blob is not what its name says"
        STORED["got"] += len(data)
        self.kept.stored = len(data)
        return unsqueeze(data)

    def piece(self, piece):
        """The bytes of one part of a blob, read by range, and held to the part's own hash: (blob, offset, length, digest)."""
        name, at, long, digest = piece
        data = self.ask("GET", name, headers={"Range": f"bytes={at}-{at + long - 1}"})
        assert len(data) == long and hashlib.sha256(data).digest()[:DIGEST] == digest, "a part of a blob is not what its name says"
        STORED["got"] += len(data)
        return unsqueeze(data)

    def many(self, work, items):
        """`work` of each item, in order, with several asked of the store at once."""
        if len(items) < 2: return [work(item) for item in items]
        if self.pool is None: self.pool = concurrent.futures.ThreadPoolExecutor(int(os.environ.get("LEDGER_BLOB_POOL", "8")))
        return list(self.pool.map(work, items))


# A request of appends is frames, and each of its transactions has a key that the one who asks makes: the request's
# ticket, which the keeper gave, times SLOTS, plus the transaction's place in the request. So the keeper can tell from
# a key alone which request a transaction is of, and tickets are given in order, so an earlier request's keys are less.
SLOTS = 1 << 20
EMPTY = ("inject", 0, ())      # a batch with nothing in it


def hash_ints(name): return list(bytes.fromhex(name))
def hash_name(ints): return bytes(ints).hex()


# A batch that is a blob names it by its hash, which is 32 bytes. The name may go on to say a part of the blob: where
# the part begins and how long it is, each as eight bytes. So one blob holds the frames of many spans, each span
# names its own part, and a writer of many spans writes one blob. A name of the hash alone is of the whole blob.
def part_ints(name, place=None):
    """A batch's name, as ints: the blob's name, and with a place, (offset, length), that part of it."""
    return list(bytes.fromhex(name) + (struct.pack(">QQ", *place[:2]) if place else b""))
def blob_of(ints):
    """The name of the blob that a batch's name is of."""
    assert len(ints) in (32, 48), "a batch's name is a hash, or a hash and a place"
    return bytes(ints[:32]).hex()
def frames_in(data, ints):
    """The frames of a batch, from the bytes of its blob: of the part that the name says, or of the whole."""
    if len(ints) == 32: return unpack(data)[1]
    at, long = struct.unpack(">QQ", bytes(ints[32:48]))
    assert at + long <= len(data), "a batch names a part that its blob does not have"
    return unpack(data[at:at + long])[1]
def several(groups):
    """The bytes of one blob that holds several groups of frames, and where each group is in it: (bytes, [(offset, length, digest)]).
    The blob is to be written as it is (`Blobs.put(.., stored=True)`): each part in it is compressed already.
    The blob begins with a line that no other blob has (see `fresh`). Each group follows as `pack` makes it, so
    a part is read as a blob of that group alone would be."""
    pieces, places = [pack({"fresh": os.urandom(8).hex()}, [])], []
    at = len(pieces[0])
    for frames in groups:
        # Each part is compressed on its own, so that it is read by range, and its hash is of it as stored.
        pieces.append(squeeze(pack({}, frames)))
        places.append((at, len(pieces[-1]), hashlib.sha256(pieces[-1]).digest()[:DIGEST]))
        at += len(pieces[-1])
    return b"".join(pieces), places
# A batch may instead name one or more parts, each in 64 bytes: the hash of the blob it is in (32), where in it the
# part begins and how long it is (8 and 8, high byte first), and the first 16 bytes of the hash of the part's own
# bytes. Its parts, one after another, are the batch. So a reader reads a part by range, and holds it to its own
# hash: it reads no more of a blob than it needs. A part is as `pack` makes it, so it is read as a small blob would be.
DIGEST, PIECE = 16, 64
def piece_ints(name, place):
    """A part of a blob, as ints: the blob's name, and (offset, length, the part's bytes) or (offset, length, digest)."""
    at, long, held = place
    digest = held if len(held) == DIGEST else hashlib.sha256(held).digest()[:DIGEST]
    return list(bytes.fromhex(name) + struct.pack(">QQ", at, long) + digest)
def pieces_of(ints):
    """The parts a batch's name names, in order: [(blob, offset, length, digest)]. A name of the short forms, a hash or
    a hash and a place, has none: it names a blob whole, or a place in one. Tests and demos still write the first."""
    if len(ints) in (32, 48): return []
    assert len(ints) % PIECE == 0, "a batch's name is a hash, a hash and a place, or parts"
    data, found = bytes(ints), []
    for at in range(0, len(data), PIECE):
        one = data[at:at + PIECE]
        found.append((one[:32].hex(), *struct.unpack(">QQ", one[32:48]), one[48:]))
    return found
def blobs_named(ints):
    """The names of the blobs that a batch's name names any part of."""
    return {blob_of(ints)} if len(ints) in (32, 48) else {piece[0] for piece in pieces_of(ints)}
# Parts of one blob that lie within this many bytes of each other are read in one range, whatever lies between them:
# a request costs more than a few hundred KB of bytes.
NEAR = int(os.environ.get("LEDGER_NEAR", str(256 << 10)))
def fetch(blobs, pieces, cache):
    """Read many parts, some of them in the same blobs, into `cache`, by (blob, offset). The parts of one blob that are
    wanted are read in one range, from the first to the end of the last, when what lies between them that is not
    wanted is no more than what is, or the range is under `NEAR` bytes: so a pass that wants several parts of an
    instance's blob asks once for them, and one that wants a little of a large blob asks for that little. Each part is held to its own hash, as it is when read alone."""
    by_blob = {}
    for piece in pieces:
        if piece[:2] not in cache: by_blob.setdefault(piece[0], {})[piece[:2]] = piece
    asks = []
    for name, wanted in by_blob.items():
        wanted = sorted(wanted.values(), key=lambda piece: piece[1])
        first, end = wanted[0][1], max(piece[1] + piece[2] for piece in wanted)
        if end - first <= max(2 * sum(piece[2] for piece in wanted), NEAR): asks.append(wanted)
        else: asks += [[piece] for piece in wanted]
    def read(group):
        if len(group) == 1: return [(group[0][:2], blobs.piece(group[0]))]
        first, end = group[0][1], max(piece[1] + piece[2] for piece in group)
        data = blobs.ask("GET", group[0][0], headers={"Range": f"bytes={first}-{end - 1}"})
        STORED["got"] += len(data)
        found = []
        for name, at, long, digest in group:
            part = data[at - first:at - first + long]
            assert len(part) == long and hashlib.sha256(part).digest()[:DIGEST] == digest, "a part of a blob is not what its name says"
            found.append(((name, at), unsqueeze(part)))
        return found
    for found in blobs.many(read, asks):
        cache.update(found)
    return len(asks)


def batch_frames(blobs, ints, cache=None):
    """The frames of a batch, read from `blobs`: the parts its name names, each read by range and held to its hash, or,
    for the short forms, the blob whole. `cache`, if given, is what was read before: by (blob, offset) for parts, and by
    blob for wholes. What is read is put there."""
    cache = {} if cache is None else cache
    pieces = pieces_of(ints)
    if not pieces:
        name = blob_of(ints)
        if name not in cache: cache[name] = blobs.get(name)
        return frames_in(cache[name], ints)
    frames = []
    for piece in pieces:
        if piece[:2] not in cache: cache[piece[:2]] = blobs.piece(piece)
        frames += unpack(cache[piece[:2]])[1]
    return frames
# A batch whose frames, as `pack` makes them, are under this many bytes is kept in its entry, as `Inline Bytes`, and no
# blob is written for it. Each byte of it is then an int of the entry's list wherever the entry is kept.
# Off unless set: inline bytes cost about four times their size in each server that holds the entries, and slowed the
# compaction policy's follower enough that compaction fell behind at 10,000 appends a second (see the step 7 report).
INLINE = int(os.environ.get("LEDGER_INLINE", "0"))
def inline(frames):
    """A batch of some frames, kept in its entry: ("inject", 1, the bytes `pack` makes of them, as ints).
    Each frame is compressed on its own, and the batch's first line says so ("z"): one without it is read as it is."""
    if COMPRESS: return ("inject", 1, list(pack({"z": 1}, [zlib.compress(frame, 1) for frame in frames])))
    return ("inject", 1, list(pack({}, frames)))
def frames_of_batch(blobs, batch, cache=None):
    """The frames of a batch, as `parse` or `decode` gives it: none if it is empty, those kept in it if it is inline, and
    those of the parts of blobs it names otherwise (see `batch_frames`)."""
    if batch[1] == 0: return []
    if batch[1] == 1:
        if not batch[2]: return []
        header, frames = unpack(bytes(batch[2]))
        return [zlib.decompress(frame) for frame in frames] if header.get("z") else frames
    return batch_frames(blobs, batch[2], cache)
def split(frames, bound):
    """Frames in groups, in order, each of under `bound` bytes as `pack` makes it, unless one frame alone is longer."""
    groups, size = [[]], 0
    for frame in frames:
        if groups[-1] and size + len(frame) + 16 > bound: groups.append([]); size = 0
        groups[-1].append(frame)
        size += len(frame) + 16
    return [group for group in groups if group]
def text_ints(text): return list(text.encode())
def ints_text(ints): return bytes(ints).decode()


class Client:
    """A caller of a keeper that serves over HTTP. The keeper holds no spans: with a follower named, what is asked of the
    spans (the entries whole, the blobs that are named) is asked of that follower, once it has all the keeper has made."""
    def __init__(self, url, follower=None): self.url, self.ticket, self.follower = url, None, follower and Follower(follower)
    def length(self):
        """How many records the keeper has made. Every one of them is in the log."""
        return json.loads(http("GET", self.url + "/counts"))["records"]
    def caught(self, *names):
        """The rows of some of the follower's views, [(key, val)] for each, of a state that has all the keeper has made."""
        return self.follower.views(names, [], after=self.length())[1]
    def append(self, appends, runners=()):
        """Some transactions of appends, in one request, as frames: what refused each that was refused, {place: why}.
        `appends` is [(place, ledger, number, lower, upper, batch)]: `place` says which of the request's transactions an
        append is of, a ledger is its id, (0, n), a frontier is a list of times, each a list of ints, and a batch is
        EMPTY or ("inject", 2, the ints of a blob's hash). `runners` is [(place, computation)], for the transactions a
        runner makes. The keeper does not open the frames. One caller of a keeper asks one request at a time: each
        answer brings the ticket for the next."""
        while True:
            if self.ticket is None: self.ticket = json.loads(http("GET", self.url + "/ticket"))["ticket"]
            base = self.ticket * SLOTS
            frames = [encode(APPENDS, [((base + place,), (ledger, number, lower, upper, batch), 1)
                                       for place, ledger, number, lower, upper, batch in appends], timed=False)]
            if runners: frames.append(encode(RUNNERS, [((base + place,), (computation,), 1) for place, computation in runners], timed=False))
            answer = json.loads(http("POST", self.url + "/append", pack({"ticket": self.ticket, "appends": 1, "runners": len(frames) - 1}, frames)))
            self.ticket = answer["ticket"]
            # A ticket that this keeper did not give, or has had used, is not taken: the request is asked again with a new one.
            if not answer.get("again"): return {int(place): why for place, why in answer["refused"].items()}
    def transact(self, actor, commands):
        body = json.dumps({"actor": actor, "commands": commands}).encode()
        return json.loads(http("POST", self.url + "/transact", body))["refused"]
    def transact_many(self, asked):
        """Several transactions, each (actor, commands), in one request: what refused each. They are judged as if each had
        been asked alone, and at once, so those of plain appends are in one round."""
        body = json.dumps({"many": [{"actor": actor, "commands": commands} for actor, commands in asked]}).encode()
        return json.loads(http("POST", self.url + "/transact", body))["refused"]
    def merges(self, at, proposed, leaving, batches=()):
        """A pass of merges, of a keeper that holds no spans (see its `merges`): the frames of what a follower's policy
        proposes, the frames of the entries those merges take out, and, for merges whose batch is not empty, frames of
        ((ledger), (batch)). `at` is how many records of the log the follower had. Returns the ledgers whose merges it
        refused, the log's length after the pass, how many merges it made, and how many entries they took out."""
        header = {"at": at, "proposed": len(proposed), "leaving": len(leaving), "batches": len(batches)}
        answer = json.loads(http("POST", self.url + "/merges", pack(header, list(proposed) + list(leaving) + list(batches)), timeout=600))
        # A pass that the keeper's server would not take a frame of is made no part of, and the keeper says so.
        if answer.get("error"): print(f"the keeper turned a pass away: {answer['error']}", flush=True)
        return answer["refused"], answer["record"], answer["made"], answer["left"]
    def peek(self, name): return json.loads(http("GET", self.url + "/peek/" + name))
    def entries(self):
        """The state, by kind. The spans are a follower's to say."""
        if self.follower is None: return by_kind(self.peek("state::entries"))
        kinds = {}
        for (entry,), _ in self.caught("state::entries")[0]: kinds.setdefault(entry[1], []).append(entry[2])
        return kinds
    def merge(self, ledger, first, last, since, blob=None):
        """Ask for one merge, as a user who has read a follower: a ledger's appends from `first` up to `last`, as one span
        held from `since`, with the spans it covers taken out. This is a pass of one (see the keeper's `merges`): what is
        proposed, and the entries that leave, written here as frames. Returns why it was refused: "late" if the ledger was
        merged since the follower read, "beyond" if it takes spans out and is held from later than the ledger's own hold,
        or none."""
        at, (entries,), _ = self.follower.views(["state::entries"], [], after=self.length())
        spans = [entry for (entry,), _ in entries if entry[1] == SPAN and entry[2][0][0] == ledger and first <= entry[2][0][1] and entry[2][0][2] <= last]
        lower = next(entry[2][1][0] for entry in spans if entry[2][0][1] == first)
        upper = next(entry[2][1][1] for entry in spans if entry[2][0][2] == last)
        proposed = encode(PROPOSED, [((0,), (ledger, first, last, [[since]], lower, upper), 1)], timed=False)
        leaving = encode(LEAVING, [((0,), (ledger, entry), 1) for entry in spans], timed=False)
        batches = [encode(BATCHES, [((ledger,), (("inject", 2, hash_ints(blob)),), 1)], timed=False)] if blob else []
        header = {"at": at, "proposed": 1, "leaving": 1, "batches": len(batches)}
        answer = json.loads(http("POST", self.url + "/merges", pack(header, [proposed, leaving] + batches), timeout=600))
        if answer.get("error"): return ["error: " + answer["error"]]
        return [] if answer["made"] else ["beyond" if answer.get("beyond") else "late"]
    def standing(self): return stands(self.peek("views::standing"))
    def snapshot(self):
        """The entries, the standing and the plan, all of one state. Read apart, they may be of different states."""
        entries, standing, plan = self.peek("state::entries,views::standing,views::plan")
        return by_kind(entries), stands(standing), [row(line) for line in plan]


class Follower:
    """A caller of one who follows the state (see follower.py), at `host:port`. Reading from it asks nothing of the keeper."""
    def __init__(self, address):
        host, _, port = address.rpartition(":")
        self.address = (host, int(port))

    def peek(self, *names):
        """The rows of each view named, all of one state."""
        with socket.create_connection(self.address, timeout=60) as sock:
            sock.sendall(json.dumps({"peek": names}).encode() + b"\n")
            return json.loads(sock.makefile("rb").readline())["rows"]

    def peek_at(self, *names):
        """The same, with how many records of the log the follower had: (records, [rows of each view, as text])."""
        with socket.create_connection(self.address, timeout=60) as sock:
            sock.sendall(json.dumps({"peek": names}).encode() + b"\n")
            answer = json.loads(sock.makefile("rb").readline())
        return answer["at"], answer["rows"]

    def where(self):
        """How many records of the log the follower has, and how many frames of them its views have not been fed."""
        with socket.create_connection(self.address, timeout=60) as sock:
            sock.sendall(json.dumps({"where": 1}).encode() + b"\n")
            return json.loads(sock.makefile("rb").readline())

    def rows(self, *names):
        """The rows of each view named, all of one state, sent as frames and decoded here: [(key, val)] for each view."""
        return self.rows_at(*names)[1]

    def views(self, decoded, whole, after=0):
        """Some views, all of one state: those named in `decoded` as rows, [(key, val)] for each, and those named in
        `whole` as the frames the follower's server gave, unopened. The follower answers once it has `after` records
        of the log. Returns how many it had, the rows, and the frames."""
        with socket.create_connection(self.address, timeout=600) as sock:
            sock.sendall(json.dumps({"frames": list(decoded) + list(whole), "after": after}).encode() + b"\n")
            header, frames = receive(sock.makefile("rb"))
        found, at = [], 0
        for count in header["counts"]:
            found.append(frames[at:at + count])
            at += count
        rows = [[(key, val) for frame in part for key, val, _, _ in decode(frame)] for part in found[:len(decoded)]]
        return header["at"], rows, found[len(decoded):]

    def rows_at(self, *names):
        """The same, with how many records of the log the follower had when it read them: (records, [rows of each view])."""
        with socket.create_connection(self.address, timeout=60) as sock:
            sock.sendall(json.dumps({"frames": names}).encode() + b"\n")
            header, frames = receive(sock.makefile("rb"))
        found, at = [], 0
        for count in header["counts"]:
            found.append([(key, val) for frame in frames[at:at + count] for key, val, _, _ in decode(frame)])
            at += count
        return header["at"], found


class Tail:
    """One view of a follower's, at one key, as its changes: frames, in the order the follower sent them.
    A thread of its own takes them, for as long as this process runs. While the follower is away nothing comes,
    and it is asked again. A follower that is asked again answers only once it has reached the state this tail
    last heard of, and then sends every row afresh: what came before is then to be put aside.
    This does not open the frames. Whoever takes them feeds them on, or decodes them."""
    def __init__(self, address, view, key=None):
        self.address, self.request = Follower(address).address, {"tail": view, "key": key}
        self.fresh, self.at = [], 0         # what has come and was not taken: (afresh, frames); the state it is of
        self.news = threading.Condition()
        threading.Thread(target=self.follow, daemon=True).start()

    def follow(self):
        while True:
            try:
                with socket.create_connection(self.address) as sock:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    sock.sendall(json.dumps(dict(self.request, after=self.at)).encode() + b"\n")
                    stream, afresh = sock.makefile("rb"), True
                    while True:
                        header, frames = receive(stream)
                        if header is None: break
                        with self.news:
                            # The first answer is all the rows there are, and the rest are changes to them.
                            self.fresh.append((afresh, frames))
                            self.at, afresh = header["at"], False
                            self.news.notify_all()
            except (OSError, ValueError, EOFError): pass
            time.sleep(0.2)

    def take(self, timeout=None):
        """Wait for something to come, and take all that has: [(afresh, frames)], in order. Each is all of one state.
        With `afresh`, the frames are every row there is, and the rows of what came before are to be put aside.
        Nothing is returned if the time ran out first."""
        with self.news:
            self.news.wait_for(lambda: self.fresh, timeout)
            fresh, self.fresh = self.fresh, []
            return fresh


class Rows:
    """The rows of a tail, kept as they change: for a reader that has no server to feed them to.
    Each row is the value of an update, which must be one that can be a key of a dict."""
    def __init__(self, tail): self.tail, self.counts = tail, {}

    def wait(self, timeout=None):
        """Wait for the rows to change. Returns the rows there are, or None if the time ran out first."""
        fresh = self.tail.take(timeout)
        for afresh, frames in fresh:
            if afresh: self.counts = {}
            for frame in frames:
                for _, val, diff, _ in decode(frame):
                    self.counts[val] = self.counts.get(val, 0) + diff
                    if not self.counts[val]: del self.counts[val]
        return set(self.counts) if fresh else None


def by_kind(lines):
    """The state, by kind: each entry as (key, val)."""
    kinds = {}
    for line in lines:
        (entry,), _ = row(line)
        kinds.setdefault(entry[1], []).append(entry[2])
    return kinds


def stands(lines):
    """Each durable ledger's number: (version, frontier), with a frontier of one time as that time, and of none as None."""
    return {key[0][1]: (val[0], val[1][0][0] if val[1] else None) for key, val in map(row, lines)}


LEDGER, SPAN, NAME, PROGRAM, COMPUTATION, RUN, HOLD = range(7)
def number(ident): return ident[1]
def instant(frontier): return frontier[0][0] if frontier else None
