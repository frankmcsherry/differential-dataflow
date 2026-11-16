"""What the keeper, the followers, the runners and their callers share:
a server session, readers of rows and of frames, a register, a blob store, and the callers of a keeper and of a follower.

Rows go between the parts as frames: the binary form the server gives with `peek .. as corgi` and `tail .. as corgi`,
and takes with `feed .. frame`. A part that only passes rows on does not open their frames.

The register is one cell that changes only by compare-and-swap, kept in etcd.
The blob store keeps bytes under their hash, in SeaweedFS.
Both are reached over HTTP with the standard library. LEDGER_ETCD may name several members of one etcd, with commas between.
"""
import base64, hashlib, json, os, socket, struct, subprocess, threading, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "release", "ddir_server")
ETCD = os.environ.get("LEDGER_ETCD", "http://127.0.0.1:2379").split(",")
FILER = os.environ.get("LEDGER_FILER", "http://127.0.0.1:8888")


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


def pack(header, frames):
    """Some frames, to send or to keep: a line of JSON that says how long each is, and then the frames."""
    return json.dumps(dict(header, sizes=[len(frame) for frame in frames])).encode() + b"\n" + b"".join(frames)


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


def http(method, url, body=None, timeout=60):
    request = urllib.request.Request(url, data=body, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def etcd(path, body):
    """Ask etcd, by the first of its members that answers."""
    for member in ETCD:
        try: return json.loads(http("POST", member + path, json.dumps(body).encode(), timeout=5))
        except (urllib.error.URLError, ConnectionError, socket.timeout) as error: failure = error
    raise failure


class Register:
    """One cell in etcd. `read` gives its value and a revision; `swap` writes only if the revision still stands."""
    def __init__(self, name):
        self.key = base64.b64encode(name.encode()).decode()

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


class Blobs:
    """Bytes kept under their hash. A blob is written before anything names it."""
    def __init__(self, space="blobs"):
        self.base = f"{FILER}/{space}/"

    def put(self, data):
        name = hashlib.sha256(data).hexdigest()
        http("PUT", self.base + name, data)
        return name

    def get(self, name):
        data = http("GET", self.base + name)
        assert hashlib.sha256(data).hexdigest() == name, "a blob is not what its name says"
        return data


def hash_ints(name): return list(bytes.fromhex(name))
def hash_name(ints): return bytes(ints).hex()
def text_ints(text): return list(text.encode())
def ints_text(ints): return bytes(ints).decode()


class Client:
    """A caller of a keeper that serves over HTTP."""
    def __init__(self, url): self.url = url
    def transact(self, actor, commands):
        body = json.dumps({"actor": actor, "commands": commands}).encode()
        return json.loads(http("POST", self.url + "/transact", body))["refused"]
    def peek(self, name): return json.loads(http("GET", self.url + "/peek/" + name))
    def entries(self): return by_kind(self.peek("committed::entries"))
    def standing(self): return stands(self.peek("before::standing"))
    def snapshot(self):
        """The entries, the standing and the plan, all of one state. Read apart, they may be of different states."""
        entries, standing, plan = self.peek("committed::entries,before::standing,before::plan")
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


def kinds(frames):
    """The state, by kind, from frames of entries as `peek .. as corgi` gives them: each entry as (key, val)."""
    found = {}
    for frame in frames:
        for (entry,), _, _, _ in decode(frame): found.setdefault(entry[1], []).append(entry[2])
    return found


def stands(lines):
    """Each durable ledger's number: (version, frontier), with a frontier of one time as that time, and of none as None."""
    return {key[0][1]: (val[0], val[1][0][0] if val[1] else None) for key, val in map(row, lines)}


LEDGER, SPAN, NAME, PROGRAM, COMPUTATION, RUN, HOLD = range(7)
def number(ident): return ident[1]
def instant(frontier): return frontier[0][0] if frontier else None


def standing_of(entries):
    """Where each durable ledger stands, worked out from entries as system.ddp's `standing` is: {ledger: (version, frontier)}.
    A ledger's version is the number of its appends. Its write frontier is the upper of its last span, or its start
    if it has none, joined with the write frontier of the clock it follows. An instance works this out from the
    entries it is told, which hold each ledger it needs and each clock above one.
    A frontier of one time is that time here, and of none is None."""
    ledgers = {number(key[0]): val for key, val in entries.get(LEDGER, [])}
    own = {ledger: (0, instant(val[2])) for ledger, val in ledgers.items()}
    for (ledger, _, last, _), (_, upper, _) in entries.get(SPAN, []):
        if last > own[number(ledger)][0]: own[number(ledger)] = (last, instant(upper))
    def write(ledger):
        upper, clock = own[ledger][1], ledgers[ledger][3]
        if clock[1] == 0: return upper
        above = write(number(clock[2]))
        return None if upper is None or above is None else max(upper, above)
    return {ledger: (own[ledger][0], write(ledger)) for ledger, val in ledgers.items() if val[4][1] == 0}
