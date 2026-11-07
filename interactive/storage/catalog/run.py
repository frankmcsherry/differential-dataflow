#!/usr/bin/env python3
"""A durable catalog on the live DDIR server, under random appenders, competing
mergers, and crashes (see SPEC.md).

Each epoch:
 1. Appenders and mergers write their data blobs, then submit transactions.
 2. resolve.ddp decides them; its conclusions (`delta`, `adds`, `removes`) are
    checked against a serial reference.
 3. The conclusions are written as one blob, and the root register is advanced
    from version v to v + 1 by compare-and-swap. Only then are they acknowledged.
 4. catalog.ddp absorbs them; its views and every invariant are checked, and
    readers accumulate chains from the blobs on disk against the ground truth.
 5. In the background, the durable log of conclusions is compacted by the
    chosen policy, and unreachable blobs older than a grace period are deleted.

A crash kills the server and forgets everything but the disk, either before the
root advances (the epoch is lost, and was never acknowledged) or after (the
epoch is durable). Recovery reads the root, feeds the log's consolidated rows
into a fresh server, and the checks continue.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from array import array
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
import random
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "bench" / "txn"))
from bench_txn import DEFAULT_BINARY, INTENTS, ServerProcess, percentile  # noqa: E402
from client import decode, encode_frame  # noqa: E402

RECORDS = (("prod", ["int"]), ("prod", ["int", "int"]))
BATCHES = (("prod", ["int"]), ("prod", ["int", "int", "int", "int"]))
EFFECTS = (("prod", ["int", "int", "int"]), ("prod", ["int", "int", "int", "int"]))


# The blob of a batch whose data its writer keeps elsewhere (`--no-data`).
EMPTY = 0


def upper_key(i: int) -> int:
    return i


def gen_key(i: int) -> int:
    return -i - 1


# ---------------------------------------------------------------- the disk
#
# Three levels of `--sync`:
#   none   nothing is forced to storage.
#   fsync  every file is fsync'd. On Linux that is durable; macOS hands the data to
#          the drive, which may still hold it in its cache.
#   full   as fsync, and each commit ends with F_FULLFSYNC, which on macOS makes the
#          drive empty its cache to permanent storage. One such flush covers every
#          file fsync'd before it, so data blobs need only fsync, and each commit
#          pays two flushes: one before the root changes, one after.


def sync_fd(fd: int, sync: str, flush: bool = False) -> None:
    """Force `fd` to storage as `sync` asks; `flush` empties the drive's cache too."""
    if sync == "none":
        return
    os.fsync(fd)
    if flush and sync == "full":
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)


def sync_path(path: Path, sync: str, flush: bool = False) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        sync_fd(fd, sync, flush)
    finally:
        os.close(fd)


class BlobStore:
    """Immutable blobs named by the first 60 bits of their SHA-256, as an int."""

    written = 0  # bytes written, across restarts

    def __init__(self, root: Path, epoch: int, sync: str):
        self.dir = root / "blobs"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.sync = sync
        # When each blob was first seen; after a restart, everything is new.
        self.born = {int(name, 16): epoch for name in os.listdir(self.dir)}

    def path(self, bid: int) -> Path:
        return self.dir / f"{bid:015x}"

    def put(self, data: bytes, epoch: int) -> int:
        """Write a blob and fsync it. It is durable once the next commit's flush returns."""
        bid = int(hashlib.sha256(data).hexdigest()[:15], 16)
        path = self.path(bid)
        if not path.exists():
            tmp = path.with_suffix(".tmp")
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                sync_fd(f.fileno(), self.sync)
            os.replace(tmp, path)
            BlobStore.written += len(data)
        self.born.setdefault(bid, epoch)
        return bid

    def get(self, bid: int) -> bytes:
        return self.path(bid).read_bytes()

    def delete(self, bid: int) -> None:
        self.path(bid).unlink()
        del self.born[bid]

    def usage(self) -> tuple[int, int]:
        sizes = [p.stat().st_size for p in self.dir.iterdir()]
        return len(sizes), sum(sizes)


class CasRegister:
    """One root: (version, manifest), replaced only by compare-and-swap on the version."""

    def __init__(self, path: Path, sync: str):
        self.path = path
        self.sync = sync

    def read(self) -> tuple[int, dict | None]:
        if not self.path.exists():
            return 0, None
        doc = json.loads(self.path.read_text())
        return doc["version"], doc["manifest"]

    def cas(self, expected: int, manifest: dict) -> bool:
        with open(self.path.with_suffix(".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self.read()[0] != expected:
                return False
            tmp = self.path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump({"version": expected + 1, "manifest": manifest}, f)
                f.flush()
                sync_fd(f.fileno(), self.sync)
            os.replace(tmp, self.path)
            # The rename is durable once the directory is synced and the drive flushed.
            sync_path(self.path.parent, self.sync)
            sync_path(self.path, self.sync, flush=True)
            return True


def encode_data(header: tuple[int, ...], updates: list[tuple[int, int, int]]) -> bytes:
    a = array("q", [*header, len(updates)])
    for u in updates:
        a.extend(u)
    return a.tobytes()


def decode_data(data: bytes) -> list[tuple[int, int, int]]:
    a = array("q")
    a.frombytes(data)
    n = a[5]
    return [(a[6 + 3 * i], a[7 + 3 * i], a[8 + 3 * i]) for i in range(n)]


def pack_frames(frames: list[tuple[int, bytes]]) -> bytes:
    """A batch of the catalog's log: frames of updates, each tagged with the catalog input
    (0 records, 1 batches) it feeds. The frames are the server's own: what `peek … as corgi`
    returns is what `feed … frame` reads, so nothing is decoded on the way to disk."""
    out = bytearray(array("q", [len(frames)]).tobytes())
    for index, frame in frames:
        out += array("q", [index, len(frame)]).tobytes()
        out += frame + bytes(-len(frame) % 8)
    return bytes(out)


def unpack_frames(data: bytes) -> list[tuple[int, bytes]]:
    words = array("q")
    words.frombytes(data[:8])
    at, frames = 8, []
    for _ in range(words[0]):
        head = array("q")
        head.frombytes(data[at:at + 16])
        index, n = head
        frames.append((index, data[at + 16:at + 16 + n]))
        at += 16 + n + (-n % 8)
    return frames


def negate_frame(frame: bytes) -> bytes:
    """The same updates with every diff negated: a frame ends with one int64 diff per row."""
    rows = array("q")
    rows.frombytes(frame[:8])
    diffs = array("q")
    diffs.frombytes(frame[len(frame) - 8 * rows[0]:])
    return frame[:len(frame) - 8 * rows[0]] + array("q", (-d for d in diffs)).tobytes()


def consolidate(c: Counter) -> Counter:
    return Counter({k: v for k, v in c.items() if v != 0})


class Fenced(Exception):
    pass


class Durable:
    """The catalog's durable form: a log of conclusion batches, named by the root.

    The manifest is {"epoch": last durable epoch, "log": [[blob, bytes, first, last], ...]}.
    """

    def __init__(self, root: Path, policy: str, period: int, sync: str, epoch: int):
        self.blobs = BlobStore(root, epoch, sync)
        self.register = CasRegister(root / "root.json", sync)
        self.sync = sync
        self.version, manifest = self.register.read()
        self.manifest = manifest or {"epoch": -1, "log": []}
        self.policy = policy
        self.period = period
        # Blobs a sweep found unreachable, and the epoch it first did.
        self.orphaned: dict[int, int] = {}
        # Seconds in each step of `commit`, for the driver to collect.
        self.timing: Counter = Counter()

    def _swap(self, manifest: dict) -> None:
        if not self.register.cas(self.version, manifest):
            raise Fenced("root moved under us")
        self.version += 1
        self.manifest = manifest

    def _put_flushed(self, frames, epoch: int) -> list:
        data = pack_frames(frames)
        bid = self.blobs.put(data, epoch)
        sync_path(self.blobs.path(bid), self.sync, flush=True)
        return [bid, len(data)]

    def commit(self, epoch: int, frames: list[tuple[int, bytes]]) -> None:
        """Make an epoch's conclusions durable. Its outcomes may be reported once this returns.

        1. Write the conclusions as one blob, and fsync it.
        2. Flush the drive: this blob, and every data blob fsync'd before it, are now
           on permanent storage, so the root may name them.
        3. Advance the root from version v to v + 1 by compare-and-swap, and flush again.
        """
        clock = time.perf_counter()
        data = pack_frames(frames)
        bid = self.blobs.put(data, epoch)
        self.timing["commit: write blob"] += time.perf_counter() - clock
        clock = time.perf_counter()
        sync_path(self.blobs.path(bid), self.sync, flush=True)
        self.timing["commit: flush"] += time.perf_counter() - clock
        clock = time.perf_counter()
        entry = [bid, len(data), epoch, epoch]
        self._swap({"epoch": epoch, "log": self.manifest["log"] + [entry]})
        self.timing["commit: root"] += time.perf_counter() - clock

    def load(self) -> list[tuple[int, bytes]]:
        """Every frame in the log, oldest first: fed in order, they rebuild the catalog."""
        return [f for bid, *_ in self.manifest["log"] for f in unpack_frames(self.blobs.get(bid))]

    def _merge(self, lo: int, hi: int, epoch: int, consolidate_frames) -> None:
        log = self.manifest["log"]
        frames = [f for bid, *_ in log[lo:hi] for f in unpack_frames(self.blobs.get(bid))]
        merged = self._put_flushed(consolidate_frames(frames), epoch) + [log[lo][2], log[hi - 1][3]]
        self._swap({"epoch": self.manifest["epoch"], "log": log[:lo] + [merged] + log[hi:]})

    def compact(self, epoch: int, consolidate_frames) -> None:
        """Merge log blobs by the policy; `consolidate_frames` sums a list of frames' updates."""
        if self.policy == "geometric":
            # Merge the last two while the older is at most twice the newer.
            while len(self.manifest["log"]) >= 2:
                log = self.manifest["log"]
                if log[-2][1] > 2 * log[-1][1]:
                    break
                self._merge(len(log) - 2, len(log), epoch, consolidate_frames)
        elif self.policy == "periodic":
            if epoch % self.period == 0 and len(self.manifest["log"]) > 1:
                self._merge(0, len(self.manifest["log"]), epoch, consolidate_frames)

    def sweep(self, epoch: int, grace: int, live_data) -> int:
        """Delete blobs the root has not reached for `grace` epochs. `live_data` is the blob
        of every batch in the catalog the root names.

        The clock starts when a sweep first finds a blob unreachable, so a reader or
        merger working from a view up to `grace` epochs old still finds its blobs.
        Blobs written less than `grace` epochs ago are spared, as they may belong to
        transactions not yet decided.
        """
        live = {bid for bid, *_ in self.manifest["log"]}
        live.update(live_data)
        for b in list(self.orphaned):
            if b in live:
                del self.orphaned[b]
        doomed = []
        for b, born in self.blobs.born.items():
            if b in live or born >= epoch - grace:
                continue
            if epoch - self.orphaned.setdefault(b, epoch) >= grace:
                doomed.append(b)
        for b in doomed:
            self.blobs.delete(b)
            del self.orphaned[b]
        return len(doomed)


# ---------------------------------------------------------------- the reference


@dataclass
class Txn:
    tid: int
    kind: str
    reads: list[tuple[int, int]]
    writes: list[tuple[int, int]]
    effects: list[tuple[int, int, tuple[int, int, int, int]]]  # (op, id, (lo, hi, since, blob))
    updates: list[tuple[int, list[tuple[int, int, int]]]] = field(default_factory=list)


class Reference:
    """The serial meaning of the catalog, and the ground truth of every id's data."""

    def __init__(self, ids: int):
        self.records = {}
        for i in range(ids):
            self.records[upper_key(i)] = (0, 0)
            self.records[gen_key(i)] = (0, 0)
        self.batches: dict[int, set] = defaultdict(set)
        self.truth: dict[int, list] = defaultdict(list)

    def apply(self, txns: list[Txn]):
        rollback, undo = set(), []
        old: dict[int, tuple[int, int]] = {}
        adds, removes = Counter(), Counter()
        for t in txns:
            if not all(self.records[k][1] == e for k, e in t.reads):
                rollback.add(t.tid)
                continue
            for k, v in t.writes:
                old.setdefault(k, self.records[k])
                undo.append(("rec", k, self.records[k]))
                self.records[k] = (t.tid, v)
            for op, i, row in t.effects:
                if op == 1:
                    assert row not in self.batches[i], f"duplicate batch {i} {row}"
                    self.batches[i].add(row)
                    adds[(i, *row)] += 1
                else:
                    # A guarded merge never removes a batch that is not there.
                    assert row in self.batches[i], f"txn {t.tid} removes absent {i} {row}"
                    self.batches[i].remove(row)
                    removes[(i, *row)] += 1
                undo.append(("bat", op, i, row))
            for i, ups in t.updates:
                self.truth[i].extend(ups)
                undo.append(("truth", i, len(ups)))
        delta = Counter()
        for k, before in old.items():
            delta[(k, *self.records[k])] += 1
            delta[(k, *before)] -= 1
        return rollback, consolidate(delta), adds, removes, undo

    def revert(self, undo) -> None:
        for entry in reversed(undo):
            if entry[0] == "rec":
                self.records[entry[1]] = entry[2]
            elif entry[0] == "bat":
                _, op, i, row = entry
                (self.batches[i].remove if op == 1 else self.batches[i].add)(row)
            else:
                del self.truth[entry[1]][-entry[2]:]

    def upper(self, i: int) -> int:
        return self.records[upper_key(i)][1]


def retention(upper: int, history: int) -> int:
    return max(0, upper - history)


def fewest(batches, upper: int, r: int) -> int | None:
    """Fewest batches with since <= r chaining 0 to upper."""
    dist = {0: 0}
    for lo, hi, s, _ in sorted(batches):
        if s <= r and lo in dist:
            dist[hi] = min(dist.get(hi, 1 << 60), dist[lo] + 1)
    return dist.get(upper)


# ---------------------------------------------------------------- the server


class Catalog:
    def __init__(self, args, program: Path):
        self.args = args
        self.program = program
        self.server = ServerProcess(args.binary.resolve(), args.workers, getattr(args, "wrap_cmd", []), True)
        self.client = self.server.client
        self.client.load("cat", program)

    def feed(self, prog: str, index: int, shape, keys, vals, diffs=None) -> None:
        if keys:
            self.send(prog, index, encode_frame(shape, keys, diffs or [1] * len(keys), vals))

    def send(self, prog: str, index: int, frame: bytes) -> None:
        c = self.client
        rid = c._id()
        c._exchange(rid, f"{rid} feed {prog} {index} frame {len(frame)}\n".encode() + frame)

    def peek_frames(self, trace: str) -> list[bytes]:
        """A trace's contents as the server's own frames, undecoded."""
        c = self.client
        rid = c._id()
        _, frames = c._exchange(rid, f"{rid} peek {trace} as corgi\n")
        return [f for kind, f in frames if kind == "data"]

    def consolidate_frames(self, frames: list[tuple[int, bytes]]) -> list[tuple[int, bytes]]:
        """Sum frames of catalog updates in a scratch program, and read back its arrangements."""
        c = self.client
        c.load("logmerge", HERE / "logmerge.ddp")
        for index, frame in frames:
            self.send("logmerge", index, frame)
        c.tick()
        out = [(0, f) for f in self.peek_frames("log_records")] + [(1, f) for f in self.peek_frames("log_batches")]
        c.drop("logmerge")
        return out

    def crash(self) -> None:
        self.server.process.kill()
        self.server.process.wait()
        self.client.close()


def peek_counter(client, trace: str) -> Counter:
    rows, _ = client.peek(trace)
    return Counter({(*k, *v): d for k, v, d in rows})


def peek_records(client) -> Counter:
    """The catalog's records, which it exports as uppers and generations apart."""
    return peek_counter(client, "records_up") + peek_counter(client, "records_gen")


def frames_counter(frames: list[bytes]) -> Counter:
    def flat(x):
        return tuple(y for part in x for y in flat(part)) if isinstance(x, list) else (x,)
    return Counter({(*flat(k), *flat(v)): d for k, v, d in decode([("data", f) for f in frames])})


# ---------------------------------------------------------------- the workload


@dataclass
class View:
    """What a merger saw: the catalog after some epoch."""

    uppers: dict[int, int]
    gens: dict[int, int]
    chain: dict[int, list]
    retirable: dict[int, list]


def advance(updates, s):
    c = Counter()
    for t, k, d in updates:
        c[(max(t, s), k)] += d
    return sorted((t, k, d) for (t, k), d in c.items() if d != 0)


def make_appends(rng, args, ref: Reference, stale_uppers, count):
    txns = []
    for _ in range(count):
        width = rng.randint(1, args.width)
        parts = []
        for i in rng.sample(range(args.ids), width):
            u = stale_uppers.get(i, 0) if rng.random() < args.stale else ref.upper(i)
            hi = u + rng.randint(1, 3)
            ups = [(rng.randrange(u, hi), rng.randrange(10), rng.choice((1, 1, 1, -1)))
                   for _ in range(rng.randint(1, 4))]
            parts.append((i, u, hi, ups))
        txns.append(("append", parts))
    return txns


def make_merges(rng, args, view: View, count):
    txns = []
    candidates = [i for i, c in view.chain.items() if len(c) >= 2 or view.retirable.get(i)]
    for i in rng.sample(candidates, min(count, len(candidates))):
        chain = sorted(view.chain.get(i, []))
        u = view.uppers[i]
        r = retention(u, args.history)
        plan = {"id": i, "u": u, "g": view.gens[i], "rollup": None, "inputs": [], "retire": []}
        if len(chain) >= 2:
            # Geometric: a suffix of the chain may be merged when its oldest batch spans
            # at most twice the rest, which keeps chains logarithmic in an id's history.
            spans = [hi - lo for lo, hi, _, _ in chain]
            starts = [j for j in range(len(chain) - 1) if spans[j] <= 2 * sum(spans[j + 1:])]
            if starts:
                j = rng.choice(starts)
                plan["inputs"] = chain[j:]
                lo, hi = chain[j][0], chain[-1][1]
                plan["rollup"] = (lo, hi, min(r, hi))
                # Sometimes only add the rollup, and leave its inputs for a later merger.
                if rng.random() >= args.add_only:
                    plan["retire"] += plan["inputs"]
        plan["retire"] += view.retirable.get(i, [])
        if plan["rollup"] or plan["retire"]:
            txns.append(("merge", plan))
    return txns


def build(rng, args, ref: Reference, views: list[View], durable: Durable, epoch: int, next_tid: int, stats):
    """One epoch's transactions, in a random order of appends and merges."""
    stale = views[-2].uppers if len(views) >= 2 else {}
    specs = make_appends(rng, args, ref, stale, args.appends)
    for _ in range(args.mergers):
        if views:
            view = views[-1 - min(len(views) - 1, rng.choice((0, 0, 1, 2)))]
            specs += make_merges(rng, args, view, args.merges)
    rng.shuffle(specs)
    txns = []
    for kind, spec in specs:
        tid = next_tid
        next_tid += 1
        if kind == "append":
            reads, writes, effects, updates = [], [], [], []
            for i, u, hi, ups in spec:
                # The writer writes its data blob before it submits the transaction.
                start = time.perf_counter()
                bid = EMPTY if args.no_data else durable.blobs.put(encode_data((i, u, hi, 0, tid), ups), epoch)
                stats.phase["append blobs"] += time.perf_counter() - start
                reads.append((upper_key(i), u))
                writes.append((upper_key(i), hi))
                effects.append((1, i, (u, hi, 0, bid)))
                updates.append((i, ups))
            txns.append(Txn(tid, kind, reads, writes, effects, updates))
        else:
            i = spec["id"]
            reads = [(upper_key(i), spec["u"]), (gen_key(i), spec["g"])]
            if args.unguarded:
                # A negative control: merges that check nothing.
                reads = []
            writes = [(gen_key(i), spec["g"] + 1)]
            effects = []
            if spec["rollup"]:
                lo, hi, s = spec["rollup"]
                # The rollup's contents: its inputs' updates, times advanced to s.
                ups = []
                start = time.perf_counter()
                try:
                    for _, _, _, b in ([] if args.no_data else spec["inputs"]):
                        ups += decode_data(durable.blobs.get(b))
                except FileNotFoundError:
                    # The view is older than the sweep's grace: give up, as a reader would.
                    stats.abandoned += 1
                    next_tid -= 1
                    continue
                bid = EMPTY if args.no_data else durable.blobs.put(encode_data((i, lo, hi, s, 0), advance(ups, s)), epoch)
                stats.phase["merge blobs"] += time.perf_counter() - start
                effects.append((1, i, (lo, hi, s, bid)))
            for row in spec["retire"]:
                effects.append((0, i, tuple(row)))
            txns.append(Txn(tid, kind, reads, writes, effects))
    return txns, next_tid


# ---------------------------------------------------------------- one epoch


@dataclass
class Stats:
    epochs: int = 0
    attempts: Counter = field(default_factory=Counter)
    commits: Counter = field(default_factory=Counter)
    crashes: Counter = field(default_factory=Counter)
    reads_checked: int = 0
    recover_ms: list = field(default_factory=list)
    swept: int = 0
    abandoned: int = 0
    # Seconds spent in each phase this epoch, and every measured epoch's phases.
    phase: Counter = field(default_factory=Counter)
    epochs_timed: list = field(default_factory=list)
    ledger_appends: int = 0


def check(cat: Catalog, ref: Reference, args, rng, durable: Durable, stats: Stats, full=True) -> View | None:
    """Every invariant, and reads from disk; or, if not `full`, only that nothing is uncovered."""
    c = cat.client
    if args.no_views:
        # Only the catalog's contents can be checked; there are no chains to read.
        if full:
            if peek_records(c) != Counter({(k, *v): 1 for k, v in ref.records.items()}):
                raise RuntimeError("records differ from the reference")
            want = Counter({(i, *row): 1 for i, rows in ref.batches.items() for row in rows})
            if peek_counter(c, "batches") != want:
                raise RuntimeError("batches differ from the reference")
        return View({}, {}, {}, {}) if full else None
    if not full:
        if peek_counter(c, "uncovered"):
            raise RuntimeError("an id's upper is not covered")
        return None
    records = peek_records(c)
    want = Counter({(k, *v): 1 for k, v in ref.records.items()})
    if records != want:
        raise RuntimeError("records differ from the reference")
    batches = peek_counter(c, "batches")
    want = Counter({(i, *row): 1 for i, rows in ref.batches.items() for row in rows})
    if batches != want:
        raise RuntimeError("batches differ from the reference")
    if peek_counter(c, "uncovered"):
        raise RuntimeError("an id's upper is not covered")
    chain = defaultdict(list)
    for (i, *row), d in peek_counter(c, "chain").items():
        assert d == 1
        chain[i].append(tuple(row))
    retirable = defaultdict(list)
    for (i, *row), d in peek_counter(c, "retirable").items():
        assert d == 1
        retirable[i].append(tuple(row))
    for i in range(args.ids):
        u = ref.upper(i)
        r = retention(u, args.history)
        links = sorted(chain.get(i, []))
        at = 0
        for lo, hi, s, b in links:
            if lo != at or s > r or (lo, hi, s, b) not in ref.batches[i]:
                raise RuntimeError(f"id {i}: bad chain {links}")
            at = hi
        if at != u or (u > 0 and len(links) != fewest(ref.batches[i], u, r)):
            raise RuntimeError(f"id {i}: chain {links} is not a fewest chain to {u}")
        if sorted(retirable.get(i, [])) != sorted(ref.batches[i] - set(links)):
            raise RuntimeError(f"id {i}: retirable differs")
    # Readers: accumulate a chain's blobs, read from disk, at a time >= R.
    for i in rng.sample(range(args.ids), 0 if args.no_data else min(args.reads, args.ids)):
        u = ref.upper(i)
        if u == 0:
            continue
        t = rng.randrange(retention(u, args.history), u)
        got, want = Counter(), Counter()
        for _, _, _, b in chain[i]:
            for time_, k, d in decode_data(durable.blobs.get(b)):
                if time_ <= t:
                    got[k] += d
        for time_, k, d in ref.truth[i]:
            if time_ <= t:
                want[k] += d
        if consolidate(got) != consolidate(want):
            raise RuntimeError(f"id {i} at {t}: read {dict(got)} want {dict(want)}")
        stats.reads_checked += 1
    gens = {(-k - 1): v for (k, _, v), _ in records.items() if k < 0}
    uppers = {k: v for (k, _, v), _ in records.items() if k >= 0}
    return View(uppers, gens, dict(chain), dict(retirable))


def query_snapshots(cat: Catalog, ref: Reference, args, rng, where: Path, stats: Stats) -> None:
    """Time one-off snapshot queries (snapshot.ddp) for random sets of ids, and check
    each answer is a fewest chain from 0 to the id's upper."""
    program = where / "snapshot.ddp"
    program.write_text((HERE / "snapshot.ddp").read_text().replace("HISTORY", str(args.history)))
    c = cat.client
    stats.queries = []
    for size in args.query_sizes:
        times = []
        for _ in range(5):
            ids = rng.sample(range(args.ids), min(size, args.ids))
            frame = encode_frame((("prod", ["int"]), "unit"), [(i,) for i in ids], [1] * len(ids), [()] * len(ids))
            clock = time.perf_counter()
            c.load("snap", program)
            cat.send("snap", 0, frame)
            c.tick()
            frames = cat.peek_frames("snapshot")
            c.drop("snap")
            times.append(1000 * (time.perf_counter() - clock))
            chain = defaultdict(list)
            for (i, *row), d in frames_counter(frames).items():
                chain[i].append(tuple(row))
            for i in ids:
                u = ref.upper(i)
                links = sorted(chain.get(i, []))
                if (u > 0 and len(links) != fewest(ref.batches[i], u, retention(u, args.history))) or \
                        (links and (links[0][0] != 0 or links[-1][1] != u)):
                    raise RuntimeError(f"snapshot of id {i} is not a fewest chain: {links}")
        times.sort()
        row = dict(ids=size, p50_ms=times[2], min_ms=times[0], max_ms=times[-1])
        stats.queries.append(row)
        print(json.dumps(row), flush=True)


def recover(args, program: Path, durable_args, epoch: int, stats: Stats):
    durable = Durable(*durable_args, epoch)
    start = time.perf_counter()
    cat = Catalog(args, program)
    # The log's frames, fed back in order as they were written.
    for index, frame in durable.load():
        cat.send("cat", index, frame)
    cat.client.tick()
    stats.recover_ms.append((time.perf_counter() - start) * 1000)
    return cat, durable


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--dir", type=Path, required=True, help="the durable store; must not exist")
    p.add_argument("--ids", type=int, default=300)
    p.add_argument("--epochs", type=int, default=200)
    p.add_argument("--appends", type=int, default=100, help="append transactions per epoch")
    p.add_argument("--width", type=int, default=4, help="most ids one append transaction touches")
    p.add_argument("--stale", type=float, default=0.1, help="chance an append reads last epoch's upper")
    p.add_argument("--mergers", type=int, default=3)
    p.add_argument("--merges", type=int, default=20, help="merge transactions per merger per epoch")
    p.add_argument("--add-only", type=float, default=0.3)
    p.add_argument("--history", type=int, default=30)
    p.add_argument("--reads", type=int, default=50, help="reader checks per epoch")
    p.add_argument("--crash", type=float, default=0.03, help="chance per epoch of a crash")
    p.add_argument("--policy", choices=["geometric", "periodic", "never"], default="geometric")
    p.add_argument("--period", type=int, default=64)
    p.add_argument("--grace", type=int, default=4)
    p.add_argument("--sweep-every", type=int, default=8)
    p.add_argument("--sync", choices=["none", "fsync", "full"], default="full",
                   help="what forcing to storage means; see the comment above sync_fd")
    p.add_argument("--check-every", type=int, default=1, help="epochs between full checks (and fresh merger views)")
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--query-sizes", type=lambda v: [int(x) for x in v.split(",")], default=[],
                   help="at the end, time one-off snapshot queries for this many random ids each")
    p.add_argument("--no-data", action="store_true",
                   help="writers write no data blobs; every batch names the empty token 0 (no reader checks)")
    p.add_argument("--no-views", action="store_true",
                   help="hold the catalog without its chain views (for measurement; use with --mergers 0)")
    p.add_argument("--unguarded", action="store_true", help="merges read nothing (expected to fail)")
    p.add_argument("--report-every", type=int, default=50)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--json", type=Path)
    args = p.parse_args()
    os.environ.setdefault("DDIR_MAX_FEED_BYTES", str(1 << 28))

    args.dir.mkdir(parents=True)
    program = args.dir / "catalog.ddp"
    text = (HERE / "catalog.ddp").read_text().replace("HISTORY", str(args.history))
    if args.no_views:
        # The catalog's contents alone: everything before the first view.
        text = text[:text.index("let uppers")]
    program.write_text(text)
    rng = random.Random(args.seed)
    ref = Reference(args.ids)
    stats = Stats()
    durable_args = (args.dir, args.policy, args.period, args.sync)

    # Epoch 0: the seeded records, durable before anything else happens.
    durable = Durable(*durable_args, 0)
    keys = sorted(ref.records)
    seed = encode_frame(RECORDS, [(k,) for k in keys], [1] * len(keys), [ref.records[k] for k in keys])
    durable.commit(0, [(0, seed)])
    cat, durable = recover(args, program, durable_args, 0, stats)
    stats.recover_ms.clear()
    views = [check(cat, ref, args, rng, durable, stats)]
    next_tid = 1
    report = []
    print("epoch  root-log  log-KiB  blobs  blob-KiB  written-KiB/epoch  live-batches  chain  commit% append/merge",
          flush=True)
    written_mark = 0
    for epoch in range(1, args.epochs + 1):
        stats.phase.clear()
        mark = BlobStore.written
        txns, next_tid = build(rng, args, ref, views, durable, epoch, next_tid, stats)
        stats.phase["bytes: data blobs"] = BlobStore.written - mark
        rollback, delta, adds, removes, undo = ref.apply(txns)
        # The clients' frames. Not timed: each client encodes its own.
        ikeys, ivals, ekeys, evals = [], [], [], []
        for t in txns:
            for k, e in t.reads:
                ikeys.append((t.tid, 1, k))
                ivals.append((e,))
            for k, v in t.writes:
                ikeys.append((t.tid, 0, k))
                ivals.append((v,))
            for op, i, row in t.effects:
                ekeys.append((t.tid, op, i))
                evals.append(row)
        iframe = encode_frame(INTENTS, ikeys, [1] * len(ikeys), ivals)
        eframe = encode_frame(EFFECTS, ekeys, [1] * len(ekeys), evals)

        c = cat.client
        clock = time.perf_counter()
        c.load("res", HERE / "resolve.ddp")
        cat.send("res", 0, iframe)
        cat.send("res", 1, eframe)
        stats.phase["submit"] += time.perf_counter() - clock
        clock = time.perf_counter()
        c.tick()
        stats.phase["resolve"] += time.perf_counter() - clock
        clock = time.perf_counter()
        got = {k for (k,), d in peek_counter(c, "rollback").items()}
        delta_frames = cat.peek_frames("delta")
        adds_frames = cat.peek_frames("adds")
        removes_frames = cat.peek_frames("removes")
        stats.phase["read conclusions"] += time.perf_counter() - clock
        got_delta = frames_counter(delta_frames)
        got_adds = frames_counter(adds_frames)
        got_removes = frames_counter(removes_frames)
        if got != rollback:
            raise RuntimeError(f"rollback differs: {sorted(got ^ rollback)[:10]}")
        if (got_delta, got_adds, got_removes) != (delta, adds, removes):
            raise RuntimeError("conclusions differ from the reference")
        appended = 0
        for t in txns:
            stats.attempts[t.kind] += 1
            stats.commits[t.kind] += t.tid not in rollback
            if t.kind == "append" and t.tid not in rollback:
                appended += len(t.reads)

        point = rng.random()
        if point < args.crash / 2:
            # Crash before the root advances: the epoch never happened.
            stats.crashes["before root"] += 1
            cat.crash()
            ref.revert(undo)
            cat, durable = recover(args, program, durable_args, epoch, stats)
            views = [check(cat, ref, args, rng, durable, stats)]
            continue
        clock = time.perf_counter()
        mark = BlobStore.written
        durable.commit(epoch, [(0, f) for f in delta_frames] + [(1, f) for f in adds_frames]
                       + [(1, negate_frame(f)) for f in removes_frames])
        stats.phase["durable commit"] += time.perf_counter() - clock
        stats.phase["bytes: results blob"] = BlobStore.written - mark
        stats.phase.update(durable.timing)
        durable.timing.clear()
        # Here, and not before, the epoch's outcomes may be reported to its clients.
        if point < args.crash:
            # Crash after the root advances: the epoch is durable.
            stats.crashes["after root"] += 1
            cat.crash()
            cat, durable = recover(args, program, durable_args, epoch, stats)
        else:
            clock = time.perf_counter()
            c.request("feed cat 0 from delta")
            c.request("feed cat 1 from adds")
            c.request("feed cat 1 from removes diff=-1")
            c.drop("res")
            c.tick()
            stats.phase["apply"] += time.perf_counter() - clock
        full = epoch % args.check_every == 0 or point < args.crash
        view = check(cat, ref, args, rng, durable, stats, full)
        if view is not None:
            views = (views + [view])[-3:]

        clock = time.perf_counter()
        mark = BlobStore.written
        durable.compact(epoch, cat.consolidate_frames)
        if epoch % args.sweep_every == 0:
            live = {row[4] for row in frames_counter(cat.peek_frames("batches"))}
            stats.swept += durable.sweep(epoch, args.grace, live)
        stats.phase["background"] += time.perf_counter() - clock
        stats.phase["bytes: background"] = BlobStore.written - mark
        stats.epochs = epoch
        if epoch > args.warmup and point >= args.crash:
            stats.epochs_timed.append(dict(stats.phase, appended=appended, attempted=len(txns)))
        if epoch % args.report_every == 0 or epoch == args.epochs:
            log = durable.manifest["log"]
            log_bytes = sum(durable.blobs.path(b).stat().st_size for b, *_ in log)
            nblobs, blob_bytes = durable.blobs.usage()
            written = (BlobStore.written - written_mark) / args.report_every
            written_mark = BlobStore.written
            live = sum(len(v) for v in ref.batches.values())
            chains = sum(len(v) for v in views[-1].chain.values()) / args.ids  # as of the last full check
            commit = 100 * sum(stats.commits.values()) / max(1, sum(stats.attempts.values()))
            row = dict(epoch=epoch, log_entries=len(log), log_bytes=log_bytes, blobs=nblobs,
                       blob_bytes=blob_bytes, written_per_epoch=written, live_batches=live,
                       mean_chain=chains, recover_ms=stats.recover_ms[-1] if stats.recover_ms else None)
            report.append(row)
            print(f"{epoch:5d} {len(log):9d} {log_bytes / 1024:8.1f} {nblobs:6d} {blob_bytes / 1024:9.1f} "
                  f"{written / 1024:18.1f} {live:13d} {chains:6.1f} {commit:7.1f} "
                  f"{stats.commits['append']}/{stats.commits['merge']}", flush=True)

    if args.query_sizes:
        query_snapshots(cat, ref, args, rng, program.parent, stats)

    # A final clean restart, timed, and checked.
    cat.crash()
    cat, durable = recover(args, program, durable_args, args.epochs, stats)
    check(cat, ref, args, rng, durable, stats)
    cat.server.close()
    summary = dict(
        policy=args.policy, epochs=stats.epochs, attempts=dict(stats.attempts),
        commits=dict(stats.commits), crashes=dict(stats.crashes), reads_checked=stats.reads_checked,
        swept=stats.swept, abandoned=stats.abandoned, recover_ms_last=stats.recover_ms[-1], report=report)
    print(json.dumps({k: v for k, v in summary.items() if k != "report"}), flush=True)
    timed = stats.epochs_timed
    if timed:
        ms = lambda e, *ks: 1000 * sum(e.get(k, 0) for k in ks)
        ack = [ms(e, "submit", "resolve", "read conclusions", "durable commit") for e in timed]
        period = [a + ms(e, "apply") for a, e in zip(ack, timed)]
        appended = sum(e["appended"] for e in timed)
        bench = dict(
            ids=args.ids, appends_per_epoch=args.appends, width=args.width, sync=args.sync, workers=args.workers,
            epochs_timed=len(timed),
            ledger_appends_committed_per_epoch=appended / len(timed),
            ack_p50_ms=percentile(ack, 0.5), ack_p95_ms=percentile(ack, 0.95),
            period_p50_ms=percentile(period, 0.5),
            committed_ledger_appends_per_s=appended / (sum(period) / 1000),
            phase_mean_ms={k: sum(ms(e, k) for e in timed) / len(timed)
                           for k in ("append blobs", "merge blobs", "submit", "resolve", "read conclusions",
                                     "durable commit", "commit: write blob", "commit: flush", "commit: root",
                                     "apply", "background")})
        summary["bench"] = bench
        summary["per_epoch"] = timed
        print(json.dumps(bench), flush=True)
    if args.json:
        args.json.write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
