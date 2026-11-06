#!/usr/bin/env python3
"""Durable ticks, serial against pipelined (see run.py for the pieces).

Serial: each tick resolves, absorbs its conclusions, and makes them durable before
the next tick starts.

Pipelined: each tick resolves and absorbs its conclusions, then hands them to a
committer thread and starts the next tick at once. The committer makes ticks
durable in order, so the drive's flushes overlap the next tick's resolution.
A tick's outcomes may be reported when its commit returns, as before. Ticks that
queue while a commit is under way are committed together, as one blob and one root
swap, so a slow disk raises latency a little rather than without bound.

The catalog in memory runs ahead of the disk by at most the ticks in flight. That
is safe because outcomes wait for their commit, and commits are in order: if
tick N+1 is durable, so is tick N, whose conclusions N+1 was resolved against.
After a crash, recovery reads the root, and every tick past it was never reported.

All transactions, expected outcomes and frames are prepared before the clock
starts, so the clock sees only the server and the disk. Outcomes are checked
against the serial reference after the clock stops. Writers' data is the empty
token, and the catalog has no maintained views.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import queue
import random
import threading
import time

from run import decode  # noqa: E402  (run.py puts the LDBC client on the path)
from run import (
    EFFECTS, INTENTS, RECORDS, Catalog, Counter, Durable, Reference, Stats, View,
    build, check, encode_frame, frames_counter, negate_frame, percentile, recover,
)

HERE = Path(__file__).resolve().parent


def prepare(args, ref: Reference, durable: Durable, rng):
    """Every tick's frames and expected outcomes, from the serial reference."""
    ticks = []
    views = [View({}, {}, {}, {})]
    next_tid = 1
    stats = Stats()
    for epoch in range(1, args.ticks + 1):
        txns, next_tid = build(rng, args, ref, views, durable, epoch, next_tid, stats)
        rollback, delta, adds, removes, _ = ref.apply(txns)
        ik, iv, ek, ev = [], [], [], []
        for t in txns:
            for k, e in t.reads:
                ik.append((t.tid, 1, k))
                iv.append((e,))
            for k, v in t.writes:
                ik.append((t.tid, 0, k))
                iv.append((v,))
            for op, i, row in t.effects:
                ek.append((t.tid, op, i))
                ev.append(row)
        appended = sum(len(t.reads) for t in txns if t.tid not in rollback)
        ticks.append(dict(
            epoch=epoch, appended=appended,
            iframe=encode_frame(INTENTS, ik, [1] * len(ik), iv),
            eframe=encode_frame(EFFECTS, ek, [1] * len(ek), ev),
            expect=(rollback, delta, adds, removes)))
    return ticks


def text(v) -> str:
    return bytes(v).decode() if isinstance(v, list) else str(v)


def time_by_op(cat) -> Counter:
    """Nanoseconds scheduled so far, by (program, statement, operator name)."""
    out = Counter()
    for key, val, diff in decode([("data", f) for f in cat.peek_frames("q.by_op")]):
        p, s, n = (text(x) for x in key)
        out[(p, s, n)] += (val[0] if isinstance(val, list) else val) * diff
    return out


def report_ops(end: Counter, start: Counter, ticks: int, path: Path) -> None:
    """Per-tick operator time between two marks, largest first, and as JSON at `path`."""
    delta = Counter(end)
    delta.subtract(start)
    rows = sorted(((ns / ticks / 1e6, k) for k, ns in delta.items() if ns > 0 and k[0] not in ("diag", "diagq")),
                  reverse=True)
    total = sum(ms for ms, _ in rows)
    print(f"operator time per tick, summed over workers: {total:.1f} ms")
    for ms, (p, s, n) in rows[:40]:
        print(f"  {ms:7.2f} ms  {100 * ms / total:5.1f}%  {p:10} {s:16} {n}")
    path.write_text(json.dumps([dict(ms=ms, program=p, statement=s, name=n) for ms, (p, s, n) in rows], indent=1))


def run(args, mode: str) -> dict:
    root = args.dir / mode
    root.mkdir(parents=True)
    program = root / "catalog.ddp"
    text = (HERE / "catalog.ddp").read_text().replace("HISTORY", str(args.history))
    program.write_text(text[:text.index("let uppers")])
    rng = random.Random(args.seed)
    ref = Reference(args.ids)
    stats = Stats()
    durable_args = (root, "never", 0, args.sync)
    durable = Durable(*durable_args, 0)
    keys = sorted(ref.records)
    durable.commit(0, [(0, encode_frame(RECORDS, [(k,) for k in keys], [1] * len(keys),
                                        [ref.records[k] for k in keys]))])
    cat, durable = recover(args, program, durable_args, 0, stats)
    ticks = prepare(args, ref, durable, rng)
    if args.diag:
        cat.client.load("diagq", HERE / "diagq.ddp")
        cat.client.tick()
    marks = {}

    # The committer: makes ticks durable in order, and notes when each may be reported.
    work: queue.Queue = queue.Queue()
    acked: dict[int, float] = {}
    done = {t["epoch"]: threading.Event() for t in ticks}
    commit_ms: dict[int, float] = {}
    groups: dict[int, int] = {}

    def committer():
        # Group commit: take every tick waiting, and make them durable with one blob and
        # one root swap, so a backlog costs one pair of flushes, not one pair per tick.
        while (item := work.get()) is not None:
            batch = [item]
            while not work.empty():
                nxt = work.get()
                if nxt is None:
                    work.put(None)
                    break
                batch.append(nxt)
            clock = time.perf_counter()
            durable.commit(batch[-1][0], [f for _, frames in batch for f in frames])
            now = time.perf_counter()
            for epoch, _ in batch:
                acked[epoch] = now
                commit_ms[epoch] = 1000 * (now - clock)
                groups[epoch] = len(batch)
                done[epoch].set()

    thread = threading.Thread(target=committer)
    thread.start()
    c = cat.client
    started, got = {}, {}
    compute_ms = {}
    for t in ticks:
        n = t["epoch"]
        started[n] = time.perf_counter()
        c.load("res", args.resolver)
        cat.send("res", 0, t["iframe"])
        cat.send("res", 1, t["eframe"])
        c.tick()
        rollback = cat.peek_frames("rollback")
        delta, adds, removes = (cat.peek_frames(x) for x in ("delta", "adds", "removes"))
        # Absorb at once: the next tick resolves against these conclusions.
        c.request("feed cat 0 from delta")
        c.request("feed cat 1 from adds")
        c.request("feed cat 1 from removes diff=-1")
        c.drop("res")
        c.tick()
        compute_ms[n] = 1000 * (time.perf_counter() - started[n])
        got[n] = (rollback, delta, adds, removes)
        frames = [(0, f) for f in delta] + [(1, f) for f in adds] + [(1, negate_frame(f)) for f in removes]
        work.put((n, frames))
        if mode == "serial":
            done[n].wait()
        if args.diag and n in (args.warmup, args.ticks):
            marks[n] = time_by_op(cat)
    work.put(None)
    thread.join()

    # Check every tick's outcomes and conclusions, then the catalog, then recovery.
    for t in ticks:
        rollback, delta, adds, removes = got[t["epoch"]]
        want_rb, want_delta, want_adds, want_removes = t["expect"]
        if {k for (k,), _ in frames_counter(rollback).items()} != want_rb:
            raise RuntimeError(f"tick {t['epoch']}: rollback differs")
        if (frames_counter(delta), frames_counter(adds), frames_counter(removes)) != \
                (want_delta, want_adds, want_removes):
            raise RuntimeError(f"tick {t['epoch']}: conclusions differ")
    args.no_views = True
    check(cat, ref, args, rng, durable, stats)
    if args.wrap_cmd:
        # Let the profiler save, and do not profile the recovery.
        cat.server.close()
        args.wrap_cmd = []
    else:
        cat.crash()
    cat, durable = recover(args, program, durable_args, args.ticks, stats)
    check(cat, ref, args, rng, durable, stats)
    cat.server.close()

    measured = [t for t in ticks if t["epoch"] > args.warmup]
    if args.diag:
        report_ops(marks[args.ticks], marks[args.warmup], len(measured), args.diag)
    first, last = measured[0]["epoch"], measured[-1]["epoch"]
    wall = acked[last] - started[first]
    appended = sum(t["appended"] for t in measured)
    latency = [1000 * (acked[t["epoch"]] - started[t["epoch"]]) for t in measured]
    return dict(
        mode=mode, ids=args.ids, appends_per_tick=args.appends, sync=args.sync, ticks=len(measured),
        committed_appends_per_s=appended / wall, wall_ms_per_tick=1000 * wall / len(measured),
        latency_p50_ms=percentile(latency, 0.5), latency_p95_ms=percentile(latency, 0.95),
        compute_ms_mean=sum(compute_ms[t["epoch"]] for t in measured) / len(measured),
        commit_ms_mean=sum(commit_ms[t["epoch"]] for t in measured) / len(measured),
        ticks_per_commit=len(measured) / len({acked[t["epoch"]] for t in measured}))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dir", type=Path, required=True, help="must not exist")
    p.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[4] / "target/release/ddir_server")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--ids", type=int, default=1_000_000)
    p.add_argument("--appends", type=int, default=25_600)
    p.add_argument("--width", type=int, default=1)
    p.add_argument("--ticks", type=int, default=40)
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--sync", choices=["none", "fsync", "full"], default="full")
    p.add_argument("--modes", default="serial,pipelined")
    p.add_argument("--history", type=int, default=30)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--resolver", type=Path, default=HERE / "resolve.ddp")
    p.add_argument("--diag", type=Path, help="with DDIR_DIAGNOSTICS=1: write per-operator time here")
    p.add_argument("--wrap", default="", help="run the server under this command (a profiler)")
    args = p.parse_args()
    # What build() and check() expect of a run.py argument set.
    args.stale, args.mergers, args.merges, args.add_only = 0.0, 0, 0, 0.0
    args.no_data, args.unguarded, args.no_views, args.reads = True, False, True, 0
    os.environ.setdefault("DDIR_MAX_FEED_BYTES", str(1 << 28))
    args.wrap_cmd = args.wrap.split()
    if args.diag:
        os.environ["DDIR_DIAGNOSTICS"] = "1"
    args.dir.mkdir(parents=True)
    for mode in args.modes.split(","):
        print(json.dumps(run(args, mode)), flush=True)


if __name__ == "__main__":
    main()
