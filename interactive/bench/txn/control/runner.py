#!/usr/bin/env python3
"""Runs one instance: reads what it should run from the catalog, runs it, and stops when told.

    1. Read instance i's spec. None (0) means nothing to run: exit.
    2. Bump i's incarnation by compare-and-swap. A predecessor still running is now fenced: its
       next write reads the old incarnation and rolls back.
    3. Start a DDIR server and install the spec's programs, whose text is in blobs.
    4. Loop: feed each input table's new batches into its program, tick, and append each
       output's change since the last append to its table, by compare-and-append on the
       table's upper. Report status. Stop if the spec changes or another incarnation starts.

An output resumes from its table: on start, the runner reads what the table already holds and
appends only the difference from there, so a restart neither loses nor repeats an update.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "catalog"))
sys.path.insert(0, str(HERE))
from run import DEFAULT_BINARY, ServerProcess, decode, encode_frame, pack_frames, unpack_frames  # noqa: E402
from client import parse_shape as _parse_shape  # noqa: E402


def parse_shape(text: str):
    """A `(key, val)` shape as `encode_frame` takes it."""
    return tuple(_parse_shape(text)[1])
from control import INC, SPEC, STATUS, Control  # noqa: E402
from store import Store, open_store  # noqa: E402


def tuplify(x):
    return tuple(tuplify(y) for y in x) if isinstance(x, list) else x


def rows(frames: list[bytes]) -> Counter:
    out = Counter()
    for key, val, diff in decode([("data", f) for f in frames]):
        out[(tuplify(key), tuplify(val))] += diff
    return out


def table_contents(store: Store, batches) -> Counter:
    out = Counter()
    for _, _, _, blob in batches:
        out.update(rows([f for _, f in unpack_frames(store.get(blob))]))
    return +out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--control", required=True)
    p.add_argument("--store", required=True)
    p.add_argument("--instance", type=int, required=True)
    p.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    p.add_argument("--poll", type=float, default=0.2, help="seconds between rounds")
    p.add_argument("--heartbeat", type=float, default=1.0, help="seconds between status reports")
    a = p.parse_args()
    control, store, i = Control(a.control), open_store(a.store), a.instance
    say = lambda *x: print(f"[runner {i} pid {os.getpid()}]", *x, flush=True)

    cells = control.cells()
    spec_blob = cells.get(SPEC + i, (0, 0))[1]
    if spec_blob <= 0:
        say("no spec; exiting")
        return
    spec = json.loads(store.get(spec_blob))
    while True:
        seen = cells.get(INC + i, (0, -1))[1]
        incarnation = max(seen, 0) + 1
        if control.txn(reads=[[INC + i, seen]], writes=[[INC + i, incarnation]])["committed"]:
            break
        cells = control.cells()
    say(f"incarnation {incarnation} on {socket.gethostname()}: {spec}")

    server = ServerProcess(a.binary.resolve(), spec.get("workers", 1), [], True)
    c = server.client

    def send(prog, index, frame):
        rid = c._id()
        c._exchange(rid, f"{rid} feed {prog} {index} frame {len(frame)}\n".encode() + frame)

    def peek_frames(trace):
        rid = c._id()
        _, frames = c._exchange(rid, f"{rid} peek {trace} as corgi\n")
        return [f for kind, f in frames if kind == "data"]

    workdir = Path(tempfile.mkdtemp(prefix=f"ddir-instance-{i}-"))
    for prog in spec["programs"]:
        path = workdir / f"{prog['name']}.ddp"
        path.write_bytes(store.get(prog["blob"]))
        c.load(prog["name"], path)

    batches = control.batches()
    frontier = {inp["table"]: 0 for inp in spec.get("inputs", [])}
    emitted = {out["table"]: table_contents(store, batches.get(out["table"], [])) for out in spec.get("outputs", [])}
    shapes = {out["table"]: parse_shape(out["shape"]) for out in spec.get("outputs", [])}
    last_status, state = 0.0, "running"
    try:
        while True:
            cells, batches = control.cells(), control.batches()
            if cells.get(INC + i, (0, 0))[1] != incarnation:
                state = "fenced"
                say("a newer incarnation started; stopping")
                return
            if cells.get(SPEC + i, (0, 0))[1] != spec_blob:
                state = "stopped"
                say("spec changed or removed; stopping")
                return
            for inp in spec.get("inputs", []):
                t = inp["table"]
                for lo, hi, _, blob in batches.get(t, []):
                    if lo >= frontier[t]:
                        for _, frame in unpack_frames(store.get(blob)):
                            send(inp["program"], inp["input"], frame)
                        frontier[t] = hi
            c.tick()
            for out in spec.get("outputs", []):
                t = out["table"]
                current = rows(peek_frames(out["export"]))
                change = Counter(current)
                change.subtract(emitted[t])
                change = Counter({k: d for k, d in change.items() if d})
                if not change:
                    continue
                keys = [k for k, _ in change]
                vals = [v for _, v in change]
                frame = encode_frame(shapes[t], keys, list(change.values()), vals)
                blob = store.put(pack_frames([(0, frame)]))
                upper = cells.get(t, (0, -1))[1]
                result = control.txn(reads=[[INC + i, incarnation], [t, upper]],
                                     writes=[[t, upper + 1]],
                                     effects=[[1, t, upper, upper + 1, 0, blob]])
                if result["committed"]:
                    emitted[t] = +current
                    cells[t] = (result["tid"], upper + 1)
                    say(f"appended {len(change)} changes to table {t} at [{upper}, {upper + 1})")
            now = time.time()
            if now - last_status >= a.heartbeat:
                report(control, store, i, incarnation, state, frontier, cells, spec)
                last_status = now
            time.sleep(a.poll)
    finally:
        # Shut the server down whatever happened; a final report is best effort.
        try:
            if state != "fenced":
                report(control, store, i, incarnation, state, frontier, control.cells(), spec)
        except OSError as error:
            say(f"could not report final status: {error}")
        server.close()


def report(control, store, i, incarnation, state, frontier, cells, spec) -> None:
    status = {"incarnation": incarnation, "state": state, "host": socket.gethostname(), "pid": os.getpid(),
              "time": time.time(), "inputs": frontier,
              "outputs": {out["table"]: cells.get(out["table"], (0, -1))[1] for out in spec.get("outputs", [])}}
    blob = store.put(json.dumps(status).encode())
    control.txn(reads=[[INC + i, incarnation]], writes=[[STATUS + i, blob]])


if __name__ == "__main__":
    main()
