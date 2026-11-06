#!/usr/bin/env python3
"""The control service: the catalog, its transactions, and its durability.

It runs one DDIR server holding the catalog (../catalog/catalog.ddp, without its views), takes
transactions from anyone over HTTP, decides them a tick at a time with ../catalog/resolve.ddp,
makes each tick's conclusions durable in the store service, and only then replies. On start it
rebuilds the catalog from the store's root.

The catalog's records are compare-and-swap cells, `key -> (txn, value)`. Tables use the keys
below 2^40 (a table's upper is its id; ../catalog/SPEC.md). Control state uses three more ranges,
one cell per instance, whose values are blob ids (0 for none):

    SPEC + i      what instance i should run: a JSON spec (machine, workers, programs, inputs, outputs)
    INC + i       instance i's incarnation; every runner bumps it first, which fences its predecessor
    STATUS + i    what instance i last reported: a JSON status

    POST /txn       {"reads": [[k, expected]], "writes": [[k, value]],
                     "effects": [[op, table, lower, upper, since, blob]]}
                    -> {"committed": bool, "tid": t, "epoch": e}, after the epoch is durable
    GET  /cells     -> {"cells": [[k, txn, value], ...]}
    GET  /batches   -> {"batches": [[table, lower, upper, since, blob], ...]}
"""

from __future__ import annotations

import argparse
import json
import queue
import sys
import threading
import time
import urllib.request
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "catalog"))
sys.path.insert(0, str(HERE))
from run import (  # noqa: E402
    DEFAULT_BINARY, EFFECTS, INTENTS, Catalog, encode_frame, frames_counter, negate_frame,
    pack_frames, peek_counter, unpack_frames,
)
from store import Store, open_store  # noqa: E402

SPEC, INC, STATUS = 1 << 40, 2 << 40, 3 << 40


class Control:
    """A client of the control service."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def _call(self, path: str, doc: dict | None = None) -> dict:
        data = None if doc is None else json.dumps(doc).encode()
        with urllib.request.urlopen(urllib.request.Request(self.url + path, data=data), timeout=120) as r:
            return json.loads(r.read())

    def txn(self, reads=(), writes=(), effects=()) -> dict:
        return self._call("/txn", {"reads": list(reads), "writes": list(writes), "effects": list(effects)})

    def cells(self) -> dict[int, tuple[int, int]]:
        return {k: (t, v) for k, t, v in self._call("/cells")["cells"]}

    def batches(self) -> dict[int, list[tuple[int, int, int, int]]]:
        out: dict[int, list] = {}
        for t, lo, hi, s, b in self._call("/batches")["batches"]:
            out.setdefault(t, []).append((lo, hi, s, b))
        return {t: sorted(v) for t, v in out.items()}


class RemoteDurable:
    """The catalog's log of conclusions, as blobs named by the store's root (see run.Durable)."""

    def __init__(self, store: Store):
        self.store = store
        self.version, manifest = store.read_root()
        self.manifest = manifest or {"epoch": -1, "log": []}

    def _swap(self, manifest: dict) -> None:
        if not self.store.cas(self.version, manifest):
            raise RuntimeError("the root moved: another control service holds it")
        self.version += 1
        self.manifest = manifest

    def commit(self, epoch: int, frames) -> None:
        data = pack_frames(frames)
        blob = self.store.put(data)
        self._swap({"epoch": epoch, "log": self.manifest["log"] + [[blob, len(data), epoch, epoch]]})

    def load(self):
        return [f for blob, *_ in self.manifest["log"] for f in unpack_frames(self.store.get(blob))]

    def compact(self, consolidate_frames) -> None:
        # Geometric: merge the last two while the older is at most twice the newer.
        while len(self.manifest["log"]) >= 2 and self.manifest["log"][-2][1] <= 2 * self.manifest["log"][-1][1]:
            log = self.manifest["log"]
            frames = [f for blob, *_ in log[-2:] for f in unpack_frames(self.store.get(blob))]
            data = pack_frames(consolidate_frames(frames))
            merged = [self.store.put(data), len(data), log[-2][2], log[-1][3]]
            self._swap({"epoch": self.manifest["epoch"], "log": log[:-2] + [merged]})


def serve(args) -> None:
    durable = RemoteDurable(open_store(args.store))
    program = args.state / "catalog.ddp"
    text = (HERE.parent / "catalog" / "catalog.ddp").read_text()
    program.write_text(text[:text.index("let uppers")])
    cat = Catalog(args, program)
    for index, frame in durable.load():
        cat.send("cat", index, frame)
    cat.client.tick()
    c = cat.client

    def cells():
        return peek_counter(c, "records_up") + peek_counter(c, "records_gen")

    next_tid = max([t for (_, t, _) in cells()] + [0]) + 1
    epoch = durable.manifest["epoch"] + 1
    print(f"control: recovered at epoch {epoch}, {len(cells())} cells", flush=True)
    work: queue.Queue = queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def ask(self, kind, doc=None):
            box = {"done": threading.Event()}
            work.put((kind, doc, box))
            box["done"].wait()
            body = json.dumps(box["result"]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.ask(self.path.strip("/"))

        def do_POST(self):
            self.ask("txn", json.loads(self.rfile.read(int(self.headers["Content-Length"]))))

    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever,
                     daemon=True).start()
    print(f"control: serving on port {args.port}", flush=True)

    while True:
        items = [work.get()]
        time.sleep(args.gather)  # let a tick's worth of requests arrive
        while not work.empty():
            items.append(work.get())
        txns = [(doc, box) for kind, doc, box in items if kind == "txn"]
        if txns:
            ik, iv, ek, ev = [], [], [], []
            for doc, box in txns:
                tid = box["tid"] = next_tid
                next_tid += 1
                for k, e in doc["reads"]:
                    ik.append((tid, 1, k))
                    iv.append((e,))
                for k, v in doc["writes"]:
                    ik.append((tid, 0, k))
                    iv.append((v,))
                for op, t, lo, hi, s, b in doc["effects"]:
                    ek.append((tid, op, t))
                    ev.append((lo, hi, s, b))
            c.load("res", HERE.parent / "catalog" / "resolve.ddp")
            if ik:
                cat.send("res", 0, encode_frame(INTENTS, ik, [1] * len(ik), iv))
            if ek:
                cat.send("res", 1, encode_frame(EFFECTS, ek, [1] * len(ek), ev))
            c.tick()
            rollback = {k for (k,), _ in frames_counter(cat.peek_frames("rollback")).items()}
            delta, adds, removes = (cat.peek_frames(x) for x in ("delta", "adds", "removes"))
            # Durable first; only then absorb, and answer.
            durable.commit(epoch, [(0, f) for f in delta] + [(1, f) for f in adds]
                           + [(1, negate_frame(f)) for f in removes])
            c.request("feed cat 0 from delta")
            c.request("feed cat 1 from adds")
            c.request("feed cat 1 from removes diff=-1")
            c.drop("res")
            c.tick()
            for doc, box in txns:
                box["result"] = {"committed": box["tid"] not in rollback, "tid": box["tid"], "epoch": epoch}
                box["done"].set()
            epoch += 1
            durable.compact(cat.consolidate_frames)
        for kind, doc, box in items:
            if kind == "cells":
                box["result"] = {"cells": [list(row) for row in cells()]}
            elif kind == "batches":
                box["result"] = {"batches": [list(row) for row in peek_counter(c, "batches")]}
            elif kind != "txn":
                box["result"] = {"error": f"unknown request {kind}"}
            if kind != "txn":
                box["done"].set()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--store", default="http://127.0.0.1:7801")
    p.add_argument("--port", type=int, default=7802)
    p.add_argument("--state", type=Path, required=True, help="a scratch directory for local files")
    p.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--gather", type=float, default=0.002, help="seconds to wait for more requests per tick")
    a = p.parse_args()
    a.wrap_cmd = []
    a.state.mkdir(parents=True, exist_ok=True)
    serve(a)
