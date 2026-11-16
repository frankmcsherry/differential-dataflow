#!/usr/bin/env python3
"""Rounds of appends, admitted by round.ddp and made durable by a thin driver: the model's short check, at speed.

This is fabric.py with the model's programs in place of the older rules. The measure is the same:
appends that are made, judged, absorbed and durable, per second.

The server runs:
  index     standing: the entries, and what round.ddp reads of them (`own`, kept by each round's output, and `bound`)
  gen       flat.ddp, one per round: shapes flat columns of ints into appends
  adm       round.ddp, one per round: admission, bound to `index` and `gen`

A round, driven from here:
  1. load `gen` and `adm`, and feed the round's appends to `gen` as binary frames
  2. tick: every transaction is made, refused, or left waiting
  3. read the round's new spans, as binary frames
  4. feed the spans into the entries and `adm::moved` into `own`, drop `gen` and `adm`, and tick
  5. write the frames as one blob, flush, swap the root, flush: the round is durable
With `--store register` the log is not files: each record is a blob in the blob store, and the root is a cell
of the register, swapped only if it has not moved (see plumbing.py; LEDGER_ETCD and LEDGER_FILER say where they are).

No rule of system.ddp or step.ddp is read in a round. `--check` reads them afterwards, of the whole state:
it kills the server, rebuilds the entries from the log on a fresh one, derives the index from them with index.ddp,
compares that with the index the rounds kept, and with `--rules` holds the state to every rule of system.ddp.

  cargo build --release -p ddir-server
  python3 interactive/examples/ledger/rounds.py --dir /tmp/rounds --ledgers 100000 --clocks 1 --spans 5000 --check --rules
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import time
from pathlib import Path

from fabric import HERE, ROOT, Server, Store, World, phase, rows_in

DIR = HERE.relative_to(ROOT)
BIND = "input0=gen::appends input1=gen::runners input2=index::own input3=index::bound"
STATE_RULES = ["repeated", "reregistered", "doubled", "ambiguous", "reprogrammed", "redefined", "contested", "reheld", "forked",
               "orphaned", "unclocked", "misnamed", "unprogrammed", "unregistered", "misheld", "undefined", "mismatched",
               "malformed", "early", "skipped", "forgotten", "spanned", "hollow", "starved", "belated",
               "misbound", "misplaced", "misshapen", "unfounded"]


def declarations() -> str:
    """The types the model's programs share, as round.ddp declares them."""
    text = (HERE / "round.ddp").read_text()
    return text[text.index("type Id"):text.index("let appends = input 0")]


class RegisterStore:
    """The log as fabric.py's Store keeps it, with blobs in the blob store and the root in a cell of the register."""

    def __init__(self, name: str):
        import plumbing
        self.register, self.blobs = plumbing.Register(name), plumbing.Blobs("rounds")
        root, self.revision = self.register.read()
        self.version, self.head = (root["version"], root["head"]) if root else (0, "")

    def commit(self, frames: list[tuple[int, bytes]]) -> dict:
        clock = time.perf_counter()
        record = bytearray(self.head.ljust(64).encode() + struct.pack("<Q", len(frames)))
        for target, frame in frames:
            record += struct.pack("<2Q", target, len(frame)) + frame
        name = self.blobs.put(bytes(record))
        written = time.perf_counter()
        revision = self.register.swap(self.revision, {"version": self.version + 1, "head": name})
        if revision is None:
            raise RuntimeError("the root moved: another writer holds this log")
        self.version, self.head, self.revision = self.version + 1, name, revision
        return dict(bytes=len(record), write=written - clock, flush=0.0, root=time.perf_counter() - written)

    def log(self) -> list[list[tuple[int, bytes]]]:
        records, name = [], self.head
        while name:
            data = self.blobs.get(name)
            name, (count,) = data[:64].decode().strip(), struct.unpack_from("<Q", data, 64)
            at, frames = 72, []
            for _ in range(count):
                target, size = struct.unpack_from("<2Q", data, at)
                frames.append((target, data[at + 16:at + 16 + size]))
                at += 16 + size
            records.append(frames)
        return records[::-1]


class Rounds:
    """One keeper of appends: a server, the entries and the index beside them, and the durable log."""

    def __init__(self, args, store: Store):
        self.args, self.store = args, store
        self.server = Server(args.binary, args.workers)
        self.server.ask("load index begin\n" + declarations() +
                        "let own = input 0 : ((Id), (int, Frontier, int, Id, int));\n"
                        "let bound = input 1 : ((Id), (Id, int));\n"
                        "let entries = input 2 : ((Entry), ());\n"
                        'output "own" = own;\noutput "bound" = bound;\noutput "entries" = entries;\nindex end-load')
        self.server.ask("tick")

    def round(self, requests: dict) -> dict:
        """Admit one round and absorb it. Returns the state's change as frames, and the timings."""
        s, mark, t = self.server, time.perf_counter(), {}

        def lap(name: str) -> None:
            nonlocal mark
            now = time.perf_counter()
            t[name], mark = now - mark, now

        s.ask(f"load gen from {DIR}/flat.ddp")
        if requests.get("creates"):
            # Ledgers are made here without a rule being read: this sets a measurement up, and is not measured.
            lap("load")
            s.feed("gen 1", requests["creates"])
            lap("feed")
            s.ask("tick")
            lap("resolve")
            added = s.frames("gen::ledgers")
            lap("read")
            s.ask("feed index 2 from gen::ledgers")
        else:
            s.ask(f"load adm from {DIR}/{self.args.round} {BIND}")
            lap("load")
            s.feed("gen 0", requests["singles"])
            lap("feed")
            s.ask("tick")
            lap("resolve")
            added = s.frames("adm::spans")
            lap("read")
            s.ask("feed index 0 from adm::moved")
            if self.args.manifest == "resident":
                s.ask("feed index 2 from adm::spans")
            s.ask("drop adm")
        s.ask("drop gen")
        s.ask("tick")
        lap("absorb")
        return dict(t, frames=[(2, frame) for frame in added], committed=rows_in(added))

    def derive(self) -> None:
        """Make the index from the entries. A keeper does this when it starts, and not again."""
        s = self.server
        s.ask(f"load derive from {DIR}/index.ddp input0=index::entries")
        s.ask("tick")
        s.ask("feed index 0 from derive::own")
        s.ask("drop derive")
        s.ask("tick")

    def recover(self) -> float:
        """On a fresh server: the entries from the log, and the index from the entries."""
        begin = time.perf_counter()
        for record in self.store.log():
            for target, frame in record:
                self.server.feed(f"index {target}", frame)
        self.server.ask("tick")
        self.derive()
        return time.perf_counter() - begin

    def rules(self) -> dict:
        """Every rule of system.ddp, of the entries as they stand: how many rows each has, which should be none.
        With them, how many ledgers system.ddp finds standing, and how long all of this took."""
        s, begin = self.server, time.perf_counter()
        s.ask(f"load audit from {DIR}/system.ddp input0=index::entries")
        s.ask("tick")
        broken = {rule: rows_in(s.frames(f"audit::{rule}")) for rule in STATE_RULES}
        standing = rows_in(s.frames("audit::standing"))
        s.ask("drop audit")
        s.ask("tick")
        return dict(broken={rule: count for rule, count in broken.items() if count}, standing=standing,
                    seconds=round(time.perf_counter() - begin, 2))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dir", type=Path, required=True, help="where the durable log goes; must not exist")
    p.add_argument("--binary", type=Path, default=ROOT / "target/release/ddir_server")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--ledgers", type=int, default=1_000_000)
    p.add_argument("--clocks", type=int, default=1, help="how many clocks the ledgers follow; 0 for none")
    p.add_argument("--still", action="store_true", help="do not advance the clocks")
    p.add_argument("--spans", default="25600", help="appends per round; one phase for each")
    p.add_argument("--contend", type=float, default=0.0, help="fraction of appends that a second transaction repeats")
    p.add_argument("--rounds", type=int, default=30)
    p.add_argument("--warmup", type=int, default=5)
    p.add_argument("--modes", default="pipelined")
    p.add_argument("--sync", choices=["none", "fsync", "full"], default="full")
    p.add_argument("--store", choices=["file", "register"], default="file", help="where the log is kept")
    p.add_argument("--manifest", choices=["resident", "none"], default="resident",
                   help="whether the server keeps every entry resident, or only the index")
    p.add_argument("--create-batch", type=int, default=50_000, help="ledgers per round while setting up")
    p.add_argument("--check", action="store_true", help="afterwards, kill the server and rebuild from the log")
    p.add_argument("--rules", action="store_true", help="with --check: hold the rebuilt state to every rule of system.ddp")
    p.add_argument("--round", default="round.ddp", help="the admission program, beside this file; for comparing two")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args()
    args.dir.mkdir(parents=True)
    name = f"rounds-{args.dir.name}-{int(time.time())}"
    store = lambda: RegisterStore(name) if args.store == "register" else Store(args.dir, args.sync)
    keeper = Rounds(args, store())
    world = World(args.ledgers, args.clocks, args.seed)
    try:
        creating = []
        if args.clocks:
            frame, count = world.clock_creation()
            creating.append(dict(creates=frame, spans=0, created=count, transactions=count))
        for first in range(0, args.ledgers, args.create_batch):
            frame, count = world.creation(first, min(args.create_batch, args.ledgers - first))
            creating.append(dict(creates=frame, spans=0, created=count, transactions=count))
        report = phase(keeper, creating, "pipelined", min(1, len(creating) - 1))
        begin = time.perf_counter()
        keeper.derive()
        print(json.dumps(dict(phase="create", ledgers=args.ledgers, clocks=args.clocks, creations_per_s=report["creations_per_s"],
                              index_s=round(time.perf_counter() - begin, 2))), flush=True)

        for spans in [int(x) for x in args.spans.split(",")]:
            for mode in args.modes.split(","):
                rounds = [world.round(spans, 1, args.contend, bool(args.clocks) and not args.still) for _ in range(args.rounds)]
                report = phase(keeper, rounds, mode, args.warmup)
                print(json.dumps(dict(phase="append", spans=spans, contend=args.contend, clocks=args.clocks,
                                      manifest=args.manifest, sync=args.sync, store=args.store, workers=args.workers, **report)), flush=True)

        if args.check:
            kept = keeper.server.rows("index::own")
            keeper.server.kill()
            keeper = Rounds(args, store())
            seconds = keeper.recover()
            derived = keeper.server.rows("index::own")
            judged = keeper.rules() if args.rules else {}
            broken = judged.get("broken")
            # The rebuilt keeper must also go on: one more round of appends, all of which must be made.
            more = world.round(min(1000, args.ledgers), 1, 0.0, bool(args.clocks) and not args.still)
            went_on = keeper.round(more)["committed"] == more["spans"]
            print(json.dumps(dict(phase="check", records=len(keeper.store.log()), ledgers=len(derived),
                                  index_kept_is_index_derived=kept == derived, rules_broken=broken,
                                  rules_standing=judged.get("standing"), rules_s=judged.get("seconds"),
                                  recovery_s=round(seconds, 2), admits_after_recovery=went_on)), flush=True)
            if kept != derived or broken or not went_on:
                raise SystemExit("the index the rounds kept is not what the entries say, or a rule is broken")
    finally:
        keeper.server.close()


if __name__ == "__main__":
    main()
