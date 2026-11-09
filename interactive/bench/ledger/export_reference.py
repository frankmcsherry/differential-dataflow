#!/usr/bin/env python3
"""Emit the old catalog pipeline's exact width-one, metadata-only requests."""

import argparse
import hashlib
import importlib
import json
from pathlib import Path
import random
import struct
import sys
from types import SimpleNamespace


HEADER = struct.Struct("<8sQQQQQQ")
REQUEST = struct.Struct("<qqqqq")
MAGIC = b"DDIRMAT1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--checkout", type=Path, required=True, help="checkout of the reference branch")
    parser.add_argument("--ids", type=int, default=1_000_000)
    parser.add_argument("--ticks", type=int, default=40)
    parser.add_argument("--appends", type=int, default=25_600)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    assert min(args.ids, args.ticks, args.appends) > 0
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.checkout / "interactive/storage/catalog"))
    old = importlib.import_module("run")
    assert old.build.__globals__["make_appends"] is old.make_appends
    options = SimpleNamespace(ids=args.ids, width=1, stale=0.0, appends=args.appends,
                              mergers=0, merges=0, add_only=0.0, no_data=True,
                              unguarded=False, history=30)
    rng = random.Random(args.seed)
    reference = old.Reference(args.ids)
    views = [old.View({}, {}, {}, {})]
    stats = old.Stats()
    next_tid = 1
    records = 0
    committed = []
    with args.output.open("wb") as output:
        output.write(HEADER.pack(MAGIC, args.ids, args.ticks, args.appends, args.seed, 0, 0))
        for epoch in range(1, args.ticks + 1):
            # no_data and mergers=0 make build's durable argument unused.
            txns, next_tid = old.build(rng, options, reference, views, None, epoch, next_tid, stats)
            rollback, _, _, _, _ = reference.apply(txns)
            committed.append(len(txns) - len(rollback))
            for txn in txns:
                assert txn.kind == "append" and len(txn.reads) == len(txn.writes) == len(txn.effects) == 1
                operation, ledger, (lower, upper, since, blob) = txn.effects[0]
                assert operation == 1 and since == 0 and blob == old.EMPTY
                assert txn.reads == [(old.upper_key(ledger), lower)]
                assert txn.writes == [(old.upper_key(ledger), upper)]
                output.write(REQUEST.pack(epoch, txn.tid, ledger, lower, upper))
                records += 1
        assert records == args.ticks * args.appends
        output.seek(0)
        output.write(HEADER.pack(MAGIC, args.ids, args.ticks, args.appends, args.seed,
                                 records, sum(committed)))
    digest = hashlib.sha256()
    with args.output.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    print(json.dumps(dict(file=str(args.output), ids=args.ids, ticks=args.ticks,
                          appends=args.appends, seed=args.seed, records=records,
                          committed=sum(committed), committed_by_epoch=committed,
                          sha256=digest.hexdigest()), sort_keys=True))


if __name__ == "__main__":
    main()
