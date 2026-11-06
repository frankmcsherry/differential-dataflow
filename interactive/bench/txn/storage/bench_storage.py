#!/usr/bin/env python3
"""Closed-loop multi-shard compare-and-append benchmark for the live DDIR server.

A transaction appends one batch to each of `width` shards, atomically: for each
shard it reads the upper it expects and writes a new upper, and it names the
blob holding the batch. Each cycle installs resolve.ddp for one epoch, admits
the batch's intents and blobs, closes the epoch, reads `rollback` and
`garbage`, feeds `delta` and `append` into shards.ddp, and drops the resolver.

Every cycle's rollbacks and garbage are checked against a serial reference, and
at the end every shard's upper and batch chain are compared in full.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import random
import shlex
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from bench_txn import (  # noqa: E402
    DEFAULT_BINARY, INTENTS, Client, ServerProcess, comma_ints, percentile,
)
from client import encode_frame  # noqa: E402

BLOBS = (("prod", ["int", "int"]), ("prod", ["int"]))
UPPERS = (("prod", ["int"]), ("prod", ["int", "int"]))


@dataclass(frozen=True)
class Append:
    """For each shard: (shard, expected upper, new upper, blob)."""

    txn_id: int
    parts: tuple[tuple[int, int, int, int], ...]


@dataclass
class Sample:
    attempts: int
    committed: int
    install_ms: float
    submit_ms: float
    resolve_ms: float
    observe_ms: float
    apply_ms: float
    drop_ms: float

    @property
    def cycle_ms(self) -> float:
        return (self.install_ms + self.submit_ms + self.resolve_ms + self.observe_ms
                + self.apply_ms + self.drop_ms)


class Shards:
    """The serial reference: each shard's upper and its batch chain."""

    def __init__(self, count: int):
        self.uppers = [0] * count
        self.batches: dict[int, list[tuple[int, int, int]]] = defaultdict(list)

    def apply(self, txns: list[Append]) -> tuple[set[int], set[int]]:
        rollback: set[int] = set()
        garbage: set[int] = set()
        for txn in txns:
            if all(self.uppers[s] == lo for s, lo, _, _ in txn.parts):
                for s, lo, hi, b in txn.parts:
                    self.uppers[s] = hi
                    self.batches[s].append((lo, hi, b))
            else:
                rollback.add(txn.txn_id)
                garbage.update(b for _, _, _, b in txn.parts)
        return rollback, garbage


def generate(rng: random.Random, first_id: int, first_blob: int, count: int, width: int,
             shards: int, pattern: str, cycle: int, uppers: list[int]) -> list[Append]:
    """Expected uppers are the shards' uppers as of the batch's start."""
    txns = []
    blob = first_blob
    for i in range(count):
        if pattern == "disjoint":
            base = (cycle * count + i) * width
            chosen = [(base + j) % shards for j in range(width)]
        else:
            chosen = rng.sample(range(shards), width)
        parts = []
        for s in chosen:
            parts.append((s, uppers[s], uppers[s] + 1, blob))
            blob += 1
        txns.append(Append(first_id + i, tuple(parts)))
    return txns


def feed_frame(client: Client, prog: str, index: int, shape, keys, vals) -> float:
    reqid = client._id()
    encode = time.perf_counter()
    frame = encode_frame(shape, keys, [1] * len(keys), vals)
    payload = f"{reqid} feed {prog} {index} frame {len(frame)}\n".encode() + frame
    client.encode_ms += (time.perf_counter() - encode) * 1000.0
    start = time.perf_counter()
    client._exchange(reqid, payload)
    return (time.perf_counter() - start) * 1000.0


def feed_from(client: Client, prog: str, index: int, trace: str) -> float:
    return client.request(f"feed {prog} {index} from {trace}")[1]


def keyset(client: Client, trace: str) -> tuple[set[int], float]:
    rows, ms = client.peek(trace)
    out = set()
    for key, _, diff in rows:
        if diff != 1 or key[0] in out:
            raise RuntimeError(f"{trace} {key} has multiplicity {diff}")
        out.add(key[0])
    return out, ms


def run_cycle(client: Client, txns: list[Append], ref: Shards, resolver: Path) -> Sample:
    expected_rollback, expected_garbage = ref.apply(txns)
    install_ms = client.load("res", resolver)
    ikeys, ivals, bkeys, bvals = [], [], [], []
    for t in txns:
        for s, lo, hi, b in t.parts:
            ikeys.append((t.txn_id, 1, s)); ivals.append((lo,))
            ikeys.append((t.txn_id, 0, s)); ivals.append((hi,))
            bkeys.append((t.txn_id, s)); bvals.append((b,))
    submit_ms = feed_frame(client, "res", 0, INTENTS, ikeys, ivals)
    submit_ms += feed_frame(client, "res", 1, BLOBS, bkeys, bvals)
    resolve_ms = client.tick()
    rollback, observe_ms = keyset(client, "rollback")
    garbage, ms = keyset(client, "garbage")
    observe_ms += ms
    if rollback != expected_rollback:
        raise RuntimeError(f"rollback mismatch: missing={sorted(expected_rollback - rollback)[:10]}, "
                           f"extra={sorted(rollback - expected_rollback)[:10]}")
    if garbage != expected_garbage:
        raise RuntimeError("garbage mismatch")
    apply_ms = feed_from(client, "shards", 0, "delta") + feed_from(client, "shards", 1, "append")
    drop_ms = client.drop("res")
    return Sample(len(txns), len(txns) - len(expected_rollback), install_ms, submit_ms,
                  resolve_ms, observe_ms, apply_ms, drop_ms)


def check_final(client: Client, ref: Shards) -> None:
    client.tick()  # apply the last cycle's delta and append
    rows, _ = client.peek("uppers")
    uppers = {}
    for key, val, diff in rows:
        if diff != 1 or key[0] in uppers:
            raise RuntimeError(f"uppers {key} has multiplicity {diff}")
        uppers[key[0]] = val[-1]
    if uppers != dict(enumerate(ref.uppers)):
        raise RuntimeError("final uppers differ from the serial reference")
    rows, _ = client.peek("batches")
    batches: dict[int, list[tuple[int, int, int]]] = defaultdict(list)
    for key, val, diff in rows:
        if diff != 1:
            raise RuntimeError(f"batch {key} {val} has multiplicity {diff}")
        batches[key[0]].append(tuple(val))
    for s, chain in batches.items():
        chain.sort()
        if any(a[1] != b[0] for a, b in zip(chain, chain[1:])):
            raise RuntimeError(f"shard {s} batches do not chain: {chain[:5]}")
    if {s: sorted(c) for s, c in batches.items()} != {s: sorted(c) for s, c in ref.batches.items()}:
        raise RuntimeError("final batches differ from the serial reference")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--resolve-program", type=Path, default=HERE / "resolve.ddp")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--widths", type=comma_ints, default=[1, 2, 4, 8, 16, 32, 64])
    parser.add_argument("--appends", type=int, default=20_000,
                        help="shard appends attempted per cycle; transactions = appends / width")
    parser.add_argument("--shards", type=comma_ints, default=[1_000_000])
    parser.add_argument("--pattern", choices=["random", "disjoint"], default="random")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--wrap", default="")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    os.environ.setdefault("DDIR_MAX_FEED_BYTES", str(1 << 28))

    print("shards    width  txns/cycle commit%  txns/s     commits/s  appends/s  "
          "cycle-p50/p95-ms  resolve-ms", flush=True)
    results = []
    for shards in args.shards:
        for width in args.widths:
            if args.appends % width:
                raise SystemExit("--appends must be divisible by every width")
            count = args.appends // width
            rng = random.Random(args.seed)
            ref = Shards(shards)
            samples = []
            with ServerProcess(args.binary.resolve(), args.workers, shlex.split(args.wrap), True) as client:
                client.load("shards", HERE / "shards.ddp")
                feed_frame(client, "shards", 0, UPPERS, [(s,) for s in range(shards)],
                           [(0, 0)] * shards)
                client.tick()
                next_id, next_blob = 1, 1
                for cycle in range(args.warmup + args.rounds):
                    txns = generate(rng, next_id, next_blob, count, width, shards,
                                    args.pattern, cycle, ref.uppers)
                    next_id += count
                    next_blob += count * width
                    sample = run_cycle(client, txns, ref, args.resolve_program.resolve())
                    if cycle >= args.warmup:
                        samples.append(sample)
                check_final(client, ref)
            secs = sum(s.cycle_ms for s in samples) / 1000.0
            attempts = sum(s.attempts for s in samples)
            committed = sum(s.committed for s in samples)
            result = {
                "shards": shards, "width": width, "txns_per_cycle": count,
                "pattern": args.pattern, "workers": args.workers,
                "commit_rate": committed / attempts,
                "attempted_tps": attempts / secs, "committed_tps": committed / secs,
                "appends_per_s": committed * width / secs,
                "cycle_p50_ms": percentile((s.cycle_ms for s in samples), 0.5),
                "cycle_p95_ms": percentile((s.cycle_ms for s in samples), 0.95),
                "phase_means_ms": {p: sum(getattr(s, p) for s in samples) / len(samples)
                                   for p in ("install_ms", "submit_ms", "resolve_ms",
                                             "observe_ms", "apply_ms", "drop_ms")},
                "samples": [asdict(s) for s in samples],
            }
            results.append(result)
            print(f"{shards:<9} {width:5d} {count:10d} {100 * result['commit_rate']:7.2f} "
                  f"{result['attempted_tps']:9.0f} {result['committed_tps']:10.0f} "
                  f"{result['appends_per_s']:10.0f} "
                  f"{result['cycle_p50_ms']:8.1f}/{result['cycle_p95_ms']:.1f} "
                  f"{result['phase_means_ms']['resolve_ms']:9.1f}", flush=True)
    if args.json:
        args.json.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
