#!/usr/bin/env python3
"""Round-trip latency of the store backends: root compare-and-swap in etcd, blob put and get in S3.

    python3 measure_backends.py --etcd http://10.0.1.1:2379 --s3 http://10.0.1.1:3900 --keys KEYFILE
"""

from __future__ import annotations

import argparse
import json
import os
import time

from backends import EtcdRoot, S3Blobs


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def report(label, ms):
    print(json.dumps({"op": label, "n": len(ms), "p50_ms": round(pct(ms, 0.5), 2),
                      "p90_ms": round(pct(ms, 0.9), 2), "p99_ms": round(pct(ms, 0.99), 2),
                      "min_ms": round(min(ms), 2)}), flush=True)


def timed(f, n):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        out.append(1000 * (time.perf_counter() - t))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--etcd", action="append", default=[])
    p.add_argument("--s3")
    p.add_argument("--keys")
    p.add_argument("--n", type=int, default=200)
    a = p.parse_args()
    manifest = {"epoch": 0, "log": [[123456789012345678, 2000000, 0, 0]] * 3}
    for endpoint in a.etcd:
        root = EtcdRoot([endpoint], key=f"ddir/measure-{os.getpid()}")
        version = [root.read_root()[0]]

        def swap():
            assert root.cas(version[0], manifest)
            version[0] += 1
        report(f"etcd cas via {endpoint}", timed(swap, a.n))
        report(f"etcd read via {endpoint}", timed(root.read_root, a.n))
    if a.s3:
        ak, sk = open(a.keys).read().split()
        blobs = S3Blobs(a.s3, "ddir-blobs", ak, sk)
        for size in (1 << 10, 64 << 10, 2 << 20):
            n = a.n if size < (1 << 20) else max(10, a.n // 10)
            made = []

            def put():
                made.append(blobs.put(os.urandom(size)))
            report(f"s3 put {size >> 10} KiB", timed(put, n))
            it = iter(made)
            report(f"s3 get {size >> 10} KiB", timed(lambda: blobs.get(next(it)), n))


if __name__ == "__main__":
    main()
