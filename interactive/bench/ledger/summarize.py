#!/usr/bin/env python3
"""Summarize the opt-in ledger profile patch's stderr log."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re
import shlex


def summarize(path, warmup, epochs):
    marks = {warmup: {}, epochs: {}}
    stages = defaultdict(list)
    compact = defaultdict(int)
    for line in path.read_text().splitlines():
        if line.startswith("OP_PROFILE "):
            fields = dict(word.split("=", 1) for word in shlex.split(line)[1:])
            key = (int(fields["worker"]), fields["program"], fields["statement"], fields["operator"])
            marks[int(fields["epoch"])][key] = int(fields["ns"])
        elif line.startswith("ATTR "):
            stages[line.split()[1]].append({k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)})
        elif line.startswith("COMPACT_PROFILE "):
            fields = dict(word.split("=", 1) for word in shlex.split(line)[1:])
            if 1 + 2 * warmup < int(fields["epoch"]) <= 1 + 2 * epochs:
                compact[fields["trace"]] += int(fields["cpu_ns"])
    if not all(marks.values()):
        raise ValueError("missing operator marks; use the patch's 40 epochs and 5 warmup batches")
    batches = epochs - warmup
    operators = defaultdict(int)
    workers = defaultdict(int)
    for key in marks[warmup].keys() | marks[epochs].keys():
        nanos = marks[epochs].get(key, 0) - marks[warmup].get(key, 0)
        worker, program, statement, name = key
        operators[(program, statement, name)] += nanos
        workers[worker] += nanos
    rows = [dict(program=p, statement=s, operator=n, scheduled_ms=ns / batches / 1e6)
            for (p, s, n), ns in operators.items()]
    rows.sort(key=lambda row: row["scheduled_ms"], reverse=True)
    means = {}
    for category, values in stages.items():
        if category in ("worker", "publish"):
            continue
        values = values[warmup:]
        means[category] = {k.removesuffix("_ns"): sum(row[k] for row in values) / batches / 1e6 for k in values[0]}
    worker_cpu = {}
    for worker in range(len(workers)):
        values = [r["cpu_ns"] for r in stages["worker"] if r["worker"] == worker][warmup:]
        worker_cpu[worker] = sum(values) / batches / 1e6
    return dict(batches=batches, operators=rows,
                scheduled_ms=sum(operators.values()) / batches / 1e6,
                scheduled_ms_by_worker={w: ns / batches / 1e6 for w, ns in workers.items()},
                stage_ms=means, worker_cpu_ms=worker_cpu,
                compaction_cpu_ms={k: ns / batches / 1e6 for k, ns in compact.items()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    result = summarize(args.log, 5, 40)
    print(f"Scheduled ms/batch summed across workers: {result['scheduled_ms']:.3f}")
    for row in result["operators"][:20]:
        print(f"{row['scheduled_ms']:8.3f}  {row['program']:12} {row['statement']:24} {row['operator']}")
    print("Foreground stage ms/batch:", json.dumps(result["stage_ms"], sort_keys=True))
    print("Compaction CPU-ms/batch:", json.dumps(result["compaction_cpu_ms"], sort_keys=True))
    if args.json:
        args.json.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
