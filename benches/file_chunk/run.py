#!/usr/bin/env python3
"""Run the file-CHUNK spike with a sampled RSS watchdog and kernel peak RSS.

The watchdog is a guard, not a hard allocator limit. Each case verifies its own
DD output. Temporary storage is unlinked and disappears when its process exits.
macOS --nocache is the default; pass --buffered on other systems.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
CASES = {
    # name: keys, fanout, rounds, hot keys, cache charge capacity, extra options
    "small": (65_536, 16, 12, 655, 11_136, []),
    "medium": (1_048_576, 16, 12, 10_485, 178_246, []),
    "large": (4_194_304, 16, 12, 41_943, 713_032, []),
    "large_profile": (4_194_304, 16, 12, 41_943, 713_032, ["--profile"]),
    "large_fixed_hot": (4_194_304, 16, 12, 64, 1_089, []),
    "medium_bypass": (1_048_576, 16, 6, 10_485, 0, []),
    "medium_clustered": (1_048_576, 16, 12, 10_485, 178_246, ["--clustered"]),
    "medium_moving": (1_048_576, 16, 24, 10_485, 178_246, ["--moving"]),
    "medium_clustered_moving": (1_048_576, 16, 24, 10_485, 178_246, ["--moving", "--clustered"]),
    "medium_steady": (1_048_576, 16, 200, 10_485, 178_246, []),
    "giant_key_bypass": (100, 2_097_152, 2, 1, 0, []),
    "cursor_small": (65_536, 16, 3, 655, 0, ["--cursor-control"]),
    "cursor_medium": (1_048_576, 16, 3, 10_485, 0, ["--cursor-control"]),
}


def run(name, config, args):
    values, flags = config[:5], config[5]
    command = [str(ROOT / "target/release/examples/file_chunk_spike"), *map(str, values), *flags]
    if not args.buffered:
        command.append("--nocache")
    output = args.output / (name + ".log")
    start = time.monotonic()
    reason = None
    sampled = 0
    lines = []
    with output.open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        def collect():
            for line in process.stdout:
                lines.append(line.strip())
                log.write(line)
                log.flush()

        reader = threading.Thread(target=collect)
        reader.start()
        while True:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(status)
                break
            result = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)], capture_output=True, text=True)
            rss = int(result.stdout.strip() or 0)
            sampled = max(sampled, rss)
            if reason is None and (rss > args.memory_mib * 1024 or time.monotonic() - start > args.seconds):
                reason = "rss_watchdog" if rss > args.memory_mib * 1024 else "timeout"
                process.kill()
            time.sleep(0.05)
        reader.join()
    peak = usage.ru_maxrss / 1024 if sys.platform == "darwin" else usage.ru_maxrss
    record = dict(case=name, command=command, exit_code=process.returncode,
                  stopped_by=reason, elapsed_s=round(time.monotonic() - start, 3),
                  sampled_peak_rss_kib=sampled, kernel_peak_rss_kib=peak,
                  watchdog_mib=args.memory_mib, verified=any(l.startswith("verified ") for l in lines),
                  log=os.path.relpath(output, ROOT))
    (args.output / (name + ".json")).write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record), flush=True)


def summarize(directory):
    summary = []
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text())
        lines = path.with_suffix(".log").read_text().splitlines()
        fields = lambda line: dict(re.findall(r"(\w+)=([^ ]+)", line))
        rows = [fields(line) for line in lines if line.startswith("round=")]
        build = next((fields(line) for line in lines if line.startswith("build ")), {})
        summary.append(dict(
            case=data["case"], verified=data["verified"],
            peak_rss_mib=round(data["kernel_peak_rss_kib"] / 1024, 3),
            file_mib=round(int(build.get("file_bytes", 0)) / 2**20, 3),
            cold_ms=round(int(rows[0]["elapsed_us"]) / 1000, 3) if rows else "",
            later_round_median_ms=round(statistics.median(int(r["elapsed_us"]) for r in rows[1:]) / 1000, 3) if len(rows) > 1 else "",
            cold_read_mib=round(int(rows[0]["read_bytes"]) / 2**20, 3) if rows else "",
            later_read_mib=round(sum(int(r["read_bytes"]) for r in rows[1:]) / 2**20, 3),
            rounds_completed=len(rows), stopped_by=data["stopped_by"] or "",
        ))
    if summary:
        with (directory / "summary.csv").open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=summary[0].keys(), lineterminator="\n")
            writer.writeheader()
            writer.writerows(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=["all", *CASES], default="all")
    parser.add_argument("--memory-mib", type=int, default=128)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--buffered", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "benches/file_chunk/results")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for name, config in CASES.items():
        if args.case in ("all", name):
            run(name, config, args)
    summarize(args.output)


if __name__ == "__main__":
    main()
