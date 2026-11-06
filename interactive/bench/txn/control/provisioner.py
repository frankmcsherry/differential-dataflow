#!/usr/bin/env python3
"""Makes sure every instance the catalog asks for has a runner somewhere.

It reads the catalog's SPEC and STATUS cells. An instance with a spec, and no status reported
within `--timeout` seconds, gets a runner started on its spec's machine: over SSH for a mini, as
a local process for "local". It does nothing else: runners read their own spec, fence their
predecessors, and stop themselves. If two runners overlap, the older one is fenced, so a timeout
that is too short costs a restart, never correctness. In a cloud, starting a runner would be
renting a machine that runs one at boot.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from control import SPEC, STATUS, Control  # noqa: E402
from store import Store, open_store  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--control", default="http://127.0.0.1:7802")
    p.add_argument("--store", default="http://127.0.0.1:7801")
    p.add_argument("--public-control", required=True, help="the control URL as runners see it")
    p.add_argument("--public-store", required=True, help="the store URL as runners see it")
    p.add_argument("--remote-root", default="~/ddir-draft", help="where runners' code lives on remote machines")
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--poll", type=float, default=1.0)
    a = p.parse_args()
    control, store = Control(a.control), open_store(a.store)
    started: dict[int, float] = {}
    specs: dict[int, dict] = {}
    while True:
        try:
            cells = control.cells()
        except OSError as error:
            # The control service is down or restarting: its state is durable, so wait for it.
            print(f"[provisioner] control unavailable ({error}); retrying", flush=True)
            time.sleep(a.poll)
            continue
        for key, (_, blob) in cells.items():
            if not SPEC <= key < SPEC + (1 << 40) or blob <= 0:
                continue
            i = key - SPEC
            if blob not in specs:
                specs[blob] = json.loads(store.get(blob))
            spec = specs[blob]
            status_blob = cells.get(STATUS + i, (0, 0))[1]
            status = json.loads(store.get(status_blob)) if status_blob > 0 else None
            fresh = status is not None and status["state"] == "running" and time.time() - status["time"] < a.timeout
            if fresh or time.time() - started.get(i, 0) < a.timeout:
                continue
            machine = spec["machine"]
            print(f"[provisioner] instance {i}: no live runner; starting one on {machine}", flush=True)
            if machine == "local":
                args = [sys.executable, str(HERE / "runner.py"), "--control", a.control, "--store", a.store,
                        "--instance", str(i)]
                subprocess.Popen(args, stdout=open(f"/tmp/ddir-draft-runner-{i}.log", "a"),
                                 stderr=subprocess.STDOUT, start_new_session=True)
            else:
                remote = (f"cd {a.remote_root} && nohup python3 interactive/bench/txn/control/runner.py "
                          f"--control {a.public_control} --store {a.public_store} --instance {i} "
                          f">> /tmp/ddir-draft-runner-{i}.log 2>&1 &")
                subprocess.run(["ssh", "-o", "BatchMode=yes", machine, remote], check=False)
            started[i] = time.time()
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
