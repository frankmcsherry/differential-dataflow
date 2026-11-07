#!/usr/bin/env python3
"""Command-line client: write the desired state, add data, and read results.

    ctl.py program reach.ddp                    store a program's text; prints its blob
    ctl.py table 1                              create table 1 (its upper becomes 0)
    ctl.py append 1 '((int), (int))' 0,1 1,2    append rows `key,value` (diff 1) to table 1
    ctl.py retract 1 '((int), (int))' 1,2       append the same rows with diff -1
    ctl.py run 7 SPEC.json                      set instance 7's spec (a JSON file)
    ctl.py stop 7                               remove instance 7's spec
    ctl.py read 2                               print table 2's contents
    ctl.py show                                 print instances, their specs and status
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from control import INC, SPEC, STATUS, Control  # noqa: E402
from runner import encode_frame, pack_frames, parse_shape, table_contents  # noqa: E402
from store import Store, open_store  # noqa: E402

CONTROL, STORE = "http://127.0.0.1:7802", "http://127.0.0.1:7801"


def put_cell(control: Control, key: int, value: int) -> None:
    while True:
        seen = control.cells().get(key, (0, -1))[1]
        if control.txn(reads=[[key, seen]], writes=[[key, value]])["committed"]:
            return


def append(control: Control, store: Store, t: int, shape: str, rows: list[str], diff: int) -> None:
    keys, vals = [], []
    for row in rows:
        k, v = (int(x) for x in row.split(","))
        keys.append((k,))
        vals.append((v,))
    blob = store.put(pack_frames([(0, encode_frame(parse_shape(shape), keys, [diff] * len(keys), vals))]))
    while True:
        upper = control.cells().get(t, (0, -1))[1]
        if control.txn(reads=[[t, upper]], writes=[[t, upper + 1]],
                       effects=[[1, t, upper, upper + 1, 0, blob]])["committed"]:
            print(f"table {t}: [{upper}, {upper + 1})")
            return


def main() -> None:
    control, store = Control(CONTROL), open_store(os.environ.get("DDIR_STORE", STORE))
    cmd, *rest = sys.argv[1:]
    if cmd == "program":
        print(store.put(Path(rest[0]).read_bytes()))
    elif cmd == "table":
        r = control.txn(reads=[[int(rest[0]), -1]], writes=[[int(rest[0]), 0]])
        print("created" if r["committed"] else "already exists")
    elif cmd in ("append", "retract"):
        append(control, store, int(rest[0]), rest[1], rest[2:], 1 if cmd == "append" else -1)
    elif cmd == "run":
        put_cell(control, SPEC + int(rest[0]), store.put(Path(rest[1]).read_bytes()))
    elif cmd == "stop":
        put_cell(control, SPEC + int(rest[0]), 0)
    elif cmd == "read":
        rows = table_contents(store, control.batches().get(int(rest[0]), []))
        for (k, v), d in sorted(rows.items()):
            print(k, v, d)
        print(f"{len(rows)} rows, upper {control.cells().get(int(rest[0]), (0, -1))[1]}")
    elif cmd == "show":
        cells = control.cells()
        for key, (_, blob) in sorted(cells.items()):
            if SPEC <= key < INC:
                i = key - SPEC
                status_blob = cells.get(STATUS + i, (0, 0))[1]
                status = json.loads(store.get(status_blob)) if status_blob > 0 else None
                print(f"instance {i}: spec {json.loads(store.get(blob)) if blob > 0 else None}")
                print(f"  incarnation {cells.get(INC + i, (0, None))[1]}, status {status}")
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
