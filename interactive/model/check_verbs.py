"""Evaluate the verb rules with an existing ddir_server binary.

Usage: python3 interactive/model/check_verbs.py /path/to/ddir_server
The server is an evaluator for the rules; no server changes are required.
"""

from pathlib import Path
import os
import subprocess
import sys


def value(x):
    if isinstance(x, tuple):
        return "tuple(" + ",".join(map(value, x)) + ")"
    if isinstance(x, list):
        return "list(" + ",".join(map(value, x)) + ")"
    return str(x)


def row(key, val):
    return value((key,)) + (" val=" + value(val) if val else "")


empty = "inject(0,tuple())"
ok = (empty,)
start = [[0, 0]]
middle = [[0, 1], [1, 0]]
end = [[1, 1]]
shape = [1, 2]
registry = [(7, (shape, start))]
span = (start, middle, start, empty)
manifest = [(7, span)]

# Each case is one snapshot and at most one verb, with explicit expected rows.
# Check all exports, including empty additions on rejection or invalid input.
cases = [
    ("create", [], [], [(7, (shape, start))], [],
     {"registry_add": [(7, (shape, start))], "created": [(7, ())]}),
    ("empty_append", registry, [], [], [(7, (start, middle, empty))],
     {"write": [(7, (start,))], "appended": [(7, ok)],
      "manifest_add": [(7, span)]}),
    ("successive_append", registry, manifest, [], [(7, (middle, end, empty))],
     {"write": [(7, (middle,))], "appended": [(7, ok)],
      "manifest_add": [(7, (middle, end, start, empty))]}),
    ("equivalent_lower", registry, manifest, [], [(7, ([[1, 0], [2, 2], [0, 1], [1, 0]], end, empty))],
     {"write": [(7, (middle,))], "appended": [(7, ok)],
      "manifest_add": [(7, ([[1, 0], [2, 2], [0, 1], [1, 0]], end, start, empty))]}),
    ("equivalent_upper", registry, [(7, (start, [[1, 0], [0, 1], [1, 0]], start, empty))], [], [(7, (middle, end, empty))],
     {"write": [(7, (middle,))], "appended": [(7, ok)],
      "manifest_add": [(7, (middle, end, start, empty))]}),
    ("stale_lower", registry, manifest, [], [(7, (start, end, empty))],
     {"write": [(7, (middle,))], "appended": [(7, ("inject(1," + value(middle) + ")",))]}),
    ("equal_bounds", registry, manifest, [], [(7, (middle, middle, empty))],
     {"write": [(7, (middle,))], "invalid": [(7, (middle, middle, empty))]}),
    ("regression", registry, manifest, [], [(7, (middle, start, empty))],
     {"write": [(7, (middle,))], "invalid": [(7, (middle, start, empty))]}),
    ("incomparable_bounds", registry, [], [], [(7, ([[0, 2]], [[1, 0]], empty))],
     {"write": [(7, (start,))], "invalid": [(7, ([[0, 2]], [[1, 0]], empty))]}),
    ("seal", registry, manifest, [], [(7, (middle, [], empty))],
     {"write": [(7, (middle,))], "appended": [(7, ok)],
      "manifest_add": [(7, (middle, [], start, empty))]}),
    ("sealed", registry, manifest + [(7, (middle, [], start, empty))], [], [(7, (middle, end, empty))],
     {"write": [(7, ([],))], "appended": [(7, ("inject(1,list())",))]}),
    ("collision", registry, [], [(7, (shape, start))], [],
     {"write": [(7, (start,))], "collision": [(7, (shape, start))]}),
    ("unknown", [], [], [], [(7, (start, end, empty))],
     {"unknown": [(7, (start, end, empty))]}),
    ("unknown_invalid", [], [], [], [(7, (start, start, empty))],
     {"invalid": [(7, (start, start, empty))]}),
    ("nonzero_start", [(8, (shape, [[2, 3]]))], [], [], [(8, ([[2, 3]], [[3, 3]], empty))],
     {"write": [(8, ([[2, 3]],))], "appended": [(8, ok)],
      "manifest_add": [(8, ([[2, 3]], [[3, 3]], [[2, 3]], empty))]}),
    ("payload", registry, [], [], [(7, (start, end, "inject(1,list(9,8))"))],
     {"write": [(7, (start,))], "appended": [(7, ok)],
      "manifest_add": [(7, (start, end, start, "inject(1,list(9,8))"))]}),
]

exports = ["registry_add", "manifest_add", "created", "appended", "write", "collision", "unknown", "invalid"]
commands = []
expected = {}
program = Path(__file__).with_name("verbs.ddp").resolve()
for name, reg, spans, create, append, outputs in cases:
    commands.append(f"{name}_load load verbs from {program}")
    for port, rows in enumerate([reg, spans, create, append]):
        if rows:
            commands.append(f"{name}_feed_{port} feed verbs {port} begin")
            commands.extend(row(key, val) for key, val in rows)
            commands.append(f"{name}_feed_{port} end-feed")
    commands.append(f"{name}_tick tick")
    for export in exports:
        tag = f"{name}_{export}"
        commands.append(f"{tag} peek verbs::{export}")
        expected[tag] = sorted(row(key, val) for key, val in outputs.get(export, []))
    commands.append(f"{name}_drop drop verbs")
commands.append("done exit")

env = dict(os.environ, DDIR_BIND="127.0.0.1:0", DDIR_WS_BIND="127.0.0.1:0")
result = subprocess.run([sys.argv[1]], input="\n".join(commands) + "\n", text=True,
                        capture_output=True, env=env, timeout=60)
if result.returncode:
    raise AssertionError(result.stdout + result.stderr)
actual = {tag: [] for tag in expected}
completed = set()
for line in result.stdout.splitlines():
    parts = line.split(" ", 2)
    if len(parts) >= 2:
        tag, kind = parts[:2]
        if kind == "err":
            raise AssertionError(line)
        if tag in actual and kind == "data":
            actual[tag].append(parts[2])
        if tag in actual and kind == "ok":
            completed.add(tag)
for tag, rows in expected.items():
    assert tag in completed, f"missing response: {tag}\n{result.stdout}\n{result.stderr}"
    assert sorted(actual[tag]) == rows, f"{tag}: expected {rows}, got {actual[tag]}"
print(f"Passed {len(cases)} verb cases.")
