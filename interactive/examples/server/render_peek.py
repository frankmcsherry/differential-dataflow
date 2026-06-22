#!/usr/bin/env python3
"""Cheap blast-radius renderer for ddir_server sessions.

Pipe a session's stdout through it: each `peek` snapshot is re-rendered as a
compact row, with the cells that *changed since that trace's previous snapshot*
highlighted. That diff is exactly the set of records DD reworked -- the
"DD touched this" overlay -- read off consecutive (now-correct) peeks.

    cargo run --release --example ddir_server -- <session> 2>/dev/null \
        | python3 examples/server/render_peek.py

Structural lines (install/tick/list/...) pass through; peek blocks are replaced.
"""
import sys, re

PEEK  = re.compile(r'peek "([^"]+)"(?: key=\S+)? \((\d+) rows?\):')
ROW   = re.compile(r'\(Tuple\(\[(.*?)\]\), Tuple\(\[(.*?)\]\)\)\s+x(-?\d+)')
INTS  = re.compile(r'Int\((-?\d+)\)')
EPOCH = re.compile(r'epoch (\d+)')

TTY = sys.stdout.isatty()
def hl(s):  return f"\033[1;31m{s}\033[0m" if TTY else s   # changed: bold red
def dim(s): return f"\033[2m{s}\033[0m"   if TTY else s

def ints(s): return tuple(int(x) for x in INTS.findall(s))

def cell(v, changed):
    txt = f"[{v}]" if changed else f" {v} "
    return hl(f"{txt:>4}") if changed else f"{txt:>4}"

def render(name, epoch, rows, state):
    # rows: list of (key_tuple, val_tuple) present (diff > 0)
    label = f"{name:<10} e{epoch} "
    all_empty = rows and all(len(v) == 0 for _, v in rows)

    if all_empty:                                   # SET trace (membership)
        new = set(k for k, _ in rows)
        old = state.get(name)
        state[name] = new
        baseline = old is None
        old = old or set()
        hi = set() if baseline else (new ^ old)     # added or removed (none on baseline)
        lo, hiidx = 0, max((k[0] for k in new | old), default=-1)
        cells = []
        for i in range(lo, hiidx + 1):
            here = (i,) in new
            mark = '#' if here else '.'
            cells.append(hl(mark) if (i,) in hi else (mark if here else dim(mark)))
        add = sorted(k[0] for k in new - old); rem = sorted(k[0] for k in old - new)
        delta = ""
        if not baseline and (add or rem):
            delta = "  Δ " + " ".join(filter(None, [
                "+" + ",".join(map(str, add)) if add else "",
                "-" + ",".join(map(str, rem)) if rem else ""]))
        print(f"{label}|{''.join(cells)}|{delta}")
        return

    new = {k: v for k, v in rows}                   # MAP trace (key -> value)
    old = state.get(name)
    state[name] = new
    changed = {k for k in set(new) | set(old or {}) if new.get(k) != (old or {}).get(k)} if old else set()

    if len(new) == 1 and next(iter(new))[0] == 0:   # scalar (single key 0)
        (v,) = next(iter(new.values()))
        ch = bool(changed)
        print(f"{label}= {hl(v) if ch else v}{'   (changed)' if ch and old else ''}")
        return

    hiidx = max(k[0] for k in new)                  # vector indexed by stage
    cells = [cell(new.get((i,), ('·',))[0], (i,) in changed) for i in range(hiidx + 1)]
    delta = ("  Δ " + ",".join(str(k[0]) for k in sorted(changed))) if (changed and old) else ""
    print(f"{label}|{''.join(cells)} |{delta}")

def main():
    state, epoch = {}, 0
    it = iter(sys.stdin)
    for line in it:
        line = line.rstrip("\n")
        m = EPOCH.search(line)
        if m and ("tick" in line or line.startswith("epoch")):
            epoch = int(m.group(1))
        p = PEEK.search(line)
        if not p:
            print(dim(line) if line.strip() else line)
            continue
        name, n = p.group(1), int(p.group(2))
        rows = []
        for _ in range(n):
            r = ROW.search(next(it))
            if r and int(r.group(3)) > 0:
                rows.append((ints(r.group(1)), ints(r.group(2))))
        render(name, epoch, rows, state)

if __name__ == "__main__":
    main()
