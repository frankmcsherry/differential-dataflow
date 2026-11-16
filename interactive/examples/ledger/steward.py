#!/usr/bin/env python3
"""Two stewards of a system of ledgers. Each reads the state and acts on what it says, and neither is told anything else.

  python3 interactive/examples/ledger/steward.py compact <keeper url> <follower>   merge the spans of each ledger, as far as its readers allow
  python3 interactive/examples/ledger/steward.py collect <keeper url>              delete the blobs that no span names

The compactor reads what is needed of each ledger: by computations, in the view `needs`, and by holders, in the holds.
It merges a ledger's spans into one, held from the earliest of those, and discards the rest, in one transaction.
The rules judge that transaction like any other, so a compactor that is wrong is refused, and does no harm.
A ledger that nothing needs and no one holds is left as it is.
So the compactor reads from a follower, whose copy of the state may be behind: the keeper judges what it asks.

The collector reads the view `blobs`. A blob that is not named there is deleted once it has been unnamed for a while,
since a blob is written before the span that names it.
The collector reads from the keeper. Nothing judges a delete, and a copy of the state that is behind does not name
the newest blobs: a collector that read it would delete them.
"""
import json, signal, sys, time, urllib.request
import plumbing
from plumbing import SPAN, HOLD, number, instant

USER = "inject(0,tuple())"
ENOUGH = 4      # a ledger is compacted once this many spans reach from 0 to its version


def compact(url, follower):
    keeper, copy, blobs, cache = plumbing.Client(url), plumbing.Follower(follower), plumbing.Blobs(), {}
    asked_away = {}     # for each ledger, the spans this asked the keeper to discard, until the copy shows them gone
    while True:
        lines, standing, needs = copy.peek("state::entries", "views::standing", "views::needs")
        entries, standing = plumbing.by_kind(lines), plumbing.stands(standing)
        asked = {}
        for key, (_, _, frontier) in map(plumbing.row, needs): asked.setdefault(number(key[0]), []).append(instant(frontier))
        for (_, ledger), (frontier,) in entries.get(HOLD, []): asked.setdefault(number(ledger), []).append(instant(frontier))
        for ledger, (version, _) in standing.items():
            spans = [(key, val) for key, val in entries.get(SPAN, []) if number(key[0]) == ledger]
            by_start, at, path = {}, 0, []
            for key, val in spans: by_start.setdefault(key[1], []).append((key, val))
            while at in by_start:
                path.append(max(by_start[at], key=lambda span: span[0][2]))
                at = path[-1][0][2]
            if ledger not in asked or len(path) < ENOUGH or at != version: continue
            if asked_away.get(ledger, set()) & {plumbing.show(key) for key, _ in spans}: continue
            # The merged span is held from the earliest need, and no earlier than any compacted span it takes in.
            # A span whose since is its own lower is not compacted, and its times are as they were written.
            since = min(asked[ledger])
            if any(instant(key[3]) > max(since, instant(val[0])) for key, val in path): continue
            sums = {}
            for _, (_, _, batch) in path:
                if batch[1] != 2: continue
                name = plumbing.hash_name(batch[2])
                if name not in cache: cache[name] = json.loads(blobs.get(name))["updates"]
                for key, val, moment, diff in cache[name]:
                    row = (key, val, max(moment, since))
                    sums[row] = sums.get(row, 0) + diff
            blob = blobs.put(json.dumps({"updates": [[key, val, moment, diff] for (key, val, moment), diff in sorted(sums.items()) if diff]}).encode())
            ident = f"tuple(0,{ledger})"
            commands = [f"inject(2,tuple({ident},0,{version},list(list({since})),inject(2,list({','.join(map(str, plumbing.hash_ints(blob)))}))))"]
            commands += [f"inject(3,tuple({ident},{key[1]},{key[2]},{plumbing.show(key[3])}))" for key, _ in spans]
            refused = keeper.transact(USER, commands)
            if not refused: asked_away[ledger] = {plumbing.show(key) for key, _ in spans}
            print(f"compactor: ledger {ledger}, {len(spans)} spans into one held from {since}" + (f": refused, {refused}" if refused else ""), flush=True)
        time.sleep(0.2)


def listing():
    """The names of the blobs in the store."""
    request = urllib.request.Request(plumbing.FILER + "/blobs/?limit=100000", headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return {entry["FullPath"].rsplit("/", 1)[1] for entry in json.loads(response.read()).get("Entries") or []}


def collect(url, grace=2.0):
    keeper, unnamed = plumbing.Client(url), {}
    while True:
        named = {plumbing.hash_name(key[0]) for key, _ in map(plumbing.row, keeper.peek("before::blobs"))}
        now = time.time()
        unnamed = {name: unnamed.get(name, now) for name in listing() - named}
        gone = [name for name, since in unnamed.items() if now - since >= grace]
        for name in gone:
            plumbing.http("DELETE", plumbing.FILER + "/blobs/" + name)
            del unnamed[name]
        if gone: print(f"collector: deleted {len(gone)} blobs that no span names", flush=True)
        time.sleep(0.5)


if __name__ == "__main__":
    for sign in (signal.SIGTERM, signal.SIGHUP): signal.signal(sign, lambda *_: sys.exit(0))
    while True:
        try: (compact if sys.argv[1] == "compact" else collect)(*sys.argv[2:])
        except OSError: time.sleep(0.5)      # the keeper or the store is away: begin again when it is back
