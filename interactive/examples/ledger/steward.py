#!/usr/bin/env python3
"""Two stewards of a system of ledgers. Each reads the state and acts on what it says, and neither is told anything else.

  python3 interactive/examples/ledger/steward.py compact <keeper url> <follower>   merge the spans of each ledger, as far as its readers allow
  python3 interactive/examples/ledger/steward.py collect <keeper url>              delete the blobs that no span names

The compactor reads a follower's view of which spans to merge next (see compact.ddp): the merge to make of a ledger,
the spans to read for it, and the spans to discard after. It makes the merged batch, and asks the keeper for the merge
and the discards in one transaction. What to merge and when is the view's to say, and the compactor decides none of it.
The merged batch is the frames of the parts, each with its times moved up to what the merged span is held from.
The compactor does not open them. So equal updates are not added together, and ones that cancel are both kept:
the merge makes a ledger's spans fewer, and not yet its batches smaller (see BACKLOG.md).
The rules judge that transaction like any other, so a compactor that is wrong is refused, and does no harm.
So the compactor reads from a follower, whose copy of the state may be behind: the keeper judges what it asks.

The collector reads the view `blobs`. A blob that is not named there is deleted once it has been unnamed for a while,
since a blob is written before the span that names it.
The collector reads from the keeper. Nothing judges a delete, and a copy of the state that is behind does not name
the newest blobs: a collector that read it would delete them.
"""
import json, signal, sys, time, urllib.request
import plumbing
from plumbing import number, instant

USER = "inject(0,tuple())"


def compact(url, follower):
    keeper, copy, blobs, cache = plumbing.Client(url), plumbing.Follower(follower), plumbing.Blobs(), {}
    asked = {}      # for each ledger, the merge this last asked the keeper for, while the copy still proposes it
    while True:
        merges, parts, spares = copy.rows("tidy::merge", "tidy::parts", "tidy::spares")
        proposed = {key[0]: val for key, val in merges}
        asked = {ledger: merge for ledger, merge in asked.items() if proposed.get(ledger) == merge}
        for ledger, (first, last, since) in proposed.items():
            # The copy may be behind. What was asked for is not asked for again until the copy proposes something else.
            if ledger in asked: continue
            # The merged batch: the frames of the parts, in order, with each time moved up to the since.
            frames = []
            for _, _, _, batch in sorted(val for key, val in parts if key[0] == ledger):
                if batch[1] != 2: continue
                name = plumbing.hash_name(batch[2])
                if name not in cache: cache[name] = plumbing.unpack(blobs.get(name))[1]
                frames += [plumbing.raised(frame, instant(since)) for frame in cache[name]]
            batch = "inject(0,tuple())"
            if frames: batch = f"inject(2,list({','.join(map(str, plumbing.hash_ints(blobs.put(plumbing.pack({}, frames)))))}))"
            ident = plumbing.show(ledger)
            gone = sorted(val for key, val in spares if key[0] == ledger)
            commands = [f"inject(2,tuple({ident},{first},{last},{plumbing.show(since)},{batch}))"]
            commands += [f"inject(3,tuple({ident},{begin},{end},{plumbing.show(held)}))" for begin, end, held in gone]
            refused = keeper.transact(USER, commands)
            asked[ledger] = (first, last, since)
            print(f"compactor: ledger {number(ledger)}, appends {first} to {last}, {len(gone)} spans into one held from {instant(since)}"
                  + (f": refused, {refused}" if refused else ""), flush=True)
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
