#!/usr/bin/env python3
"""Two stewards of a system of ledgers. Each reads the state and acts on what it says, and neither is told anything else.

  python3 interactive/examples/ledger/steward.py compact <keeper url> <follower>   merge the spans of each ledger, as far as its readers allow
  python3 interactive/examples/ledger/steward.py collect <keeper url> <follower>   delete the blobs that no span names

THIS IS A SCRATCH VARIANT, for a keeper that holds no spans.
The compactor reads a follower's view of which spans to merge next (see compact.ddp): the merge to make of each ledger,
and the entries each merge takes out. It asks the keeper for every merge of the pass together, and hands both over as
the frames the follower gave it: it opens neither. Of its own it says one line: how many records of the log its follower
had. What to merge and when is the view's to say, and the compactor decides none of it.
After a pass it reads the proposals again only of a copy of the state that has that pass, so it asks nothing twice.
A merge whose spans hold blobs has a batch to make: the frames of the parts, each with its times moved up to what the
merged span is held from. The compactor does not open them. So equal updates are not added together, and ones that
cancel are both kept: the merge makes a ledger's spans fewer, and not yet its batches smaller (see BACKLOG.md).
It names each batch it made to the keeper in a frame it writes itself: the ledger, and the batch. That is the one
thing it reads rows for: which blobs to fetch. A merge of spans that hold none has an empty batch, and nothing is said of it.
THIS STEP: a merged batch is made in a server of the compactor's own (see `Merger`), so equal updates are added
together and ones that cancel are gone. A batch is written to blobs of its own ledger, in parts of a bounded size,
each its own blob, and names its parts in order; batches under a small size share one blob of the pass, a part each.
A part is read by range and held to its own hash: no blob is read whole to read a part of it.
The frames of a pass may be too long for a server to take. The policy's views are under shards, 64 of them by a ledger's
id, and the compactor then asks for them a few shards at a time: as few as should fit, by how long they were.
The policy bounds what one merge takes out and how many merges one shard offers, so one shard can always be handed
over, however far behind compaction is, and a pass of fewer shards is always a smaller one.
The keeper judges only that a ledger was not merged since the compactor's follower read, and that the merged span is
held from no later than the ledger is needed from. Whether the entries are right is for a follower's rules to say after.

The collector reads the view `blobs` of a follower that keeps the rules, since the keeper holds no spans and cannot say
which blobs are named. Nothing judges a delete, so the collector deletes only what is unnamed in a state whose every rule
that follower finds empty, and only once that state has all that the keeper had made when the blob was first seen
unnamed (see `sweep`). A span names a blob or a part of one, and a blob is named while any span names any part of it.
A blob that was named and is no longer is deleted on that alone. One that was never seen named
is also left a while, since a blob is written before the span that names it, and nothing bounds how long before.
"""
import subprocess
import json, os, signal, sys, time, urllib.request
import keeper as model
import plumbing
from plumbing import number, instant

USER = "inject(0,tuple())"


PAUSE = float(os.environ.get("COMPACT_PAUSE", "0.2"))       # seconds between a compactor's passes
# The most bytes of frames a compactor hands over in a pass. What a pass takes out goes on to the log as one frame, with
# what it puts in, and every follower's server must take that frame: so this is half of what a server takes, unless it is set.
MOST = int(os.environ.get("COMPACT_MOST", str(plumbing.CHUNK)))     # a pass is held to the one chunk size every producer keeps to
SHARDS = 64                                                 # how many shards the policy's views are under
KEPT = int(os.environ.get("COMPACT_KEPT", str(256 << 20)))  # the most bytes of parts a compactor keeps of those it has read
PART = int(os.environ.get("COMPACT_PART", str(64 << 20)))   # the most bytes of one part of a merged batch: a longer batch is several
# A merged batch under this many bytes is written beside the small batches of other ledgers of the same pass, in one
# blob, each its own part. One at or over it has blobs of its own. 0 gives every merged batch blobs of its own.
SHARE = int(os.environ.get("COMPACT_SHARE", "0"))
MERGERS = int(os.environ.get("COMPACT_MERGERS", "4"))      # how many servers merge a pass's batches, each a share of its ledgers
PORT = int(os.environ.get("COMPACT_PORT", "8030"))          # the compactor's own server, in which batches are merged


class Merger:
    """A server of the compactor's own, in which the merged batch of each ledger is made, so that equal updates are
    added together and ones that cancel are gone. The frames are not opened here: the server merges them.
    For each shape of rows a pass has, one program is loaded, piloted, with one input, and tails of its outputs. The
    ledgers of that shape each have a slot of times: the frames of the k-th are fed with each time moved up by k slots,
    and its input is then moved past its slot, and the server run until the tails have sent what that made: the frames
    the tails send then are that ledger's merged batch, and their times are moved back down.
    A time is moved by writing the frame's last words, which opens no column (see `shifted` in plumbing.py).
    A slot is one more than the latest time any ledger of the pass has, so the times of two ledgers never meet.
    The server sends a ledger's merged batch as one frame for each output. So that a long batch is several frames, each
    under a part's bound, the program has as many outputs as the longest batch of the pass needs, and each output takes
    the rows whose key's hash falls to it: the server divides the rows, and nothing here opens them."""
    def __init__(self, port):
        self.server, self.count, self.failed = plumbing.Server(port), 0, set()

    def merge(self, shape, ledgers, part=None):
        """`ledgers` is [the frames of one ledger's merged batch, its times moved up]. Returns [the frames of each,
        merged], or None if the server would not take the shape. With `part`, a ledger's frames are each about that
        many bytes at most, as far as the hash of the keys divides them evenly."""
        if shape in self.failed: return None
        latest = [plumbing.latest(frame) for frames in ledgers for frame in frames]
        slot = max([each[1] for each in latest if each], default=0) + 1
        if slot * (len(ledgers) + 1) >= 1 << 62:
            raise ValueError("the times of a pass do not fit in its slots")
        # As many outputs as the longest ledger's frames need, to be under `part` each: what goes in is at least as
        # long as what comes out.
        outputs = max(1, -(-max([sum(map(len, frames)) for frames in ledgers], default=0) // part)) if part else 1
        self.count += 1
        name = f"m{self.count}"
        text = f"let rows = input 0 : {shape};\n" + ('output "p0" = rows;\n' if outputs == 1 else "".join(
            f'output "p{at}" = rows | filter((key, _) -> ((key hash shr_b64 1, {outputs}) rem, {at}) eq);\n' for at in range(outputs)))
        try: self.server.ask(f"load {name} piloted begin\n{text}{name} end-load")
        except RuntimeError as error:
            print(f"compactor: the rows of shape {shape} are not merged, only put one after another: {str(error)[:200]}", flush=True)
            self.failed.add(shape)
            return None
        tails = []
        for at in range(outputs):
            self.server.ask(f"tail {name}::p{at} as corgi")
            tails.append(self.server.last)
            self.server.tails[self.server.last] = []
        # Each ledger's frames are fed just before its slot is closed. Fed all at once, ahead of their times, they cost
        # the server work in each advance for all that is still ahead, and a pass of 1,600 ledgers took a second.
        requests = []
        for at, frames in enumerate(ledgers):
            requests += [(f"feed {name} 0 frame {len(frame)}", frame) for frame in (plumbing.shifted(each, at * slot) for each in frames) if plumbing.rows_of(frame)]
            requests += [f"advance {name} 0 {(at + 1) * slot}", "spin"]
        self.server.many(requests)
        merged = [[] for _ in ledgers]
        for tail in tails:
            for kind, frame in self.server.tails.pop(tail):
                if kind != "data" or not plumbing.rows_of(frame): continue
                least, most = plumbing.latest(frame)
                at = least // slot
                assert most // slot == at, "a frame of the tail holds the times of two ledgers"
                merged[at].append(plumbing.shifted(frame, -at * slot))
        self.server.many([f"stop {tail}" for tail in tails] + [f"drop {name}"])
        return merged


def compact(url, follower):
    keeper, copy, blobs = plumbing.Client(url), plumbing.Follower(follower), plumbing.Blobs()
    # Several servers merge a pass's batches at once, each the ledgers of a share of it: a server merges one ledger
    # at a time, in about 0.1 ms, and a pass has thousands.
    mergers = [Merger(PORT + 2 * at) for at in range(MERGERS)]
    pool = __import__("concurrent.futures").futures.ThreadPoolExecutor(MERGERS)
    after = 0       # the log's length after this compactor's last pass: it reads proposals of a copy that has that pass
    width, at_shard = SHARDS, 0     # how many shards a pass takes, and where the next begins
    cache = {}                      # parts that were read, by (blob, offset), the least lately used first
    while True:
        # The proposals, all of one state: the merge of each ledger and the entries it takes out, both unopened, and the
        # parts to read for the merges that have any. The follower answers once it has this compactor's last pass, so
        # nothing is proposed of a state that pass has changed, and the compactor need not remember what it asked.
        shards = None if width == SHARDS else [(at_shard + each) % SHARDS for each in range(width)]
        names = lambda view: [view] if shards is None else [f"{view} tuple({shard})" for shard in shards]
        clock = [time.perf_counter()]
        at, parts, whole = copy.views(names("tidy::parts"), names("tidy::merge") + names("tidy::leaves"), after)
        clock.append(time.perf_counter())
        some = [[frame for frame in frames if plumbing.rows_of(frame)] for frames in whole]
        proposed = [frame for frames in some[:len(some) // 2] for frame in frames]
        leaving = [frame for frames in some[len(some) // 2:] for frame in frames]
        long = sum(map(len, proposed + leaving))
        if long > MOST and width > 1:
            # The frames are too long to hand over. The pass is asked for again, of as few shards as should fit:
            # the policy bounds what one shard offers, so fewer shards is fewer bytes, and one shard is the least.
            width = max(1, min(width // 2, int(width * MOST / long / 1.25)))
            continue
        # The batches of the merges that have parts to read: for each, the frames of its parts, in order, with each time
        # moved up to what the merged span is held from, merged in the compactor's server. A part is read by range, and
        # only the parts a merge names are read: a blob that holds the parts of many spans is not read whole.
        reads, shape_of = {}, {}
        for rows in parts:
            for _, (ledger, first, _, _, batch, since, shape) in rows:
                reads.setdefault(ledger, []).append((first, batch, since))
                shape_of[ledger] = plumbing.ints_text(shape)
        need, wholes = {}, set()
        for spans in reads.values():
            for _, batch, _ in spans:
                if batch[1] != 2: continue          # an inline batch is in the entry, and is read from nowhere
                for piece in plumbing.pieces_of(batch[2]):
                    if piece[:2] not in cache: need[piece[:2]] = piece
                if not plumbing.pieces_of(batch[2]) and plumbing.blob_of(batch[2]) not in cache: wholes.add(plumbing.blob_of(batch[2]))
        before = plumbing.STORED["got"]
        asks = plumbing.fetch(blobs, list(need.values()), cache)
        wholes = sorted(wholes)
        cache.update(zip(wholes, blobs.many(blobs.get, wholes)))
        read = plumbing.STORED["got"] - before
        need = list(need.items()) + [(name, None) for name in wholes]
        clock.append(time.perf_counter())
        raised = {ledger: [plumbing.raised(frame, instant(since)) for _, batch, since in sorted(spans, key=lambda span: span[0])
                           for frame in plumbing.frames_of_batch(blobs, batch, cache)] for ledger, spans in reads.items()}
        made = []
        for shape in sorted(set(shape_of.values())):
            ledgers = [ledger for ledger in raised if shape_of[ledger] == shape and raised[ledger]]
            shares = [ledgers[at::MERGERS] for at in range(MERGERS)]
            results = list(pool.map(lambda pair: pair[0].merge(shape, [raised[ledger] for ledger in pair[1]], PART) if pair[1] else [],
                                    zip(mergers, shares)))
            merged = {}
            for share, result in zip(shares, results):
                for ledger, frames in zip(share, result if result is not None else [raised[ledger] for ledger in share]): merged[ledger] = frames
            merged = [merged[ledger] for ledger in ledgers]
            made += [(ledger, frames) for ledger, frames in zip(ledgers, merged)]
        clock.append(time.perf_counter())
        # A merged batch under INLINE bytes is kept in its entry. One under SHARE bytes is written together with the
        # others of the pass, one part each, in one blob: so a pass of many small batches is a few writes. A longer one
        # is written to blobs of its own ledger, in parts of at most PART bytes, each part its own blob. A batch that
        # merges to nothing has no parts, and is empty.
        size = lambda frames: len(plumbing.pack({}, frames))
        inlined = {ledger: plumbing.inline(frames) for ledger, frames in made if frames}     # as an inline batch: compressed, unless that is off
        kept = lambda ledger: len(inlined[ledger][2])
        inside = [(ledger, frames) for ledger, frames in made if frames and kept(ledger) < plumbing.INLINE]
        small = [(ledger, frames) for ledger, frames in made if frames and kept(ledger) >= plumbing.INLINE and size(frames) < SHARE]
        large = [(ledger, frames) for ledger, frames in made if frames and kept(ledger) >= plumbing.INLINE and size(frames) >= SHARE]
        writes, named = [], {}
        if small:
            data, places = plumbing.several([frames for _, frames in small])
            writes.append(data)
            for (ledger, _), place in zip(small, places): named[ledger] = [(len(writes) - 1, place)]
        for ledger, frames in large:
            named[ledger] = []
            for group in plumbing.split(frames, PART):
                data, places = plumbing.several([group])
                writes.append(data)
                named[ledger].append((len(writes) - 1, places[0]))
        written = blobs.many(lambda data: blobs.put(data, stored=True), writes)
        clock.append(time.perf_counter())
        batches = [((ledger,), (("inject", 2, [n for at, place in named[ledger] for n in plumbing.piece_ints(written[at], place)]),), 1) for ledger in named]
        batches += [((ledger,), (inlined[ledger],), 1) for ledger, frames in inside]
        batches += [((ledger,), (plumbing.EMPTY,), 1) for ledger, frames in made if not frames]
        # What was read is kept for the next passes, the latest read first to go last, up to a bound on its bytes.
        for key, _ in need: cache[key] = cache.pop(key)
        held = sum(map(len, cache.values()))
        for key in list(cache):
            if held <= KEPT: break
            held -= len(cache.pop(key))
        if proposed:
            # The merges of the pass are asked for together. The keeper refuses those of ledgers that were merged after the
            # copy read, and those held from later than their ledger is needed from, and says how long the log then is.
            begin = time.perf_counter()
            turned, record, fused, left = keeper.merges(at, proposed, leaving, [plumbing.encode(plumbing.BATCHES, batches, timed=False)] if batches else [])
            took = time.perf_counter() - begin
            if not record:
                # The keeper made no part of the pass: a frame of it, or of what it would change, was too long for a
                # server. The same shards are asked for again, half as many, and one shard at the least.
                print(f"compactor: a pass of {width} of {SHARDS} shards was turned away, {long} bytes of frames", flush=True)
                if width == 1: at_shard = (at_shard + 1) % SHARDS       # one shard cannot be made smaller here: the next is tried
                width = max(1, width // 2)
                time.sleep(PAUSE)
                continue
            after = record
            times = [1000 * (b - a) for a, b in zip(clock, clock[1:])]
            sizes = [len(data) for data in writes]
            print(f"compactor: {fused + len(turned)} ledgers, {left} spans into {fused}, answered in {1000 * took:.0f} ms"
                  + (f"; {len(turned)} refused" if turned else "") + (f"; {width} of {SHARDS} shards" if shards is not None else "")
                  + f"; the pass, ms: the follower's answer {times[0]:.0f}, {asks + len(wholes)} blobs read {times[1]:.0f}, batches made {times[2]:.0f}, "
                  + f"{len(written)} blobs written {times[3]:.0f}; {long} bytes handed over"
                  + f"; parts read {read} bytes; written {sum(sizes)} bytes, the largest blob {max(sizes, default=0)}, "
                  + f"{len(inside)} inline, {len(small)} small batches shared, {len(large)} with blobs of their own"
                  + f"; the store {'touched' if need or writes else 'not touched'}; rows in {sum(plumbing.rows_of(f) for fs in raised.values() for f in fs)}, "
                  + f"rows out {sum(plumbing.rows_of(f) for _, fs in made for f in fs)}", flush=True)
        if shards is not None: at_shard = (at_shard + width) % SHARDS
        # A pass that was well within what can be handed over is followed by a wider one.
        if long < MOST // 3 and width < SHARDS: width = min(SHARDS, width * 2)
        # Passes begin PAUSE apart at the least: one that took longer is followed by the next at once.
        # So is one that handed over a good part of what can be: there is then more to merge than a pass takes.
        if long < MOST // 16: time.sleep(max(0.0, PAUSE - (time.perf_counter() - clock[0])))


def listing():
    """The names of the blobs in the store."""
    found, last = set(), ""
    while True:
        request = urllib.request.Request(plumbing.FILER + f"/{plumbing.SPACES['blobs']}/?limit=100000&lastFileName={last}", headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=60) as response: page = json.loads(response.read())
        found |= {entry["FullPath"].rsplit("/", 1)[1] for entry in page.get("Entries") or []}
        if not page.get("ShouldDisplayLoadMore") or not page.get("Entries"): return found
        last = page["LastFileName"]


# Seconds a blob that was never seen named is left, at the least. Nothing bounds how long a writer takes from writing
# a blob to having it named: a compactor's pass of 2,000 ledgers took two seconds, and an instance waits out a keeper
# that is away. So this is a guess, and a long one; what would make it exact is a lease on a write (see the report).
LINGER = float(os.environ.get("COLLECT_LINGER", "30.0"))


def sweep(pending, known, store, length, at, broken, named, now, linger=LINGER):
    """One look by the collector, and the blobs it may delete after it.
    `store` is the blobs in the store, and `length` the keeper's log length, asked after the store was listed.
    `named` is the blobs that spans name in the state a follower that keeps the rules has, `at` how many records of the
    log that state is of, and `broken` the rules that are not empty in it.
    `known` is the blobs this collector has seen named, and `pending` those it has seen in the store and not named:
    for each, when it first saw that, the keeper's log length then, and whether it had seen the blob named before.
    A blob is deleted only if it is unnamed in a state that keeps every rule, and that state has every record the
    keeper had made when the blob was first seen unnamed: a span that was already made then, and names it, is in that
    state. One that was named before, and is not now, is deleted on that alone: a merge took its span out, and the
    rules have passed what the merge left. That leans on a blob having one writer (see `plumbing.fresh`), who does not
    name it again once a span that named it is gone: with two writers of one blob, the second could name it after this.
    (That happened: a run of 200 computations lost a blob so, when blobs were named by their frames alone.)
    One that was never seen named may be a blob whose span is yet to be asked
    for: nothing says how long its writer will take, so it is also left for a while."""
    known |= named
    for name in [name for name in pending if name not in store or name in named]: del pending[name]
    for name in store - named: pending.setdefault(name, (now, length, name in known))
    if broken: return []
    gone = [name for name, (first, needed, was) in pending.items() if at >= needed and (was or now - first >= linger)]
    for name in gone:
        del pending[name]
        known.discard(name)
    return sorted(gone)


def named_of(rows):
    """The blobs that are named, given the rows of the view `blobs`: each row's key is a batch's name, which names
    parts of blobs (or, in the older forms, a blob, or a part of one). A blob is named while any span names any part of it."""
    return {name for key, _ in rows for name in plumbing.blobs_named(key[0])}


def collect(url, follower):
    """Delete the blobs that no span names. `follower` is one that keeps the rules (the role `audit`)."""
    keeper, copy, known, pending, said = plumbing.Client(url), plumbing.Follower(follower), set(), {}, None
    while True:
        # The store is listed first, and then the keeper is asked how long its log is: a span that was made before the
        # listing is within that length.
        store = listing()
        length = keeper.length()
        # A span names a blob, or a part of one. A blob is named while any span names any part of it.
        at, rows = copy.rows_at("views::blobs", *[f"views::{rule}" for rule in model.STATE_RULES])
        named = named_of(rows[0])
        broken = [rule for rule, found in zip(model.STATE_RULES, rows[1:]) if found]
        if broken != said:
            if broken: print(f"collector: a follower finds rules broken, {broken}: nothing is deleted while that is so", flush=True)
            said = broken
        gone = sweep(pending, known, store, length, at, broken, named, time.time())
        for name in gone: plumbing.http("DELETE", plumbing.FILER + f"/{plumbing.SPACES['blobs']}/" + name)
        if gone: print(f"collector: deleted {len(gone)} blobs that no span names", flush=True)
        time.sleep(0.5)


OWN = (0, 0)        # the holder of a ledger's own hold (see own.ddp)
STEP = int(os.environ.get("HOLD_STEP", "100"))         # a ledger's own hold moves in steps of this many times, so seldom
EVERY = float(os.environ.get("HOLD_EVERY", "5"))       # seconds between looks


def hold(url, follower, view="needs::needs"):
    """Move each ledger's own hold forward, now and then: to what its readers need, rounded down to a multiple of `STEP`.
    `follower` is one that keeps the compaction policy (the role `tidy`): it has the needs, and each ledger's own hold.
    The moves of a look are one transaction. The rules refuse it if a reader needs a ledger from before its new hold:
    then each move is asked alone, and those refused wait for the next look. Times of one coordinate only are moved."""
    keeper, copy = plumbing.Client(url), plumbing.Follower(follower)
    show = plumbing.show
    while True:
        time.sleep(EVERY)
        needs, held = copy.rows(view, "own::holds")
        least = {}
        for (source,), (_, _, frontier) in needs:
            if len(frontier) != 1 or len(frontier[0]) != 1: continue
            least[source] = min(least.get(source, frontier[0][0]), frontier[0][0])
        now = {ledger: frontier[0][0] for (ledger,), (frontier,) in held if len(frontier) == 1 and len(frontier[0]) == 1}
        moves = []
        for ledger, need in least.items():
            target = (need // STEP) * STEP
            if target > now.get(ledger, -1) and target > 0:
                commands = ([f"inject(13,tuple({show(OWN)},{show(ledger)}))"] if ledger in now else [])
                commands.append(f"inject(12,tuple({show(OWN)},{show(ledger)},list(list({target}))))")
                moves.append(commands)
        if not moves: continue
        refused = keeper.transact(model.USER, [command for move in moves for command in move])
        if refused:
            alone = sum(1 for move in moves if keeper.transact(model.USER, move) == [])
            print(f"steward: the moves of {len(moves)} own holds together were refused ({refused}); {alone} made alone", flush=True)
        else: print(f"steward: moved {len(moves)} own holds", flush=True)


def vacuum(seconds):
    """Ask the store to give back the space of what was deleted, every so many seconds. The store keeps a deleted blob's
    bytes until it is asked: without this, what the collector and the log's cut delete is never given back."""
    while True:
        time.sleep(float(seconds))
        found = subprocess.run(["docker", "exec", "-i", "ledger-spike-weed", "weed", "shell", "-master=localhost:9333"],
                               input=b"lock\nvolume.vacuum -garbageThreshold 0.05\nunlock\n", capture_output=True, timeout=900)
        print(f"steward: the store was vacuumed ({'done' if found.returncode == 0 else 'failed'})", flush=True)


if __name__ == "__main__":
    for sign in (signal.SIGTERM, signal.SIGHUP): signal.signal(sign, lambda *_: sys.exit(0))
    roles = {"compact": compact, "collect": collect, "hold": hold, "vacuum": vacuum}
    while True:
        try: roles[sys.argv[1]](*sys.argv[2:])
        except OSError: time.sleep(0.5)      # the keeper or the store is away: begin again when it is back
