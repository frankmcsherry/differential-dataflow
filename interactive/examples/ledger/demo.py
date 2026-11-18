#!/usr/bin/env python3
"""A walk through the system of ledgers, in acts, across four machines. Everything after act 0 is a transaction.

  python3 interactive/examples/ledger/demo.py            the walk: it stops before each act, and goes on at Enter
  python3 interactive/examples/ledger/demo.py --auto     the same, not stopping (to try it)
  python3 interactive/examples/ledger/demo.py scale [computations] [instances] [ticks a second] [seconds]
                                                         the last act alone: a system under load, on this machine

It needs what fleet.py needs: one etcd of three members on machines 0, 1 and 2, SeaweedFS on this machine, and the
stock server built. Run it from the repository root. A page of the state's views, to watch beside it:
  python3 interactive/examples/ledger/watch.py 100=A 101=B 102=C
For the system left running, with churn, see steady.py, and its page `watch.py steady`.

The system is the one fleet.py checks. This machine, number 3, has the keeper and the blob store. Each of the four
machines has a follower, which holds a copy of the state, and a launcher, which starts the instances the state gives
its machine. Nothing is told what to do except by the state, and the state is changed only by transactions.

What is set up: lines of words go into a ledger, and three computations follow one another, each on its own machine.
  A  counts   how many lines each word is in             lines  -> counts
  B  often    the words that are in two lines or more    counts -> often
  C  summary  how many words are in each number of lines often  -> summary
A word is an int here, and so is a line: line t holds the words t % 5, t % 7 and 100 + t.
"""
import json, os, re, statistics, subprocess, sys, threading, time
import plumbing
from plumbing import Client, Blobs, SPAN, number
from live import (URL, HTTP, FOLLOW, FOLLOWER, AUDIT, AUDITOR, USER, COUNT, OFTEN, ROW, ints, ident, create, append, publish, define,
                  start, stop, spawn, up, ready, contents, agree, wait, batch)
from fleet import MACHINES, HOME, ETCD, deploy, session, instances, settle

HERE = os.path.dirname(os.path.abspath(__file__))
AUTO = "--auto" in sys.argv
LOCAL = list(plumbing.ETCD)                 # the etcd on this machine, which the last act uses

SUMMARY = f"""-- How many words are in each number of lines.
let often = input 0 : {ROW};
output "out0" = often | map((_, (lines)) -> ((lines), ())) | count | map(((lines), words) -> ((lines), (words)));
"""
LINES, COUNTS, OFTENS, SUMS = 1, 2, 3, 4                    # the four ledgers
NAMES = {LINES: "lines", COUNTS: "counts", OFTENS: "often", SUMS: "summary"}
A, B, C = 100, 101, 102                                     # the three computations

def alias(name, ledger): return f"inject(5,tuple({ints(plumbing.text_ints(name))},{ident(ledger)}))"
def drop(ledger): return f"inject(4,{ident(ledger)})"


def act(title, *lines):
    """Say what is about to be asked, and what to look for, and wait to be told to go on."""
    print(f"\n{title}", flush=True)
    for line in lines: print(f"    {line}", flush=True)
    if AUTO: time.sleep(1.5)
    else: input("    [Enter] ")


def said(text): print(f"  {text}", flush=True)


def walk():
    for machine in MACHINES: deploy(machine)
    plumbing.ETCD = ETCD.split(",")         # the register of this walk is the one etcd of three members
    register = f"ledger-demo-{int(time.time())}"
    keeper_py, runner_py, steward_py, follower_py = (os.path.join(HERE, name) for name in ("keeper.py", "runner.py", "steward.py", "follower.py"))
    processes = {}
    try:
        act("ACT 0. A system with nothing in it.",
            "A keeper and a blob store on this machine. A follower and a launcher on each of four machines.",
            "Look for: no ledgers, no computations, no instances. Nothing has been asked for.")
        begin = time.perf_counter()
        processes["keeper"] = spawn(keeper_py, "serve", register, str(HTTP), LEDGER_ETCD=ETCD)
        up(URL)
        processes["follower 3"] = spawn(follower_py, "serve", register, URL, str(FOLLOW), LEDGER_ETCD=ETCD)
        ready(FOLLOWER)
        processes["launcher 3"] = spawn(runner_py, "launch", URL, FOLLOWER, str(HOME))
        for machine in MACHINES:
            # A machine's launcher is started first: its session carries the ports that the machine's follower needs too.
            processes[f"launcher {machine}"] = session(machine, f"runner.py launch {URL} {FOLLOWER} {machine}")
            processes[f"follower {machine}"] = session(machine, f"follower.py serve {register} {URL} {FOLLOW}", carry=False)
        # The keeper holds no spans: what is asked of them here is asked of this machine's follower, once it has all the keeper made.
        keeper, blobs = Client(URL, FOLLOWER), Blobs()
        said(f"up in {time.perf_counter() - begin:.1f} s: the keeper here, and a follower and a launcher on machines 0, 1, 2 and 3")
        said(f"entries in the state: {len(keeper.caught('state::entries')[0])}; instances running: {sum(len(instances(m)) for m in (0, 1, 2, HOME))}")

        # What the ledgers should hold, worked out here, in a few lines.
        words = {}
        def should():
            sums = {}
            for lines in words.values():
                if lines >= 2: sums[lines] = sums.get(lines, 0) + 1
            return {((lines,), (count,)): 1 for lines, count in sums.items()}
        def write(at):
            line = sorted({at % 5, at % 7, 100 + at})
            for word in line: words[word] = words.get(word, 0) + 1
            return keeper.transact(USER, [append(LINES, at, at, at + 1, batch(blobs, [((word,), (at,), 1, at) for word in line]))])
        def carry(first, last, seconds=60):
            """Append lines `first` to `last`, wait for the summary to follow, and say whether it holds what it should."""
            begin = time.perf_counter()
            for at in range(first, last): assert write(at) == []
            wait(keeper, SUMS, last, seconds=seconds)
            held, right = contents(keeper, blobs, SUMS), should()
            ok = agree(held, right)
            said(f"lines {first} to {last - 1} appended; the summary stood at {last} after {time.perf_counter() - begin:.2f} s, and is "
                 + ("what a few lines of Python say it should be" if ok else "NOT what it should be"))
            if not ok: raise SystemExit("the summary is wrong")
            return held
        def show(held):
            said("the summary: " + "; ".join(f"{count} word{' is' if count == 1 else 's are'} in {lines} lines" for ((lines,), (count,)), _ in sorted(held.items())))
        def where():
            said("instances running: " + ", ".join(f"machine {m}: {' '.join(instances(m)) or 'none'}" for m in (0, 1, 2, HOME)))

        act("ACT 1. One transaction sets up a pipeline across three machines.",
            "Four ledgers with names, three programs, three computations A -> B -> C, and where each runs: machines 0, 1, 2.",
            "Look for: each launcher starting an instance, because the state now asks for one on its machine.")
        setup = ([create(ledger) for ledger in NAMES] + [alias(name, ledger) for ledger, name in NAMES.items()]
                 + [publish(COUNT), publish(OFTEN), publish(SUMMARY)]
                 + [define(A, COUNT, LINES, COUNTS), define(B, OFTEN, COUNTS, OFTENS), define(C, SUMMARY, OFTENS, SUMS)]
                 + [start((0, 500), A), start((1, 501), B), start((2, 502), C)])
        begin = time.perf_counter()
        refused = keeper.transact(USER, setup)
        said(f"one transaction of {len(setup)} commands: {'refused by ' + str(refused) if refused else 'made'}, in {1000 * (time.perf_counter() - begin):.0f} ms")
        assert refused == []
        settle(lambda: all(instances(m) for m in (0, 1, 2)))
        where()
        show(carry(0, 12))
        # What each instance was told: a follower gives it the entries it needs, and no others.
        whole, copy = len(keeper.caught("state::entries")[0]), plumbing.Follower(FOLLOWER)
        told = {name: len(copy.peek(f"bundle::bundle tuple(tuple({machine},{instance}))")[0]) for name, (machine, instance) in (("A", (0, 500)), ("B", (1, 501)), ("C", (2, 502)))}
        said(f"the state has {whole} entries; the instance that runs A was told {told['A']} of them, B's {told['B']}, C's {told['C']}")
        begin = time.perf_counter()
        assert write(12) == []
        wait(keeper, SUMS, 13, seconds=60)
        said(f"one more line: the summary had it {1000 * (time.perf_counter() - begin):.0f} ms after it was asked for, through three computations on three machines")

        act("ACT 2. The rules refuse what would break the state, and say which rule.",
            "A user asks to drop the ledger that A reads. Then a user asks to append to a ledger that B writes.",
            "Look for: the names that come back. Each is a query of a few lines over the state.")
        said(f"drop the ledger `lines`: refused by {keeper.transact(USER, [drop(LINES)])}")
        version = keeper.standing()[OFTENS]
        said(f"a user's append to `often`: refused by {keeper.transact(USER, [append(OFTENS, version[0], version[1], version[1] + 1, None)])}")
        said(f"start a computation that is not defined: refused by {keeper.transact(USER, [start((0, 500), 999)])}")
        said(f"entries in the state are as they were: the state keeps every rule")

        act("ACT 3. A computation is moved to another machine, by one transaction, while lines come in.",
            "B stops on machine 1 and starts on machine 2, on the instance that already runs C.",
            "Look for: B under machine 2. The summary stays right: B goes on from where its ledger stood.",
            "For a moment both instances may run B. Then the keeper takes one's append and refuses the other's, and the one",
            "refused says it must start over, and does, from where its ledger stands: that is the answer to two writers.")
        for at in range(13, 16): assert write(at) == []
        begin = time.perf_counter()
        assert keeper.transact(USER, [stop((1, 501), B), start((2, 502), B)]) == []
        said(f"moved, in {1000 * (time.perf_counter() - begin):.0f} ms")
        show(carry(16, 24))
        settle(lambda: instances(1) == [] and instances(2) == ["2.502"])
        where()

        act("ACT 4. A machine is lost.",
            "Machine 2 goes away: its follower, its launcher and its instance, with B and C on it. Lines keep coming.",
            "Look for: `counts` moves on and `often` and `summary` stand still. Then one transaction gives B and C new homes.")
        processes.pop("launcher 2").terminate()
        processes.pop("follower 2").terminate()
        settle(lambda: instances(2) == [])
        stood = keeper.standing()
        for at in range(24, 30): assert write(at) == []
        settle(lambda: keeper.standing()[COUNTS][1] == 30)
        now = keeper.standing()
        said(f"`counts` stands at {now[COUNTS][1]}; `often` at {now[OFTENS][1]} and `summary` at {now[SUMS][1]}, where they stood ({stood[SUMS][1]})")
        begin = time.perf_counter()
        assert keeper.transact(USER, [stop((2, 502), B), stop((2, 502), C), start((1, 504), B), start((HOME, 505), C)]) == []
        wait(keeper, SUMS, 30, seconds=60)
        said(f"B to machine 1 and C to machine 3, by one transaction: the summary caught up to 30 in {time.perf_counter() - begin:.2f} s")
        show(carry(30, 36))
        where()

        act("ACT 5. The keeper is stopped, and another is started.",
            "The new keeper reads the state back from the register and the blobs. No other machine is told.",
            "Look for: the page says the keeper is away, and then the counts return. The instances carry on.")
        before = (sorted(keeper.peek("state::entries")), sorted(keeper.peek("rows::own")))
        gone = processes.pop("keeper")
        gone.terminate(); gone.wait()
        begin = time.perf_counter()
        processes["keeper"] = spawn(keeper_py, "serve", register, str(HTTP), LEDGER_ETCD=ETCD)
        up(URL)
        same = (sorted(keeper.peek("state::entries")), sorted(keeper.peek("rows::own"))) == before
        said(f"a new keeper has {'the same' if same else 'ANOTHER'} configuration, of {len(before[0])} entries, and the same row for each of "
             f"{len(before[1])} ledgers, in {time.perf_counter() - begin:.1f} s")
        assert same
        show(carry(36, 42))

        act("ACT 6. Compaction is one more client.",
            "A compactor reads a view that says which spans to merge, makes the merged batch, and asks by a transaction.",
            "Look for: the spans of each ledger falling to a few, and staying few as lines come in.")
        def spans():
            counts = {}
            for key, _ in keeper.entries().get(SPAN, []): counts[number(key[0])] = counts.get(number(key[0]), 0) + 1
            return counts
        said("spans before: " + ", ".join(f"`{NAMES[l]}` {n}" for l, n in sorted(spans().items())))
        processes["compactor"] = spawn(steward_py, "compact", URL, FOLLOWER, LEDGER_ETCD=ETCD)
        # The collector reads a follower that keeps the rules in full, and deletes only what is unnamed in a state they pass.
        processes["auditor"] = spawn(follower_py, "serve", register, URL, str(AUDIT), "audit", LEDGER_ETCD=ETCD)
        ready(AUDITOR)
        processes["collector"] = spawn(steward_py, "collect", URL, AUDITOR, LEDGER_ETCD=ETCD)
        tidied = lambda: not plumbing.Follower(FOLLOWER).rows("tidy::merge")[0]
        settle(lambda: tidied() and max(spans().values()) <= 6, seconds=60)
        said("spans after:  " + ", ".join(f"`{NAMES[l]}` {n}" for l, n in sorted(spans().items())))
        show(carry(42, 54))
        settle(lambda: tidied() and max(spans().values()) <= 6, seconds=60)
        said("spans now:    " + ", ".join(f"`{NAMES[l]}` {n}" for l, n in sorted(spans().items())))

        act("THE END of the walk.", "Every rule of the state is read, from a follower that keeps them all, and everything is stopped.")
        import keeper as model
        auditor = plumbing.Follower(AUDITOR)
        settle(lambda: auditor.where()["at"] == keeper.length())
        broken = [rule for rule, rows in zip(model.STATE_RULES, auditor.peek(*[f"views::{rule}" for rule in model.STATE_RULES])) if rows]
        said(f"rules that are broken, of {len(model.STATE_RULES)}: {broken or 'none'}")
        said(f"transactions and rounds in the log: {plumbing.Register(register).read()[0]['count']}; keeper: {json.loads(plumbing.http('GET', URL + '/counts'))}")
        where()
    finally:
        for process in processes.values(): process.terminate()
        for process in processes.values(): process.wait()
        subprocess.run(["pkill", "-f", "runner.py run "])
        time.sleep(1)
        print("  left running: " + (", ".join(f"machine {m}: {instances(m)}" for m in MACHINES if instances(m)) or "nothing"), flush=True)


# Under load, each tick appends to every source ledger, in one request. These read the log as the keeper writes it.
def uppers(frame):
    """Of a frame of appends as a request holds them, ((tx), (ledger, number, lower, upper, batch)): each append's
    ledger and the time its upper is. Only those two columns are read."""
    words = plumbing.Words(frame)
    rows, _ = words.word(), words.word()
    words.column()
    assert (words.word(), words.word()) == (1, 5)
    ledger, _, _, upper, _ = [words.column() for _ in range(5)]
    return [(ledger(row)[1], upper(row)[0][0]) for row in range(rows)]


PREV = re.compile(rb'"prev": "?(\w+)"?')
SOURCE, OUTPUT, COMPUTATION = 100000, 200000, 300000


def scale(count=200, hosts=4, rate=10.0, seconds=20):
    """A fresh system on this machine alone: `count` source ledgers, `count` computations on `hosts` instances, and
    `count` output ledgers. Each tick a user appends to every source ledger. A tick is done when every output ledger
    has it, and its delay is from when it was due. One line is printed for each second."""
    act(f"ACT 7. Load: {count} ledgers in, {count} computations on {hosts} instances, {count} ledgers out, {rate:g} ticks a second.",
        "A fresh system, on this machine alone. Each tick appends to every source ledger, and every computation follows.",
        "Look for: ticks done a second keeping to ticks asked, and the delay from when a tick was due.")
    plumbing.ETCD = LOCAL                   # the register of this act is the etcd on this machine
    register = f"ledger-demo-load-{int(time.time())}"
    processes = {"keeper": spawn(os.path.join(HERE, "keeper.py"), "serve", register, str(HTTP))}
    try:
        up(URL)
        processes["follower"] = spawn(os.path.join(HERE, "follower.py"), "serve", register, URL, str(FOLLOW))
        ready(FOLLOWER)
        processes["launcher"] = spawn(os.path.join(HERE, "runner.py"), "launch", URL, FOLLOWER)
        keeper, blobs, ks = Client(URL), Blobs(), list(range(count))
        begin = time.perf_counter()
        assert keeper.transact(USER, [publish(COUNT)]) == []
        for at in range(0, count, 100):
            part = ks[at:at + 100]
            assert keeper.transact(USER, [create(SOURCE + k) for k in part] + [create(OUTPUT + k) for k in part]
                                   + [define(COMPUTATION + k, COUNT, SOURCE + k, OUTPUT + k) for k in part]
                                   + [start((0, k % hosts), COMPUTATION + k) for k in part]) == []
        said(f"set up by {1 + (count + 99) // 100} transactions, in {time.perf_counter() - begin:.1f} s")

        due, done, lock = {}, {}, threading.Lock()
        def follow():
            # The log is read from the register and the blobs, so that watching asks nothing of the keeper or a follower.
            # A round's record holds, after the spans it made, what it changed in the keeper's rows: where each ledger stands.
            cell, log, read, stood = plumbing.Register(register), Blobs("log"), 0, {}
            while True:
                try:
                    state, _ = cell.read()
                    fresh, name = [], state and state["head"]
                    for _ in range(state["count"] - read if state else 0):
                        blob = log.get(name)
                        fresh.append(plumbing.unpack(blob))
                        name = PREV.search(blob.split(b"\n", 1)[0]).group(1).decode()
                except (OSError, ValueError, AttributeError): time.sleep(0.05); continue
                for record, frames in reversed(fresh):
                    if "round" not in record: continue
                    # What the round changed in the keeper's rows says where each ledger it appended to now stands.
                    for ledger, upper in plumbing.round_uppers(record, frames): stood[ledger] = max(stood.get(ledger, 0), upper)
                read += len(fresh)
                least, now = min(stood.get(OUTPUT + k, 0) for k in ks), time.perf_counter()
                with lock:
                    for t in due:
                        if t < least and t not in done: done[t] = now
                time.sleep(0.004)
        threading.Thread(target=follow, daemon=True).start()

        def tick(t, when):
            # One span for every source ledger: the same rows, so one blob. A ledger's appends are asked for in order.
            blob = batch(blobs, [((word % 7,), (1000 * t + word,), 1, t) for word in range(10)])
            with lock: due[t] = when
            refused = keeper.transact_many([(USER, [append(SOURCE + k, t, t, t + 1, blob)]) for k in ks])
            assert not any(refused), [r for r in refused if r][:2]
        tick(0, time.perf_counter())
        assert settle(lambda: 0 in done, seconds=120), "the first tick did not reach every output ledger in two minutes"
        said("the instances are up, and the first tick is through every computation")

        ticks, begin, told = int(rate * seconds), time.perf_counter(), 0
        print("    second   ticks asked   ticks done   median delay, ms   slowest, ms", flush=True)
        def tell(second):
            with lock: lags = [1000 * (done[t] - due[t]) for t in done if t and begin + second <= due[t] < begin + second + 1]
            with lock: asked = sum(1 for t in due if t and begin + second <= due[t] < begin + second + 1)
            if asked: print(f"    {second + 1:6d}   {asked:11d}   {len(lags):10d}   {statistics.median(lags) if lags else 0:16.0f}   {max(lags) if lags else 0:11.0f}", flush=True)
        for at in range(ticks):
            when = begin + at / rate
            if when > time.perf_counter(): time.sleep(when - time.perf_counter())
            tick(at + 1, when)
            while told + 2 <= time.perf_counter() - begin: tell(told); told += 1
        settle(lambda: ticks in done, seconds=60)
        while told < seconds: tell(told); told += 1
        with lock: lags = sorted(1000 * (done[t] - due[t]) for t in done if t)
        counts = json.loads(plumbing.http("GET", URL + "/counts"))
        said(f"{len(lags)} of {ticks} ticks done, {2 * count * len(lags) / max(seconds, 1):.0f} appends a second asked for, sources and outputs")
        if lags: said(f"delay from when a tick was due to every output having it: median {statistics.median(lags):.0f} ms, nine in ten under {lags[int(0.9 * len(lags))]:.0f} ms")
        said(f"the keeper made {counts['rounds']} rounds for {ticks + 1} ticks, and judged {counts['alone']} transactions by the rules")
    finally:
        for process in processes.values(): process.terminate()
        for process in processes.values(): process.wait()
        subprocess.run(["pkill", "-f", "runner.py run "])


if __name__ == "__main__":
    args = [arg for arg in sys.argv[1:] if arg != "--auto"]
    if args[:1] == ["scale"]:
        numbers = args[1:]
        scale(*(int(numbers[0]) if len(numbers) > 0 else 200, int(numbers[1]) if len(numbers) > 1 else 4,
                float(numbers[2]) if len(numbers) > 2 else 10.0, int(numbers[3]) if len(numbers) > 3 else 20))
    else:
        walk()
        if "--no-load" not in args:
            time.sleep(2)
            scale()
