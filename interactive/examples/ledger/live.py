#!/usr/bin/env python3
"""A system of ledgers that runs, and is told what to do only by transactions against its state.

It needs etcd on 127.0.0.1:2379 and a SeaweedFS filer on 127.0.0.1:8888 (see plumbing.py), and the stock server built.
Four kinds of process are started here, and none is told anything but where the keeper and the follower are:
  a keeper     keeps the state to its rules, and writes each transaction it makes to blobs whose head is in the register
  a follower   holds a copy of the state, from what the keeper sends it and from the log, and answers those who read
  a launcher   starts an instance for each one the state asks for, and stops each one it no longer asks for
  instances    run the computations the state gives them: they read ledgers, run programs, and append what comes out
Launchers and instances read from the follower, each by one tail of what it needs, and write to the keeper.

Everything else below is a user, transacting: it makes ledgers, publishes two programs, defines two computations,
asks instances to run them, appends data, and reads what the last ledger holds.

Run from the repository root:
  python3 interactive/examples/ledger/live.py
"""
import hashlib, json, os, subprocess, sys, threading, time, urllib.error
import plumbing
from plumbing import Client, Blobs, SPAN, number
import runner

HERE = os.path.dirname(os.path.abspath(__file__))
URL, HTTP = "http://127.0.0.1:7970", 7970
FOLLOW = 7990
FOLLOWER = f"127.0.0.1:{FOLLOW}"
USER = "inject(0,tuple())"
ROW = "((int), (int))"

COUNT = f"""-- How many lines each word is in.
let lines = input 0 : {ROW};
output "out0" = lines | map(((word), _) -> ((word), ())) | count | map(((word), lines) -> ((word), (lines)));
"""
OFTEN = f"""-- The words that are in two lines or more, each with how many.
let counts = input 0 : {ROW};
output "out0" = counts | filter((_, (lines)) -> (lines, 2) ge);
"""

TALLY = "(((int, int)), (int))"
def spans_program():
    """How many spans each ledger has, read from the state."""
    import keeper
    return keeper.declarations() + f"""-- How many spans each ledger has.
let entries = input 0 : ((Entry), ());
output "out0" = entries((Span(((ledger, _, _, _), _))), ()) | count | map(((ledger), spans) -> ((ledger), (spans)));
"""
MANY = f"""-- The ledgers that have two spans or more, each with how many.
let tallies = input 0 : {TALLY};
output "out0" = tallies | filter((_, (spans)) -> (spans, 2) ge);
"""
BOTH = f"""-- The rows of either of two ledgers.
let one = input 0 : {ROW};
let other = input 1 : {ROW};
output "out0" = one + other;
"""

def ints(values): return "list(" + ",".join(map(str, values)) + ")"
def ident(n): return f"tuple({n[0]},{n[1]})" if isinstance(n, tuple) else f"tuple(0,{n})"
def frontier(t): return f"list(list({t}))"
def shape(text=ROW): return ints(plumbing.text_ints(text))
def name_of(text): return hashlib.sha256(text.encode()).hexdigest()
def create(ledger, rows=ROW): return f"inject(0,tuple({ident(ledger)},{shape(rows)},1,{frontier(0)},inject(0,tuple()),inject(0,tuple())))"
def append(ledger, at, lower, upper, blob):
    batch = f"inject(2,{ints(plumbing.hash_ints(blob))})" if blob else "inject(0,tuple())"
    return f"inject(1,tuple({ident(ledger)},{at},{frontier(lower)},{frontier(upper)},{batch}))"
def merge(ledger, first, last, since, blob):
    return f"inject(2,tuple({ident(ledger)},{first},{last},{frontier(since)},inject(2,{ints(plumbing.hash_ints(blob))})))"
def discard(ledger, first, last, since): return f"inject(3,tuple({ident(ledger)},{first},{last},{frontier(since)}))"
def create_derived(ledger, rows):
    return f"inject(0,tuple({ident(ledger)},{shape(rows)},1,{frontier(0)},inject(0,tuple()),inject(1,tuple())))"
def publish(text, takes=ROW, gives=ROW):
    """`takes` is the shape of the program's one input, or a list of the shapes of its inputs."""
    takes = ",".join(shape(rows) for rows in (takes if isinstance(takes, list) else [takes]))
    return (f"inject(7,tuple({ints(plumbing.hash_ints(name_of(text)))},list({takes}),list({shape(gives)}),"
            f"{ints(plumbing.text_ints(text))}))")
def define(computation, text, reads, writes):
    """`reads` is what the computation's one input is, a ledger or "state", or a list of what its inputs are."""
    sources = ",".join("inject(0,tuple())" if source == "state" else f"inject(1,{ident(source)})"
                       for source in (reads if isinstance(reads, list) else [reads]))
    return (f"inject(8,tuple({ident(computation)},{ints(plumbing.hash_ints(name_of(text)))},"
            f"list({sources}),list(inject(1,{ident(writes)}))))")
def retire(computation): return f"inject(9,{ident(computation)})"
def start(instance, computation): return f"inject(10,tuple({ident(instance)},{ident(computation)}))"
def stop(instance, computation): return f"inject(11,tuple({ident(instance)},{ident(computation)}))"
def hold(holder, ledger, since): return f"inject(12,tuple({ident(holder)},{ident(ledger)},{frontier(since)}))"


def check(label, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + label + (f"  [{detail}]" if detail else ""), flush=True)
    if not ok: raise SystemExit(1)


def spawn(*args, **env):
    return subprocess.Popen([sys.executable, *args], env=dict(os.environ, **env), cwd=plumbing.ROOT)


def up(url):
    """Wait for a keeper to answer."""
    for _ in range(600):
        try: return Client(url).peek("before::standing")
        except (urllib.error.URLError, ConnectionError): time.sleep(0.05)
    raise RuntimeError("the keeper never came up")


def ready(address):
    """Wait for a follower to answer."""
    for _ in range(600):
        try: return plumbing.Follower(address).peek("views::standing")
        except (OSError, ValueError): time.sleep(0.05)
    raise RuntimeError("the follower never came up")


def settle(done, seconds=30):
    """Wait for something to be so. Returns whether it came to be."""
    begin = time.perf_counter()
    while not done():
        if time.perf_counter() - begin > seconds: return False
        time.sleep(0.05)
    return True


def merged(keeper, blobs, ledger, since):
    """A blob of everything a ledger holds, as a reader from `since` on would see it."""
    spans, sums = [(key, val) for key, val in keeper.entries().get(SPAN, []) if number(key[0]) == ledger], {}
    for key, val, at, diff in runner.history(spans, blobs, {})[0]:
        row = (key, val, max(at, since))
        sums[row] = sums.get(row, 0) + diff
    return blobs.put(json.dumps({"updates": [[key, val, at, diff] for (key, val, at), diff in sorted(sums.items()) if diff]}).encode())


def contents(keeper, blobs, ledger):
    """What a ledger holds once all its times have passed: {(key, val): count}."""
    spans = [(key, val) for key, val in keeper.entries().get(SPAN, []) if number(key[0]) == ledger]
    rows = {}
    for key, val, _, diff in runner.history(spans, blobs, {})[0]:
        rows[(key, val)] = rows.get((key, val), 0) + diff
    return {row: count for row, count in rows.items() if count}


def agree(held, should):
    """Whether a ledger holds what it should, saying how it differs if it does not."""
    for row in sorted(set(held) | set(should)):
        if held.get(row, 0) != should.get(row, 0): print(f"  {row}: holds {held.get(row, 0)}, should hold {should.get(row, 0)}", flush=True)
    return held == should


def wait(keeper, ledger, until, seconds=30):
    """Wait for a ledger to stand at a time."""
    begin = time.perf_counter()
    while time.perf_counter() - begin < seconds:
        if keeper.standing().get(ledger, (0, 0))[1] == until: return time.perf_counter() - begin
        time.sleep(0.02)
    raise SystemExit(f"FAIL ledger {ledger} never reached {until}: {keeper.standing()}")


def main():
    register = f"ledger-spike-{int(time.time())}"
    keeper_py, runner_py, follower_py = (os.path.join(HERE, name) for name in ("keeper.py", "runner.py", "follower.py"))
    processes = {"keeper": spawn(keeper_py, "serve", register, str(HTTP))}
    try:
        up(URL)
        processes["follower"] = spawn(follower_py, "serve", register, URL, str(FOLLOW))
        ready(FOLLOWER)
        processes["launcher"] = spawn(runner_py, "launch", URL, FOLLOWER)
        keeper, copy, blobs = Client(URL), plumbing.Follower(FOLLOWER), Blobs()
        def caught_up(): return sorted(copy.peek("state::entries")[0]) == sorted(keeper.peek("committed::entries"))

        # The user sets the system up: three ledgers, two programs, two computations, two instances.
        setup = [create(1), create(2), create(3), publish(COUNT), publish(OFTEN),
                 define(100, COUNT, 1, 2), define(101, OFTEN, 2, 3), start(500, 100), start(501, 101)]
        check("the system is set up by one transaction", keeper.transact(USER, setup) == [])

        # The user appends lines of words to ledger 1: at time t, line t holds the words t % 5, t % 7 and 100 + t.
        words, expected = {}, lambda: {(f"tuple({w})", f"tuple({n})"): 1 for w, n in words.items() if n >= 2}
        def write(at):
            line = sorted({at % 5, at % 7, 100 + at})
            for word in line: words[word] = words.get(word, 0) + 1
            blob = blobs.put(json.dumps({"updates": [[f"tuple({word})", f"tuple({at})", at, 1] for word in line]}).encode())
            return keeper.transact(USER, [append(1, at, at, at + 1, blob)])
        for at in range(20): assert write(at) == []
        took = wait(keeper, 3, 20)
        check("two instances carry 20 appends through two computations", agree(contents(keeper, blobs, 3), expected()),
              f"ledger 3 stood at 20 after {took:.2f} s, with {len(expected())} words")

        # The instances read from the follower, and each is told only what it needs.
        check("the follower holds the state the keeper has, from what the keeper sent it", settle(caught_up))
        def told(instance):
            rows = copy.peek(f"bundle::bundle tuple(tuple(0,{instance}))")[0]
            return plumbing.by_kind([line.partition(" val=")[2] for line in rows])
        about = lambda entries: sorted({number(key[0]) for key, _ in entries.get(SPAN, [])})
        count = lambda entries: sum(map(len, entries.values()))
        check("each instance is told of the ledgers its computation reads and writes, and of no others",
              about(told(500)) == [1, 2] and about(told(501)) == [2, 3],
              f"{count(told(500))} and {count(told(501))} of the state's {len(keeper.peek('committed::entries'))} entries")
        entries, standing, _ = keeper.snapshot()
        check("where the ledgers stand, worked out from spans as an instance does, is what the model says",
              plumbing.standing_of(entries) == standing)

        # A computation that reads two ledgers. Ledger 1 stands at 20, and ledger 5 is new, and is appended to in two parts.
        # What the computation reads of ledger 1 past where ledger 5 stands has to wait, and must not be lost.
        check("a computation over two ledgers is set up",
              keeper.transact(USER, [create(5), create(6), publish(BOTH, [ROW, ROW]), define(102, BOTH, [1, 5], 6), start(504, 102)]) == [])
        for part, (first, last) in enumerate([(0, 7), (7, 20)]):
            rows = [[f"tuple({1000 + at})", f"tuple({at})", at, 1] for at in range(first, last)]
            assert keeper.transact(USER, [append(5, part, first, last, blobs.put(json.dumps({"updates": rows}).encode()))]) == []
            wait(keeper, 6, last)
        either = {**contents(keeper, blobs, 1), **contents(keeper, blobs, 5)}
        check("a computation whose inputs stand at different times loses nothing", agree(contents(keeper, blobs, 6), either),
              f"ledger 6 holds {len(either)} rows")
        check("it is stopped and retired", keeper.transact(USER, [stop(504, 102), retire(102)]) == [])

        # Appends that come together. The keeper takes those that have come while it was busy as one round.
        crowd = list(range(40, 80))
        check("forty ledgers are made by one transaction", keeper.transact(USER, [create(ledger) for ledger in crowd]) == [])
        counts = lambda: json.loads(plumbing.http("GET", URL + "/counts"))
        def together(ledgers, number):
            """Append to each of some ledgers, all at once: what each was answered, how many rounds it took, and how long."""
            rounds, answers, begin = counts()["rounds"], [None] * len(ledgers), time.perf_counter()
            def one(index): answers[index] = keeper.transact(USER, [append(ledgers[index], number, number, number + 1, None)])
            threads = [threading.Thread(target=one, args=(index,)) for index in range(len(ledgers))]
            for thread in threads: thread.start()
            for thread in threads: thread.join()
            return answers, counts()["rounds"] - rounds, time.perf_counter() - begin
        begin = time.perf_counter()
        for ledger in crowd: assert keeper.transact(USER, [append(ledger, 0, 0, 1, None)]) == []
        alone = time.perf_counter() - begin
        answers, rounds, took = together(crowd, 1)
        check("forty appends to forty ledgers that come together are made in a few rounds", answers == [[]] * 40 and rounds < 10,
              f"{rounds} rounds in {1000 * took:.0f} ms; one after another they took {1000 * alone:.0f} ms")
        answers, rounds, took = together([40] * 40, 2)
        check("of forty appends to one ledger that come together, one is made and the rest are told they are stale",
              sorted(answers) == [[]] + [["stale"]] * 39, f"{rounds} rounds in {1000 * took:.0f} ms")

        # A computation that reads the state, and a second that reads the derived ledger the first writes.
        # Neither ledger is stored: instance 503 holds them, and answers for them.
        tally = spans_program()
        derived = [create_derived(20, TALLY), create_derived(21, TALLY), publish(tally, ROW, TALLY), publish(MANY, TALLY, TALLY),
                   define(110, tally, "state", 20), define(111, MANY, 20, 21), start(503, 110), start(503, 111)]
        check("two computations over the state are set up by one transaction", keeper.transact(USER, derived) == [])
        check("a durable ledger cannot be written from the state",
              keeper.transact(USER, [create(4, TALLY), define(112, MANY, 20, 4)]) == ["belated"])
        def follows(seconds=30):
            """Whether instance 503's derived ledger 21 comes to hold what the state says: the ledgers with two spans or more."""
            begin = time.perf_counter()
            while time.perf_counter() - begin < seconds:
                counts = {}
                for key, _ in keeper.entries().get(SPAN, []): counts[number(key[0])] = counts.get(number(key[0]), 0) + 1
                should = sorted(f"tuple(tuple(0,{ledger})) val=tuple({spans})" for ledger, spans in counts.items() if spans >= 2)
                try:
                    session = plumbing.Server(runner.port((0, 503)), start=False)
                    holds = sorted(session.ask("peek c111::out0"))
                    session.close()
                    if holds == should: return True
                except (RuntimeError, OSError): pass
                time.sleep(0.1)
            return False
        check("an instance answers for a derived ledger made from the state", follows())

        # An instance is killed. The launcher starts it again, and it begins from where its ledger stands.
        subprocess.run(["pkill", "-f", "runner.py run 0.500 "])
        for at in range(20, 30): assert write(at) == []
        took = wait(keeper, 3, 30)
        check("an instance that is killed is started again, and nothing is made twice or lost",
              agree(contents(keeper, blobs, 3), expected()), f"caught up after {took:.2f} s")

        # A computation is moved to another instance, by a transaction.
        check("a computation is moved to another instance by a transaction",
              keeper.transact(USER, [stop(501, 101), start(502, 101)]) == [])
        for at in range(30, 40): assert write(at) == []
        took = wait(keeper, 3, 40)
        check("the new instance carries on from where the ledger stood", agree(contents(keeper, blobs, 3), expected()),
              f"caught up after {took:.2f} s")

        # Compaction. Ledger 2 is read by computation 101, whose ledger stands at 40.
        updates = [[key, val, 40, count] for (key, val), count in contents(keeper, blobs, 2).items()]
        whole = blobs.put(json.dumps({"updates": updates}).encode())
        version = keeper.standing()[2][0]
        parts = [(key[1], key[2], plumbing.instant(key[3])) for key, _ in keeper.entries()[SPAN] if number(key[0]) == 2]
        early = [merge(2, 0, version, 41, whole)] + [discard(2, first, last, since) for first, last, since in parts]
        check("ledger 2 cannot be compacted past where its reader stands", keeper.transact(USER, early) == ["forgotten"])
        compact = [merge(2, 0, version, 40, whole)] + [discard(2, first, last, since) for first, last, since in parts]
        check(f"ledger 2 is compacted: {len(parts)} spans into one", keeper.transact(USER, compact) == [])
        for at in range(40, 50): assert write(at) == []
        took = wait(keeper, 3, 50)
        check("its reader carries on over the compacted ledger", agree(contents(keeper, blobs, 3), expected()), f"caught up after {took:.2f} s")

        # The reader of ledger 2 is held still while more is appended, and ledger 2 is then merged whole:
        # the appends the reader has read, with ones it has not. It cannot tell them apart in the merged span.
        # It reads the span whole, takes away what it has fed, and goes on. It is not started again.
        reader = lambda: subprocess.run(["pgrep", "-f", "runner.py run 0.502 "], capture_output=True, text=True).stdout.split()
        same = reader()
        subprocess.run(["pkill", "-STOP", "-f", "runner.py run 0.502 "])
        for at in range(50, 55): assert write(at) == []
        wait(keeper, 2, 55)
        version = keeper.standing()[2][0]
        parts = [(key[1], key[2], plumbing.instant(key[3])) for key, _ in keeper.entries()[SPAN] if number(key[0]) == 2]
        whole = [merge(2, 0, version, 50, merged(keeper, blobs, 2, 50))] + [discard(2, first, last, since) for first, last, since in parts]
        check("ledger 2 is merged whole while its reader is held still", keeper.transact(USER, whole) == [])
        subprocess.run(["pkill", "-CONT", "-f", "runner.py run 0.502 "])
        took = wait(keeper, 3, 55)
        check("the reader goes on over a merge of what it had read with what it had not, and is not started again",
              agree(contents(keeper, blobs, 3), expected()) and len(same) == 1 and reader() == same, f"caught up after {took:.2f} s")

        # A second keeper on the same register. The first makes a transaction, and the second then cannot write.
        processes["second"] = spawn(keeper_py, "serve", register, str(HTTP + 10), KEEPER_PORT="7975")
        up(f"http://127.0.0.1:{HTTP + 10}")
        assert write(55) == []
        try:
            Client(f"http://127.0.0.1:{HTTP + 10}").transact(USER, [create(9)])
            fenced = False
        except (urllib.error.URLError, ConnectionError):
            fenced = True
        check("a second keeper finds the register has moved, and does not write", fenced)
        processes.pop("second").terminate()

        # Everything is stopped, and started again from the register and the blobs alone.
        before = sorted(keeper.peek("committed::entries"))
        for process in processes.values(): process.terminate()
        for process in processes.values(): process.wait()
        subprocess.run(["pkill", "-f", "runner.py run "])
        begin = time.perf_counter()
        processes["follower"] = spawn(follower_py, "serve", register, URL, str(FOLLOW))
        ready(FOLLOWER)
        check("a new follower has the same state, from the register and the blobs, with no keeper there",
              sorted(copy.peek("state::entries")[0]) == before, f"{len(before)} entries, in {time.perf_counter() - begin:.1f} s")
        begin = time.perf_counter()
        processes["keeper"] = spawn(keeper_py, "serve", register, str(HTTP))
        up(URL)
        check("a new keeper has the same state, from the register and the blobs", sorted(keeper.peek("committed::entries")) == before,
              f"{len(before)} entries, in {time.perf_counter() - begin:.1f} s")
        processes["launcher"] = spawn(runner_py, "launch", URL, FOLLOWER)
        # Ledger 2 is waited for after each append, so that it has a span for each: the compactor, below, wants several.
        for at in range(56, 65):
            assert write(at) == []
            wait(keeper, 2, at + 1)
        took = wait(keeper, 3, 65)
        check("and the system carries on", agree(contents(keeper, blobs, 3), expected()), f"caught up after {took:.2f} s")
        # Two stewards are started: a compactor and a collector. They read the state, and act by transactions.
        # Holder 900 asks for ledger 1 from time 10, so the compactor may merge it, and only as far as 10.
        check("a holder asks for ledger 1 from time 10", keeper.transact(USER, [hold(900, 1, 10)]) == [])
        steward_py = os.path.join(HERE, "steward.py")
        processes["compactor"] = spawn(steward_py, "compact", URL, FOLLOWER)
        processes["collector"] = spawn(steward_py, "collect", URL)
        def spans_of(ledger): return [key for key, _ in keeper.entries()[SPAN] if number(key[0]) == ledger]
        check("the compactor merges ledger 1 into one span, held from 10",
              settle(lambda: [(key[1], plumbing.instant(key[3])) for key in spans_of(1)] == [(0, 10)]))
        check("ledger 2 is merged too, and ledger 3, which nothing needs, is left as it is",
              settle(lambda: len(spans_of(2)) == 1) and len(spans_of(3)) > 1)
        # The hold is released, and more is appended. The compactor then follows the reader, past 10.
        check("the hold is released", keeper.transact(USER, [f"inject(13,tuple({ident(900)},{ident(1)}))"]) == [])
        for at in range(65, 75): assert write(at) == []
        took = wait(keeper, 3, 75)
        check("ledger 1 is then held from where its reader stands",
              settle(lambda: len(spans_of(1)) == 1 and plumbing.instant(spans_of(1)[0][3]) > 10))
        check("and the ledgers hold what they should", agree(contents(keeper, blobs, 3), expected()))
        import steward
        check("the collector leaves only the blobs that spans name",
              settle(lambda: steward.listing() == {plumbing.hash_name(key[0]) for key, _ in map(plumbing.row, keeper.peek("before::blobs"))}),
              f"{len(steward.listing())} blobs in the store")
        check("the derived ledger has followed the state through all of this", follows())
        import keeper as model
        check("the follower has all of the state", settle(caught_up))
        check("the state keeps every rule, as the follower reads them",
              not any(copy.peek(*[f"views::{rule}" for rule in model.STATE_RULES])), f"{len(model.STATE_RULES)} rules")
        print(f"blobs in use: {len(keeper.peek('before::blobs'))}; transactions in the log: "
              f"{plumbing.Register(register).read()[0]['count']}", flush=True)
    finally:
        for process in processes.values(): process.terminate()
        subprocess.run(["pkill", "-f", "runner.py run "])


if __name__ == "__main__":
    main()
