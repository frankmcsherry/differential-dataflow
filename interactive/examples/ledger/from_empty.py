#!/usr/bin/env python3
"""A system built up from nothing, one transaction at a time, with what each transaction caused.

Before the first transaction there is a keeper, a follower and a launcher, and the state has no entries.
Everything after that is a user, transacting. After each step this prints the state's size, the computations
no instance runs, the instances that are running here, and where each ledger stands.

It needs what live.py needs. Run from the repository root:
  python3 interactive/examples/ledger/from_empty.py
"""
import json, os, subprocess, sys, time
import plumbing, runner
import keeper as model
from plumbing import Client, Blobs, SPAN, number
from live import (URL, HTTP, FOLLOW, FOLLOWER, USER, COUNT, OFTEN, BOTH, TALLY, ROW, spans_program, create, create_derived, append,
                  publish, define, start, stop, spawn, up, ready, settle, contents, wait, batch)

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    register = f"from-empty-{int(time.time())}"
    processes = {"keeper": spawn(os.path.join(HERE, "keeper.py"), "serve", register, str(HTTP))}
    try:
        up(URL)
        processes["follower"] = spawn(os.path.join(HERE, "follower.py"), "serve", register, URL, str(FOLLOW))
        ready(FOLLOWER)
        processes["launcher"] = spawn(os.path.join(HERE, "runner.py"), "launch", URL, FOLLOWER)
        keeper, copy, blobs = Client(URL), plumbing.Follower(FOLLOWER), Blobs()

        def instances():
            found = subprocess.run(["pgrep", "-fl", "runner.py run "], capture_output=True, text=True).stdout.split("\n")
            return sorted(line.split("runner.py run ")[1].split()[0] for line in found if "runner.py run " in line)

        def report(label, refused=None, took=None):
            entries = keeper.peek("committed::entries")
            idle = sorted(number(plumbing.row(line)[0][0]) for line in keeper.peek("before::idle"))
            stands = ", ".join(f"{ledger} at {at}" for ledger, (_, at) in sorted(keeper.standing().items()))
            print(f"\n{label}" + (f"  [{1000 * took:.0f} ms to transact]" if took is not None else ""))
            if refused: print(f"  REFUSED: {refused}")
            print(f"  state: {len(entries)} entries, {plumbing.Register(register).read()[0]['count'] if entries else 0} records in the log")
            print(f"  computations no instance runs: {idle or 'none'}")
            print(f"  instances running: {instances() or 'none'}")
            print(f"  durable ledgers: {stands or 'none'}", flush=True)

        def transact(commands):
            """Make one transaction, as a user. Returns how long it took."""
            begin = time.perf_counter()
            refused = keeper.transact(USER, commands)
            took = time.perf_counter() - begin
            assert not refused, refused
            return took

        def reach(ledger, until):
            took = wait(keeper, ledger, until)
            print(f"  ledger {ledger} stood at {until} after {took:.2f} s, and holds {len(contents(keeper, blobs, ledger))} rows", flush=True)

        words = {}
        def write(first, last):
            """Append lines of words to ledger 1: at time t, line t holds the words t % 5, t % 7 and 100 + t."""
            begin = time.perf_counter()
            for at in range(first, last):
                line = sorted({at % 5, at % 7, 100 + at})
                for word in line: words[word] = words.get(word, 0) + 1
                blob = batch(blobs, [((word,), (at,), 1, at) for word in line])
                assert keeper.transact(USER, [append(1, at, at, at + 1, blob)]) == []
            return time.perf_counter() - begin

        report("0. A keeper, a follower and a launcher are up, on an empty register.")

        took = transact([create(1)])
        report("1. Create ledger 1.", took=took)

        took = write(0, 5)
        report("2. Append five batches to ledger 1, one transaction each.", took=took)

        took = transact([publish(COUNT)])
        report("3. Publish a program: how many lines each word is in.", took=took)

        took = transact([create(2), define(100, COUNT, 1, 2)])
        report("4. Create ledger 2, and define computation 100: the program, from ledger 1 to ledger 2.", took=took)

        took = transact([start(500, 100)])
        settle(lambda: instances() == ["0.500"])
        reach(2, 5)
        report("5. Start computation 100 on instance 0.500.", took=took)
        assert contents(keeper, blobs, 2) == {((w,), (n,)): 1 for w, n in words.items()}

        took = transact([publish(OFTEN), create(3), define(101, OFTEN, 2, 3), start(501, 101)])
        settle(lambda: instances() == ["0.500", "0.501"])
        reach(3, 5)
        report("6. One transaction: publish a second program, create ledger 3, define computation 101 from ledger 2 to 3, start it on 0.501.", took=took)

        took = write(5, 10)
        reach(3, 10)
        report("7. Append five more batches to ledger 1.", took=took)
        often = lambda: {((w,), (n,)): 1 for w, n in words.items() if n >= 2}
        assert contents(keeper, blobs, 3) == often()

        took = transact([create(4), publish(BOTH, [ROW, ROW]), define(102, BOTH, [1, 3], 4), start(501, 102)])
        reach(4, 10)
        report("8. One transaction: a third program, ledger 4, and computation 102 from ledgers 1 and 3 to 4, on the instance that is there.", took=took)

        tally = spans_program()
        took = transact([create_derived(20, TALLY), publish(tally, ROW, TALLY), define(110, tally, "state", 20), start(502, 110)])
        settle(lambda: instances() == ["0.500", "0.501", "0.502"])
        def derived():
            try:
                session = plumbing.Server(runner.port((0, 502)), start=False)
                rows = sorted(session.ask("peek c110::out0")); session.close()
                return rows
            except (RuntimeError, OSError): return None
        counts = {}
        for key, _ in keeper.entries().get(SPAN, []): counts[number(key[0])] = counts.get(number(key[0]), 0) + 1
        should = sorted(f"tuple(tuple(0,{ledger})) val=tuple({spans})" for ledger, spans in counts.items())
        begin = time.perf_counter()
        assert settle(lambda: derived() == should)
        print(f"  instance 0.502 holds derived ledger 20 after {time.perf_counter() - begin:.2f} s: " +
              ", ".join(f"ledger {ledger} has {spans} spans" for ledger, spans in sorted(counts.items())))
        report("9. One transaction: a derived ledger 20, a program over the state, computation 110, on a new instance 0.502.", took=took)

        took = transact([stop(500, 100), start(501, 100)])
        settle(lambda: instances() == ["0.501", "0.502"])
        write(10, 15)
        reach(3, 15); reach(4, 15)
        report("10. Move computation 100 from 0.500 to 0.501 by one transaction, then append five more batches.", took=took)
        assert contents(keeper, blobs, 3) == often()
        both = dict(contents(keeper, blobs, 1))
        for row, count in contents(keeper, blobs, 3).items(): both[row] = both.get(row, 0) + count
        assert contents(keeper, blobs, 4) == both

        broken = [rule for rule, rows in zip(model.STATE_RULES, copy.peek(*[f"views::{rule}" for rule in model.STATE_RULES])) if rows]
        print(f"\nRules that are broken, of {len(model.STATE_RULES)}, as the follower reads them: {broken or 'none'}")
        print(f"Blobs that spans name: {len(keeper.peek('before::blobs'))}", flush=True)
    finally:
        for process in processes.values(): process.terminate()
        subprocess.run(["pkill", "-f", "runner.py run "])


if __name__ == "__main__":
    main()
