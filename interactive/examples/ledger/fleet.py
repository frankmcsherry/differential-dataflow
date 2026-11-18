#!/usr/bin/env python3
"""The system of live.py, across four machines. What runs where is decided by transactions against the state.

This machine, number 3, has the keeper and the blob store. Machines 0, 1 and 2 are reached by SSH.
  the register   one etcd of three members, on machines 0, 1 and 2
  the blobs      SeaweedFS on this machine
  the keeper     on this machine
  a follower     on each of the four machines: a copy of the state, for those on that machine who read
  a launcher     on each of the four machines: it starts the instances whose first number is its machine's
A launcher and its instances read from the follower on their own machine, and write to the keeper.
A follower on another machine reads the log from that machine's own member of the register.

An instance is named `(machine, number)`, so to start a computation on a machine is one command, `Start`,
and to move it to another is `Stop` and `Start` in one transaction.

A machine's launcher runs under an SSH session that is held open from here. The session carries two ports back
to this machine, the keeper's and the blob store's, so both are bound only to 127.0.0.1 on every machine.
When the session ends, the launcher and its instances end with it: this is how a machine is lost, below.

Before the first run the programs and the server binary are copied to `~/ledger-spike-rows` on each machine.

Run from the repository root:
  python3 interactive/examples/ledger/fleet.py
"""
import json, os, subprocess, sys, time, urllib.error
import plumbing
from plumbing import Client, Blobs, SPAN, number
from live import (URL, HTTP, FOLLOW, FOLLOWER, AUDIT, AUDITOR, USER, COUNT, OFTEN, TALLY, ROW, MANY, spans_program, create, create_derived, append,
                  publish, define, start, stop, hold, check, spawn, up, ready, contents, agree, wait, batch)

HERE = os.path.dirname(os.path.abspath(__file__))
DIR = "interactive/examples/ledger"
# Each other machine: its name, and its address on the link from this one.
MACHINES = {0: ("mini-00", "10.0.1.1"), 1: ("mini-01", "10.0.5.1"), 2: ("mini-02", "10.0.6.1")}
HOME = 3
REMOTE = "ledger-spike-rows"      # this tree's own folder on the other machines: the worktree's deployment is left alone
ETCD = ",".join(f"http://{address}:2379" for _, address in MACHINES.values())
FILES = [f"{DIR}/{name}" for name in ("plumbing.py", "keeper.py", "follower.py", "runner.py", "steward.py",
                                      "commands.ddp", "system.ddp", "bundle.ddp", "instance.ddp", "compact.ddp", "needs.ddp", "own.ddp")] + ["target/release/ddir_server"]


def ssh(machine, *options):
    name, address = MACHINES[machine]
    return ["ssh", "-o", "BatchMode=yes", "-o", f"HostKeyAlias={name}", *options, address]


def deploy(machine):
    """Copy what an instance needs to a machine."""
    name, address = MACHINES[machine]
    shell = f"ssh -o BatchMode=yes -o HostKeyAlias={name}"
    subprocess.run(["rsync", "-aRL", "-e", shell, *FILES, f"{address}:{REMOTE}/"], cwd=plumbing.ROOT, check=True)


def session(machine, command, carry=True):
    """Run a command on a machine, under a session that ends it when the session ends.
    With `carry`, the session also carries the keeper's port and the blob store's back to here, for all on that machine.
    The command is told where that machine's own member of the register is."""
    carried = ["-R", f"{HTTP}:127.0.0.1:{HTTP}", "-R", "8888:127.0.0.1:8888", "-o", "ExitOnForwardFailure=yes"] if carry else []
    member = (f"LEDGER_ETCD=http://{MACHINES[machine][1]}:2379 "
              f"LEDGER_LOG_SPACE={plumbing.SPACES['log']} LEDGER_BLOB_SPACE={plumbing.SPACES['blobs']}")
    return subprocess.Popen(ssh(machine, "-tt", *carried) + [f"cd {REMOTE} && {member} exec python3 {DIR}/{command}"],
                            stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)


def on(machine, command):
    """Run a short command on a machine, and return what it prints."""
    return subprocess.run(ssh(machine) + [command], capture_output=True, text=True).stdout


def instances(machine):
    """The instances that are running on a machine, as the machine itself says."""
    listing = on(machine, "pgrep -fl 'runner[.]py run '") if machine != HOME else \
              subprocess.run(["pgrep", "-fl", "runner[.]py run "], capture_output=True, text=True).stdout
    return sorted(line.split("runner.py run ")[1].split()[0] for line in listing.splitlines() if "runner.py run " in line)


def copy_on(machine):
    """The entries that a machine's follower holds, sorted, or None if it does not answer."""
    if machine == HOME:
        try: return sorted(plumbing.Follower(FOLLOWER).peek("state::entries")[0])
        except OSError: return None
    answer = on(machine, f"cd {REMOTE} && python3 {DIR}/follower.py peek {FOLLOWER} state::entries 2>/dev/null")
    return sorted(json.loads(answer)[0]) if answer else None


def settle(done, seconds=30):
    begin = time.perf_counter()
    while not done():
        if time.perf_counter() - begin > seconds: return False
        time.sleep(0.1)
    return True


def main():
    for machine in MACHINES: deploy(machine)
    register = f"ledger-fleet-{int(time.time())}"
    keeper_py, runner_py, steward_py, follower_py = (os.path.join(HERE, name) for name in ("keeper.py", "runner.py", "steward.py", "follower.py"))
    processes = {"keeper": spawn(keeper_py, "serve", register, str(HTTP), LEDGER_ETCD=ETCD)}
    try:
        up(URL)
        processes["follower 3"] = spawn(follower_py, "serve", register, URL, str(FOLLOW), LEDGER_ETCD=ETCD)
        ready(FOLLOWER)
        processes["launcher 3"] = spawn(runner_py, "launch", URL, FOLLOWER, str(HOME))
        # A machine's launcher is started first: its session carries the ports that the machine's follower needs too.
        for machine in MACHINES:
            processes[f"launcher {machine}"] = session(machine, f"runner.py launch {URL} {FOLLOWER} {machine}")
            processes[f"follower {machine}"] = session(machine, f"follower.py serve {register} {URL} {FOLLOW}", carry=False)
        # The keeper holds no spans: what is asked of them here is asked of this machine's follower, once it has all the keeper made.
        keeper, blobs = Client(URL, FOLLOWER), Blobs()
        # One transaction sets the system up, and says where each computation runs: 100 on machine 0, 101 on machine 1.
        setup = [create(1), create(2), create(3), publish(COUNT), publish(OFTEN),
                 define(100, COUNT, 1, 2), define(101, OFTEN, 2, 3), start((0, 500), 100), start((1, 501), 101)]
        check("the system is set up by one transaction", keeper.transact(USER, setup) == [])
        heads = []
        for member in ETCD.split(","):
            plumbing.ETCD = [member]
            heads.append(plumbing.Register(register).read()[0])
        plumbing.ETCD = ETCD.split(",")
        check("each of the three members of the register, on three machines, has the log's head",
              heads[0]["count"] == 1 and heads.count(heads[0]) == 3)

        words, expected = {}, lambda: {((w,), (n,)): 1 for w, n in words.items() if n >= 2}
        def write(at):
            line = sorted({at % 5, at % 7, 100 + at})
            for word in line: words[word] = words.get(word, 0) + 1
            blob = batch(blobs, [((word,), (at,), 1, at) for word in line])
            return keeper.transact(USER, [append(1, at, at, at + 1, blob)])
        def carry(first, last):
            """Append lines `first` to `last`, wait for ledger 3 to follow, and say whether it holds what it should."""
            for at in range(first, last): assert write(at) == []
            took = wait(keeper, 3, last, seconds=60)
            return agree(contents(keeper, blobs, 3), expected()), f"ledger 3 stood at {last} after {took:.2f} s"

        check("instances on machines 0 and 1 carry 20 appends through two computations", *carry(0, 20))
        where = {machine: instances(machine) for machine in (0, 1, 2, HOME)}
        check("each machine runs the instances the state gives it, and no others",
              where == {0: ["0.500"], 1: ["1.501"], 2: [], HOME: []}, str(where))
        def copies(machines):
            """Whether the followers on some machines all hold one state, which has every record the keeper made and
            the keeper's configuration, while it is not changing. The spans are the followers' alone."""
            at, (home,) = plumbing.Follower(FOLLOWER).peek_at("state::entries")
            whole = sorted(home)
            return (at == keeper.length() and all(copy_on(machine) == whole for machine in machines)
                    and sorted(line for line in whole if not line.startswith("tuple(inject(1,")) == sorted(keeper.peek("state::entries")))
        check("the follower on each of the four machines holds the state the keeper has", settle(lambda: copies((0, 1, 2, HOME))))

        # A computation is moved from machine 1 to machine 2, by a transaction.
        check("a computation is moved to another machine by a transaction",
              keeper.transact(USER, [stop((1, 501), 101), start((2, 502), 101)]) == [])
        check("the instance on machine 2 carries on from where the ledger stood", *carry(20, 30))
        check("machine 1 has stopped its instance, and machine 2 has started one",
              settle(lambda: instances(1) == [] and instances(2) == ["2.502"]))

        # An instance on machine 0 is killed. That machine's launcher starts it again.
        on(0, "pkill -f 'runner[.]py run 0[.]500 '")
        check("an instance that is killed on machine 0 is started again there", *carry(30, 40))

        # The follower on machine 0 is stopped. The instance there hears nothing more, and does nothing, and is not stopped.
        # A new follower is started. It reads the log, and the instance asks it, and goes on from where it was.
        same = on(0, "pgrep -f 'runner[.]py run 0[.]500 '")
        processes.pop("follower 0").terminate()
        check("the follower on machine 0 is stopped", settle(lambda: copy_on(0) is None))
        for at in range(40, 45): assert write(at) == []
        check("ledger 2 waits while its instance hears nothing",
              not settle(lambda: keeper.standing()[2][1] != 40, seconds=1) and keeper.standing()[1][1] == 45)
        processes["follower 0"] = session(0, f"follower.py serve {register} {URL} {FOLLOW}", carry=False)
        check("a new follower on machine 0 reads the log, and the instance there goes on", *carry(45, 50))
        check("and the instance was not started again", bool(same) and on(0, "pgrep -f 'runner[.]py run 0[.]500 '") == same)

        # Machine 2 is lost: its sessions end, and with them the follower, the launcher and the instance.
        # Nothing notices on its own. A user moves the computation to this machine, by a transaction.
        processes.pop("launcher 2").terminate()
        processes.pop("follower 2").terminate()
        check("machine 2 is lost, and its instance with it", settle(lambda: instances(2) == []))
        for at in range(50, 55): assert write(at) == []
        check("ledger 3 waits while its computation has nowhere to run",
              settle(lambda: keeper.standing()[2][1] == 55) and keeper.standing()[3][1] == 50)
        check("the computation is moved to this machine by a transaction",
              keeper.transact(USER, [stop((2, 502), 101), start((HOME, 503), 101)]) == [])
        check("and it catches up from where the ledger stood", *carry(55, 60))

        # Two computations over the state, on machine 1. Their ledgers are derived: the instance holds them, and answers.
        tally = spans_program()
        derived = [create_derived(20, TALLY), create_derived(21, TALLY), publish(tally, ROW, TALLY), publish(MANY, TALLY, TALLY),
                   define(110, tally, "state", 20), define(111, MANY, 20, 21), start((1, 510), 110), start((1, 510), 111)]
        check("two computations over the state are started on machine 1 by one transaction", keeper.transact(USER, derived) == [])
        def follows():
            counts = {}
            for key, _ in keeper.entries().get(SPAN, []): counts[number(key[0])] = counts.get(number(key[0]), 0) + 1
            should = sorted(f"tuple(tuple(0,{ledger})) val=tuple({spans})" for ledger, spans in counts.items() if spans >= 2)
            answer = on(1, f"cd {REMOTE} && python3 {DIR}/runner.py ask 1.510 'peek c111::out0' 2>/dev/null")
            return bool(answer) and sorted(json.loads(answer)) == should
        check("the instance on machine 1 answers for a derived ledger made from the state", settle(follows))

        # A compactor on machine 0, and a collector here. Each reads the state, and acts by transactions.
        processes["compactor"] = session(0, f"steward.py compact {URL} {FOLLOWER}", carry=False)
        # The collector reads a follower here that keeps the rules in full, and deletes only what is unnamed in a state they pass.
        processes["auditor"] = spawn(follower_py, "serve", register, URL, str(AUDIT), "audit", LEDGER_ETCD=ETCD)
        ready(AUDITOR)
        auditor = plumbing.Follower(AUDITOR)
        processes["collector"] = spawn(steward_py, "collect", URL, AUDITOR, LEDGER_ETCD=ETCD)
        def spans_of(ledger): return [key for key, _ in keeper.entries()[SPAN] if number(key[0]) == ledger]
        tidied = lambda: not plumbing.Follower(FOLLOWER).rows("tidy::merge")[0]
        check("a compactor on machine 0 merges ledgers 1 and 2 into a few spans each",
              settle(lambda: tidied() and len(spans_of(1)) <= 6 and len(spans_of(2)) <= 6), f"{len(spans_of(1))} and {len(spans_of(2))} spans")
        check("and the computations carry on over the merged ledgers", *carry(60, 70))
        check("the derived ledger on machine 1 has followed the state", settle(follows))

        # A second keeper on the same register. The first makes a transaction, and the second then cannot write.
        processes["second"] = spawn(keeper_py, "serve", register, str(HTTP + 10), KEEPER_PORT="7975", LEDGER_ETCD=ETCD)
        up(f"http://127.0.0.1:{HTTP + 10}")
        assert write(70) == []
        try:
            Client(f"http://127.0.0.1:{HTTP + 10}").transact(USER, [create(9)])
            fenced = False
        except (urllib.error.URLError, ConnectionError):
            fenced = True
        check("a second keeper finds the register has moved, and does not write", fenced)
        processes.pop("second").terminate()

        # The keeper is stopped, and started again from the register and the blobs. The other machines are not told.
        before = (sorted(keeper.peek("state::entries")), sorted(keeper.peek("rows::own")))
        keeper_process = processes.pop("keeper")
        keeper_process.terminate(); keeper_process.wait()
        begin = time.perf_counter()
        processes["keeper"] = spawn(keeper_py, "serve", register, str(HTTP), LEDGER_ETCD=ETCD)
        up(URL)
        check("a new keeper has the same configuration and the same row for each ledger, from the register and the blobs",
              (sorted(keeper.peek("state::entries")), sorted(keeper.peek("rows::own"))) == before,
              f"{len(before[0])} entries and {len(before[1])} rows, in {time.perf_counter() - begin:.1f} s")
        check("and the instances and followers on the other machines carry on, having waited for it", *carry(71, 80))
        check("the followers that are left hold the state the keeper has", settle(lambda: copies((0, 1, HOME))))
        where = {machine: instances(machine) for machine in (0, 1, 2, HOME)}
        check("each machine runs what the state gives it", where == {0: ["0.500"], 1: ["1.510"], 2: [], HOME: ["3.503"]}, str(where))
        import keeper as model
        check("the state keeps every rule of the model in full, as the follower here that audits reads them",
              settle(lambda: auditor.where()["at"] == keeper.length()) and not any(auditor.peek(*[f"views::{rule}" for rule in model.STATE_RULES])),
              f"{len(model.STATE_RULES)} rules")
        print(f"blobs in use: {len(auditor.peek('views::blobs')[0])}; transactions in the log: "
              f"{plumbing.Register(register).read()[0]['count']}", flush=True)
    finally:
        for process in processes.values(): process.terminate()
        for process in processes.values(): process.wait()
        subprocess.run(["pkill", "-f", "runner.py run "])
        for machine in MACHINES: print(f"left on machine {machine}: {instances(machine)}", flush=True)


if __name__ == "__main__":
    main()
