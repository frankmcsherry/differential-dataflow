#!/usr/bin/env python3
"""The system of ledgers, running: across four machines, with the data and the configuration both churning, until stopped.

  python3 interactive/examples/ledger/steady.py [computations] [ticks a second]   start it, and leave it running
  python3 interactive/examples/ledger/steady.py lose <machine>                    machine 0, 1 or 2 goes away
  python3 interactive/examples/ledger/steady.py restore <machine>                 it comes back
  python3 interactive/examples/ledger/steady.py stop                              everything stops
A page to watch beside it, on this machine:
  python3 interactive/examples/ledger/watch.py steady

It needs what fleet.py needs: one etcd of three members on machines 0, 1 and 2, the blob store on this machine, and
the stock server built. Run it from the repository root (the scratch tree's).

What runs. This machine, number 3, has the keeper, three followers (one that tells instances what to run, one that keeps
the compaction policy, one that keeps every rule of the model), a compactor, a collector, a steward that moves each
ledger's own read hold, a steward that has the blob store give back what was deleted, and a launcher. Machines 0, 1 and
2 each have a follower that tells their instances what to run, and a launcher, both under an SSH session held from here.
Each machine has one instance, (machine, 600). Nothing is told what to do except by the state.

What churns. A user appends one span of rows to every source ledger each tick, and takes back the rows it put in
`CHURN` ticks before. Of the computations, `FIXED` are fixed for the whole run, and the rest are units, each with a
source ledger of its own: a single (COUNT, from the source to a ledger) or a pair (COUNT to a ledger A, and OFTEN from
A to a ledger B). A unit lives `LIFE` seconds: every `LIFE / units` seconds the oldest is retired, by stopping and
retiring its computations, and a new one of its kind is made in the same transaction, on the next machine in turn.
`GRACE` seconds later the old unit's ledgers are dropped. So computations and ledgers come and go all the time.

A machine that is lost (`lose`) has its sessions ended, so its follower, its launcher and its instance end. Its
computations then stand still, and `PAUSE` seconds later one transaction starts each of them on another machine. New
units are not placed on it until it is restored. A machine that is restored has its sessions started again, and new
units go to it from then on, so within a unit's life its share comes back.

This process answers on 127.0.0.1:8921: /status (what the page shows of the run), /lose/<m>, /restore/<m>, /stop.
"""
import json, os, signal, statistics, subprocess, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import plumbing
from plumbing import Client, Blobs
from live import (URL, HTTP, FOLLOW, FOLLOWER, FULL, USER, COUNT, OFTEN, ROW, ident, shape, frontier, publish, define,
                  start, stop as stopping, retire, spawn, up)
from fleet import MACHINES, HOME, ETCD, deploy, session

HERE = os.path.dirname(os.path.abspath(__file__))
CONTROL = 8921
FIXED = int(os.environ.get("STEADY_FIXED", "20"))        # computations fixed for the whole run
LIFE = float(os.environ.get("STEADY_LIFE", "60"))        # seconds a unit lives
GRACE = float(os.environ.get("STEADY_GRACE", "5"))       # seconds between retiring a unit and dropping its ledgers
CHURN = int(os.environ.get("STEADY_CHURN", "50"))        # a row is taken back this many ticks after it went in
PAUSE = float(os.environ.get("STEADY_PAUSE", "8"))       # seconds between losing a machine and moving its computations
ROWS = 10                                                # rows a span
SOURCE, OUTPUT, COMPUTATION = 100000, 200000, 300000
ROLES = {"serve": FOLLOW, "tidy": 8000, "audit": 8010}   # the followers on this machine, by role, and their ports
INSTANCE = 600                                           # each machine's one instance is (machine, 600)


def create_at(ledger, at):
    return f"inject(0,tuple({ident(ledger)},{shape(ROW)},1,{frontier(at)},inject(0,tuple()),inject(0,tuple())))"
def drop(ledger): return f"inject(4,{ident(ledger)})"


class Run:
    """What the driver knows of the run: what it made, where each computation was placed, and what happened when."""
    def __init__(self, count, rate):
        self.count, self.rate, self.begun = count, rate, time.time()
        self.lock = threading.Lock()
        self.config = {"computations created": 0, "computations retired": 0, "ledgers created": 0, "ledgers dropped": 0,
                       "transactions": 0, "refused": 0, "holds moved": 0, "passes": 0}
        self.placed = {}                    # computation -> instance
        self.live = {0: True, 1: True, 2: True, HOME: True}
        self.events = []                    # (time, what happened)
        self.due, self.done = {}, {}        # each tick's due time, and when every output had it
        self.actions = []                   # what /lose and /restore asked for, for the churn to do in turn
        self.stop = threading.Event()

    def event(self, text):
        with self.lock: self.events.append((time.time(), text))
        print(f"{time.strftime('%H:%M:%S')} {text}", flush=True)

    def status(self):
        now = time.time()
        with self.lock:
            # The delay of the ticks due in each 5 s of the last 10 minutes: median and nine in ten, in ms.
            buckets = {}
            for t, when in self.due.items():
                if now - when > 600 or t not in self.done: continue
                buckets.setdefault(int((when - self.begun) // 5), []).append(1000 * (self.done[t] - when))
            delays = []
            for bucket in sorted(buckets):
                ms = sorted(buckets[bucket])
                delays.append([round(self.begun + 5 * bucket - now), round(statistics.median(ms)), round(ms[min(len(ms) - 1, int(0.9 * len(ms)))])])
            late = sum(1 for t, when in self.due.items() if t not in self.done and now - when > 5)
            on = {}
            for computation, instance in self.placed.items(): on[instance[0]] = on.get(instance[0], 0) + 1
            return {"mode": "steady", "running for": round(now - self.begun), "computations": self.count, "rate": self.rate,
                    "ticks sent": len(self.due), "ticks done": len(self.done), "ticks late": late,
                    "delays": delays, "config": dict(self.config), "machines": {str(m): "here" if up else "lost" for m, up in self.live.items()},
                    "placed": {str(m): on.get(m, 0) for m in self.live},
                    "events": [[time.strftime("%H:%M:%S", time.localtime(at)), text] for at, text in self.events[-8:]]}


def main(count, rate):
    for machine in MACHINES: deploy(machine)
    plumbing.ETCD = ETCD.split(",")         # the register is the one etcd of three members
    register = f"ledger-steady-{int(time.time())}"
    run = Run(count, rate)
    path = lambda name: os.path.join(HERE, name)
    running, sessions = {}, {}

    def hear(name, stream, each=None):
        # A part's lines go to this process's output, and some are counted.
        for line in stream:
            if each: each(line)
            if "Traceback" in line or "Error" in line or "stopping" in line: print(f"  [{name}] {line.rstrip()[:300]}", flush=True)

    def part(name, *args, each=None, **env):
        process = subprocess.Popen([sys.executable, "-u", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                   cwd=plumbing.ROOT, env=dict(os.environ, LEDGER_ETCD=ETCD, **env))
        threading.Thread(target=hear, args=(name, process.stdout, each), daemon=True).start()
        running[name] = process
        return process

    def open_machine(machine):
        # A machine's launcher is started first: its session carries the ports that the machine's follower needs too.
        sessions[machine] = [session(machine, f"runner.py launch {URL} {FOLLOWER} {machine}"),
                             session(machine, f"follower.py serve {register} {URL} {FOLLOW} serve", carry=False)]

    def close_machine(machine):
        # Ending a session ends what runs under it on that machine: the launcher stops its instances as it goes.
        gone = sessions.pop(machine, [])
        for process in gone: process.terminate()
        for process in gone:
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill()
        # An instance that was starting as its launcher went may outlive it: it is ended by name, as fleet.py does.
        subprocess.run(["ssh", "-o", "ConnectTimeout=5", MACHINES[machine][0], f"pkill -f 'runner[.]py run {machine}[.]{INSTANCE} '"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)

    churning = None
    try:
        part("keeper", path("keeper.py"), "serve", register, str(HTTP))
        up(URL)
        for role, port in ROLES.items():
            env = {"FOLLOWER_RULES": FULL} if role == "audit" else {"FOLLOWER_WORKERS": "2"} if role == "tidy" else {}
            part(f"follower ({role})", path("follower.py"), "serve", register, URL, str(port), role, **env)
        for role, port in ROLES.items():
            for _ in range(600):
                try: plumbing.Follower(f"127.0.0.1:{port}").where(); break
                except (OSError, ValueError): time.sleep(0.05)
        part("launcher 3", path("runner.py"), "launch", URL, FOLLOWER, str(HOME))
        for machine in MACHINES: open_machine(machine)
        keeper, blobs = Client(URL), Blobs()

        def made(commands):
            refused = keeper.transact(USER, commands)
            assert refused == [], refused

        # The fixed computations, spread over the four machines, and the units.
        machines = [0, 1, 2, HOME]
        turn = [0]
        def instance():
            with run.lock: here = [m for m in machines if run.live[m]]
            turn[0] += 1
            return (here[turn[0] % len(here)], INSTANCE)
        ks = list(range(FIXED))
        made([publish(COUNT), publish(OFTEN)])
        made([create_at(SOURCE + k, 0) for k in ks] + [create_at(OUTPUT + k, 0) for k in ks]
             + [define(COMPUTATION + k, COUNT, SOURCE + k, OUTPUT + k) for k in ks])
        for k in ks: run.placed[COMPUTATION + k] = instance()
        made([start(run.placed[COMPUTATION + k], COMPUTATION + k) for k in ks])
        pairs = (count - FIXED) // 3
        singles = count - FIXED - 2 * pairs
        ids = {"ledger": 1000000, "computation": 2000000}
        def fresh(kind):
            ids[kind] += 1
            return ids[kind]
        def make(kind, at):
            unit = {"kind": kind, "source": fresh("ledger"), "a": fresh("ledger"), "A": fresh("computation"), "on": {}}
            commands = [create_at(unit["source"], at), create_at(unit["a"], at), define(unit["A"], COUNT, unit["source"], unit["a"])]
            unit["on"][unit["A"]] = instance()
            if kind == "pair":
                unit["b"], unit["B"] = fresh("ledger"), fresh("computation")
                commands += [create_at(unit["b"], at), define(unit["B"], OFTEN, unit["a"], unit["b"])]
                unit["on"][unit["B"]] = instance()
            commands += [start(where, computation) for computation, where in unit["on"].items()]
            return unit, commands
        units = [make(kind, 0) for kind in ["pair"] * pairs + ["single"] * singles]
        for at in range(0, len(units), 20): made([command for _, commands in units[at:at + 20] for command in commands])
        units = [unit for unit, _ in units]
        for unit in units: run.placed.update(unit["on"])
        run.config["computations created"] = FIXED + sum(len(unit["on"]) for unit in units)
        run.config["ledgers created"] = 2 * FIXED + sum(3 if unit["kind"] == "pair" else 2 for unit in units)

        # The stewards. The compactor's passes and the hold steward's moves are counted from what they say.
        def counted(key, mark):
            def each(line):
                if mark in line:
                    with run.lock:
                        if key == "holds moved": run.config[key] += int(line.split("moved ")[1].split()[0])
                        else: run.config[key] += 1
            return each
        part("compactor", path("steward.py"), "compact", URL, f"127.0.0.1:{ROLES['tidy']}", each=counted("passes", "spans into"))
        part("collector", path("steward.py"), "collect", URL, f"127.0.0.1:{ROLES['audit']}")
        part("hold steward", path("steward.py"), "hold", URL, f"127.0.0.1:{ROLES['tidy']}", each=counted("holds moved", "steward: moved"))
        part("vacuum", path("steward.py"), "vacuum", "300")
        run.event(f"up: register {register}; {count} computations on four machines; {rate:g} ticks a second")

        # The ticks. A source's place is the number of its next append, where it begins, and the tick of its first.
        spot = lambda at: {"at": at, "number": 0, "lower": at, "first": None}
        active = {SOURCE + k: spot(0) for k in ks}
        outputs = {OUTPUT + k: 0 for k in ks}
        for unit in units:
            active[unit["source"]] = spot(0)
            outputs[unit["a"]] = 0
            if unit["kind"] == "pair": outputs[unit["b"]] = 0
        sending = [0]

        def watch():
            # The log is read as it is written, from the register and the blobs: this asks nothing of the keeper.
            # What a round changed in the keeper's rows says where each ledger it appended to now stands.
            import re
            cell, log, read, stood, next_tick = plumbing.Register(register), Blobs("log"), 0, {}, 0
            prev = re.compile(rb'"prev": "?(\w+)"?')
            while not run.stop.is_set():
                try:
                    state, _ = cell.read()
                    begin = max(read, state.get("from", 0)) if state else read
                    fresh_records, name = [], state and state["head"]
                    for _ in range(state["count"] - begin if state else 0):
                        blob = log.get(name)
                        fresh_records.append(plumbing.unpack(blob))
                        name = prev.search(blob.split(b"\n", 1)[0]).group(1).decode()
                except (OSError, ValueError, AttributeError): time.sleep(0.2); continue
                for record, frames in reversed(fresh_records):
                    if "round" in record:
                        for ledger, upper in plumbing.round_uppers(record, frames): stood[ledger] = max(stood.get(ledger, 0), upper)
                read = state["count"] if state else read
                if fresh_records:
                    with run.lock:
                        waited = [(ledger, at) for ledger, at in outputs.items() if at <= next_tick]
                        least = min((stood.get(ledger, at) for ledger, at in waited), default=next_tick + 1)
                        now = time.time()
                        while next_tick < least and next_tick in run.due:
                            run.done[next_tick] = now
                            next_tick += 1
                        for old in [t for t in run.due if t < next_tick - 6000]:
                            run.due.pop(old, None); run.done.pop(old, None)
                time.sleep(0.02)
        threading.Thread(target=watch, daemon=True).start()

        def tick(t, when):
            put = [((word % 7,), (1000 * t + word,), 1, t) for word in range(ROWS)]
            back = [((word % 7,), (1000 * (t - CHURN) + word,), -1, t) for word in range(ROWS)]
            young = ("inject", 2, plumbing.hash_ints(blobs.put(plumbing.fresh([plumbing.encode(ROW, put)]))))
            old = ("inject", 2, plumbing.hash_ints(blobs.put(plumbing.fresh([plumbing.encode(ROW, put + back)]))))
            with run.lock:
                run.due[t] = when
                sending[0] = t + 1
                ledgers = sorted((ledger, dict(where)) for ledger, where in active.items() if where["at"] <= t)
            refused = keeper.append([(at, (0, ledger), where["number"], [[where["lower"]]], [[t + 1]],
                                      old if where["first"] is not None and t - CHURN >= where["first"] else young)
                                     for at, (ledger, where) in enumerate(ledgers)])
            with run.lock:
                for at, (ledger, where) in enumerate(ledgers):
                    if at in refused or ledger not in active: continue
                    active[ledger].update(number=where["number"] + 1, lower=t + 1, first=t if where["first"] is None else where["first"])

        def asked(commands):
            began = time.perf_counter()
            refused = keeper.transact(USER, commands)
            with run.lock:
                run.config["transactions"] += 1
                if refused: run.config["refused"] += 1
            return refused, 1000 * (time.perf_counter() - began)

        def lose(machine):
            close_machine(machine)
            with run.lock: run.live[machine] = False
            run.event(f"machine {machine} is lost: its follower, launcher and instance are gone")
            time.sleep(PAUSE)
            with run.lock: theirs = sorted(c for c, where in run.placed.items() if where[0] == machine)
            moves = {computation: instance() for computation in theirs}
            commands = [command for computation, where in moves.items()
                        for command in (stopping((machine, INSTANCE), computation), start(where, computation))]
            refused, ms = asked(commands) if commands else ([], 0)
            if refused: run.event(f"moving machine {machine}'s computations was refused: {refused}")
            else:
                with run.lock: run.placed.update(moves)
                run.event(f"{len(moves)} computations of machine {machine} moved to the others, by one transaction, in {ms:.0f} ms")

        def restore(machine):
            open_machine(machine)
            with run.lock: run.live[machine] = True
            run.event(f"machine {machine} is back: new computations go to it again")

        def churn():
            period, drops, began, number = LIFE / max(1, len(units)), [], time.perf_counter(), 0
            while not run.stop.is_set():
                with run.lock: actions, run.actions = run.actions, []
                for what, machine in actions:
                    try: (lose if what == "lose" else restore)(machine)
                    except Exception as error: run.event(f"{what} {machine} failed: {str(error)[:200]}")
                number += 1
                pause = began + number * period - time.perf_counter()
                if pause > 0: time.sleep(pause)
                elif pause < -10: began, number = time.perf_counter(), 0         # after a long action, start the beat again
                now = time.perf_counter()
                try:
                    while drops and drops[0][0] <= now:
                        _, gone = drops.pop(0)
                        refused, _ = asked([drop(ledger) for ledger in gone])
                        if not refused:
                            with run.lock: run.config["ledgers dropped"] += len(gone)
                    old = units.pop(0)
                    with run.lock:
                        at = sending[0] + 2
                        active.pop(old["source"], None)
                        outputs.pop(old["a"], None)
                        if old["kind"] == "pair": outputs.pop(old["b"], None)
                    new, commands = make(old["kind"], at)
                    with run.lock: whose = {computation: run.placed.get(computation, where) for computation, where in old["on"].items()}
                    retiring = [stopping(where, computation) for computation, where in whose.items()] + [retire(c) for c in old["on"]]
                    refused, _ = asked(retiring + commands)
                    if refused:
                        units.append(old)
                        with run.lock:
                            active[old["source"]] = spot(sending[0])
                        continue
                    with run.lock:
                        for computation in old["on"]: run.placed.pop(computation, None)
                        run.placed.update(new["on"])
                        run.config["computations retired"] += len(old["on"]); run.config["computations created"] += len(new["on"])
                        run.config["ledgers created"] += 3 if new["kind"] == "pair" else 2
                        active[new["source"]] = spot(at)
                        outputs[new["a"]] = at
                        if new["kind"] == "pair": outputs[new["b"]] = at
                    units.append(new)
                    drops.append((time.perf_counter() + GRACE, [old["source"], old["a"]] + ([old["b"]] if old["kind"] == "pair" else [])))
                except (OSError, ValueError) as error:
                    run.event(f"the churn could not ask the keeper: {str(error)[:200]}")
                    time.sleep(1)

        class Control(BaseHTTPRequestHandler):
            def do_GET(self):
                parts = self.path.strip("/").split("/")
                if parts[0] == "status": body = run.status()
                elif parts[0] in ("lose", "restore") and len(parts) == 2 and parts[1].isdigit() and int(parts[1]) in MACHINES:
                    with run.lock: run.actions.append((parts[0], int(parts[1])))
                    body = {"asked": parts[0], "machine": int(parts[1])}
                elif parts[0] == "stop":
                    run.stop.set()
                    body = {"asked": "stop"}
                else: body = {"error": "ask for /status, /lose/<0, 1 or 2>, /restore/<0, 1 or 2>, or /stop"}
                data = json.dumps(body).encode()
                self.send_response(200); self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
            def log_message(self, *args): pass
        threading.Thread(target=ThreadingHTTPServer(("127.0.0.1", CONTROL), Control).serve_forever, daemon=True).start()

        for t in range(3):
            tick(t, time.time())
        churning = threading.Thread(target=churn, daemon=True)
        churning.start()
        begin, t = time.time(), 3
        while not run.stop.is_set():
            when = begin + (t - 3) / rate
            pause = when - time.time()
            if pause > 0: time.sleep(pause)
            try: tick(t, when)
            except (OSError, ValueError, KeyError) as error:
                run.event(f"the user's append of tick {t} failed: {str(error)[:200]}; trying again in a second")
                time.sleep(1)
                begin, t = time.time() - (t - 3) / rate, t
                continue
            t += 1
        run.event("stopping")
    finally:
        run.stop.set()
        if churning: churning.join(timeout=20)              # what the churn is asking is answered before the keeper goes
        for machine in list(sessions): close_machine(machine)
        for process in running.values(): process.terminate()
        for process in running.values():
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill()
        subprocess.run(["pkill", "-f", "runner.py run "])
        print("everything is stopped; the log and the blobs are left as they are", flush=True)


def ask(path):
    try: print(urllib.request.urlopen(f"http://127.0.0.1:{CONTROL}/{path}", timeout=10).read().decode())
    except OSError as error: print(f"the running system did not answer on port {CONTROL}: {error}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] in (["lose"], ["restore"]): ask(f"{args[0]}/{args[1]}")
    elif args[:1] == ["stop"]: ask("stop")
    elif args[:1] == ["status"]: ask("status")
    else:
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        main(int(args[0]) if args else 200, float(args[1]) if len(args) > 1 else 10.0)
