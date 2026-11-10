#!/usr/bin/env python3
"""Rounds of ledger transactions on a stock ddir_server, with and without clocks.

Usage, from the repository root, after `cargo build --release -p ddir-server`:
  python3 interactive/bench/ledger/clock_bench.py flat   # no clocks, clocks unused, one clock over every ledger
  python3 interactive/bench/ledger/clock_bench.py tree   # 1,000 clocks of 1,000 ledgers under one root clock
LEDGERS (default 1000000) and WORKERS (default 4) are read from the environment.

Each round loads the rules against a resident catalog, feeds appends as text rows, ticks, checks that
nothing was rejected, feeds the conclusions back into the catalog, and drops the rules.
It reports the server's own tick times: the resolve tick and the tick that absorbs conclusions into the
catalog. `round_wall` also includes parsing the rules and admitting the text rows.
See CLOCKS.md for recorded results."""
import os, random, re, statistics, subprocess, sys, time

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
BIN = os.path.join(ROOT, "target/release/ddir_server")
N = int(os.environ.get("LEDGERS", 1_000_000))
WORKERS = os.environ.get("WORKERS", "4")
CLOCK = "tuple(tuple(1,0))"

class Server:
    def __init__(self, port):
        env = dict(os.environ, DDIR_WORKERS=WORKERS, DDIR_MAX_FEED_BYTES=str(1 << 30),
                   DDIR_BIND=f"127.0.0.1:{port}", DDIR_WS_BIND=f"127.0.0.1:{port + 1}")
        self.p = subprocess.Popen([BIN], cwd=ROOT, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL)
    def send(self, text):
        self.p.stdin.write(text.encode() + b"\n"); self.p.stdin.flush()
        data = []
        while True:
            line = self.p.stdout.readline().decode()
            if not line: raise RuntimeError("server closed")
            parts = line.split(None, 2)
            if len(parts) >= 2 and parts[1] == "ok": return data, line.strip()
            if len(parts) >= 2 and parts[1] == "err": raise RuntimeError(line)
            data.append(line)
    def close(self):
        try: self.send("exit")
        except Exception: pass
        self.p.wait()

def ms(reply):
    value, unit = re.search(r"elapsed=([0-9.]+)(µs|ms|s)", reply).groups()
    return float(value) * {"µs": 1e-3, "ms": 1.0, "s": 1e3}[unit]

def feed(server, target, rows, chunk=200_000):
    for i in range(0, len(rows), chunk):
        server.send(f"feed {target} begin\n" + "\n".join(rows[i:i + chunk]) + "\nend-feed")

def setup(port, follow):
    s = Server(port)
    s.send("""load cat begin
let current = input 0 : (((int, int)), (List(List(int))));
let registered = input 1 : (((int, int)), (List(List(int))));
let follows = input 2 : (((int, int)), ((int, int)));
output "current" = current;
output "registered" = registered;
output "follows" = follows;
cat end-load""")
    zero = [f"tuple(tuple(0,{n})) val=tuple(list(list(0)))" for n in range(1, N + 1)] + [f"{CLOCK} val=tuple(list(list(0)))"]
    feed(s, "cat 0", zero); feed(s, "cat 1", zero)
    if follow: feed(s, "cat 2", [f"tuple(tuple(0,{n})) val=tuple(tuple(1,0))" for n in range(1, N + 1)])
    s.send("tick")
    return s

def append(key, ledger, lower, upper):
    return f"{key} val=tuple(list(tuple({ledger},inject(1,tuple(list(list({lower})),list(list({upper})),inject(0,tuple()))))))"

def run(server, rules, appends, rounds, state, clock, rng):
    """`state` holds each ledger's own upper and the round counter, across calls on one server."""
    bindings = "input0=cat::current input2=cat::registered" + (" input3=cat::follows" if "clocks" in rules else "")
    resolve, absorb, wall = [], [], []
    for _ in range(rounds):
        r = state["round"]
        rows = []
        for key, n in enumerate(rng.sample(range(1, N + 1), appends)):
            lower = r if clock else state["upper"].get(n, 0)
            rows.append(append(key, f"tuple(0,{n})", lower, lower + 1))
            state["upper"][n] = lower + 1
        if clock: rows.append(append(len(rows), "tuple(1,0)", r, r + 1))
        text = f"feed res 1 begin\n" + "\n".join(rows) + "\nend-feed" if rows else None
        begin = time.perf_counter()
        server.send(f"load res from interactive/model/{rules} {bindings}")
        if text: server.send(text)
        resolve.append(ms(server.send("tick")[1]))
        aborted, _ = server.send("peek res::aborted")
        if aborted: raise RuntimeError(f"{len(aborted)} aborted, e.g. {aborted[0]}")
        server.send("feed cat 0 from res::initial_delta")
        server.send("drop res")
        absorb.append(ms(server.send("tick")[1]))
        wall.append(1000 * (time.perf_counter() - begin))
        state["round"] += 1
    w = 2  # warmup rounds dropped
    med = lambda xs: statistics.median(xs[w:])
    print(f"{rules:26} clock={'yes' if clock else 'no ':3} appends={appends:6}  resolve_tick={med(resolve):7.2f} ms  "
          f"absorb_tick={med(absorb):6.2f} ms  round_wall={med(wall):7.2f} ms", flush=True)

rng = random.Random(1)
ROUNDS = 12
MODE = sys.argv[1] if len(sys.argv) > 1 else "flat"
if MODE == "flat":
    # 1. The rules without clocks: every frontier that moves needs its own append.
    s = setup(7801, follow=False); st = {"round": 0, "upper": {}}
    for a in (256, 2560, 25600): run(s, "transactions_keyed.ddp", a, ROUNDS, st, False, rng)
    s.close()
    # 2. The clock rules, but no ledger follows a clock: what the extra machinery costs when unused.
    s = setup(7811, follow=False); st = {"round": 0, "upper": {}}
    for a in (256, 2560, 25600): run(s, "transactions_clocks.ddp", a, ROUNDS, st, False, rng)
    s.close()
    # 3. Every ledger follows one clock, which advances every round; only some ledgers have data.
    s = setup(7821, follow=True); st = {"round": 0, "upper": {}}
    for a in (0, 256, 2560, 25600): run(s, "transactions_clocks.ddp", a, ROUNDS, st, True, rng)
    data, _ = s.send("peek cat::current")
    print("catalog rows:", len(data), flush=True)
    s.close()
else:
    # 4. Two levels: 1,000 clocks of 1,000 ledgers each, all following one root clock, which alone advances.
    s = setup(7831, follow=False); st = {"round": 0, "upper": {}}
    mids = [f"tuple(tuple(2,{k})) val=tuple(list(list(0)))" for k in range(1000)]
    feed(s, "cat 0", mids); feed(s, "cat 1", mids)
    feed(s, "cat 2", [f"tuple(tuple(0,{n})) val=tuple(tuple(2,{n % 1000}))" for n in range(1, N + 1)]
                   + [f"tuple(tuple(2,{k})) val=tuple(tuple(1,0))" for k in range(1000)])
    s.send("tick")
    for a in (0, 256, 2560, 25600): run(s, "transactions_clocks.ddp", a, ROUNDS, st, True, rng)
    data, _ = s.send("peek cat::follows")
    print("follows rows:", len(data), flush=True)
    s.close()
