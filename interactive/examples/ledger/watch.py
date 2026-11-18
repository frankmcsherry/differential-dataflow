#!/usr/bin/env python3
"""A page of the system of ledgers, as it changes, to put on a projector beside a demo.

  python3 interactive/examples/ledger/watch.py steady          beside steady.py: the system running, with churn
  python3 interactive/examples/ledger/watch.py [id=label ..]   beside demo.py's walk: few ledgers, shown one by one

It serves a page on 127.0.0.1:8910, and stops at once if another program has that port.

Everything on the page is read from the system itself, a few times a second: the keeper's own counts (its rates, and
how long its requests took in the last minute), views that followers keep of the state (what runs where, where each
ledger stands, whether the rules hold), how far each follower on this machine is behind the keeper, and, beside
steady.py, what the driver made and when (computations and ledgers created and retired, holds moved, machines lost,
and the delay of each tick, from when it was due to when every output had it). Nothing is worked out from what a
process says of itself, except those last.

While the keeper, a follower or the driver is away the page says so, keeps what it last saw, and goes on asking.

The page is made here, as HTML, and the browser only fetches it again and again. So a picture of it can be had with no
browser that runs scripts: `/?static=1` is the whole page as it stands, `&theme=dark` or `&theme=light` picks a theme,
and `&w=1600` lays it out at 1600 pixels wide and scales it to 1024, which is how wide Quick Look lays a page out.
"""
import html, json, statistics, sys, threading, time, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import plumbing
import keeper as model

STEADY = "steady" in sys.argv[1:]
KEEPER = "http://127.0.0.1:7970"
LABELS = dict(arg.split("=", 1) for arg in sys.argv[1:] if "=" in arg and "//" not in arg)
if STEADY:
    VIEWS, ROSTER, DRIVER = "127.0.0.1:8010", "127.0.0.1:7990", "http://127.0.0.1:8921/status"
    LAGS = {"tells instances": "127.0.0.1:7990", "compaction policy": "127.0.0.1:8000", "audit, every rule": "127.0.0.1:8010"}
else:
    VIEWS, ROSTER, DRIVER = "127.0.0.1:7990", "127.0.0.1:7990", None
    LAGS = {"this machine's": "127.0.0.1:7990"}          # this follower keeps the rules too (the role `all`)
RULES = model.STATE_RULES
MANY = 40                   # with more computations than this, each instance's are counted, not listed
PORT = 8910

seen = {"keeper": None, "views": None, "roster": None, "lags": {}, "driver": None, "rates": {}, "at": {}}
lock = threading.Lock()


def label(computation): return LABELS.get(str(computation), str(computation))


def look_views(copy):
    """One look at the views a follower keeps: what runs, where each ledger stands, the rules, and the entries."""
    names = ["views::standing", "views::plan", "views::idle", "views::names", "views::bound", "checked::entries"] + [f"views::{rule}" for rule in RULES]
    rows = dict(zip(names, copy.peek(*names)))
    parse = lambda name: [plumbing.row(line) for line in rows[name]]
    standing = plumbing.stands(rows["views::standing"])
    kinds = {}
    for line in rows["checked::entries"]:
        found = line.split("inject(", 1)[1].split(",", 1)[0] if "inject(" in line else None
        kinds[found] = kinds.get(found, 0) + 1
    runs = {}
    for key, val in parse("views::plan"): runs.setdefault(key[0], []).append(val[0][1])
    found = {"standing": standing, "runs": runs, "idle": [key[0][1] for key, _ in parse("views::idle")],
             "names": {val[0][1]: plumbing.ints_text(key[0]) for key, val in parse("views::names")},
             "writer": {key[0][1]: val[0][1] for key, val in parse("views::bound")},
             "broken": [rule for rule in RULES if rows[f"views::{rule}"]],
             "entries": len(rows["checked::entries"]), "spans": kinds.get("1", 0), "ledgers": kinds.get("0", 0),
             "computations": kinds.get("4", 0), "holds": kinds.get("6", 0)}
    if len(standing) <= MANY:
        # Few ledgers: each is shown, with its spans counted.
        spans = {}
        for line in rows["checked::entries"]:
            if line.startswith("tuple(inject(1,"):
                (entry,), _ = plumbing.row(line)
                spans[entry[2][0][0][1]] = spans.get(entry[2][0][0][1], 0) + 1
        found["spans by ledger"] = spans
    return found


def ask():
    """Go on looking, for as long as this runs. What cannot be reached is said to be away, and asked again."""
    views, roster = plumbing.Follower(VIEWS), plumbing.Follower(ROSTER)
    lags = {name: plumbing.Follower(address) for name, address in LAGS.items()}
    history = []            # (time, keeper's counts), for its rates
    while True:
        now = time.time()
        try: counts = json.loads(plumbing.http("GET", KEEPER + "/counts", timeout=2))
        except Exception: counts = None
        behind = {}
        for name, follower in lags.items():
            try: behind[name] = follower.where()["at"]
            except Exception: behind[name] = None
        try: found = look_views(views)
        except Exception: found = None
        try:
            hosted = {}
            for key, val in (plumbing.row(line) for line in roster.peek("bundle::roster")[0]): hosted.setdefault(key[0], []).append(val[0])
        except Exception: hosted = None
        driver = None
        if DRIVER:
            try: driver = json.loads(urllib.request.urlopen(DRIVER, timeout=2).read())
            except Exception: driver = None
        with lock:
            if counts:
                # A keeper that started again counts its rounds and appends from nothing: what came before it is put aside.
                if history and any(counts.get(key, 0) < history[-1][1].get(key, 0) for key in ("records", "rounds", "appends", "alone", "passes")):
                    history.clear()
                history.append((now, counts))
                history[:] = [each for each in history if now - each[0] <= 6]
                first, last = history[0], history[-1]
                span = last[0] - first[0]
                if span > 0.5:
                    seen["rates"] = {key: (last[1].get(key, 0) - first[1].get(key, 0)) / span for key in ("appends", "rounds", "alone", "passes", "records")}
                seen["keeper"], seen["at"]["keeper"] = counts, now
            elif seen["keeper"] is not None: seen["keeper away"] = now
            if counts: seen.pop("keeper away", None)
            for name, value in (("views", found), ("roster", hosted), ("driver", driver)):
                if value is not None: seen[name], seen["at"][name] = value, now
            # The followers are asked just after the keeper, so one that is caught up can be a few records ahead.
            seen["lags"] = {name: (max(0, counts["records"] - at) if counts and at is not None else None) for name, at in behind.items()}
            seen["lags here"] = {name: at is not None for name, at in behind.items()}
        time.sleep(0.5)


# The page is made here. A cell whose text changed in the last two seconds is marked.
changed, shown = {}, {}
def mark(key, text):
    text = str(text)
    if key in shown and shown[key] != text: changed[key] = time.time()
    shown[key] = text
    return " fresh" if time.time() - changed.get(key, 0) < 2 else ""

def esc(text): return html.escape(str(text))

def number(value, digits=0):
    if value is None: return "–"
    return f"{value:,.{digits}f}" if isinstance(value, float) else f"{value:,}"

def tile(key, title, value, unit=""):
    return (f'<div class="tile"><div class="label">{esc(title)}</div>'
            f'<div class="value">{esc(value)}<span class="unit">{esc(unit)}</span></div></div>')

def table(heads, rows):
    head = "".join(f'<th class="{"n" if h.startswith("#") else ""}">{esc(h.lstrip("#"))}</th>' for h in heads)
    return f"<table><tr>{head}</tr>{''.join(rows)}</table>"

def cell(key, text, cls="", marked=False):
    return f'<td class="{cls}{mark(key, text) if marked else ""}">{esc(text)}</td>'

def away(name, then):
    if then is None: return f'<span class="away">{esc(name)}: not seen yet</span>'
    ago = time.time() - then
    return "" if ago < 3 else f'<span class="away">{esc(name)} is away: shown as it was {ago:.0f} s ago</span>'


def chart(delays):
    """The delay of the ticks over the last minutes: the median of each 5 s, and the nine in ten, as lines."""
    if not delays: return '<p class="dim">No ticks yet.</p>'
    width, height, left, bottom, roof = 1000, 220, 70, 30, 14
    span = max(300, -min(at for at, _, _ in delays))
    top = max(200, max(nine for _, _, nine in delays))
    top = next(step for step in (200, 500, 1000, 2000, 5000, 10000, 20000, 60000, 10 ** 9) if step >= top)
    x = lambda at: left + (width - left - 10) * (1 + at / span)
    y = lambda ms: roof + (height - bottom - roof) * (1 - min(ms, top) / top)
    line = lambda which: " ".join(f"{x(each[0]):.1f},{y(each[which]):.1f}" for each in delays)
    grid = "".join(f'<line x1="{left}" x2="{width - 10}" y1="{y(v):.1f}" y2="{y(v):.1f}" class="grid"/>'
                   f'<text x="{left - 10}" y="{y(v) + 6:.1f}" class="axis" text-anchor="end">{v:,}</text>' for v in (0, top // 2, top))
    minutes = span // 60
    ticks = "".join(f'<text x="{x(-60 * m):.1f}" y="{height}" class="axis" text-anchor="{"end" if m == 0 else "middle"}">{"now" if m == 0 else f"−{m} min"}</text>'
                    for m in range(0, minutes + 1, max(1, minutes // 5)))
    return (f'<svg viewBox="0 0 {width} {height + 4}" role="img" aria-label="delay of each tick, in ms">{grid}{ticks}'
            f'<polyline points="{line(2)}" class="nine"/><polyline points="{line(1)}" class="median"/></svg>'
            f'<div class="key"><span class="sw median"></span>median <span class="sw nine"></span>nine in ten, ms, from when a tick was due to when every output had it</div>')


def frag():
    """The page's body, as it stands."""
    with lock:
        k, rates, v, hosted, d, lags = seen["keeper"], seen["rates"], seen["views"], seen["roster"], seen["driver"], seen["lags"]
        at = dict(seen["at"])
    out = []
    warn = [away("the keeper", at.get("keeper")), away("the follower with the views", at.get("views"))]
    if DRIVER: warn.append(away("the running system's driver", at.get("driver")))
    status = " ".join(w for w in warn if w)
    if d: status += f'<span class="stat">running for <b>{d["running for"] // 60} min {d["running for"] % 60:02d} s</b></span>'
    out.append(f'<header><h1>Ledgers, live</h1>{status}</header>')

    # The keeper's rates, and how long its requests took.
    late = (k or {}).get("lately", {})
    tiles = [tile("appends", "appends a second", number(rates.get("appends"))),
             tile("rounds", "rounds a second", number(rates.get("rounds"), 1)),
             tile("alone", "configuration transactions a second", number(rates.get("alone"), 1)),
             tile("passes", "compaction passes a second", number(rates.get("passes"), 1)),
             tile("latency", "the keeper's answer, last minute", f'{number(late.get("median"))} / {number(late.get("nine"))}', " ms")]
    # While the keeper is away its rates are as they were, and are shown faded.
    stale = " stale" if time.time() - at.get("keeper", 0) >= 3 else ""
    out.append(f'<div class="tiles{stale}">{"".join(tiles)}</div>')

    sections = []
    # Where it runs: each machine's instances, and how many computations the state asks each to run.
    if v and hosted is not None:
        runs, rows = v["runs"], []
        machines = d["machines"] if d else {}
        for machine in sorted(set(hosted) | {int(m) for m in machines}):
            instances = hosted.get(machine, [])
            lost = machines.get(str(machine)) == "lost"
            count = sum(len(runs.get(i, [])) for i in instances)
            if STEADY:
                rows.append(f'<tr>{cell(f"m{machine}", f"machine {machine}", "name")}'
                            f'{cell(f"ms{machine}", "lost" if lost else "here", "bad" if lost else "good", True)}'
                            f'{cell(f"mc{machine}", count, "n big", True)}</tr>')
            else:
                many = sum(len(each) for each in runs.values()) > MANY
                for i in instances:
                    them = sorted(runs.get(i, []))
                    text = f"{len(them)} computations" if many else ", ".join(label(c) for c in them) or "nothing"
                    rows.append(f'<tr>{cell(f"m{i}", f"machine {machine}", "name")}{cell(f"i{i}", f"instance {i[0]}.{i[1]}", "name")}'
                                f'{cell(f"r{i}", text, "", True)}</tr>')
        idle = f'<p class="bad">No instance runs {len(v["idle"])}: {esc(", ".join(label(c) for c in v["idle"][:12]))}</p>' if v["idle"] else '<p class="good">Every computation has an instance.</p>'
        heads = ["machine", "", "#computations"] if STEADY else ["machine", "instance", "runs"]
        sections.append(f'<section class="{"" if STEADY else "two"}"><h2>Where it runs</h2>{table(heads, rows) if rows else "<p class=dim>Nothing is asked to run.</p>"}{idle}</section>')

    # The state: how much of it there is.
    if v:
        each = f'{v["spans"] / max(1, v["ledgers"]):.1f}'
        rows = [f'<tr><td>ledgers</td>{cell("sl", number(v["ledgers"]), "n")}</tr>',
                f'<tr><td>computations</td>{cell("sc", number(v["computations"]), "n")}</tr>',
                f'<tr><td>spans a ledger</td>{cell("ss", each, "n")}</tr>',
                f'<tr><td>entries in the state</td>{cell("se", number(v["entries"]), "n")}</tr>',
                f'<tr><td>records in the log</td>{cell("sr", number((k or {}).get("records")), "n")}</tr>']
        sections.append(f'<section><h2>The state</h2>{table(["", "#"], rows)}</section>')

    # What the driver made, and what happened.
    if d:
        c = d["config"]
        rows = [f'<tr><td>computations</td>{cell("cc", number(c["computations created"]), "n")}{cell("cr", number(c["computations retired"]), "n")}</tr>',
                f'<tr><td>ledgers</td>{cell("lc", number(c["ledgers created"]), "n")}{cell("lr", number(c["ledgers dropped"]), "n")}</tr>']
        more = (f'<p>own read holds moved: <b>{number(c["holds moved"])}</b>; '
                f'transactions refused: <b>{number(c["refused"])}</b> of {number(c["transactions"])}</p>')
        sections.append(f'<section><h2>Churn, since it began</h2>{table(["", "#created", "#retired"], rows)}{more}</section>')

    # How far each follower here is behind the keeper, and the rules.
    rows = []
    for name, behind in lags.items():
        text = "away" if behind is None else "caught up" if behind == 0 else "1 record" if behind == 1 else f"{behind:,} records"
        rows.append(f'<tr><td>{esc(name)}</td>{cell("f" + name, text, "n " + ("bad" if behind is None or behind > 50 else ""))}</tr>')
    rules = ""
    if v:
        rules = (f'<p class="bad">Broken: {esc(", ".join(v["broken"]))}</p>' if v["broken"]
                 else f'<p class="good">All {len(RULES)} rules of the model hold.</p>')
    sections.append(f'<section><h2>Followers here, behind the keeper</h2>{table(["", "#"], rows)}<h2 class="second">The rules</h2>{rules}</section>')

    if not STEADY and v and len(v["standing"]) <= MANY:
        # Few ledgers: each is shown.
        names, writer, standing = v["names"], v["writer"], v["standing"]
        rows = []
        for ledger in sorted(standing):
            version, front = standing[ledger]
            rows.append(f'<tr>{cell(f"ln{ledger}", names.get(ledger, f"ledger {ledger}"), "name")}{cell(f"lv{ledger}", version, "n", True)}'
                        f'{cell(f"lf{ledger}", "" if front is None else front, "n", True)}{cell(f"ls{ledger}", v.get("spans by ledger", {}).get(ledger, 0), "n", True)}'
                        f'{cell(f"lw{ledger}", label(writer[ledger]) if ledger in writer else "anyone", "", True)}</tr>')
        sections.append(f'<section class="wide"><h2>Where each ledger stands</h2>{table(["ledger", "#appends", "#frontier", "#spans", "written by"], rows) if rows else "<p class=dim>There are no ledgers.</p>"}</section>')

    out.append(f'<main>{"".join(sections)}</main>')
    if d:
        events = "".join(f'<li><span class="dim">{esc(at)}</span> {esc(text)}</li>' for at, text in reversed(d["events"]))
        out.append(f'<div class="lower"><section class="chart"><h2>Delay of each tick, the last minutes</h2>{chart(d["delays"])}</section>'
                   f'<section class="events"><h2>What happened</h2><ul>{events}</ul></section></div>')
    return "".join(out)


STYLE = """
:root { --bg: #f4f7f5; --sheet: #fbfdfb; --fg: #16201b; --muted: #55655c; --rule: #d3ddd6; --accent: #1c6b4c; --out: #8a4b08; --wash: #e6efe9; --fresh: #cfe8d9; --nine: #9fb8aa;
  --text: "Inter", system-ui, -apple-system, "Segoe UI", sans-serif; --display: "Space Grotesk", "Inter", system-ui, sans-serif; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #0f1512; --sheet: #141b17; --fg: #e2eae5; --muted: #93a39a; --rule: #28332d; --accent: #6fcf9f; --out: #e9b872; --wash: #18231d; --fresh: #24503a; --nine: #3d5c4c; color-scheme: dark } }
:root[data-theme="dark"] { --bg: #0f1512; --sheet: #141b17; --fg: #e2eae5; --muted: #93a39a; --rule: #28332d; --accent: #6fcf9f; --out: #e9b872; --wash: #18231d; --fresh: #24503a; --nine: #3d5c4c; color-scheme: dark }
* { box-sizing: border-box; }
html, body { background: var(--bg); }
body { margin: 0; color: var(--fg); font: 22px/1.35 var(--text); }
#page { padding: 18px 26px 26px; }
header { display: flex; flex-wrap: wrap; gap: 6px 26px; align-items: baseline; margin-bottom: 14px; }
h1 { font: 650 32px/1.1 var(--display); margin: 0; }
.stat { color: var(--muted); } .stat b { color: var(--fg); font-variant-numeric: tabular-nums; }
.away { color: var(--out); font-weight: 650; }
.tiles { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 14px; margin-bottom: 14px; }
.tiles.stale .tile { opacity: 0.45; }
.tile { background: var(--sheet); border: 1px solid var(--rule); padding: 10px 16px 12px; min-width: 0; }
.tile .label { font-size: 15px; letter-spacing: 0.05em; text-transform: uppercase; color: var(--muted); font-weight: 600; }
.tile .value { font: 650 44px/1.15 var(--display); font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.tile .unit { font-size: 22px; color: var(--muted); font-weight: 500; }
main { display: grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); gap: 14px; align-items: start; }
section { background: var(--sheet); border: 1px solid var(--rule); padding: 12px 16px 14px; min-width: 0; }
section.wide { grid-column: 1 / -1; }
section.two { grid-column: span 2; }
h2 { margin: 0 0 6px; font-size: 15px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--accent); font-weight: 650; }
h2.second { margin-top: 14px; }
table { border-collapse: collapse; width: 100%; }
th { text-align: left; font-size: 14px; letter-spacing: 0.05em; text-transform: uppercase; color: var(--muted); font-weight: 600; padding: 2px 12px 4px 0; }
td { padding: 4px 12px 4px 0; border-top: 1px solid var(--rule); vertical-align: baseline; font-variant-numeric: tabular-nums; }
td.n, th.n { text-align: right; } td.big { font: 650 30px var(--display); }
.fresh { background: var(--fresh); transition: background 0.4s; }
.name { font-weight: 650; white-space: nowrap; } .dim { color: var(--muted); }
.good { color: var(--accent); font-weight: 650; } .bad { color: var(--out); font-weight: 650; }
p { margin: 8px 0 0; }
.lower { display: grid; grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); gap: 14px; margin-top: 14px; }
.chart svg { width: 100%; height: auto; display: block; }
.chart .grid { stroke: var(--rule); stroke-width: 1; }
.chart .axis { fill: var(--muted); font-size: 18px; font-family: var(--text); }
.chart polyline { fill: none; stroke-width: 3; stroke-linejoin: round; }
.chart .median { stroke: var(--accent); } .chart .nine { stroke: var(--nine); stroke-dasharray: 6 5; }
.key { color: var(--muted); font-size: 16px; margin-top: 4px; }
.sw { display: inline-block; width: 22px; height: 0; border-top: 3px solid var(--accent); margin: 0 6px 5px 0; vertical-align: middle; }
.sw.nine { border-top: 3px dashed var(--nine); margin-left: 12px; }
.events ul { list-style: none; margin: 0; padding: 0; font-size: 17px; }
.events li { padding: 3px 0; border-top: 1px solid var(--rule); }
/* A narrow window. A page laid out at a chosen width for a picture (class `laid`) keeps the wide layout. */
@media (max-width: 1100px) {
  html:not(.laid) body { font-size: 18px; }
  html:not(.laid) #page { padding: 12px 16px; }
  html:not(.laid) .tiles { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  html:not(.laid) .tile .value { font-size: 34px; }
  html:not(.laid) .lower { grid-template-columns: minmax(0, 1fr); }
}
"""

SCRIPT = """
async function turn() {
  try {
    const answer = await fetch("/frag" + location.search, {cache: "no-store"});
    if (answer.ok) document.getElementById("page").innerHTML = await answer.text();
  } catch (error) {}
  setTimeout(turn, 700);
}
turn();
"""


def page(query):
    theme = query.get("theme", [""])[0]
    width = int(query.get("w", ["0"])[0] or 0)
    body = frag()
    # Laid out `width` pixels wide, and shown 1024 wide: how wide Quick Look lays a page out.
    frame = f' style="width:{width}px;zoom:{1024 / width:.4f}"' if width > 1024 else ""
    script = "" if query.get("static") else f"<script>{SCRIPT}</script>"
    data_theme = f' data-theme="{esc(theme)}"' if theme in ("dark", "light") else ""
    if width > 1024: data_theme += ' class="laid"'
    return (f'<!doctype html><html lang="en"{data_theme}><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
            f'<title>Ledgers live</title><style>{STYLE}</style></head><body><div id="page"{frame}>{body}</div>{script}</body></html>')


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        if url.path == "/frag": body = frag()
        elif url.path == "/data":
            with lock: body = json.dumps({key: value for key, value in seen.items() if key != "views"}, default=str)
        else: body = page(query)
        data = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json" if url.path == "/data" else "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data))); self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)
    def log_message(self, *args): pass


if __name__ == "__main__":
    threading.Thread(target=ask, daemon=True).start()
    try: server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    except OSError: sys.exit(f"port {PORT} is taken: another watch.py is running? (pgrep -fl watch.py)")
    print(f"the system, live: http://127.0.0.1:{server.server_address[1]}/", flush=True)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
