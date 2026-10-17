#!/usr/bin/env python3
"""Compare explicit IC6/BI5 writings against independent graph oracles; no optimizer.

Uses the tiny fixture (--people 0), an SNB snapshot, or a deterministic synthetic
graph. Synthetic results characterize these plans/selectivities, not LDBC scores.
Reports bind+tick, release+tick, graph changes, and setup wire latencies separately.
Peeks and independent answer validation are outside all reported query phases.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess

from client import Server, decode
from workload import HERE, TABLES, Reference, expected, fingerprint, load, parameters


def synthetic(people, posts):
    g = {t: set() for t in TABLES}
    g['place'] = {(10, 'City', 20), (20, 'Country', -1)}
    g['tag'] = {(i, f'T{i:02d}') for i in range(64)}
    for p in range(people):
        g['person'].add((p, f'P{p}', 'Last', 10))
        for step in (1, 17, 61, 149):
            a, b = sorted((p, (p + step) % people))
            g['knows'].add((a, b, 1000 + a))
        for j in range(posts):
            mid = 1_000_000 + 2 * (p * posts + j)
            g['message'].add((mid, 0, p, -1))
            tags = {0 if j % 4 == 0 else 1 + (p * 17 + j * 7) % 63,
                    1 + (p * 31 + j * 11) % 63}
            g['message_tag'].update((mid, tag) for tag in tags)
            g['likes'].update((((p + 1) % people, mid), ((p + 17) % people, mid)))
            if j % 3 == 0:
                g['message'].add((mid + 1, 1, (p + 1) % people, mid))
                g['message_tag'].add((mid + 1, min(tags)))
    return g


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--server', type=Path, required=True)
    ap.add_argument('--snapshot', type=Path)
    ap.add_argument('--people', type=int, default=2000, help='synthetic people, or 0 for the tiny fixture')
    ap.add_argument('--posts', type=int, default=16, help='synthetic posts per person')
    ap.add_argument('--bindings', type=int, default=16)
    ap.add_argument('--rounds', type=int, default=7)
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--maintained', action='store_true', help='also check/timestamp graph changes while requests remain bound')
    ap.add_argument('--backend', choices=('corgi', 'vec'), default='corgi')
    ap.add_argument('--queries', nargs='+', choices=('ic6', 'bi5'), default=['ic6', 'bi5'])
    ap.add_argument('--variants', nargs='+', choices=('original', 'rewrite', 'bounded', 'indexed'), default=['original', 'rewrite', 'bounded', 'indexed'])
    ap.add_argument('--output', type=Path, required=True, help='new result directory')
    args = ap.parse_args()
    if (args.people != 0 and args.people < 512) or min(args.posts, args.bindings, args.rounds, args.workers) < 1:
        ap.error('synthetic people must be 0 or >=512; other counts must be positive')
    args.server = args.server.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    graph = load(args.snapshot) if args.snapshot or args.people == 0 else synthetic(args.people, args.posts)
    ref = Reference(graph)
    bank, standing = parameters(ref)
    requests = {'ic6': [(i, *bank['ic6'][i % len(bank['ic6'])]) for i in range(args.bindings)]}
    # Hot, less frequent, and absent tag probes expose the selectivity tradeoff.
    tags = sorted({tag for names in ref.tags.values() for tag in names})
    requests['bi5'] = [(i, tag) for i, tag in enumerate(dict.fromkeys([standing['bi5'][0], tags[-1], tags[len(tags)//2], '__absent__']))]
    delta = {'knows': sorted(graph['knows'])[:4],
             'message_tag': sorted(graph['message_tag'])[:8], 'likes': sorted(graph['likes'])[:8]}
    changed = {t: rows - set(delta.get(t, [])) for t, rows in graph.items()}
    answers = {state: {q: expected(Reference(g), q, requests[q]) for q in args.queries}
               for state, g in [('initial', graph), ('changed', changed)]}
    report = dict(status='running', boundary=__doc__, platform=platform.platform(),
                  revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                  server_sha256=sha(args.server), harness_sha256=sha(Path(__file__)),
                  cargo_lock_sha256=sha(HERE.parents[3] / 'Cargo.lock'),
                  graph_sha256=fingerprint(graph), rows={t: len(rows) for t, rows in graph.items()},
                  arguments={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  graph_plan_sha256=sha(HERE / 'graph.ddp'),
                  synthetic=not args.snapshot and args.people != 0, backend=args.backend,
                  workers=args.workers, requests=requests, delta=delta, runs=[])
    def save():
        (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    save()
    for query in args.queries:
        for variant in args.variants:
            if variant == 'indexed' and query != 'ic6':
                continue
            suffix = f'_{variant}' if variant in ('bounded', 'indexed') else ''
            plan = HERE / ('rewrites' if variant != 'original' else '') / f'{query}{suffix}.ddp'
            program = plan.read_text()
            (args.output / f'{query}-{variant}.ddp').write_text(program)
            record = dict(query=query, variant=variant, plan_sha256=sha(plan), events=[], setup_events=[])
            report['runs'].append(record)
            with Server(args.server, args.output / f'{query}-{variant}.log', args.backend, args.workers, 120, 6) as server:
                def verify(state, phase, active=True):
                    _, replies = server.commands([f'peek {query}.answer'])
                    actual = decode(replies[0])
                    want = answers[state][query] if active else []
                    if actual != want:
                        raise AssertionError(f'{query}/{variant}/{state}/{phase}: {actual[:2]!r} != {want[:2]!r}')

                for name, src in [('graph', (HERE / 'graph.ddp').read_text()), ('q', program)]:
                    metric, _ = server.commands([f'load {name} begin\n{src}\n{{rid}} end-load', 'tick'])
                    record['setup_events'].append(dict(phase=f'install:{name}', **metric))
                for index, table in enumerate(TABLES):
                    rows = sorted(graph[table])
                    for start in range(0, len(rows), 1000):
                        metric, _ = server.commands([Server.feed('graph', index, rows[start:start+1000], 1)])
                        record['setup_events'].append(dict(phase=f'feed:{table}', **metric))
                setup, _ = server.commands(['tick'])
                record['setup_settle_ms'] = setup['wire_ms']
                record['setup_events'].append(dict(phase='tick', **setup))
                record['setup_total_wire_ms'] = sum(e['wire_ms'] for e in record['setup_events'])
                for cycle in range(args.rounds + 1):
                    for state in ('initial', 'changed'):
                        if state == 'changed':
                            metrics, _ = server.commands([Server.feed('graph', TABLES.index(t), rows, -1) for t, rows in delta.items() if rows] + ['tick'])
                            if cycle:
                                record['events'].append(dict(round=cycle, state='changed', phase='graph_delete', **metrics))
                        for phase, diff in [('bind', 1), ('release', -1)]:
                            metrics, _ = server.commands([Server.feed('q', 0, requests[query], diff), 'tick'])
                            if cycle:
                                record['events'].append(dict(round=cycle, state=state, phase=phase, **metrics))
                            verify(state, phase, phase == 'bind')
                    metrics, _ = server.commands([Server.feed('graph', TABLES.index(t), rows, 1) for t, rows in delta.items() if rows] + ['tick'])
                    if cycle:
                        record['events'].append(dict(round=cycle, state='restored', phase='graph_restore', **metrics))
                    if args.maintained:
                        server.commands([Server.feed('q', 0, requests[query], 1), 'tick'])
                        verify('initial', 'standing_bind')
                        for state, diff in [('changed', -1), ('initial', 1)]:
                            phase = 'maintained_delete' if diff == -1 else 'maintained_restore'
                            metrics, _ = server.commands([Server.feed('graph', TABLES.index(t), rows, diff) for t, rows in delta.items() if rows] + ['tick'])
                            if cycle:
                                record['events'].append(dict(round=cycle, state=state, phase=phase, **metrics))
                            verify(state, phase)
                        server.commands([Server.feed('q', 0, requests[query], -1), 'tick'])
                        verify('initial', 'standing_release', False)
                record['peak_rss_bytes_sampled'] = server.peak_rss or None
            record['median_ms'] = {f'{state}/{phase}': statistics.median(e['wire_ms'] for e in record['events'] if e['state'] == state and e['phase'] == phase)
                                   for state, phase in sorted({(e['state'], e['phase']) for e in record['events']})}
            print(query, variant, record['median_ms'], flush=True)
            save()
    report['status'] = 'passed'
    save()


if __name__ == '__main__':
    main()
