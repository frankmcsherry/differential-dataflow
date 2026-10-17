#!/usr/bin/env python3
"""TPC-H Q6 integer control through the server; optional DuckDB query-only control.

DDIR timings INCLUDE file read, integer row parsing, row-to-column conversion and
initial settlement. DuckDB timings query an already-loaded identical projection.
These different boundaries must not be presented as an engine speedup ratio.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'ldbc'))
from client import Server, decode


PREDICATE = ('$0[0] >= 19940101 && $0[0] < 19950101 && '
             '$0[1] < 2400 && $0[3] >= 5 && $0[3] <= 7')
SOURCE = f'let rows = input 0 : ((int, int, int, int) ; ()) | filter({PREDICATE});\n'
PLANS = {
    'collect': SOURCE + '''export "answer" = rows | key(; $0[2] * $0[3])
        | collect | map(; fold($1, 0, ^1 + ^0[0]));''',
    'reduce': SOURCE + '''export "answer" = rows | key(; $0[2] * $0[3])
        | reduce(list(tuple(tuple(fold($1, 0, ^1 + ^0[0][0] * ^0[1])), 1)));''',
    'segmented': SOURCE + '''export "answer" = rows | key(; $0[2] * $0[3])
        | reduce((_, vals) -> ((vals map (p -> (p.0.0, p.1) mul) fold_add), 1) enlist)
        | map(; $1);''',
    'weighted': SOURCE + '''export "answer" = rows
        | join_function(list(tuple(tuple(tuple(), tuple()), tuple(0), $0[2] * $0[3])))
        | count | map(; $1);''',
}


def scaled(text):
    whole, fraction = text.split('.')
    if len(fraction) != 2:
        raise ValueError(f'expected two decimal places: {text}')
    return int(whole) * 100 + int(fraction)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as data:
        for block in iter(lambda: data.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--lineitem', type=Path, required=True)
    parser.add_argument('--rows', type=int, default=100_000, help='prefix size; 0 means all rows')
    parser.add_argument('--rounds', type=int, default=5)
    parser.add_argument('--backend', choices=('corgi', 'vec'), default='corgi')
    parser.add_argument('--duckdb', type=Path, help='optional DuckDB CLI')
    parser.add_argument('--output', type=Path, required=True, help='new directory')
    args = parser.parse_args()
    if args.rows < 0 or args.rounds < 1:
        parser.error('rows must be nonnegative and rounds positive')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    projection = args.output / 'lineitem.txt'
    count, revenue = 0, 0
    with args.lineitem.open() as source, projection.open('w') as dest:
        for line in source:
            if args.rows and count == args.rows:
                break
            fields = line.rstrip('\n').split('|')
            ship = int(fields[10].replace('-', ''))
            qty, price, discount = map(scaled, (fields[4], fields[5], fields[6]))
            dest.write(f'{ship} {qty} {price} {discount}\n')
            count += 1
            if 19940101 <= ship < 19950101 and qty < 2400 and 5 <= discount <= 7:
                revenue += price * discount
    if not count or not revenue:
        raise ValueError('this control requires a nonempty input and nonzero Q6 answer')
    report = dict(status='running', boundary=__doc__, rows=count, revenue_1e4=revenue,
                  projection_sha256=sha256(projection), platform=platform.platform(),
                  server_sha256=sha256(args.server), harness_sha256=sha256(Path(__file__)),
                  client_sha256=sha256(Path(__file__).resolve().parents[1] / 'ldbc/client.py'),
                  revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                  worktree=subprocess.check_output(['git', 'status', '--porcelain'], text=True),
                  backend=args.backend, workers=1, rounds=args.rounds, runs={})
    lock = Path(__file__).resolve().parents[4] / 'Cargo.lock'
    if lock.exists():
        shutil.copyfile(lock, args.output / 'Cargo.lock')
        report['cargo_lock_sha256'] = sha256(lock)
    report_path = args.output / 'report.json'
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    for name, program in PLANS.items():
        (args.output / f'{name}.ddp').write_text(program + '\n')
        samples = []
        with Server(str(args.server.resolve()), args.output / f'{name}.log',
                    args.backend, 1, 120, 4) as server:
            for index in range(args.rounds + 1):
                server.commands([f'load q begin\n{program}\n{{rid}} end-load', 'tick'])
                metrics, _ = server.commands([f'feed q 0 from {projection}', 'tick'])
                _, replies = server.commands(['peek answer'])
                actual = decode(replies[0])
                assert actual == [[[], [revenue], 1]], (name, actual, revenue)
                if index:
                    samples.append(metrics['wire_ms'])
                server.commands(['drop q', 'tick'])
        report['runs'][name] = dict(ingest_settle_ms=samples, median_ms=statistics.median(samples))
        report_path.write_text(json.dumps(report, indent=2) + '\n')
        print(f'{name}: ingest + settle {statistics.median(samples):.3f} ms; answer verified')
    if args.duckdb:
        database = args.output / 'control.duckdb'
        # The path is an SQL literal, never a shell command.
        path = str(projection).replace("'", "''")
        create = ("SET threads=1; CREATE TABLE data AS SELECT * FROM read_csv('" + path +
                  "', delim=' ', header=false, columns={'ship':'BIGINT', 'qty':'BIGINT', "
                  "'price':'BIGINT', 'discount':'BIGINT'});")
        start = time.perf_counter()
        subprocess.run([str(args.duckdb), str(database), '-c', create], check=True, capture_output=True)
        report['duckdb_load_process_ms'] = 1000 * (time.perf_counter() - start)
        query = ('SELECT SUM(price * discount) FROM data WHERE ship >= 19940101 '
                 'AND ship < 19950101 AND qty < 2400 AND discount BETWEEN 5 AND 7;')
        (args.output / 'query.sql').write_text(query + '\n')
        sql = 'SET threads=1;\n.timer on\n' + (query + '\n') * (args.rounds + 1)
        result = subprocess.run([str(args.duckdb), str(database), '-csv', '-noheader'],
                                input=sql, text=True, capture_output=True, check=True)
        (args.output / 'duckdb.txt').write_text(result.stdout + result.stderr)
        assert result.stdout.splitlines().count(str(revenue)) == args.rounds + 1, result.stdout
        times = re.findall(r'Run Time \(s\): real ([0-9.]+)', result.stdout + result.stderr)
        if len(times) == args.rounds + 1:
            report['duckdb_query_ms'] = [float(x) * 1000 for x in times[1:]]
        report['duckdb_version'] = subprocess.check_output([str(args.duckdb), '--version'], text=True).strip()
        print('DuckDB answers verified; query-only CLI timings in duckdb.txt')
    report['status'] = 'passed'
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(f'Report: {report_path}')


if __name__ == '__main__':
    main()
