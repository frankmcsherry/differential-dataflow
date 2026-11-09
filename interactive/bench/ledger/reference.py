#!/usr/bin/env python3
"""Run the old catalog pipeline on the current server, adapting syntax and trace names.

Usage: python3 interactive/bench/ledger/reference.py --source OLD_CHECKOUT --binary CURRENT_SERVER --dir NEW_OUTPUT [pipeline.py options]
The original workload, frames, timers, reference checks, and durable store code remain in use.
"""

import argparse
import importlib
from pathlib import Path
import re
import sys
import tempfile

# Importing the pristine benchmark must not create __pycache__ files in its checkout.
sys.dont_write_bytecode = True

ALIASES = {
    **{name: f"cat::{name}" for name in ("records_up", "records_gen", "batches")},
    **{name: f"res::{name}" for name in ("rollback", "delta", "adds", "removes")},
    "q.by_op": "diagq::q.by_op",
    "log_records": "logmerge::log_records",
    "log_batches": "logmerge::log_batches",
}


def adapt_program(text):
    text = re.sub(r"(?m)^(\s*)export\b", r"\1output", text)
    return re.sub(r'(\bimport\s+")([^"]+)(")',
                  lambda match: match[1] + ALIASES.get(match[2], match[2]) + match[3], text)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--resolver", type=Path)
    if sys.argv[1:] in (["--help"], ["-h"]):
        print(__doc__)
        parser.print_help()
        print("Pass --source REFERENCE --help for the reference pipeline's full options.")
        return
    options, arguments = parser.parse_known_args()
    source = options.source.resolve()
    catalog = source / "interactive/storage/catalog"
    if not (catalog / "pipeline.py").is_file():
        raise SystemExit(f"no old catalog/pipeline.py under {source}")
    sys.path.insert(0, str(catalog))
    pipeline = importlib.import_module("pipeline")
    run = importlib.import_module("run")
    client = importlib.import_module("bench_txn").Client

    # Only lookup names change; none of the data frames is decoded or rewritten.
    peek_frames = run.Catalog.peek_frames
    run.Catalog.peek_frames = lambda self, trace: peek_frames(self, ALIASES.get(trace, trace))
    peek = client.peek
    client.peek = lambda self, trace: peek(self, ALIASES.get(trace, trace))
    request = client.request

    def adapted_request(self, command):
        match = re.match(r"^(feed\s+\S+\s+\S+\s+from\s+)(\S+)(.*)$", command)
        if match:
            command = match[1] + ALIASES.get(match[2], match[2]) + match[3]
        return request(self, command)

    client.request = adapted_request
    with tempfile.TemporaryDirectory(prefix="ddir-catalog-current-") as directory:
        adapted = Path(directory)
        for name in ("catalog.ddp", "resolve.ddp", "diagq.ddp", "logmerge.ddp"):
            path = catalog / name
            if path.is_file():
                (adapted / name).write_text(adapt_program(path.read_text()))
        if options.resolver:
            (adapted / "resolve.ddp").write_text(adapt_program(options.resolver.resolve().read_text()))
        # pipeline.py constructs the catalog and resolver paths from HERE at run time.
        # run.py only needs its auxiliary DDP files from HERE; keep its Python imports untouched.
        pipeline.HERE = adapted
        run.HERE = adapted
        print(f"adapter: old_source={source}; current trace namespaces; frames and benchmark unchanged", file=sys.stderr)
        sys.argv = [str(catalog / "pipeline.py"), *arguments]
        pipeline.main()


if __name__ == "__main__":
    main()
