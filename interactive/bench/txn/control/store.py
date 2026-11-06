#!/usr/bin/env python3
"""The durable store, as a small HTTP service: immutable blobs named by their hash, and one
root register replaced only by compare-and-swap on its version.

Its API is what object stores offer (put, get, conditional write), so an S3-like store can
replace it without changing its clients. Every write is on permanent storage before it is
acknowledged: fsync, then F_FULLFSYNC (see catalog/run.py's `sync_fd`).

    PUT  /blob          body: bytes          -> {"blob": <int>}
    GET  /blob/<int>                         -> bytes
    GET  /root                               -> {"version": v, "manifest": m}
    POST /root          {"expected": v, "manifest": m} -> {"ok": true} | 409 {"version": v}
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "catalog"))
from run import BlobStore, CasRegister, sync_path  # noqa: E402


class Store:
    """A client of the store service."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def _call(self, method: str, path: str, body: bytes | None = None):
        request = urllib.request.Request(self.url + path, data=body, method=method)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def put(self, data: bytes) -> int:
        status, body = self._call("PUT", "/blob", data)
        assert status == 200, body
        return json.loads(body)["blob"]

    def get(self, blob: int) -> bytes:
        status, body = self._call("GET", f"/blob/{blob}")
        if status == 404:
            raise FileNotFoundError(blob)
        assert status == 200, body
        return body

    def read_root(self) -> tuple[int, dict | None]:
        status, body = self._call("GET", "/root")
        doc = json.loads(body)
        return doc["version"], doc["manifest"]

    def cas(self, expected: int, manifest: dict) -> bool:
        body = json.dumps({"expected": expected, "manifest": manifest}).encode()
        status, _ = self._call("POST", "/root", body)
        return status == 200


def open_store(url: str):
    """A store client from its URL: `http://host:port` for this service, or
    `split:s3=<endpoint>,bucket=<name>,keys=<file>;etcd=<endpoint>|<endpoint>...` for blobs in an
    S3-compatible bucket and the root in etcd (see backends.py)."""
    if not url.startswith("split:"):
        return Store(url)
    from backends import EtcdRoot, S3Blobs, Split
    parts = dict(part.split("=", 1) for part in url[len("split:"):].split(";"))
    s3 = dict(kv.split("=", 1) for kv in parts["s3"].split(","))
    access, secret = open(s3["keys"]).read().split()
    blobs = S3Blobs(s3["endpoint"], s3["bucket"], access, secret)
    return Split(blobs, EtcdRoot(parts["etcd"].split("|"), key=parts.get("key", "ddir/root")))


def serve(directory: Path, port: int) -> None:
    blobs = BlobStore(directory, 0, "full")
    register = CasRegister(directory / "root.json", "full")
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status: int, body: bytes, kind: str = "application/json"):
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_PUT(self):
            data = self.rfile.read(int(self.headers["Content-Length"]))
            with lock:
                blob = blobs.put(data, 0)
                sync_path(blobs.path(blob), "full", flush=True)
            self.reply(200, json.dumps({"blob": blob}).encode())

        def do_GET(self):
            if self.path == "/root":
                version, manifest = register.read()
                self.reply(200, json.dumps({"version": version, "manifest": manifest}).encode())
            elif self.path.startswith("/blob/"):
                try:
                    self.reply(200, blobs.get(int(self.path[6:])), "application/octet-stream")
                except FileNotFoundError:
                    self.reply(404, b"{}")
            else:
                self.reply(404, b"{}")

        def do_POST(self):
            doc = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with lock:
                if register.cas(doc["expected"], doc["manifest"]):
                    self.reply(200, b'{"ok": true}')
                else:
                    self.reply(409, json.dumps({"version": register.read()[0]}).encode())

    print(f"store: {directory} on port {port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dir", type=Path, required=True)
    p.add_argument("--port", type=int, default=7801)
    a = p.parse_args()
    a.dir.mkdir(parents=True, exist_ok=True)
    serve(a.dir, a.port)
