"""Store backends with the same four calls as `store.Store`: put and get a blob, read the root,
and compare-and-swap the root.

    S3Blobs     blobs in an S3-compatible bucket (Garage, here), signed with AWS Signature V4.
                Blobs are immutable and named by their hash, so no conditional writes are needed.
    EtcdRoot    the root as one etcd key, through etcd's HTTP gateway. Compare-and-swap is a
                transaction comparing the key's version (its count of writes) with the one read.
    Split       blobs from one backend and the root from another.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import json
import urllib.error
import urllib.request


def _call(request: urllib.request.Request, timeout: float = 30) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


class S3Blobs:
    def __init__(self, endpoint: str, bucket: str, access_key: str, secret_key: str, region: str = "garage"):
        self.endpoint, self.bucket = endpoint.rstrip("/"), bucket
        self.access, self.secret, self.region = access_key, secret_key, region
        self.host = self.endpoint.split("://", 1)[1]

    def _signed(self, method: str, key: str, body: bytes = b"") -> urllib.request.Request:
        now = datetime.datetime.utcnow()
        amz_date, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
        payload = hashlib.sha256(body).hexdigest()
        path = f"/{self.bucket}/{key}"
        headers = {"host": self.host, "x-amz-content-sha256": payload, "x-amz-date": amz_date}
        signed = ";".join(sorted(headers))
        canonical = "\n".join([method, path, "", "".join(f"{k}:{headers[k]}\n" for k in sorted(headers)),
                               signed, payload])
        scope = f"{day}/{self.region}/s3/aws4_request"
        to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
        k = ("AWS4" + self.secret).encode()
        for part in (day, self.region, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
        headers["Authorization"] = (f"AWS4-HMAC-SHA256 Credential={self.access}/{scope}, "
                                    f"SignedHeaders={signed}, Signature={signature}")
        del headers["host"]
        return urllib.request.Request(self.endpoint + path, data=body if method == "PUT" else None,
                                      method=method, headers=headers)

    def put(self, data: bytes) -> int:
        blob = int(hashlib.sha256(data).hexdigest()[:15], 16)
        status, body = _call(self._signed("PUT", f"{blob:015x}", data))
        assert status == 200, (status, body[:200])
        return blob

    def get(self, blob: int) -> bytes:
        status, body = _call(self._signed("GET", f"{blob:015x}"))
        if status == 404:
            raise FileNotFoundError(blob)
        assert status == 200, (status, body[:200])
        return body


class EtcdRoot:
    def __init__(self, endpoints: list[str], key: str = "ddir/root"):
        self.endpoints = [e.rstrip("/") for e in endpoints]
        self.key = base64.b64encode(key.encode()).decode()

    def _post(self, path: str, doc: dict) -> dict:
        last = None
        for endpoint in self.endpoints:  # any member will do; a follower forwards to the leader
            request = urllib.request.Request(endpoint + path, data=json.dumps(doc).encode(), method="POST")
            try:
                status, body = _call(request, timeout=10)
                if status == 200:
                    return json.loads(body)
                last = (status, body[:200])
            except OSError as error:
                last = error
        raise RuntimeError(f"etcd unavailable: {last}")

    def read_root(self) -> tuple[int, dict | None]:
        kvs = self._post("/v3/kv/range", {"key": self.key}).get("kvs", [])
        if not kvs:
            return 0, None
        return int(kvs[0]["version"]), json.loads(base64.b64decode(kvs[0]["value"]))

    def cas(self, expected: int, manifest: dict) -> bool:
        value = base64.b64encode(json.dumps(manifest).encode()).decode()
        result = self._post("/v3/kv/txn", {
            "compare": [{"key": self.key, "target": "VERSION", "result": "EQUAL", "version": str(expected)}],
            "success": [{"requestPut": {"key": self.key, "value": value}}],
        })
        return bool(result.get("succeeded"))


class Split:
    """Blobs from one backend, the root from another."""

    def __init__(self, blobs, root):
        self.blobs, self.root = blobs, root

    def put(self, data: bytes) -> int:
        return self.blobs.put(data)

    def get(self, blob: int) -> bytes:
        return self.blobs.get(blob)

    def read_root(self):
        return self.root.read_root()

    def cas(self, expected: int, manifest: dict) -> bool:
        return self.root.cas(expected, manifest)
