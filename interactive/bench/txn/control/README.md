# A draft control plane

The catalog says what should run where, and the system makes it so.
Everything durable lives in one store: tables, program texts, instance specs, and their status.

- [`store.py`](store.py): the store service. Immutable blobs named by hash, and one root register replaced by compare-and-swap. Every write is flushed to the drive before it is acknowledged.
- [`control.py`](control.py): the control service. It holds the catalog in a DDIR server, decides transactions a tick at a time with [`../catalog/resolve.ddp`](../catalog/resolve.ddp), makes each tick durable in the store, and only then replies. It rebuilds itself from the store's root when it starts.
- [`provisioner.py`](provisioner.py): makes sure each instance with a spec has a runner. With no status reported within a timeout, it starts one on the spec's machine, over SSH.
- [`runner.py`](runner.py): runs one instance. It bumps the instance's incarnation (fencing any predecessor), starts a DDIR server, installs the spec's programs, feeds them their input tables, appends their outputs to output tables by compare-and-append, reports status, and stops when the spec changes or a newer incarnation starts.
- [`ctl.py`](ctl.py): a command-line client for writing specs and data and reading results.

Control state is three compare-and-swap cells per instance: `SPEC + i` (the spec's blob), `INC + i` (the incarnation), and `STATUS + i` (the last status's blob).
Tables are the catalog's tables: an upper cell and a set of batches.
An output resumes from its table: a runner reads what the table holds and appends only the difference, so restarts neither lose nor repeat updates.

## Tried on the minis, 2026-11-03

Store, control and provisioner on mini-03; instances on mini-00, mini-01 and mini-02.

| Step | What happened |
|---|---|
| Specs for three instances: reachability (mini-00), out-degree (mini-01), and the size of the reachability table (mini-02) | Runners started on each machine; the third reads the first's output table |
| Append edges | All three output tables updated; the size followed reachability across machines |
| Kill instance 1's runner and server | Restarted after the 5 s timeout as incarnation 2; it appended only the new rows |
| Stop instance 2 | Its runner and server on mini-01 exited |
| Move instance 3 to mini-00 | mini-02's runner stopped; mini-00's took over and continued its table |
| Start a second runner for an instance by hand | The older runner saw the newer incarnation and stopped |
| Kill the control service, restart it | It recovered from the store; runners exited, the provisioner restarted them, and outputs continued |

## Running it

```console
cargo build --release -p ddir-server
D=interactive/bench/txn/control
python3 $D/store.py --dir /tmp/cp/store &
python3 $D/control.py --state /tmp/cp/control &
python3 $D/provisioner.py --public-control http://mini-03:7802 --public-store http://mini-03:7801 &
python3 $D/ctl.py table 10; python3 $D/ctl.py table 11
python3 $D/ctl.py append 10 '((int), (int))' 0,1 1,2
python3 $D/ctl.py run 1 spec.json     # see below
python3 $D/ctl.py read 11
python3 $D/ctl.py stop 1
```

A spec, with `blob` from `ctl.py program reach.ddp`:

```json
{"machine": "mini-00", "workers": 2, "programs": [{"name": "reach", "blob": 1050250199733702625}],
 "inputs": [{"table": 10, "program": "reach", "input": 0}],
 "outputs": [{"program": "reach", "export": "reach", "table": 11, "shape": "((int), ())"}]}
```

Remote machines need the server binary and these scripts under `~/ddir-draft` (same layout as the repository).
`"machine": "local"` runs the instance on the provisioner's machine.

## Not yet

- A runner recomputes its outputs from all of its inputs on start, then appends the difference. Resuming incrementally from an output's upper needs inputs retained back to it (the since rule in the earlier discussion).
- Runners poll the catalog; nothing pushes changes to them.
- A server whose runner is killed with SIGKILL survives it unless its process group is killed too.
- The store and control services listen on all interfaces without authentication.
- Data tables are never compacted; their batches accumulate.

## On etcd and Garage

[`backends.py`](backends.py) adds two backends behind the same four calls: blobs in an S3-compatible bucket, and the root as one etcd key whose compare-and-swap compares the key's version.
`store.open_store` takes `split:s3=endpoint=<url>,bucket=<name>,keys=<file>;etcd=<url>|<url>|...;key=<root key>` in place of the store service's URL.
Blobs need no conditional writes, because they are immutable and named by their hash; only the root needs compare-and-swap.

Tried 2026-11-05: etcd 3.7.3 and Garage 2.5.0, one member each on mini-00, mini-01 and mini-02, replication factor 3, Garage with `data_fsync` and `metadata_fsync` on; clients on mini-03.
Both talk over the Thunderbolt links.
Each mini's link to each other mini is its own /30 network, so every etcd member advertises all three of its addresses, and every Garage node lists its two peers' link addresses in `bootstrap_peers`.

macOS refuses outgoing connections to local-network addresses (LAN and Thunderbolt, not Tailscale) from a process whose SSH session has ended, with "No route to host": its Local Network privacy check.
A process started over SSH connects normally while that session is open, and fails once it closes, even under `screen`.
So the daemons here run under SSH sessions held open from mini-03 (`ssh mini-0x 'exec …'`); a durable fix needs Local Network permission for the binaries, or a launchd daemon.

| Operation, from mini-03 | p50 | p90 |
|---|---:|---:|
| etcd compare-and-swap of the root | 7.0–7.3 ms | 7.3–8.2 ms |
| etcd read of the root | 2.6–3.2 ms | 2.8–3.5 ms |
| Garage put, 1 KiB | 2.6 ms | 3.8 ms |
| Garage put, 64 KiB | 20.0 ms | 21.7 ms |
| Garage put, 2 MiB | 53.1 ms | 66.1 ms |
| Garage get, 1 KiB / 64 KiB / 2 MiB | 3.1 / 4.1 / 6.1 ms | 3.4 / 4.5 / 6.7 ms |
| A durable control transaction, local disk store | 22.5 ms | 24.7 ms |
| A durable control transaction, Garage + etcd | 28.4 ms | 32.6 ms |

Garage keeps objects this small in its metadata database (SQLite here), which syncs with plain `fsync`; on macOS that does not flush the drive's cache. Larger objects are data blocks, which Garage syncs with Rust's `sync_all` (`F_FULLFSYNC` on macOS).
Before the session fix, Garage's nodes could only peer over the LAN and Tailscale, whose round trips are 5–70 ms here (the minis' default route is Wi-Fi), and the same operations took 22 / 57 / 141 ms to put and 43 ms per control transaction.

The control service recovered from the etcd root and Garage blobs after a restart, and a local runner continued its output table; with one mini's etcd member and Garage node stopped, transactions continued.
[`measure_backends.py`](measure_backends.py) measures the first rows.

## On SeaweedFS

SeaweedFS 4.52 (the release binary for macOS), with the S3 gateway in front of a filer, the same clients and `measure_backends.py`.
Its fsync rule is off by default; `fs.configure -locationPrefix=/buckets/ddir-blobs/ -fsync=true -apply` turns it on for the bucket.
Since 4.49 the rule also syncs the filer's metadata and each volume's index, and replicas flush too; SeaweedFS is Go, whose `Sync` on macOS is `F_FULLFSYNC`.

| Put, p50 (p90) | 1 KiB | 64 KiB | 2 MiB |
|---|---:|---:|---:|
| One node on mini-00, over Thunderbolt, fsync off | 4.4 ms (4.8) | 6.6 ms (6.9) | 26.8 ms (44.1) |
| One node on mini-00, over Thunderbolt, fsync on | 11.9 ms (13.0) | 14.1 ms (15.2) | 45.5 ms (52.9) |
| Three nodes, three copies, over Tailscale, fsync on | 35.2 ms (40.6) | 127 ms (184) | 2.5 s (3.6 s) |

Gets were 3–6 ms on one node.
Three copies could not run over Thunderbolt: each volume server registers one address, and replicas forward writes to each other there, so every server's address must be reachable from every other; each mini's Thunderbolt link to each other mini is its own /30 network, which rules that out without routes.
Over Tailscale the minis' Wi-Fi path dominates, so the last row measures the network, not SeaweedFS.
