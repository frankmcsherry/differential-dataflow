# Deploying the store on the minis

How etcd 3.7.3, Garage 2.5.0 and SeaweedFS 4.52 were run on mini-00, mini-01 and mini-02 on 2026-11-05, with clients on mini-03.

- `etcd-start.sh <e0|e1|e2>` starts one etcd member over the Thunderbolt links.
  Each mini's link to each other mini is its own /30 network, so each member advertises all three of its link addresses.
- `garage.toml.example` is one Garage node's configuration.
  Each node also needs `bootstrap_peers` naming its two peers at their link addresses, and its saved peer list deleted, or it remembers slower addresses.
- SeaweedFS ran from its release binary: `weed server -dir=<dir> -ip=<addr> -filer -s3 -s3.config=<s3.json>` for one node, with `fs.configure -locationPrefix=/buckets/<bucket>/ -fsync=true -apply` in `weed shell`.

macOS refuses outgoing connections to LAN and Thunderbolt addresses ("No route to host") from a process whose SSH session has ended.
Start every daemon under an SSH session held open from another machine, `nohup ssh mini-0x 'cd ~/ddir-draft && exec <daemon> …' &`, until the binaries have Local Network permission or run as launchd daemons.
