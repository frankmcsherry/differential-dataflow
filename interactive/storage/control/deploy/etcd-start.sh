#!/bin/bash
# Start etcd member $1 (e0, e1 or e2) on this mini, listening on its Thunderbolt links only.
ips() { case $1 in e0) echo "10.0.2.1 10.0.3.1 10.0.1.1";; e1) echo "10.0.2.2 10.0.4.1 10.0.5.1";; e2) echo "10.0.3.2 10.0.4.2 10.0.6.1";; esac; }
urls() { local out=""; for ip in $(ips $1); do out="$out,http://$ip:$2"; done; echo ${out#,}; }
me=$1
cluster=""; for n in e0 e1 e2; do for ip in $(ips $n); do cluster="$cluster,$n=http://$ip:2380"; done; done
cd ~/ddir-draft && nohup ./bin/etcd --name $me --data-dir ~/ddir-draft/etcd-$me \
  --listen-peer-urls $(urls $me 2380) --initial-advertise-peer-urls $(urls $me 2380) \
  --listen-client-urls $(urls $me 2379) --advertise-client-urls $(urls $me 2379) \
  --initial-cluster ${cluster#,} --initial-cluster-token ddir-draft --initial-cluster-state new \
  < /dev/null > /tmp/ddir-etcd-$me.log 2>&1 &
echo "started $me pid $!"
