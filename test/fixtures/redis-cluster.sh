#!/bin/sh
set -eu

redis-server --port 17300 --cluster-enabled yes --cluster-config-file /tmp/primary.conf \
  --cluster-announce-ip 127.0.0.1 --save '' --appendonly no &
primary_pid=$!
redis-server --port 17301 --cluster-enabled yes --cluster-config-file /tmp/replica.conf \
  --cluster-announce-ip 127.0.0.1 --save '' --appendonly no &
replica_pid=$!
trap 'kill "$primary_pid" "$replica_pid" 2>/dev/null || true' EXIT INT TERM

for port in 17300 17301; do
  attempts=0
  until redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 100
    sleep 0.1
  done
done

redis-cli -p 17300 cluster addslotsrange 0 16383
primary_id=$(redis-cli -p 17300 cluster myid)
redis-cli -p 17301 cluster meet 127.0.0.1 17300
attempts=0
until redis-cli -p 17301 cluster replicate "$primary_id" | grep -q '^OK'; do
  attempts=$((attempts + 1))
  test "$attempts" -lt 100
  sleep 0.1
done

attempts=0
until redis-cli -p 17300 cluster info | grep -q 'cluster_state:ok' && \
  redis-cli -p 17301 info replication | grep -q 'master_link_status:up' && \
  redis-cli -p 17300 --raw cluster slots | grep -q '^17301$'; do
  attempts=$((attempts + 1))
  test "$attempts" -lt 100
  sleep 0.1
done
touch /tmp/cluster-ready
wait "$primary_pid" "$replica_pid"
