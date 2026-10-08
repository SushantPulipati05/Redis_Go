#!/usr/bin/env bash
# Benchmark this server against real Redis with redis-benchmark.
# Usage: ./bench.sh   (needs redis-server and redis-benchmark installed)
set -e
go build -o redis-go .
rm -f bench-ours.aof
./redis-go -port 6380 -aof bench-ours.aof >/dev/null 2>&1 & OURS=$!
redis-server --port 6390 --save "" --appendonly yes --appendfsync everysec --dir /tmp --daemonize yes >/dev/null
trap 'kill $OURS 2>/dev/null; redis-cli -p 6390 shutdown nosave >/dev/null 2>&1; rm -f bench-ours.aof' EXIT
sleep 0.5

for target in "6380 ours" "6390 redis"; do
  set -- $target
  echo "== $2, one command at a time (50 clients)"
  redis-benchmark -p $1 -t set,get -n 200000 -c 50 --csv | cut -d, -f1,2
  echo "== $2, pipelined 16 commands (50 clients)"
  redis-benchmark -p $1 -t set,get -n 1000000 -c 50 -P 16 --csv | cut -d, -f1,2
done
