#!/usr/bin/env bash
# Compares this server with Redis using redis-benchmark (50 clients,
# appendfsync everysec on both). Requires redis-server and redis-benchmark.
set -e
cd "$(dirname "$0")/.."
go build -o bin/redis-go ./cmd/redis-go
rm -f bench.aof
bin/redis-go -port 6380 -aof bench.aof >/dev/null 2>&1 & OURS=$!
redis-server --port 6390 --save "" --appendonly yes --appendfsync everysec --dir /tmp --daemonize yes >/dev/null
trap 'kill $OURS 2>/dev/null; redis-cli -p 6390 shutdown nosave >/dev/null 2>&1; rm -f bench.aof' EXIT
sleep 0.5

for target in "6380 redis-go" "6390 redis"; do
  set -- $target
  echo "== $2"
  redis-benchmark -p $1 -t set,get -n 200000 -c 50 --csv | cut -d, -f1,2
  echo "== $2, pipeline 16"
  redis-benchmark -p $1 -t set,get -n 1000000 -c 50 -P 16 --csv | cut -d, -f1,2
done
