#!/usr/bin/env bash
# Benchmark the SAGE relay hot path against the in-process mock protocol
# backend. Builds the gateway, starts it with bench/mock-config.yaml, drives
# load with `hey`, and captures pprof heap/allocs profiles.
#
# Usage: ./bench/run.sh [duration] [concurrency]
#   duration    hey load duration (default 30s)
#   concurrency concurrent connections (default 100)
set -euo pipefail

DURATION="${1:-30s}"
CONCURRENCY="${2:-100}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/bench/results/$(date +%Y%m%d-%H%M%S)"
URL="http://localhost:3069/v1"
PPROF="http://localhost:6060/debug/pprof"
BODY='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

command -v hey >/dev/null || { echo "hey not installed (brew install hey)"; exit 1; }

mkdir -p "$OUT"
echo "results -> $OUT"

echo "== build =="
make -C "$ROOT" sage_build

echo "== start gateway (mock protocol) =="
"$ROOT/bin/sagegw" -config "$ROOT/bench/mock-config.yaml" >"$OUT/gateway.log" 2>&1 &
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT

for i in $(seq 1 50); do
  curl -sf "http://localhost:3069/health" >/dev/null 2>&1 && break
  sleep 0.2
  [ "$i" = 50 ] && { echo "gateway did not become healthy"; exit 1; }
done
echo "gateway up (pid $GW_PID)"

echo "== warmup (5s) =="
hey -z 5s -c "$CONCURRENCY" -m POST \
  -H "Content-Type: application/json" -H "Target-Service-Id: eth" \
  -d "$BODY" "$URL" >/dev/null

# Baseline allocation counters before the measured run.
curl -s "$PPROF/allocs" -o "$OUT/allocs-before.pb.gz"

echo "== measured run ($DURATION, c=$CONCURRENCY) =="
hey -z "$DURATION" -c "$CONCURRENCY" -m POST \
  -H "Content-Type: application/json" -H "Target-Service-Id: eth" \
  -d "$BODY" "$URL" | tee "$OUT/hey.txt"

# 10s CPU profile under load (background hey keeps pressure on).
echo "== cpu profile (10s under load) =="
hey -z 12s -c "$CONCURRENCY" -m POST \
  -H "Content-Type: application/json" -H "Target-Service-Id: eth" \
  -d "$BODY" "$URL" >/dev/null &
LOAD_PID=$!
curl -s "$PPROF/profile?seconds=10" -o "$OUT/cpu.pb.gz"
wait "$LOAD_PID"

curl -s "$PPROF/allocs" -o "$OUT/allocs-after.pb.gz"
curl -s "$PPROF/heap" -o "$OUT/heap.pb.gz"

kill "$GW_PID" 2>/dev/null || true
wait "$GW_PID" 2>/dev/null || true
trap - EXIT

echo
echo "== top allocators (measured window) =="
go tool pprof -top -nodecount=15 -sample_index=alloc_space \
  -base "$OUT/allocs-before.pb.gz" "$OUT/allocs-after.pb.gz" | tee "$OUT/allocs-top.txt"

echo
echo "== top cpu =="
go tool pprof -top -nodecount=15 "$OUT/cpu.pb.gz" | tee "$OUT/cpu-top.txt"

echo
echo "done: $OUT"
