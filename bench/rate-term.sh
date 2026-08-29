#!/usr/bin/env bash
# Soak the chronic-failure rate term (docs/scoring.md §7.3) against the mock
# backend with bench/rate-term-config.yaml, and print each endpoint's
# reputation state as it converges.
#
# Usage: [SNAPSHOT_EVERY=60] ./bench/rate-term.sh [duration] [concurrency]
#   duration    hey load duration (default 75m — see below)
#   concurrency concurrent connections (default 16; throughput is not the
#               bottleneck, the probe cycle is)
#
# Why 75 minutes: one critical (-25) drops an endpoint out of tier 1 and only
# health-check probes (two per 30s, +5 each) bring it back, so a 0.216%
# endpoint takes in roughly 460 attempts per ~60s cycle whatever the offered
# load. The first rate-term demotion needs 12k–22k attempts (docs/scoring.md
# §7.3), i.e. 30–50 minutes of wall time.
#
# Pass criteria (steady state, docs/scoring.md §7.3 table):
#   supplier-000 (0.216%)   score < 80  (tier 2), penalty within [-30, -17]
#   supplier-001 (0.065%)   score >= 80 (tier 1), penalty within [-16, -8]
#   supplier-002 (0.00003%) penalty == 0
set -euo pipefail

DURATION="${1:-75m}"
CONCURRENCY="${2:-16}"
SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-60}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/bench/results/rate-term-$(date +%Y%m%d-%H%M%S)"
URL="http://localhost:3069/v1"
ADMIN="http://localhost:9091/admin/reputation/eth"
BODY='{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

command -v hey >/dev/null || { echo "hey not installed (brew install hey)"; exit 1; }
command -v jq >/dev/null || { echo "jq not installed (brew install jq)"; exit 1; }

mkdir -p "$OUT"
echo "results -> $OUT"

echo "== build =="
make -C "$ROOT" sage_build >/dev/null

echo "== start gateway (mock protocol, chronic faults) =="
"$ROOT/bin/sagegw" -config "$ROOT/bench/rate-term-config.yaml" >"$OUT/gateway.log" 2>&1 &
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT

for i in $(seq 1 50); do
  curl -sf "http://localhost:3069/health" >/dev/null 2>&1 && break
  sleep 0.2
  [ "$i" = 50 ] && { echo "gateway did not become healthy"; exit 1; }
done
echo "gateway up (pid $GW_PID)"

snapshot() {
  curl -sf "$ADMIN" | jq -r 'to_entries | sort_by(.key)[] |
    "\(.key | sub("^.*supplier-"; "supplier-") | sub("\\.mock\\.local.*$"; ""))  attempts=\(.value.attempts)  additive=\(.value.additive)  rate=\(.value.rate * 100 | . * 10000 | round / 10000)%  penalty=\(.value.penalty | . * 10 | round / 10)  score=\(.value.score | . * 10 | round / 10)"'
}

echo "== load ($DURATION, c=$CONCURRENCY) =="
hey -z "$DURATION" -c "$CONCURRENCY" -m POST -H 'Content-Type: application/json' \
  -H 'Target-Service-Id: eth' -d "$BODY" "$URL" >"$OUT/hey.txt" 2>&1 &
HEY_PID=$!

while kill -0 "$HEY_PID" 2>/dev/null; do
  sleep "$SNAPSHOT_EVERY"
  echo "--- $(date +%T)"
  snapshot | tee -a "$OUT/snapshots.txt" || true
done
wait "$HEY_PID" || true

echo "== final =="
curl -sf "$ADMIN" | jq . >"$OUT/final.json"
snapshot | tee "$OUT/final.txt"
grep -E "Requests/sec|Total:" "$OUT/hey.txt" || true
echo "results in $OUT"
