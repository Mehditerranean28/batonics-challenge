#!/usr/bin/env bash
set -euo pipefail

DUR=${DUR:-30}
WARMUP=${WARMUP:-5}
CHUNK=${CHUNK:-16384}

ENGINE_PORT=8081
REPLAY_PORT=9999

get_metric() {
  local key="$1"
  curl -sf "http://127.0.0.1:${ENGINE_PORT}/metrics" \
    | awk -v k="$key" '$1==k {print $2; exit}' \
    || echo 0
}

cleanup() {
  kill "${ENGINE_PID:-}" 2>/dev/null || true
  kill "${REPLAY_PID:-}" 2>/dev/null || true
  wait "${ENGINE_PID:-}" 2>/dev/null || true
  wait "${REPLAY_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Building..."
cargo build --release --quiet

echo "Starting replay..."
# Continuous streaming replay (keeps connection open)
timeout $((DUR+WARMUP+5)) ./target/release/batonics-challenge replay \
  --bind "127.0.0.1:${REPLAY_PORT}" \
  --file data/feed.bin \
  --chunk "${CHUNK}" \
  --max-bps 2000000 >/dev/null 2>&1 &
REPLAY_PID=$!

sleep 0.5

echo "Starting engine..."
./target/release/batonics-challenge run \
  --connect "127.0.0.1:${REPLAY_PORT}" \
  --http-bind "127.0.0.1:${ENGINE_PORT}" \
  --out /tmp/stream_out.json \
  --depth 50 >/dev/null 2>&1 &
ENGINE_PID=$!

echo "Waiting for /metrics..."
for _ in {1..100}; do
  curl -sf "http://127.0.0.1:${ENGINE_PORT}/metrics" >/dev/null && break
  sleep 0.05
done

echo "Warmup ${WARMUP}s..."
sleep "${WARMUP}"

t0=$(date +%s%N)
m0=$(get_metric batonics_msgs_total)
e0=$(get_metric batonics_msgs_parse_err_total)
g0=$(get_metric batonics_msgs_seq_gap_total)

echo "Measuring ${DUR}s..."
sleep "${DUR}"

t1=$(date +%s%N)
m1=$(get_metric batonics_msgs_total)
e1=$(get_metric batonics_msgs_parse_err_total)
g1=$(get_metric batonics_msgs_seq_gap_total)

dt_ns=$((t1 - t0))
dt_s=$(awk "BEGIN{print ${dt_ns}/1000000000.0}")

msgs=$((m1 - m0))
errs=$((e1 - e0))
gaps=$((g1 - g0))
good=$((msgs - errs))
mps=$(awk "BEGIN{print ${msgs}/${dt_s}}")
gps=$(awk "BEGIN{print ${good}/${dt_s}}")

echo "---- TCP Results ----"
echo "duration_s=${dt_s}"
echo "msgs=${msgs}  errs=${errs}  gaps=${gaps}"
echo "throughput_msgs_per_s=${mps}"
echo "goodput_msgs_per_s=${gps}"

# Show detailed error breakdown
echo ""
echo "---- Error Breakdown ----"
curl -s "http://127.0.0.1:${ENGINE_PORT}/metrics" | grep -E "(batonics_apply_|batonics_shard_dropped)" || echo "No detailed error metrics available"

# Hard fail if you're dead in the water
if [ "${msgs}" -le 0 ]; then
  echo "ERROR: no messages processed"
  exit 1
fi
