
# Batonics Challenge

HFT order book engine with DBN/NDJSON processing, TCP streaming, REST/WebSocket APIs, and Prometheus metrics.

## Quick Start

```bash
cargo build --release
./benchmark.sh
```

(Sat Dec-12 1:14:50am)-(CPU 3.9%:0:Net 5)-(emehhmi:/mnt/c/Users/USER/batonics-challenge)-(164K:17)
> ./benchmark.sh
Building...
Starting replay...
Starting engine...
Waiting for /metrics...
Warmup 5s...
Measuring 30s...
---- TCP Results ----
duration_s=33.6103
msgs=1026658  errs=0  gaps=687735
throughput_msgs_per_s=30545.9
goodput_msgs_per_s=30545.9

---- Error Breakdown ----
# TYPE batonics_shard_dropped_total counter
batonics_shard_dropped_total 0
# TYPE batonics_apply_unknown_order_total counter
batonics_apply_unknown_order_total 132096
# TYPE batonics_apply_qty_too_large_total counter
batonics_apply_qty_too_large_total 136
# TYPE batonics_apply_level_underflow_total counter
batonics_apply_level_underflow_total 0
# TYPE batonics_apply_overflow_total counter
batonics_apply_overflow_total 0
# TYPE batonics_apply_other_total counter
batonics_apply_other_total 0

## Commands

- `run --file <path>`: Process file to JSON output
- `run --connect <addr>`: Live TCP streaming
- `replay --bind <addr> --file <path>`: TCP replay server

## Manual Testing

```bash
# Terminal 1: Start replay server
./target/release/batonics-challenge replay --bind 127.0.0.1:9999 --file data/feed.bin --max-bps 2000000 &

# Terminal 2: Start engine
./target/release/batonics-challenge run --connect 127.0.0.1:9999 --http-bind 127.0.0.1:8080

# Terminal 3: Monitor metrics
curl http://127.0.0.1:8080/metrics
```

## Docker Manual Testing

```bash
# Terminal 1: Start replay server
docker run -d --name replay --network host -v $(pwd)/data:/data batonics-challenge \
  replay --bind 127.0.0.1:9999 --file /data/feed.bin --max-bps 2000000

# Terminal 2: Start engine
docker run -d --name engine --network host -v $(pwd)/data:/data batonics-challenge \
  run --connect 127.0.0.1:9999 --http-bind 127.0.0.1:8080

# Terminal 3: Monitor metrics (using curl container)
docker run --rm --network host curlimages/curl \
  http://127.0.0.1:8080/metrics

# Or monitor continuously
docker run --rm --network host curlimages/curl \
  -s http://127.0.0.1:8080/metrics | grep msgs_total

# Cleanup
docker stop replay engine && docker rm replay engine
```

## Docker Compose

```bash
docker-compose up --build
```

See [BENCHMARKING.md](BENCHMARKING.md) for performance details.
