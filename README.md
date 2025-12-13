
# Batonics Challenge

HFT order book engine with DBN/NDJSON processing, TCP streaming, REST/WebSocket APIs, and Prometheus metrics.

✅ (1) Data Streaming: sustaining ~64k msgs/s over TCP (requirement says 50k–500k).

✅ (2) Order Book Reconstruction: reconstruct and output JSON deterministically; also have invariants tooling in book.rs.

✅ (6) API Layer: got REST + WS endpoints (non-perf mode).

✅ (11) Performance Optimization: perf mode + batching + counters on hot path.

✅ (12) Observability: Prometheus metrics + structured logs.

## Quick Start

```bash
cargo build --release
./benchmark.sh
```

(Sat Dec-12 4:05:08pm)-(CPU 2.5%:0:Net 5)-(emehhmi:/mnt/c/Users/USER/Desktop/KDP/batonics-challenge)-(172K:17)
> ./benchmark.sh 
Building...

Starting replay...

Starting engine in perf mode...

Waiting for /metrics...

Warmup 5s...

Measuring 30s...

**---- TCP Results ----**

duration_s=32.2804

msgs=2053316  errs=0  gaps=29

**throughput_msgs_per_s=63608.8**

goodput_msgs_per_s=63608.8

**---- Error Breakdown ----**

 TYPE batonics_shard_dropped_total counter

batonics_shard_dropped_total 0

 TYPE batonics_apply_unknown_order_total counter

batonics_apply_unknown_order_total 132096

 TYPE batonics_apply_qty_too_large_total counter

batonics_apply_qty_too_large_total 136

 TYPE batonics_apply_level_underflow_total counter

batonics_apply_level_underflow_total 0

 TYPE batonics_apply_overflow_total counter

batonics_apply_overflow_total 0

 TYPE batonics_apply_other_total counter
 
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
