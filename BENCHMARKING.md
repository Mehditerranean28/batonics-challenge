# Benchmarking

## Quick Benchmark
```bash
./benchmark.sh
```

## Manual Throughput Test
```bash
# Terminal 1: Rate-limited replay
./target/release/batonics-challenge replay --bind 127.0.0.1:9999 --file data/feed.bin --max-bps 2000000 &

# Terminal 2: Engine with metrics
./target/release/batonics-challenge run --connect 127.0.0.1:9999 --http-bind 127.0.0.1:8080

# Terminal 3: Monitor throughput
watch -n 1 'curl -s http://127.0.0.1:8080/metrics | grep msgs_total'
```

## Advanced Profiling
```bash
# CPU flame graph
cargo flamegraph --bin batonics-challenge -- run --file data/feed.bin

# System perf
perf stat -d ./target/release/batonics-challenge run --file data/feed.bin

# Memory
heaptrack ./target/release/batonics-challenge run --file data/feed.bin
```

## Docker Benchmarking

```bash
# Build and run full benchmark suite
docker-compose up --build

# Or run individual components
docker run --rm -v $(pwd)/data:/data batonics-challenge \
  run --file /data/feed.bin --depth 10

# Monitor metrics in Docker
docker run --rm --network host curlimages/curl \
  http://localhost:8080/metrics
```

## Metrics
- HTTP: `http://localhost:8080/metrics`
- Throughput: msgs/sec, latency buckets
- Errors: parse errors, sequence gaps

## Tuning
- `--depth N`: Book depth (lower = faster)
- `--shards N`: Parallel processing
