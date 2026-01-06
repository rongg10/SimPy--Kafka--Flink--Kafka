# Kafka to Flink Pipeline

A SimPy → Kafka → Flink pipeline for simulating and processing distributed service call logs.

## Overview

- SimPy generates service-call traces.
- Kafka stores the raw logs.
- Flink partitions by match IP, links parents/children, and emits results with watermarks and an idle flush.

## Message Flow

1. SimPy generates log entries and writes to Kafka (`test-topic`).
2. Flink consumes Kafka, partitions by match IP, links messages, and aggregates by message id.
3. Event-time watermarks delay emission for reciprocity, with a 60s idle flush.

## Complexity and Performance

### Current complexity (per IP key)

- `IpLinkingProcess`: let `N` be the number of active messages for an IP (parents + children). Each new message scans all opposite-role messages for that IP, so per-event work is `O(N)` in the worst case. Over a window, total comparisons are `O(N^2)` in the worst case (more precisely `O(P*C)` where `P` and `C` are active parents/children). Memory is `O(N)` per IP until `end_at_ms` timers or idle flush clear the state.
- `MessageAggregationProcess`: each message id keeps one base message plus parent/child lists. Each link update rewrites a JSON list, so work is proportional to list size; total work per message is proportional to the number of links added. Memory grows with the number of linked parent/child ids until `end_at_ms` or idle flush.
- `linking_utils.WatermarkMatcher` (test/helper): each new message compares against all buffered messages; emitting scans the buffer. That is linear per message with linear memory, and quadratic overall in the worst case if everything overlaps.

### What has been done to optimize performance

- Partitioning by matching IP keeps linking local and enables parallelism across keys.
- Event-time watermarks (`SIMPY_MAX_OUT_OF_ORDER_MS`) and per-record `end_at_ms` timers evict old state instead of retaining it indefinitely.
- Idle flush (`SIMPY_IDLE_FLUSH_MS`) clears inactive keys to cap memory in sparse streams.
- Two-stage pipeline (linking → updates → aggregation) avoids repeated full-message rewrites and limits aggregation to message-id keys.

### What can be done to optimize further

- Replace full scans with time-bounded indexing (e.g., bucket state by `start_at_ms`/`end_at_ms`, or use an interval-join style CoProcessFunction) to avoid `O(P+C)` scans on every message.
- Store typed objects in state instead of JSON strings to remove repeated `json.loads`/`json.dumps` per element.
- Replace JSON list updates with `MapState`/`SetState` for parents/children to avoid decoding and rewriting the full list on each update.
- Use state TTLs and/or a priority-queue of end times to reduce timer overhead and keep state tighter under heavy load.
- Tune parallelism/Kafka partitions and consider a RocksDB state backend when state grows large.

## How to Run

### 1) Start services
```bash
docker compose up -d
```

### 2) Submit the Flink job
```bash
docker exec jobmanager ./bin/flink run -py /opt/flink/usrlib/simpy_kafka_reader.py
```

### 3) Generate messages (all examples use realtime)
```bash
# Continuous streaming
python simpy_message_generator.py --stream --interval 500 --realtime --ip-pool-size 6

# Fixed count
python simpy_message_generator.py --count 5 --interval 50 --realtime --ip-pool-size 3

# Variable intervals
python simpy_message_generator.py --count 5 --interval 100 --std-dev 200 --realtime --ip-pool-size 3

# Print each message
python simpy_message_generator.py --count 5 --interval 50 --realtime --print-msg --ip-pool-size 3
```

### Simulation flags

- `--realtime`: run in wall-clock time; 1 simulation unit = 1 ms (uses `simpy.RealtimeEnvironment`).
- `--count N`: generate N traces and exit (default: unlimited when `--stream` is used).
- `--stream`: run continuously until interrupted; overrides `--count`.
- `--interval MS`: mean interval between requests in milliseconds (default: 1000).
- `--std-dev MS`: standard deviation for interval jitter; 0 disables jitter (default: 0).
- `--print-msg`: print each generated message to stdout.
- `--debug`: do not send to Kafka (still simulates and logs locally).
- `--topic NAME`: Kafka topic to publish to (default: `test-topic` or `KAFKA_TOPIC` env).
- `--bootstrap HOSTS`: Kafka bootstrap servers (default: `localhost:9092` or `KAFKA_BOOTSTRAP` env).
- `--ip-pool-size N`: number of IPs per service pool (default: `SIMPY_IP_POOL_SIZE` or 10).

Message delivery delay is sampled from a chi-square distribution and capped:

- Defaults: `SIMPY_DELAY_CHISQ_DF=2.0`, `SIMPY_DELAY_SCALE_MS=1000`, `SIMPY_MAX_DELAY_MS=30000`.
- Each log entry is delayed by `chisq(df) * scale`, then capped at `SIMPY_MAX_DELAY_MS` to simulate out-of-order delivery.

### 4) Watch output
```bash
# Tail
docker logs taskmanager --since 1m 2>&1 | grep -E "^\+I\\["
```

```bash
# Continuous
docker logs -f taskmanager 2>&1 | grep -E "^\+I\[|linked_message"

docker logs -f taskmanager 2>&1 | rg "\\+I\\["
```

## Common Commands

```bash
# List running Flink jobs
docker exec jobmanager ./bin/flink list

# Cancel a job
docker exec jobmanager ./bin/flink cancel <job-id>

# See raw Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic test-topic \
  --from-beginning \
  --max-messages 5

# Clean all historical TaskManager logs (recreate the container)
docker compose rm -sf taskmanager
docker compose up -d taskmanager
```

## Configuration Overrides

```bash
# Flink job tuning
export KAFKA_TOPIC=test-topic
export KAFKA_BOOTSTRAP=kafka:29092
export KAFKA_AUTO_OFFSET_RESET=earliest
export SIMPY_MAX_OUT_OF_ORDER_MS=1000
export SIMPY_IDLE_FLUSH_MS=5000
export SIMPY_IP_POOL_SIZE=10
```

## Cleanup Messages in the Pipe

```bash
# Delete the Kafka topic (messages are removed; auto-created on next produce)
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic test-topic

# Optional: reset the Flink consumer group offsets
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group flink-simpy-consumer-group
```

## Next Steps

- Add aggregations or windowed latency metrics in Flink.
- Write linked output to a sink (database, file system).
- Tune watermark delay and idle flush duration for your traffic patterns.

## 已进行的测试

- 普通fixed count, 及不同count, interval, std-dev, print-msg, realtime等
- 高压流十分钟，interval 50高并发，每分钟约6000多条，约等于一天800多万条
