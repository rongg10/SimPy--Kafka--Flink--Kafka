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
python simpy_message_generator.py --stream --interval 500 --realtime

# Fixed count
python simpy_message_generator.py --count 5 --interval 50 --realtime

# Variable intervals
python simpy_message_generator.py --count 5 --interval 100 --std-dev 200 --realtime

# Print each message
python simpy_message_generator.py --count 5 --interval 50 --realtime --print-msg
```

### 4) Watch output
```bash
# Tail
docker logs taskmanager --since 1m 2>&1 | grep -E "^\+I\\["
```

```bash
# Continuous
docker logs -f taskmanager 2>&1 | grep -E "^\+I\[|linked_message"
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
