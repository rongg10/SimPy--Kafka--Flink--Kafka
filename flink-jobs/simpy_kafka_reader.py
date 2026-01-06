"""
Flink job to read SimPy service call logs from Kafka using DataStream API.
Reads messages with format: id, src_ip, dst_ip, start_at_ms, latency_msec, end_at_ms
Implements chain linking with matching rules:
- parent.dst_ip == child.src_ip
- parent.start_at_ms <= child.start_at_ms
- parent.end_at_ms >= child.end_at_ms
Uses event-time watermarks (based on start_at_ms) and delays emission
until watermark passes each message's end_at_ms to make matching reciprocal.
"""
import json
import os
from typing import List

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction, TimeDomain
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common import Row

from linking_utils import is_parent

MAX_OUT_OF_ORDER_MS = int(os.getenv("SIMPY_MAX_OUT_OF_ORDER_MS", "30000"))
IDLE_FLUSH_MS = int(os.getenv("SIMPY_IDLE_FLUSH_MS", "60000"))
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

ROLE_PARENT = "parent"
ROLE_CHILD = "child"

EVENT_MESSAGE = "message"
EVENT_LINK = "link"

UPDATE_ADD_PARENT = "add_parent"
UPDATE_ADD_CHILD = "add_child"


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_parallelism() -> int:
    for key in ("SIMPY_PARALLELISM", "KAFKA_PARTITIONS", "KAFKA_NUM_PARTITIONS"):
        value = os.getenv(key)
        if value is None:
            continue
        parsed = _to_int(value, 0)
        if parsed > 0:
            return parsed
    return 0


def parse_message(message: str) -> Row:
    """Parse JSON message string into a Row object."""
    data = json.loads(message)
    return Row(
        id=data.get("id", ""),
        src_ip=data.get("src_ip", ""),
        dst_ip=data.get("dst_ip", ""),
        start_at_ms=_to_int(data.get("start_at_ms")),
        latency_msec=_to_float(data.get("latency_msec")),
        end_at_ms=_to_int(data.get("end_at_ms")),
    )


def row_to_message(value: Row) -> dict:
    """Convert a Row to a dict with parents/children fields."""
    return {
        "id": value[0],
        "src_ip": value[1],
        "dst_ip": value[2],
        "start_at_ms": value[3],
        "latency_msec": value[4],
        "end_at_ms": value[5],
        "parents": [],
        "children": [],
    }


def expand_roles(value: Row) -> List[Row]:
    """Emit parent/child role records keyed by the matching IP."""
    message_json = json.dumps(row_to_message(value))
    dst_ip = value[2] or ""
    src_ip = value[1] or ""
    return [
        Row(ROLE_PARENT, dst_ip, message_json),
        Row(ROLE_CHILD, src_ip, message_json),
    ]


def expand_updates(value: Row) -> List[Row]:
    """Explode link events into per-message updates."""
    event_type = value[0]
    if event_type == EVENT_MESSAGE:
        return [Row(EVENT_MESSAGE, value[1], "", value[3])]
    parent_id = value[1]
    child_id = value[2]
    return [
        Row(UPDATE_ADD_CHILD, parent_id, child_id, ""),
        Row(UPDATE_ADD_PARENT, child_id, parent_id, ""),
    ]


class IpLinkingProcess(KeyedProcessFunction):
    """Match parents/children within a single IP partition."""

    def open(self, runtime_context):
        parent_descriptor = MapStateDescriptor(
            "parent_state",
            Types.STRING(),
            Types.STRING(),
        )
        child_descriptor = MapStateDescriptor(
            "child_state",
            Types.STRING(),
            Types.STRING(),
        )
        idle_descriptor = ValueStateDescriptor(
            "idle_timer_state",
            Types.LONG(),
        )
        self.parent_state = runtime_context.get_map_state(parent_descriptor)
        self.child_state = runtime_context.get_map_state(child_descriptor)
        self.idle_timer_state = runtime_context.get_state(idle_descriptor)

    def process_element(self, value: Row, ctx):
        role = value[0]
        message_json = value[2]
        current = json.loads(message_json)

        if role == ROLE_PARENT:
            for child_json in self.child_state.values():
                child = json.loads(child_json)
                if is_parent(current, child):
                    yield Row(EVENT_LINK, current["id"], child["id"], "")
            self.parent_state.put(current["id"], message_json)
            yield Row(EVENT_MESSAGE, current["id"], "", message_json)
        else:
            for parent_json in self.parent_state.values():
                parent = json.loads(parent_json)
                if is_parent(parent, current):
                    yield Row(EVENT_LINK, parent["id"], current["id"], "")
            self.child_state.put(current["id"], message_json)

        ctx.timer_service().register_event_time_timer(current["end_at_ms"])
        self._reset_idle_timer(ctx)

    def on_timer(self, timestamp, ctx):
        if ctx.time_domain() == TimeDomain.PROCESSING_TIME:
            idle_ts = self.idle_timer_state.value()
            if idle_ts is not None and timestamp == idle_ts:
                self.parent_state.clear()
                self.child_state.clear()
                self.idle_timer_state.clear()
            return []

        expired_parents: List[str] = []
        for msg_id, msg_json in self.parent_state.items():
            if json.loads(msg_json)["end_at_ms"] <= timestamp:
                expired_parents.append(msg_id)
        for msg_id in expired_parents:
            self.parent_state.remove(msg_id)

        expired_children: List[str] = []
        for msg_id, msg_json in self.child_state.items():
            if json.loads(msg_json)["end_at_ms"] <= timestamp:
                expired_children.append(msg_id)
        for msg_id in expired_children:
            self.child_state.remove(msg_id)
        return []

    def _reset_idle_timer(self, ctx):
        current_pt = ctx.timer_service().current_processing_time()
        idle_ts = current_pt + IDLE_FLUSH_MS
        prev = self.idle_timer_state.value()
        if prev is not None:
            ctx.timer_service().delete_processing_time_timer(prev)
        self.idle_timer_state.update(idle_ts)
        ctx.timer_service().register_processing_time_timer(idle_ts)


class MessageAggregationProcess(KeyedProcessFunction):
    """Aggregate link updates into full messages keyed by id."""

    def open(self, runtime_context):
        message_descriptor = ValueStateDescriptor(
            "message_state",
            Types.STRING(),
        )
        end_descriptor = ValueStateDescriptor(
            "end_time_state",
            Types.LONG(),
        )
        parents_descriptor = ValueStateDescriptor(
            "parents_state_json",
            Types.STRING(),
        )
        children_descriptor = ValueStateDescriptor(
            "children_state_json",
            Types.STRING(),
        )
        idle_descriptor = ValueStateDescriptor(
            "idle_timer_state",
            Types.LONG(),
        )
        self.message_state = runtime_context.get_state(message_descriptor)
        self.end_time_state = runtime_context.get_state(end_descriptor)
        self.parents_state = runtime_context.get_state(parents_descriptor)
        self.children_state = runtime_context.get_state(children_descriptor)
        self.idle_timer_state = runtime_context.get_state(idle_descriptor)

    def process_element(self, value: Row, ctx):
        update_type = value[0]
        other_id = value[2]

        if update_type == EVENT_MESSAGE:
            message_json = value[3]
            if self.message_state.value() is None:
                self.message_state.update(message_json)
                end_at_ms = json.loads(message_json)["end_at_ms"]
                self.end_time_state.update(end_at_ms)
                ctx.timer_service().register_event_time_timer(end_at_ms)
            self._reset_idle_timer(ctx)
            return

        if update_type == UPDATE_ADD_PARENT:
            self._append_id(self.parents_state, other_id)
        elif update_type == UPDATE_ADD_CHILD:
            self._append_id(self.children_state, other_id)

        self._reset_idle_timer(ctx)

    def on_timer(self, timestamp, ctx):
        if ctx.time_domain() == TimeDomain.PROCESSING_TIME:
            idle_ts = self.idle_timer_state.value()
            if idle_ts is not None and timestamp == idle_ts:
                message_json = self._build_message_json()
                if message_json is not None:
                    yield Row(message_json)
                self._clear_state()
            return

        end_at_ms = self.end_time_state.value()
        if end_at_ms is None or end_at_ms > timestamp:
            return

        message_json = self._build_message_json()
        if message_json is not None:
            yield Row(message_json)
        self._clear_state()

    def _build_message_json(self) -> str:
        message_json = self.message_state.value()
        if message_json is None:
            return None
        message = json.loads(message_json)
        message["parents"] = self._read_ids(self.parents_state)
        message["children"] = self._read_ids(self.children_state)
        return json.dumps(message)

    def _clear_state(self) -> None:
        self.message_state.clear()
        self.end_time_state.clear()
        self.parents_state.clear()
        self.children_state.clear()
        self.idle_timer_state.clear()

    def _read_ids(self, state) -> List[str]:
        raw = state.value()
        if not raw:
            return []
        try:
            values = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(values, list):
            return []
        return values

    def _append_id(self, state, msg_id: str) -> None:
        if not msg_id:
            return
        values = self._read_ids(state)
        if msg_id in values:
            return
        values.append(msg_id)
        state.update(json.dumps(values))

    def _reset_idle_timer(self, ctx) -> None:
        current_pt = ctx.timer_service().current_processing_time()
        idle_ts = current_pt + IDLE_FLUSH_MS
        prev = self.idle_timer_state.value()
        if prev is not None:
            ctx.timer_service().delete_processing_time_timer(prev)
        self.idle_timer_state.update(idle_ts)
        ctx.timer_service().register_processing_time_timer(idle_ts)


class StartAtAssigner(TimestampAssigner):
    """Assign event time from start_at_ms field."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        return value[3]


def main():
    """Main function to set up and run the Flink job for SimPy messages."""
    # Create a streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_python_file("/opt/flink/usrlib/linking_utils.py")

    # Enable checkpointing for exactly-once processing
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds

    parallelism = _get_parallelism()
    if parallelism > 0:
        env.set_parallelism(parallelism)

    # Kafka configuration
    kafka_topic = os.getenv("KAFKA_TOPIC", "test-topic")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

    # Define the schema for the messages
    row_type_info = RowTypeInfo(
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.LONG()],
        ["id", "src_ip", "dst_ip", "start_at_ms", "latency_msec", "end_at_ms"],
    )

    # Create Kafka source
    kafka_source = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": "flink-simpy-consumer-group",
            "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        },
    )

    # Read from Kafka and parse messages
    stream = env.add_source(kafka_source)
    if parallelism > 0:
        stream = stream.set_parallelism(parallelism)
    stream = stream.map(lambda x: parse_message(x), output_type=row_type_info)

    # Assign timestamps/watermarks using start_at_ms
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(MAX_OUT_OF_ORDER_MS)) \
        .with_timestamp_assigner(StartAtAssigner())
    stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

    # Split into parent/child role records keyed by the matching IP.
    role_row_type = RowTypeInfo(
        [Types.STRING(), Types.STRING(), Types.STRING()],
        ["role", "ip_key", "message_json"],
    )
    role_stream = stream.flat_map(expand_roles, output_type=role_row_type)

    # IP-partitioned linking (local matching per IP).
    event_row_type = RowTypeInfo(
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
        ["event_type", "left_id", "right_id", "payload"],
    )
    ip_linked = role_stream \
        .key_by(lambda x: x[1], key_type=Types.STRING()) \
        .process(IpLinkingProcess(), output_type=event_row_type)

    # Expand link events into per-message updates and aggregate by message id.
    update_stream = ip_linked.flat_map(expand_updates, output_type=event_row_type)

    output_row_type_info = RowTypeInfo([Types.STRING()], ["linked_message"])
    linked_stream = update_stream \
        .key_by(lambda x: x[1], key_type=Types.STRING()) \
        .process(MessageAggregationProcess(), output_type=output_row_type_info)

    # Print the linked stream (this will output to Flink's stdout)
    linked_stream.print()

    # Execute the job
    env.execute("SimPy Kafka Reader - DataStream API with Chain Linking")


if __name__ == "__main__":
    main()
