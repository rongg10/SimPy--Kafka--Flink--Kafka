"""
Service Call Simulation using SimPy
Simulates distributed service call chains and sends logs to Kafka.
Pipeline: SimPy -> Kafka -> Flink
"""

from datetime import datetime
import os
import random
from functools import wraps
from typing import List
import threading

import argparse
import simpy
import json
import time
from kafka import KafkaProducer

random.seed(42)

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")
DELAY_MAX_MS = int(os.getenv("SIMPY_MAX_DELAY_MS", "30000"))
DELAY_CHISQ_DF = float(os.getenv("SIMPY_DELAY_CHISQ_DF", "2.0"))
DELAY_SCALE_MS = float(os.getenv("SIMPY_DELAY_SCALE_MS", "1000"))
DEFAULT_IP_POOL_SIZE = os.getenv("SIMPY_IP_POOL_SIZE", "10")

# Kafka Producer Configuration
producer = None
_send_errors = []
_partition_lock = threading.Lock()
_partition_counter = 0


def round_robin_partitioner(key_bytes, all_partitions, available):
    """Round-robin partition selection for unkeyed messages."""
    global _partition_counter
    partitions = available if available else all_partitions
    if not partitions:
        return None
    with _partition_lock:
        idx = _partition_counter % len(partitions)
        _partition_counter += 1
    return partitions[idx]

def _on_send_success(record_metadata):
    """Callback for successful message send."""
    pass

def _on_send_error(exception):
    """Callback for failed message send."""
    _send_errors.append(exception)
    if SIM_CONFIG.get("print_msg"):
        print(f"Error sending message to Kafka: {exception}")

def create_producer(bootstrap_servers, retries: int = 5, retry_delay: float = 1.0):
    """Create a Kafka producer with a simple retry loop."""
    last_error = None
    for _ in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,   # Retry failed sends
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                partitioner=round_robin_partitioner,
            )
        except Exception as exc:
            last_error = exc
            time.sleep(retry_delay)
    print(f"Warning: Kafka connection failed ({last_error}). Proceeding without Kafka.")
    return None

TOPIC_NAME = DEFAULT_TOPIC

def _ip_range(prefix: str, start: int, end: int) -> List[str]:
    return [f"{prefix}{i}" for i in range(start, end + 1)]


def _parse_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_ip_pools(size: int):
    size = max(1, int(size))
    end = size - 1
    return (
        _ip_range("10.0.0.", 0, end),
        _ip_range("10.1.0.", 0, end),
        _ip_range("10.2.0.", 0, end),
        _ip_range("10.3.0.", 0, end),
        _ip_range("10.4.0.", 0, end),
    )


# Service IP address pools (default size is 10)
_default_pool_size = _parse_int(DEFAULT_IP_POOL_SIZE, 10)
IP_CLIENT_POOL, IP_MAIN_POOL, IP_SUB1_POOL, IP_SUB2_POOL, IP_SUB3_POOL = _build_ip_pools(
    _default_pool_size
)


def _set_ip_pools(size: int) -> None:
    global IP_CLIENT_POOL, IP_MAIN_POOL, IP_SUB1_POOL, IP_SUB2_POOL, IP_SUB3_POOL
    IP_CLIENT_POOL, IP_MAIN_POOL, IP_SUB1_POOL, IP_SUB2_POOL, IP_SUB3_POOL = _build_ip_pools(size)

SIM_CONFIG = {
    "print_msg": False,
    "debug": False
}

# Global message ID counter
msg_id_counter = 0


def generate_message_delay():
    """
    Generate a random delay for message sending.
    
    Distribution:
    - Chi-square with degrees of freedom = DELAY_CHISQ_DF
    - Scaled by DELAY_SCALE_MS
    - Capped at DELAY_MAX_MS
    
    Returns:
        float: Delay in seconds
    """
    delay_ms = random.gammavariate(DELAY_CHISQ_DF / 2.0, 2.0) * DELAY_SCALE_MS
    delay_ms = min(delay_ms, DELAY_MAX_MS)
    return delay_ms / 1000.0  # Convert to seconds


def send_message_with_delay(log_entry, delay_seconds):
    """
    Send a message to Kafka after a specified delay.
    
    This function runs in a separate thread to avoid blocking the simulation.
    
    Args:
        log_entry: The message to send
        delay_seconds: Delay in seconds before sending
    """
    time.sleep(delay_seconds)
    
    if not SIM_CONFIG.get("debug") and producer:
        try:
            future = producer.send(TOPIC_NAME, log_entry)
            # Wait for send to complete to ensure message is actually sent
            future.get(timeout=5)
        except Exception as e:
            print(f"Failed to send to Kafka: {e}")
            _send_errors.append(e)


def log_service_call(func):
    """
    Service call logging decorator.
    
    Records service method execution details and sends to Kafka.
    Handles generator methods (using yield) correctly.
    """
    @wraps(func)
    def wrapper(self, **kwargs):
        trace_id = kwargs.get('trace_id', 'N/A')
        caller_ip = self.caller_ip
        service_ip = self.service_ip
        function_name = func.__name__
        start_at = self.env.now

        # Execute generator method
        gen = func(self, **kwargs)
        yield from gen

        # Calculate end time and duration
        end_at = self.env.now
        duration = end_at - start_at

        start_at_val = start_at
        end_at_val = end_at

        # Convert simulation time to real timestamp (ms) if in realtime mode
        if SIM_CONFIG.get("is_realtime"):
            base_time = SIM_CONFIG.get("base_time_ms", 0)
            start_at_val = int(base_time + start_at)
            end_at_val = int(base_time + end_at)

        global msg_id_counter
        msg_id_counter += 1

        # Format log entry: id, src_ip, dst_ip, start_at_ms, latency_msec, end_at_ms
        log_entry = {
            "id": f"msg_{msg_id_counter}",
            "src_ip": caller_ip,
            "dst_ip": service_ip,
            "start_at_ms": start_at_val,
            "latency_msec": duration,
            "end_at_ms": end_at_val
        }

        # Send log to Kafka with random delay to cause out-of-order delivery
        # The delay is applied in a separate thread to avoid blocking the simulation
        delay_seconds = generate_message_delay()
        thread = threading.Thread(target=send_message_with_delay, args=(log_entry, delay_seconds))
        thread.daemon = True  # Allow program to exit even if thread is still running
        thread.start()

        # Print message if enabled
        if SIM_CONFIG["print_msg"]:
            print(f"Message: {log_entry} (will be sent after {delay_seconds*1000:.2f}ms delay)")

    return wrapper


class Service:
    """Base service class."""
    
    def __init__(
        self,
        service_name: str,
        ip_pool: List[str],
        env: simpy.Environment,
        caller_ip: str,
    ):
        self.service_name = service_name
        self.ip_pool = ip_pool
        self.env = env
        self.service_ip = random.choice(ip_pool)
        self.caller_ip = caller_ip


class ClientService(Service):
    """Client service that initiates requests."""

    def __init__(self, env: simpy.Environment, caller_ip: str = None):
        super().__init__("ClientService", IP_CLIENT_POOL, env, caller_ip)
   
    def request(self, **kwargs):
        main_service = MainService(self.env, self.service_ip)
        yield self.env.process(main_service.handle_request(**kwargs))


class MainService(Service):
    """Main service that handles client requests."""

    def __init__(self, env: simpy.Environment, caller_ip: str):
        super().__init__("MainService", IP_MAIN_POOL, env, caller_ip)

    @log_service_call
    def handle_request(self, **kwargs):
        # Phase 1: Process for 20 time units
        exec_time = 20
        yield self.env.timeout(exec_time)

        # Call sub-service 1 and 2 in parallel
        sub_service1 = SubService1(self.env, self.service_ip)
        sub_service2 = SubService2(self.env, self.service_ip)
        proc1 = self.env.process(sub_service1.process(**kwargs))
        proc2 = self.env.process(sub_service2.process(**kwargs))
        yield proc1
        yield proc2

        # Phase 2: Process for 30 time units
        exec_time_2 = 30
        yield self.env.timeout(exec_time_2)

        # 40% chance to call sub-service 2 again
        if random.random() < 0.4:
            sub_service2 = SubService2(self.env, self.service_ip)
            yield self.env.process(sub_service2.process(**kwargs))
        
        # Phase 3: Process for 40 time units
        exec_time_3 = 40
        yield self.env.timeout(exec_time_3)


class SubService1(Service):
    """Sub-service 1."""

    def __init__(self, env: simpy.Environment, caller_ip: str):
        super().__init__("SubService1", IP_SUB1_POOL, env, caller_ip)
    
    @log_service_call
    def process(self, **kwargs):
        exec_time = 30
        yield self.env.timeout(exec_time)


class SubService2(Service):
    """Sub-service 2."""

    def __init__(self, env: simpy.Environment, caller_ip: str):
        super().__init__("SubService2", IP_SUB2_POOL, env, caller_ip)
    
    @log_service_call
    def process(self, **kwargs):
        exec_time = 30
        yield self.env.timeout(exec_time)

        sub_service3 = SubService3(self.env, self.service_ip)
        yield self.env.process(sub_service3.process_sub3(**kwargs))


class SubService3(Service):
    """Sub-service 3."""

    def __init__(self, env: simpy.Environment, caller_ip: str):
        super().__init__("SubService3", IP_SUB3_POOL, env, caller_ip)
    
    @log_service_call
    def process_sub3(self, **kwargs):
        exec_time = 10
        yield self.env.timeout(exec_time)

# Trace ID generator
trace_id_counter = 0


def generate_trace_id():
    """Generate unique trace ID."""
    global trace_id_counter
    trace_id_counter += 1
    return trace_id_counter


def request_generator(env, client_service, count, mean_interval, std_dev, stream_mode=False):
    """
    Generate requests with specified interval distribution.
    
    Args:
        env: SimPy environment
        client_service: Client service instance
        count: Number of requests to generate (None for infinite streaming)
        mean_interval: Average interval between requests
        std_dev: Standard deviation of the interval (0 for constant)
        stream_mode: If True, stream continuously until interrupted
    """
    if stream_mode or count is None:
        print(f"Starting continuous streaming with: interval={mean_interval}, std_dev={std_dev}")
        i = 0
        while True:
            env.process(client_service.request(trace_id=generate_trace_id()))
            i += 1
            if i % 100 == 0:
                print(f"Generated {i} requests so far...")
            
            if std_dev > 0:
                interval = random.gauss(mean_interval, std_dev)
                interval = max(0, interval)
            else:
                interval = mean_interval
                
            yield env.timeout(interval)
    else:
        print(f"Starting simulation with: count={count}, interval={mean_interval}, std_dev={std_dev}")
        for i in range(count):
            env.process(client_service.request(trace_id=generate_trace_id()))
            
            if i < count - 1:
                if std_dev > 0:
                    interval = random.gauss(mean_interval, std_dev)
                    interval = max(0, interval)
                else:
                    interval = mean_interval
                    
                yield env.timeout(interval)


if __name__ == "__main__":
    print("Simulation start time:", datetime.now())
    
    parser = argparse.ArgumentParser(description="Service Call Simulation with SimPy")
    parser.add_argument("--realtime", action="store_true", help="Enable real-time synchronization (1 unit = 1 ms)")
    parser.add_argument("--count", type=int, default=None, help="Number of traces to generate (default: None for infinite streaming)")
    parser.add_argument("--stream", action="store_true", help="Stream continuously until interrupted (overrides --count)")
    parser.add_argument("--interval", type=int, default=1000, help="Average interval between requests (ms)")
    parser.add_argument("--std-dev", type=float, default=0.0, help="Standard deviation of the interval")
    parser.add_argument("--print-msg", action="store_true", help="Print message details to console")
    parser.add_argument("--debug", action="store_true", help="Debug mode (do not send to Kafka)")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka topic name")
    parser.add_argument("--bootstrap", type=str, default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument(
        "--ip-pool-size",
        type=int,
        default=_default_pool_size,
        help="Number of IPs per service pool (default: SIMPY_IP_POOL_SIZE or 10)",
    )
    args = parser.parse_args()
    
    # If --stream is set, ignore count
    if args.stream:
        args.count = None

    # Update global configuration
    SIM_CONFIG["print_msg"] = args.print_msg
    SIM_CONFIG["debug"] = args.debug
    SIM_CONFIG["is_realtime"] = args.realtime
    if args.realtime:
        SIM_CONFIG["base_time_ms"] = int(time.time() * 1000)

    # Update Kafka settings
    TOPIC_NAME = args.topic
    _set_ip_pools(args.ip_pool_size)
    if args.debug:
        producer = None
    else:
        producer = create_producer(args.bootstrap)

    # Create SimPy environment
    if args.realtime:
        env = simpy.RealtimeEnvironment(factor=0.001, strict=False)
    else:
        env = simpy.Environment()

    # Create client service
    client_service = ClientService(env=env)

    print("Preparation time:", datetime.now())

    # Start request generator
    stream_mode = args.stream or args.count is None
    env.process(request_generator(env, client_service, args.count, args.interval, args.std_dev, stream_mode))

    # Run simulation
    try:
        if stream_mode:
            print("Streaming mode: Press Ctrl+C to stop...")
        # Run simulation (will run indefinitely in streaming mode due to while True loop)
        env.run()
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, shutting down gracefully...")
    finally:
        # Ensure all messages are sent before exiting
        # Wait for delayed messages (max delay is DELAY_MAX_MS) plus buffer for network
        if producer:
            try:
                # Wait for maximum delay + buffer for network operations
                time.sleep(1)  # Give threads a moment to start
                max_delay_s = max(0.0, DELAY_MAX_MS / 1000.0)
                producer.flush(timeout=max_delay_s + 5)
            except Exception as e:
                print(f"Warning: Error flushing producer: {e}")
            
            if _send_errors:
                print(f"Warning: {len(_send_errors)} messages failed to send")
            
            producer.close()
        
        print("Simulation end time:", datetime.now())
