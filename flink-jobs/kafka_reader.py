"""
Simple Kafka to Flink read-only job using DataStream API.
Reads messages from Kafka topic and prints them.
"""
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common import Row


def parse_message(message: str) -> Row:
    """Parse JSON message string into a Row object."""
    data = json.loads(message)
    return Row(
        message_id=data.get("message_id", ""),
        message_content=data.get("message_content", ""),
        timestamp_col=data.get("timestamp_col", 0)
    )


def main():
    """Main function to set up and run the Flink job."""
    # Create a streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable checkpointing for exactly-once processing
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds
    
    # Kafka configuration
    kafka_topic = "test-topic"
    kafka_bootstrap_servers = "kafka:29092"  # Internal Docker network address
    
    # Define the schema for the messages
    row_type_info = RowTypeInfo(
        [Types.STRING(), Types.STRING(), Types.LONG()],
        ["message_id", "message_content", "timestamp_col"]
    )
    
    # Create Kafka source
    kafka_source = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-consumer-group',
            'auto.offset.reset': 'earliest'
        }
    )
    
    # Read from Kafka and parse messages
    stream = env.add_source(kafka_source) \
        .map(lambda x: parse_message(x), output_type=row_type_info)
    
    # Print the stream (this will output to Flink's stdout)
    stream.print()
    
    # Execute the job
    env.execute("Kafka Reader - DataStream API")


if __name__ == "__main__":
    main()

