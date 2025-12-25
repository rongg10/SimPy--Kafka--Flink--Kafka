"""
Simple script to produce test messages to Kafka for testing the Flink job.
"""
import json
import time
from kafka import KafkaProducer
from datetime import datetime


def produce_messages(topic: str, bootstrap_servers: str, num_messages: int = 10):
    """Produce test messages to Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Producing {num_messages} messages to topic '{topic}'...")
    
    for i in range(num_messages):
        message = {
            "message_id": f"msg-{i+1}",
            "message_content": f"Hello from Kafka message {i+1}",
            "timestamp_col": int(time.time() * 1000)
        }
        
        producer.send(topic, value=message)
        print(f"Sent message {i+1}: {message['message_content']}")
        time.sleep(0.5)  # Small delay between messages
    
    producer.flush()
    producer.close()
    print(f"\nFinished producing {num_messages} messages.")


if __name__ == "__main__":
    # Use localhost since this script runs on the host machine
    produce_messages("test-topic", "localhost:9092", num_messages=10)

