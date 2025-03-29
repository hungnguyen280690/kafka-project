from confluent_kafka import Consumer, KafkaException
import json
import socket
import sys

def create_consumer():
    """Tạo và cấu hình Kafka Consumer"""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'client.id': socket.gethostname()
    }
    return Consumer(config)

def main():
    """Hàm chính để chạy consumer"""
    consumer = create_consumer()
    topic = 'app-logs'
    
    try:
        # Subscribe to topic
        consumer.subscribe([topic])
        
        print(f"Started consuming from topic: {topic}")
        print("Press Ctrl+C to exit")
        
        # Poll for messages
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                raise KafkaException(msg.error())
                
            # Parse the message
            key = msg.key().decode('utf-8') if msg.key() else "None"
            value = json.loads(msg.value().decode('utf-8'))
            
            print(f"Received message:")
            print(f"  Key: {key}")
            print(f"  Value: {value}")
            print(f"  Partition: {msg.partition()}, Offset: {msg.offset()}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        # Close down consumer
        consumer.close()

if __name__ == "__main__":
    main()
