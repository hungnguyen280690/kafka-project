from confluent_kafka import Producer
import json
import socket
import time
import sys
sys.path.append('..')
from utils.log_generator import generate_log

def create_producer():
    """Tạo và cấu hình Kafka Producer"""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'acks': '1',  # Wait for leader acknowledgment
    }
    return Producer(config)

def delivery_callback(err, msg):
    """Callback function được gọi sau khi message được gửi"""
    if err:
        print(f'ERROR: Message failed delivery: {err}')
    else:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        print(f'Message delivered to {topic} [{partition}] at offset {offset}')

def main():
    """Hàm chính để chạy producer"""
    producer = create_producer()
    topic = 'app-logs'
    
    print("Starting to produce messages to Kafka...")
    
    for i in range(10):
        # Tạo log message
        log_data = generate_log()
        key = log_data['service']  # Sử dụng service làm key
        value = json.dumps(log_data)
        
        # Gửi message đến Kafka
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_callback
        )
        
        # Đảm bảo các messages được gửi từ local buffer
        producer.poll(0)
        
        print(f"Produced message {i+1}: {key}")
        time.sleep(0.5)  # Đợi nửa giây giữa các messages
    
    # Đợi tất cả các messages trong queue được gửi
    remaining = producer.flush(timeout=10)
    if remaining > 0:
        print(f"WARNING: {remaining} messages weren't delivered")
    else:
        print("All messages delivered successfully!")

if __name__ == "__main__":
    main()
