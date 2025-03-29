import asyncio
import json
import time
import sys
import socket
from confluent_kafka import Producer
sys.path.append('..')
from utils.log_generator import generate_log
from utils.kafka_config import HIGH_THROUGHPUT_PRODUCER_CONFIG

# Biến để theo dõi hiệu suất
message_count = 0
start_time = None
TOTAL_MESSAGES = 10000
TARGET_THROUGHPUT = 1000
FLUSH_INTERVAL = 1.0  # Seconds between explicit flush calls

def create_producer():
    """Tạo Kafka Producer với cấu hình tối ưu"""
    config = {**HIGH_THROUGHPUT_PRODUCER_CONFIG}
    config['client.id'] = f'async-producer-{socket.gethostname()}'
    return Producer(config)

def delivery_callback(err, msg):
    """Callback được gọi khi message được gửi thành công hoặc gặp lỗi"""
    global message_count
    
    if err is not None:
        print(f'ERROR: Message failed delivery: {err}')
    else:
        message_count += 1
        if message_count % 1000 == 0:
            elapsed = time.time() - start_time
            throughput = message_count / elapsed
            print(f"Sent: {message_count}/{TOTAL_MESSAGES} messages, Throughput: {throughput:.2f} msgs/sec")

async def produce_messages(producer, topic, batch_size=100):
    """Coroutine để gửi một batch messages"""
    for _ in range(batch_size):
        log_data = generate_log()
        key = log_data['service']
        value = json.dumps(log_data)
        
        # Produce message không block event loop
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=delivery_callback
        )
        
        # Poll để xử lý các delivery callbacks
        producer.poll(0)
        
    # Trả về số lượng messages đã gửi
    return batch_size

async def periodic_flush(producer):
    """Coroutine để định kỳ flush producer buffer"""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        producer.flush(0)  # Non-blocking flush

async def rate_limiter(target_throughput, batch_size):
    """Coroutine để điều chỉnh tốc độ gửi để đạt target throughput"""
    batch_start = time.time()
    while True:
        # Tính thời gian cần thiết để đạt target throughput
        target_batch_time = batch_size / target_throughput
        
        # Tính thời gian đã trôi qua từ lúc bắt đầu batch
        elapsed = time.time() - batch_start
        
        # Nếu chưa đủ thời gian, sleep để đạt đúng throughput
        if elapsed < target_batch_time:
            await asyncio.sleep(target_batch_time - elapsed)
        
        # Reset thời gian bắt đầu batch mới
        batch_start = time.time()
        
        # Trả về để thông báo có thể gửi batch mới
        yield

async def main():
    """Chương trình chính sử dụng asyncio"""
    global start_time, message_count
    
    # Reset counters
    message_count = 0
    start_time = time.time()
    
    print(f"Starting async benchmark")
    print(f"Target: {TARGET_THROUGHPUT} messages/second")
    print(f"Total messages to send: {TOTAL_MESSAGES}")
    
    # Tạo producer
    producer = create_producer()
    
    # Các tác vụ cần chạy
    tasks = []
    
    # Tạo rate limiter
    batch_size = 100
    rate_limit = rate_limiter(TARGET_THROUGHPUT, batch_size)
    
    # Tạo task periodic flush chạy nền
    flush_task = asyncio.create_task(periodic_flush(producer))
    
    # Tạo các producer tasks
    messages_sent = 0
    while messages_sent < TOTAL_MESSAGES:
        # Đợi rate limiter
        await anext(rate_limit)
        
        # Tính số lượng messages còn lại cần gửi
        remaining = TOTAL_MESSAGES - messages_sent
        current_batch = min(batch_size, remaining)
        
        # Tạo và đăng ký task để gửi batch messages
        task = asyncio.create_task(produce_messages(producer, 'app-logs', current_batch))
        tasks.append(task)
        
        messages_sent += current_batch
    
    # Đợi tất cả producer tasks hoàn thành
    await asyncio.gather(*tasks)
    
    # Hủy flush task không cần thiết nữa
    flush_task.cancel()
    
    # Đảm bảo tất cả messages đã được gửi
    remaining = producer.flush(timeout=30)
    
    # Tính toán kết quả cuối cùng
    end_time = time.time()
    elapsed = end_time - start_time
    final_throughput = message_count / elapsed
    
    # In kết quả benchmark
    print("\n===== ASYNC BENCHMARK RESULTS =====")
    print(f"Total messages sent: {message_count}")
    print(f"Time elapsed: {elapsed:.2f} seconds")
    print(f"Average throughput: {final_throughput:.2f} messages/second")
    print(f"Remaining in queue: {remaining}")
    if remaining > 0:
        print(f"WARNING: {remaining} messages weren't delivered")
    print("===================================")

if __name__ == "__main__":
    # Chạy coroutine main
    asyncio.run(main())
