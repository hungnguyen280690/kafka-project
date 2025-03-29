from confluent_kafka import Producer
import json
import socket
import time
import threading
import sys
import psutil
from concurrent.futures import ThreadPoolExecutor
import os

# Thêm thư mục gốc vào sys.path để import module utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log_generator import generate_log

# Thông số benchmark
THREAD_COUNT = 4
TARGET_THROUGHPUT = 1000  # messages/second
TOTAL_MESSAGES = 10000    # Tổng số messages cần gửi
TOPIC_NAME = 'app-logs'
BOOTSTRAP_SERVERS = 'localhost:9092'

# Biến đếm toàn cục để tracking performance
success_count = 0
failure_count = 0
start_time = None

# Lock để đồng bộ hóa khi cập nhật biến toàn cục
count_lock = threading.Lock()

def create_optimized_producer():
    """Tạo producer được tối ưu cho throughput cao"""
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': f'throughput-producer-{socket.gethostname()}',
        
        # Cấu hình tối ưu cho confluent-kafka-python
        'acks': '1',                              # Chỉ đợi leader xác nhận
        'linger.ms': 5,                          # Đợi 5ms để gom các messages thành batch
        'batch.size': 32768,                     # Kích thước batch tối đa (32KB)
        'compression.type': 'snappy',           # Nén dữ liệu để giảm băng thông
        
        # Cấu hình retry 
        'retries': 3,                            # Số lần retry nếu gặp lỗi
        'retry.backoff.ms': 100,                # Thời gian giữa các lần retry
        
        # Cấu hình hiệu suất
        'queue.buffering.max.messages': 100000, # Số lượng messages tối đa trong buffer
        'queue.buffering.max.kbytes': 32768,    # Kích thước buffer tối đa (32MB)
        'max.in.flight.requests.per.connection': 5, # Số lượng requests chưa hoàn thành tối đa
    }
    return Producer(config)

def delivery_callback(err, msg):
    """Callback khi message được gửi đi"""
    global success_count, failure_count
    
    with count_lock:
        if err is None:
            # Message gửi thành công
            success_count += 1
            
            # Log mỗi 1000 messages
            if success_count % 1000 == 0:
                elapsed = time.time() - start_time
                current_rate = success_count / elapsed
                print(f"Sent {success_count} messages. Rate: {current_rate:.2f} msgs/sec")
        else:
            # Message gửi thất bại
            failure_count += 1
            print(f"Message delivery failed: {err}")

def producer_task(producer, thread_id, messages_per_thread):
    """Task cho mỗi producer thread"""
    messages_sent = 0
    
    # Số lượng message cần gửi mỗi batch (100ms)
    batch_size = TARGET_THROUGHPUT // (THREAD_COUNT * 10)  # 10 batches per second
    
    while messages_sent < messages_per_thread:
        batch_start = time.time()
        
        # Gửi một batch messages
        remaining = min(batch_size, messages_per_thread - messages_sent)
        for _ in range(remaining):
            log_data = generate_log()
            key = log_data['service']
            value = json.dumps(log_data)
            
            # Gửi message không đồng bộ
            producer.produce(
                topic=TOPIC_NAME,
                key=key,
                value=value,
                callback=delivery_callback
            )
        
        # Poll để xử lý callbacks (không blocking)
        producer.poll(0)
        
        # Cập nhật số lượng đã gửi
        messages_sent += remaining
        
        # Điều chỉnh tốc độ gửi
        elapsed = time.time() - batch_start
        if elapsed < 0.1:  # Mỗi batch nên kéo dài 100ms
            time.sleep(0.1 - elapsed)
    
    print(f"Thread {thread_id} completed: sent {messages_sent} messages")

def log_stats(duration):
    """In thông tin hiệu suất"""
    global success_count, failure_count
    
    rate = success_count / duration
    cpu_percent = psutil.cpu_percent()
    memory_use = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)  # MB
    
    print("\n========== BENCHMARK RESULTS ==========")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Messages sent successfully: {success_count}")
    print(f"Messages failed: {failure_count}")
    print(f"Throughput: {rate:.2f} messages/second")
    print(f"CPU usage: {cpu_percent:.1f}%")
    print(f"Memory usage: {memory_use:.1f} MB")
    print("=======================================\n")

def benchmark():
    """Thực hiện benchmark đa luồng"""
    global start_time, success_count, failure_count
    
    print(f"Starting benchmark with {THREAD_COUNT} threads")
    print(f"Target: {TARGET_THROUGHPUT} messages/second")
    print(f"Total messages to send: {TOTAL_MESSAGES}")
    
    # Reset counters
    success_count = 0
    failure_count = 0
    
    # Số lượng messages mỗi thread cần gửi
    messages_per_thread = TOTAL_MESSAGES // THREAD_COUNT
    
    # Tạo producer (một producer cho mỗi thread để tránh concurrency issues)
    producers = [create_optimized_producer() for _ in range(THREAD_COUNT)]
    
    # Ghi lại thời gian bắt đầu
    start_time = time.time()
    
    # Khởi tạo thread pool
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        # Submit producer tasks
        futures = [
            executor.submit(producer_task, producers[i], i, messages_per_thread)
            for i in range(THREAD_COUNT)
        ]
        
        # Đợi tất cả các tasks hoàn thành
        for future in futures:
            future.result()
    
    # Đảm bảo tất cả messages được gửi đi
    for producer in producers:
        producer.flush(timeout=10)
    
    # Ghi lại thời gian kết thúc
    end_time = time.time()
    duration = end_time - start_time
    
    # In kết quả benchmark
    log_stats(duration)

if __name__ == "__main__":
    benchmark()
