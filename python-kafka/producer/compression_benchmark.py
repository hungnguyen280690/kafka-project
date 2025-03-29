from confluent_kafka import Producer
import json
import time
import sys
import os
import socket
import threading
from tabulate import tabulate
sys.path.append('..')
from utils.log_generator import generate_log
from utils.kafka_config import HIGH_THROUGHPUT_PRODUCER_CONFIG

# Cài đặt tabulate nếu chưa có
# pip install tabulate

# Cấu hình benchmark
MESSAGES_PER_TEST = 10000       # Số lượng messages cho mỗi test case
WARMUP_MESSAGES = 1000          # Số lượng messages để warmup trước khi đo
TOPIC_NAME = 'benchmark-logs'   # Topic riêng cho benchmark

def create_producer(compression, batch_size=None, linger_ms=None):
    """Tạo producer với cấu hình cho benchmark"""
    config = HIGH_THROUGHPUT_PRODUCER_CONFIG.copy()
    
    # Cập nhật các tham số test
    config['compression.type'] = compression
    if batch_size:
        config['batch.size'] = str(batch_size)
    if linger_ms:
        config['linger.ms'] = str(linger_ms)
    
    config['client.id'] = f'benchmark-{socket.gethostname()}'
    
    return Producer(config)

def run_benchmark(compression_type, batch_size, linger_ms):
    """Chạy một test case với cấu hình cụ thể"""
    producer = create_producer(compression_type, batch_size, linger_ms)
    
    # Biến để theo dõi messages đã gửi
    messages_sent = 0
    messages_delivered = 0
    delivery_finished = threading.Event()
    
    # Callback function
    def delivery_callback(err, msg):
        nonlocal messages_delivered
        if err is None:
            messages_delivered += 1
            if messages_delivered >= MESSAGES_PER_TEST:
                delivery_finished.set()
    
    # Warmup phase
    print(f"Warming up producer with {WARMUP_MESSAGES} messages...")
    for _ in range(WARMUP_MESSAGES):
        log_data = generate_log()
        producer.produce(
            topic=TOPIC_NAME,
            key=log_data['service'],
            value=json.dumps(log_data),
            callback=None  # Không cần callback trong warmup
        )
        producer.poll(0)
    
    producer.flush()
    
    # Đo hiệu suất
    print(f"Starting benchmark with compression={compression_type}, batch_size={batch_size}, linger_ms={linger_ms}")
    start_time = time.time()
    
    # Gửi messages
    for _ in range(MESSAGES_PER_TEST):
        log_data = generate_log()
        producer.produce(
            topic=TOPIC_NAME,
            key=log_data['service'],
            value=json.dumps(log_data),
            callback=delivery_callback
        )
        messages_sent += 1
        producer.poll(0)
    
    # Đợi tất cả messages được xác nhận hoặc timeout
    producer.flush()
    delivery_success = delivery_finished.wait(timeout=30)
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Tính toán metrics
    throughput = messages_delivered / elapsed
    result = {
        'compression': compression_type,
        'batch_size': batch_size,
        'linger_ms': linger_ms,
        'messages': messages_delivered,
        'time': elapsed,
        'throughput': throughput
    }
    
    print(f"Benchmark completed: {messages_delivered} messages in {elapsed:.2f} seconds")
    print(f"Throughput: {throughput:.2f} messages/second")
    
    return result

def main():
    """Chạy tất cả các test case và so sánh kết quả"""
    # Các kiểu nén để test
    compression_types = ['none', 'gzip', 'snappy', 'lz4']
    
    # Các kích thước batch để test (bytes)
    batch_sizes = [16384, 32768, 65536]
    
    # Các giá trị linger_ms để test (milliseconds)
    linger_ms_values = [0, 5, 20]
    
    # Đảm bảo topic tồn tại (có thể bỏ qua nếu topic đã được tạo)
    
    # Test matrix
    results = []
    
    # Test các kiểu nén
    print("\n=== TESTING COMPRESSION TYPES ===")
    for compression in compression_types:
        result = run_benchmark(compression, 32768, 5)
        results.append(result)
    
    # Test các kích thước batch với snappy compression
    print("\n=== TESTING BATCH SIZES WITH SNAPPY COMPRESSION ===")
    for batch_size in batch_sizes:
        result = run_benchmark('snappy', batch_size, 5)
        results.append(result)
    
    # Test các giá trị linger_ms với snappy compression
    print("\n=== TESTING LINGER_MS VALUES WITH SNAPPY COMPRESSION ===")
    for linger_ms in linger_ms_values:
        result = run_benchmark('snappy', 32768, linger_ms)
        results.append(result)
    
    # In kết quả tổng hợp
    print("\n=== BENCHMARK RESULTS ===")
    
    # Format kết quả
    table_data = []
    for r in results:
        table_data.append([
            r['compression'],
            r['batch_size'],
            r['linger_ms'],
            f"{r['messages']}",
            f"{r['time']:.2f}s",
            f"{r['throughput']:.2f}"
        ])
    
    # In kết quả dạng bảng
    print(tabulate(
        table_data,
        headers=["Compression", "Batch Size", "Linger (ms)", "Messages", "Time", "Msgs/sec"],
        tablefmt="grid"
    ))
    
    # Tìm cấu hình tốt nhất
    best_result = max(results, key=lambda x: x['throughput'])
    print("\nBest configuration:")
    print(f"Compression: {best_result['compression']}")
    print(f"Batch Size: {best_result['batch_size']}")
    print(f"Linger ms: {best_result['linger_ms']}")
    print(f"Throughput: {best_result['throughput']:.2f} messages/second")

if __name__ == "__main__":
    main()
