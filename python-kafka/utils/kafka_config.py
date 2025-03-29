"""Các cấu hình cho Kafka Producer và Consumer"""

# Cấu hình cơ bản
DEFAULT_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}

# Cấu hình Producer cho throughput cao
HIGH_THROUGHPUT_PRODUCER_CONFIG = {
    **DEFAULT_CONFIG,
    'acks': '1',                        # Chỉ đợi leader xác nhận
    'linger.ms': '5',                   # Đợi 5ms để gom messages thành batch
    'batch.size': '32768',              # Kích thước batch (32KB) 
    'buffer.memory': '33554432',        # 32MB buffer
    'compression.type': 'snappy',       # Nén dữ liệu để giảm network overhead
    'queue.buffering.max.messages': '100000',  # Số lượng messages tối đa trong queue
    'max.in.flight.requests.per.connection': '5'  # Số lượng requests chưa hoàn thành đồng thời
}

# Cấu hình Producer cho độ tin cậy cao
HIGH_RELIABILITY_PRODUCER_CONFIG = {
    **DEFAULT_CONFIG,
    'acks': 'all',                      # Đợi tất cả replicas xác nhận
    'retries': '10',                    # Số lần thử lại khi gặp lỗi
    'retry.backoff.ms': '100',          # Thời gian giữa các lần thử lại
    'max.in.flight.requests.per.connection': '1',  # Đảm bảo thứ tự messages
    'enable.idempotence': 'true',       # Tránh duplicate messages
    'compression.type': 'snappy',       # Vẫn nén để tối ưu network
}

# Cấu hình Consumer với offset tự động commit
AUTO_COMMIT_CONSUMER_CONFIG = {
    **DEFAULT_CONFIG,
    'auto.offset.reset': 'earliest',     # Đọc từ đầu topic nếu không có offset
    'enable.auto.commit': 'true',        # Tự động commit offset
    'auto.commit.interval.ms': '1000',   # Commit mỗi 1 giây
    'max.poll.records': '500',           # Số records tối đa mỗi lần poll 
}

# Cấu hình Consumer với offset thủ công commit (để xử lý chính xác)
MANUAL_COMMIT_CONSUMER_CONFIG = {
    **DEFAULT_CONFIG,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',       # Tắt auto commit
    'max.poll.records': '100',           # Giảm số lượng records để xử lý an toàn hơn
}
