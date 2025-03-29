import time
import psutil
import threading
import os
from tabulate import tabulate

class BenchmarkMonitor:
    """Lớp để theo dõi hiệu suất hệ thống và benchmark của Kafka producer/consumer"""
    
    def __init__(self, interval=1.0):
        """
        Khởi tạo monitor
        
        Args:
            interval (float): Khoảng thời gian giữa các lần ghi nhận metrics (giây)
        """
        self.interval = interval
        self.start_time = None
        self.metrics = []
        self.running = False
        self.monitor_thread = None
        self.message_count = 0
        self.process = psutil.Process(os.getpid())
    
    def start(self):
        """Bắt đầu monitor"""
        self.start_time = time.time()
        self.running = True
        self.metrics = []
        self.message_count = 0
        
        # Bắt đầu thread monitor
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        print(f"Benchmark monitor started at {time.strftime('%H:%M:%S')}")
    
    def stop(self):
        """Dừng monitor"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
        
        print(f"Benchmark monitor stopped at {time.strftime('%H:%M:%S')}")
        
        # In summary
        self.print_summary()
    
    def record_message(self, count=1):
        """Ghi nhận số lượng messages đã xử lý"""
        self.message_count += count
    
    def _collect_metrics(self):
        """Thu thập metrics của hệ thống và ứng dụng"""
        now = time.time()
        elapsed = now - self.start_time
        
        # CPU và Memory
        cpu_percent = self.process.cpu_percent()
        memory_info = self.process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)  # MB
        
        # Throughput
        throughput = self.message_count / elapsed if elapsed > 0 else 0
        
        # Lưu metrics
        self.metrics.append({
            'timestamp': now,
            'elapsed': elapsed,
            'message_count': self.message_count,
            'throughput': throughput,
            'cpu_percent': cpu_percent,
            'memory_mb': memory_mb
        })
    
    def _monitor_loop(self):
        """Loop chính để thu thập metrics định kỳ"""
        while self.running:
            self._collect_metrics()
            time.sleep(self.interval)
    
    def print_summary(self):
        """In tóm tắt kết quả benchmark"""
        if not self.metrics:
            print("No metrics collected")
            return
        
        # Lấy metric cuối cùng cho kết quả tổng thể
        final = self.metrics[-1]
        
        print("\n===== BENCHMARK SUMMARY =====")
        print(f"Duration: {final['elapsed']:.2f} seconds")
        print(f"Total messages: {final['message_count']}")
        print(f"Avg throughput: {final['throughput']:.2f} msgs/sec")
        print(f"Max CPU usage: {max(m['cpu_percent'] for m in self.metrics):.2f}%")
        print(f"Max memory: {max(m['memory_mb'] for m in self.metrics):.2f} MB")
        
        # In biểu đồ throughput theo thời gian
        self._print_throughput_timeline()
    
    def _print_throughput_timeline(self):
        """In biểu đồ throughput theo thời gian"""
        # Chỉ lấy mỗi phút một điểm để tránh quá nhiều dữ liệu
        sample_interval = max(1, int(len(self.metrics) / 20))
        samples = self.metrics[::sample_interval]
        
        data = []
        for i, m in enumerate(samples):
            data.append([
                f"{m['elapsed']:.1f}s",
                f"{m['throughput']:.2f}",
                f"{m['cpu_percent']:.1f}%",
                f"{m['memory_mb']:.1f}"
            ])
        
        print("\nPerformance Timeline:")
        print(tabulate(
            data,
            headers=["Time", "Msgs/sec", "CPU%", "Mem(MB)"],
            tablefmt="grid"
        ))

def display_producer_config(config, title="Producer Configuration"):
    """Hiển thị cấu hình producer theo định dạng đẹp"""
    print(f"\n===== {title} =====")
    
    # Nhóm các cấu hình theo loại
    groups = {
        "Basic": ["bootstrap.servers", "client.id"],
        "Performance": ["batch.size", "linger.ms", "buffer.memory", "compression.type", 
                       "max.in.flight.requests.per.connection"],
        "Reliability": ["acks", "retries", "retry.backoff.ms", "enable.idempotence"],
        "Other": []
    }
    
    # Phân loại các cấu hình
    for key in config:
        categorized = False
        for group in groups:
            if key in groups[group]:
                categorized = True
                break
        if not categorized:
            groups["Other"].append(key)
    
    # Hiển thị theo nhóm
    for group, keys in groups.items():
        if not keys:
            continue
            
        print(f"\n{group}:")
        for key in keys:
            if key in config:
                print(f"  {key}: {config[key]}")
    
    print("==========================")
