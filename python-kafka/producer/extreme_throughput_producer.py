#!/usr/bin/env python3
"""
Extreme throughput Kafka producer using multiprocessing
Designed to achieve 100K+ messages per second on a single machine
"""

from confluent_kafka import Producer
import json
import socket
import time
import sys
import os
import signal
import psutil
from multiprocessing import Process, Queue, Value, Array, Manager
import queue  # Import queue module for Queue.Empty exception
import ctypes
import numpy as np
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log_generator import generate_log

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'app-logs'
NUM_PROCESSES = max(1, psutil.cpu_count() - 1)  # Use all but one CPU core
TOTAL_MESSAGES = 2_000_000  # Tăng lên 2M messages để benchmark lâu hơn
TARGET_THROUGHPUT = 100_000  # Target msgs/sec
BATCH_SIZE = 10_000  # Messages per batch for generator
QUEUE_SIZE = 100  # Number of batches in queue
MONITOR_INTERVAL = 0.5  # Seconds between monitoring updates

# Shared memory for stats
class SharedStats:
    def __init__(self):
        self.manager = Manager()
        self.success_count = Value(ctypes.c_longlong, 0)
        self.failure_count = Value(ctypes.c_longlong, 0)
        self.bytes_sent = Value(ctypes.c_longlong, 0)
        self.running = Value(ctypes.c_bool, True)
        self.start_time = Value(ctypes.c_double, 0.0)
        # For monitoring throughput history (circular buffer)
        self.throughput_history = self.manager.list([0.0] * 10)
        self.history_index = Value(ctypes.c_int, 0)

def create_optimized_producer():
    """Create extremely optimized Kafka producer"""
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': f'extreme-producer-{socket.gethostname()}-{os.getpid()}',
        
        # Ultra-optimized settings
        'batch.size': 1048576,        # 1MB batch size
        'linger.ms': 20,              # Wait up to 20ms to form batches
        'compression.type': 'zstd',   # Best compression/speed trade-off
        'acks': '1',                  # Only wait for leader acknowledgment
        
        # Massive buffer settings
        'queue.buffering.max.messages': 1000000,   # 1M messages in buffer
        'queue.buffering.max.kbytes': 1048576,     # 1GB buffer
        
        # Socket tuning
        'socket.send.buffer.bytes': 12582912,      # 12MB socket buffer
        'socket.receive.buffer.bytes': 4194304,    # 4MB receive buffer
        
        # Performance settings
        'max.in.flight.requests.per.connection': 30,
        'request.timeout.ms': 30000,               # 30 second timeout
        'message.timeout.ms': 45000,               # 45 second message timeout
        
        # Retry settings
        'retries': 2,                              # Fewer retries for speed
        'retry.backoff.ms': 100                    # Quick retry
    }
    return Producer(config)

def message_generator_process(message_queue, stats):
    """Process that continuously generates message batches"""
    print(f"[Generator] Starting message generator process")
    batch_count = 0
    
    try:
        while stats.running.value:
            # Generate a batch of messages
            batch = []
            for _ in range(BATCH_SIZE):
                log_data = generate_log()
                key = log_data['service'].encode('utf-8')
                value = json.dumps(log_data).encode('utf-8')
                # Pre-encode to bytes to avoid extra processing in producer
                batch.append((key, value))
            
            # Put batch in the queue, block if queue is full
            message_queue.put(batch)
            batch_count += 1
            
            # Print status occasionally
            if batch_count % 10 == 0:
                print(f"[Generator] Created {batch_count} batches ({batch_count * BATCH_SIZE:,} messages)")
                
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[Generator] Generator shutting down, created {batch_count * BATCH_SIZE:,} messages")

def delivery_callback(err, msg, stats):
    """Callback function when message is delivered"""
    if err:
        with stats.failure_count.get_lock():
            stats.failure_count.value += 1
        # Only print some errors to avoid console spam
        if stats.failure_count.value % 1000 == 0:
            print(f"ERROR: Message failed delivery: {err}")
    else:
        with stats.success_count.get_lock():
            stats.success_count.value += 1
        
        # Also track bytes sent
        if msg.value():
            with stats.bytes_sent.get_lock():
                stats.bytes_sent.value += len(msg.value())

def producer_process(process_id, message_queue, stats):
    """Producer process that sends messages from queue to Kafka"""
    # Create a dedicated producer for this process
    producer = create_optimized_producer()
    
    # Set per-process counters
    local_count = 0
    last_report_time = time.time()
    messages_since_report = 0
    
    print(f"[Producer-{process_id}] Starting producer process")
    
    try:
        while stats.running.value:
            try:
                # Get a batch from the queue with timeout
                batch = message_queue.get(timeout=1.0)
                
                # Send all messages in the batch
                for key, value in batch:
                    producer.produce(
                        topic=TOPIC_NAME,
                        key=key,
                        value=value,
                        callback=lambda err, msg, stats=stats: delivery_callback(err, msg, stats)
                    )
                
                # Poll to trigger callbacks
                producer.poll(0)
                
                # Update local counters
                local_count += len(batch)
                messages_since_report += len(batch)
                
                # Local throughput reporting
                now = time.time()
                if now - last_report_time >= 5.0:  # Report every 5 seconds
                    elapsed = now - last_report_time
                    local_throughput = messages_since_report / elapsed
                    print(f"[Producer-{process_id}] Sent {messages_since_report:,} messages "
                          f"({local_throughput:.1f} msgs/sec)")
                    last_report_time = now
                    messages_since_report = 0
                    
            except queue.Empty:  # Fixed: Now queue module is imported
                # Queue was empty, just continue
                continue
                
    except KeyboardInterrupt:
        pass
    finally:
        # Ensure all messages are sent before shutting down
        print(f"[Producer-{process_id}] Flushing remaining messages...")
        remaining = producer.flush(timeout=30)
        
        if remaining > 0:
            print(f"[Producer-{process_id}] WARNING: {remaining} messages weren't delivered")
        
        print(f"[Producer-{process_id}] Producer process completed: sent {local_count:,} messages")

def monitor_process(stats):
    """Process that monitors overall throughput and resource usage"""
    print(f"[Monitor] Starting monitoring process")
    
    # Initialize monitoring variables
    last_count = 0
    last_bytes = 0
    last_time = time.time()
    
    # For calculating moving average
    throughput_window = []
    
    try:
        while stats.running.value:
            time.sleep(MONITOR_INTERVAL)
            
            current_time = time.time()
            current_count = stats.success_count.value
            current_failures = stats.failure_count.value
            current_bytes = stats.bytes_sent.value
            
            # Calculate metrics
            interval_count = current_count - last_count
            interval_bytes = current_bytes - last_bytes
            interval_time = current_time - last_time
            
            if interval_time > 0:
                current_throughput = interval_count / interval_time
                bytes_per_sec = interval_bytes / interval_time
                
                # Update throughput history
                with stats.history_index.get_lock():
                    idx = stats.history_index.value
                    stats.throughput_history[idx] = current_throughput
                    stats.history_index.value = (idx + 1) % len(stats.throughput_history)
                
                # Calculate totals
                elapsed_total = current_time - stats.start_time.value if stats.start_time.value > 0 else 0
                avg_throughput = current_count / elapsed_total if elapsed_total > 0 else 0
                
                # Get current resource usage
                cpu_percent = psutil.cpu_percent(interval=None)
                memory_usage = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
                
                # Calculate a running average
                throughput_window.append(current_throughput)
                if len(throughput_window) > 5:  # Keep last 5 readings
                    throughput_window.pop(0)
                avg_recent_throughput = sum(throughput_window) / len(throughput_window)
                
                # Format throughput with appropriate units
                if current_throughput > 1000000:
                    throughput_str = f"{current_throughput/1000000:.2f}M"
                elif current_throughput > 1000:
                    throughput_str = f"{current_throughput/1000:.1f}K"
                else:
                    throughput_str = f"{current_throughput:.1f}"
                
                # Format transfer rate
                if bytes_per_sec > 1048576:
                    transfer_str = f"{bytes_per_sec/1048576:.1f} MB/s"
                else:
                    transfer_str = f"{bytes_per_sec/1024:.1f} KB/s"
                
                # Clear line and print status
                print(f"\r[{elapsed_total:.1f}s] "
                      f"Msgs: {current_count:,} "
                      f"| Rate: {throughput_str} msgs/sec "
                      f"| Avg: {avg_recent_throughput:.1f} msgs/sec "
                      f"| {transfer_str} "
                      f"| CPU: {cpu_percent:.1f}% "
                      f"| Mem: {memory_usage:.0f}MB "
                      f"| Queue: {message_queue.qsize()}", end="")
                
                # Print newline occasionally to avoid long output lines
                if int(current_time) % 10 == 0 and int(last_time) % 10 != 0:
                    print("")
            
            # Update for next interval
            last_count = current_count
            last_bytes = current_bytes
            last_time = current_time
            
    except KeyboardInterrupt:
        pass
    finally:
        print("\n[Monitor] Monitoring process shutting down")

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully shutdown"""
    print("\n\nShutting down all processes... Please wait.")
    stats.running.value = False

def calculate_final_stats(stats):
    """Calculate and print final performance statistics"""
    end_time = time.time()
    duration = end_time - stats.start_time.value
    
    success_count = stats.success_count.value
    failure_count = stats.failure_count.value
    total_sent = success_count + failure_count
    
    if duration > 0:
        throughput = success_count / duration
        mb_sent = stats.bytes_sent.value / (1024 * 1024)
        mb_per_sec = mb_sent / duration
    else:
        throughput = 0
        mb_sent = 0
        mb_per_sec = 0
    
    # Get max throughput from history
    max_throughput = max(stats.throughput_history)
    
    # Print final stats
    print("\n")
    print("=" * 70)
    print(f"EXTREME THROUGHPUT BENCHMARK RESULTS")
    print("=" * 70)
    print(f"Duration:              {duration:.2f} seconds")
    print(f"Messages sent:         {success_count:,}")
    print(f"Messages failed:       {failure_count:,}")
    print(f"Total attempted:       {total_sent:,}")
    print(f"Average throughput:    {throughput:.2f} messages/second")
    print(f"Peak throughput:       {max_throughput:.2f} messages/second")
    print(f"Data transferred:      {mb_sent:.2f} MB")
    print(f"Transfer rate:         {mb_per_sec:.2f} MB/second")
    print(f"Processes used:        {NUM_PROCESSES}")
    print("=" * 70)

def run_benchmark():
    """Main function to run the extreme throughput benchmark"""
    global message_queue, stats
    
    print("\n" + "=" * 70)
    print("KAFKA EXTREME THROUGHPUT PRODUCER BENCHMARK")
    print(f"Target: {TARGET_THROUGHPUT:,} messages/second")
    print(f"Processes: {NUM_PROCESSES}")
    print(f"Total messages: {TOTAL_MESSAGES:,}")
    print("=" * 70 + "\n")
    
    # Initialize shared statistics
    stats = SharedStats()
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create message queue
    message_queue = Queue(maxsize=QUEUE_SIZE)
    
    # Start generator process first
    generator = Process(target=message_generator_process, args=(message_queue, stats))
    generator.daemon = True
    generator.start()
    
    # Give generator a head start to fill the queue
    time.sleep(1)
    
    # Start monitor process
    monitor = Process(target=monitor_process, args=(stats,))
    monitor.daemon = True
    monitor.start()
    
    # Set start time just before starting producers
    stats.start_time.value = time.time()
    
    # Start producer processes
    producers = []
    for i in range(NUM_PROCESSES):
        p = Process(target=producer_process, args=(i, message_queue, stats))
        producers.append(p)
        p.start()
    
    # Wait for producers to finish or until target message count is reached
    try:
        while any(p.is_alive() for p in producers):
            if stats.success_count.value >= TOTAL_MESSAGES:
                print(f"\nReached target of {TOTAL_MESSAGES:,} messages, stopping...")
                stats.running.value = False
                break
            time.sleep(0.1)
    except KeyboardInterrupt:
        # This will be caught by signal handler
        pass
    
    # Wait for all processes to finish
    print("\nWaiting for all processes to finish...")
    for p in producers:
        p.join(timeout=10)
    
    # Stop generator and monitor
    stats.running.value = False
    generator.join(timeout=5)
    monitor.join(timeout=5)
    
    # Calculate and display final stats
    calculate_final_stats(stats)
    
    print("\nExtreme throughput benchmark completed.")

if __name__ == "__main__":
    run_benchmark()