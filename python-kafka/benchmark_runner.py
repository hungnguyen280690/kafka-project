#!/usr/bin/env python3
import time
import argparse
import sys
import os
import subprocess
import signal
from tabulate import tabulate

def run_command(command, timeout=None):
    """Chạy một lệnh và trả về output, trạng thái"""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )
    
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return stdout, stderr, process.returncode
    except subprocess.TimeoutExpired:
        process.kill()
        return "", "Timeout expired", -1

def ensure_topic_exists(topic, partitions=4):
    """Đảm bảo topic đã tồn tại, tạo nếu chưa có"""
    # Kiểm tra topic đã tồn tại chưa
    cmd = f'docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092'
    stdout, _, _ = run_command(cmd)
    
    if topic not in stdout.split():
        print(f"Creating topic {topic} with {partitions} partitions...")
        cmd = f'docker exec -it kafka kafka-topics --create --topic {topic} --bootstrap-server localhost:9092 --partitions {partitions} --replication-factor 1'
        _, stderr, status = run_command(cmd)
        
        if status != 0:
            print(f"Error creating topic: {stderr}")
            return False
    
    return True

def run_benchmark(producer_type, duration=30):
    """Chạy benchmark với producer được chỉ định"""
    print(f"\n=== Running benchmark with {producer_type} for {duration} seconds ===\n")
    
    # Map producer type to actual command
    commands = {
        'simple': 'python -m producer.simple_producer',
        'high-throughput': 'python -m producer.high_throughput_producer',
        'async': 'python -m producer.async_producer',
        'compression': 'python -m producer.compression_benchmark'
    }
    
    if producer_type not in commands:
        print(f"Unknown producer type: {producer_type}")
        return
    
    # Run the command with timeout
    cmd = commands[producer_type]
    process = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Set up a timer to terminate the process after duration
    def terminate_process():
        if process.poll() is None:  # If process is still running
            process.send_signal(signal.SIGINT)  # Send CTRL+C
            time.sleep(2)
            if process.poll() is None:  # If still running after 2s
                process.terminate()
    
    # We'll let the benchmark run for its duration and then collect output
    try:
        if duration > 0:
            time.sleep(duration)
            terminate_process()
        
        stdout, stderr = process.communicate()
        
        # Process and display results
        print("=== Results ===")
        print(stdout)
        
        if stderr:
            print("=== Errors ===")
            print(stderr)
        
    except KeyboardInterrupt:
        terminate_process()
        print("\nBenchmark interrupted by user")

def main():
    """Main function to parse arguments and run benchmarks"""
    parser = argparse.ArgumentParser(description='Run Kafka Producer benchmarks')
    parser.add_argument('type', choices=['simple', 'high-throughput', 'async', 'compression', 'all'],
                        help='Type of producer to benchmark')
    parser.add_argument('--duration', type=int, default=30,
                        help='Duration of each benchmark in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Ensure the necessary topics exist
    ensure_topic_exists('app-logs', 4)
    ensure_topic_exists('benchmark-logs', 4)
    
    # Run the specified benchmark
    if args.type == 'all':
        # Run all benchmarks in sequence
        for producer_type in ['simple', 'high-throughput', 'async', 'compression']:
            run_benchmark(producer_type, args.duration)
    else:
        run_benchmark(args.type, args.duration)

if __name__ == "__main__":
    main()
