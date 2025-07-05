#!/usr/bin/env python3
"""
Enhanced Throughput Benchmark for Prospector with detailed per-second metrics.
"""

import time
import json
import argparse
import signal
import statistics
from typing import Dict, List, Optional
from datetime import datetime
import numpy as np
from collections import deque

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient
import redis

class EnhancedBenchmark:
    def __init__(self):
        self.kafka_brokers = "localhost:9092"
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.consumer_group = f"benchmark-{int(time.time())}"
        self.running = True
        
        # Per-second tracking
        self.per_second_metrics = []
        
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        print("\n\n⚠️  Interrupt received, stopping benchmark...")
        self.running = False
    
    def benchmark_kafka_with_detailed_metrics(self, 
                                            topic: str = "portfolio-updates-v2",
                                            duration_seconds: int = 30,
                                            messages_to_read: Optional[int] = None,
                                            from_beginning: bool = False,
                                            from_offset: Optional[int] = None) -> Dict:
        """
        Benchmark Kafka read performance with detailed per-second metrics.
        """
        print(f"\n📊 Enhanced Kafka Read Performance Benchmark")
        print(f"   Topic: {topic}")
        print(f"   Consumer Group: {self.consumer_group}")
        
        # Get topic metadata
        admin = AdminClient({'bootstrap.servers': self.kafka_brokers})
        metadata = admin.list_topics(timeout=10)
        
        if topic not in metadata.topics:
            print(f"   ❌ Topic '{topic}' not found!")
            return {}
        
        topic_metadata = metadata.topics[topic]
        num_partitions = len(topic_metadata.partitions)
        
        # Get partition sizes
        consumer_temp = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f'temp-{int(time.time())}'
        })
        
        total_messages = 0
        partition_info = []
        
        for p in range(num_partitions):
            tp = TopicPartition(topic, p)
            low, high = consumer_temp.get_watermark_offsets(tp)
            messages = high - low
            partition_info.append(f"  Partition {p}: {messages:,} messages")
            total_messages += messages
        
        consumer_temp.close()
        
        print("\n".join(partition_info))
        print(f"   Total Messages Available: {total_messages:,}")
        print(f"   Partitions: {num_partitions}")
        
        if messages_to_read:
            print(f"   Messages to Read: {messages_to_read:,}")
        else:
            print(f"   Test Duration: {duration_seconds} seconds")
        
        # Configure consumer for maximum throughput
        config = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': self.consumer_group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
            'fetch.min.bytes': 10 * 1024 * 1024,  # 10MB
            'fetch.max.bytes': 50 * 1024 * 1024,  # 50MB
            'fetch.wait.max.ms': 10,
            'max.partition.fetch.bytes': 10 * 1024 * 1024,
            'queued.min.messages': 100000,
            'queued.max.messages.kbytes': 1048576,
            'receive.message.max.bytes': 100 * 1024 * 1024,
        }
        
        consumer = Consumer(config)
        
        # Set up partitions
        partitions = []
        for p in range(num_partitions):
            tp = TopicPartition(topic, p)
            if from_beginning:
                tp.offset = OFFSET_BEGINNING
            elif from_offset is not None:
                tp.offset = from_offset
            partitions.append(tp)
        
        if from_beginning or from_offset is not None:
            consumer.assign(partitions)
        else:
            consumer.subscribe([topic])
        
        print("\n⏱️  Starting benchmark... (Per-second metrics will be displayed)")
        print("\n" + "="*80)
        print(f"{'Time':>6} | {'Messages/s':>12} | {'MB/s':>10} | {'Total Msgs':>12} | {'Avg Latency':>12}")
        print("="*80)
        
        # Metrics tracking
        messages_read = 0
        bytes_read = 0
        start_time = time.time()
        last_second = int(time.time() - start_time)
        
        # Per-second tracking
        second_messages = 0
        second_bytes = 0
        second_latencies = deque()
        
        # Overall tracking
        all_latencies = []
        
        try:
            while self.running:
                # Check limits
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                if messages_to_read and messages_read >= messages_to_read:
                    break
                
                # Read message
                batch_start = time.time()
                msg = consumer.poll(timeout=0.1)  # Shorter timeout for responsiveness
                
                if msg is None:
                    continue
                    
                if msg.error():
                    print(f"\nError: {msg.error()}")
                    continue
                
                # Record metrics
                messages_read += 1
                second_messages += 1
                
                msg_size = len(msg.value()) if msg.value() else 0
                bytes_read += msg_size
                second_bytes += msg_size
                
                batch_latency = (time.time() - batch_start) * 1000  # ms
                all_latencies.append(batch_latency)
                second_latencies.append(batch_latency)
                
                # Check if we've moved to a new second
                current_second = int(time.time() - start_time)
                if current_second > last_second:
                    # Calculate per-second metrics
                    if second_messages > 0:
                        avg_latency = statistics.mean(second_latencies)
                        mb_per_sec = second_bytes / (1024 * 1024)
                        
                        # Store metrics
                        self.per_second_metrics.append({
                            'second': last_second + 1,
                            'messages': second_messages,
                            'mb': mb_per_sec,
                            'avg_latency_ms': avg_latency
                        })
                        
                        # Print per-second update
                        print(f"{last_second + 1:6d} | {second_messages:12,} | {mb_per_sec:10.2f} | "
                              f"{messages_read:12,} | {avg_latency:10.2f} ms")
                    
                    # Reset per-second counters
                    last_second = current_second
                    second_messages = 0
                    second_bytes = 0
                    second_latencies.clear()
            
            # Print final second if any messages remain
            if second_messages > 0:
                avg_latency = statistics.mean(second_latencies) if second_latencies else 0
                mb_per_sec = second_bytes / (1024 * 1024)
                print(f"{last_second + 1:6d} | {second_messages:12,} | {mb_per_sec:10.2f} | "
                      f"{messages_read:12,} | {avg_latency:10.2f} ms")
            
            print("="*80)
            
            # Calculate final metrics
            total_time = time.time() - start_time
            
            if total_time > 0 and messages_read > 0:
                overall_rate = messages_read / total_time
                overall_mb_rate = (bytes_read / (1024 * 1024)) / total_time
                avg_msg_size = bytes_read / messages_read
                
                print(f"\n📊 Final Results:")
                print(f"   Total Messages Read: {messages_read:,}")
                print(f"   Total Data Read: {bytes_read / (1024 * 1024):.2f} MB")
                print(f"   Duration: {total_time:.2f} seconds")
                print(f"   Average Rate: {overall_rate:,.0f} messages/second")
                print(f"   Average Throughput: {overall_mb_rate:.2f} MB/second")
                print(f"   Average Message Size: {avg_msg_size:.0f} bytes")
                
                if all_latencies:
                    print(f"\n📈 Latency Statistics:")
                    print(f"   P50: {np.percentile(all_latencies, 50):.2f} ms")
                    print(f"   P95: {np.percentile(all_latencies, 95):.2f} ms")
                    print(f"   P99: {np.percentile(all_latencies, 99):.2f} ms")
                    print(f"   Average: {np.mean(all_latencies):.2f} ms")
                
                # Show peak performance
                if self.per_second_metrics:
                    peak_msgs = max(m['messages'] for m in self.per_second_metrics)
                    peak_mb = max(m['mb'] for m in self.per_second_metrics)
                    print(f"\n🚀 Peak Performance:")
                    print(f"   Peak Messages/s: {peak_msgs:,}")
                    print(f"   Peak MB/s: {peak_mb:.2f}")
            
            return {
                'messages_read': messages_read,
                'bytes_read': bytes_read,
                'duration_seconds': total_time,
                'messages_per_second': overall_rate if 'overall_rate' in locals() else 0,
                'mb_per_second': overall_mb_rate if 'overall_mb_rate' in locals() else 0,
                'per_second_metrics': self.per_second_metrics
            }
            
        finally:
            consumer.close()

def main():
    parser = argparse.ArgumentParser(description='Enhanced Prospector Throughput Benchmark')
    parser.add_argument('--topic', type=str, default='portfolio-updates-v2',
                        help='Kafka topic to benchmark')
    parser.add_argument('--duration', type=int, default=30,
                        help='Duration in seconds (default: 30)')
    parser.add_argument('--messages', type=int, default=None,
                        help='Number of messages to read (overrides duration)')
    parser.add_argument('--from-beginning', action='store_true',
                        help='Read from beginning of topic')
    parser.add_argument('--from-offset', type=int, default=None,
                        help='Read from specific offset')
    
    args = parser.parse_args()
    
    benchmark = EnhancedBenchmark()
    
    print("🚀 Starting Enhanced Prospector Throughput Benchmark")
    print(f"   Press Ctrl+C to stop at any time\n")
    
    # Run benchmark
    benchmark.benchmark_kafka_with_detailed_metrics(
        topic=args.topic,
        duration_seconds=args.duration if not args.messages else None,
        messages_to_read=args.messages,
        from_beginning=args.from_beginning,
        from_offset=args.from_offset
    )

if __name__ == "__main__":
    main()