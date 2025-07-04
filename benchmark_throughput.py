#!/usr/bin/env python3
"""
Throughput Benchmark for Prospector Risk Calculator

Tests read and processing speeds for:
- Kafka message consumption
- Bytewax processing pipeline
- Redis read/write operations

Does NOT clear existing data - measures actual throughput on real data.
"""

import time
import json
import redis
import argparse
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END
from collections import defaultdict
import threading
import signal
import sys


class ThroughputBenchmark:
    def __init__(self, 
                 kafka_brokers: str = "localhost:9092",
                 redis_host: str = "localhost",
                 redis_port: int = 6379,
                 consumer_group: str = None):
        
        self.kafka_brokers = kafka_brokers
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.consumer_group = consumer_group or "benchmark"
        self.running = True
        
        # Metrics storage
        self.metrics = {
            'kafka_read': {'count': 0, 'bytes': 0, 'latencies': []},
            'redis_read': {'count': 0, 'latencies': []},
            'redis_write': {'count': 0, 'latencies': []},
            'processing': {'count': 0, 'latencies': []}
        }
        self.start_time = None
        
    def get_topic_size(self, topic: str) -> Tuple[int, int]:
        """Get total messages and size for a topic across all partitions."""
        consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': 'size-checker',
            'enable.auto.commit': False
        })
        
        try:
            metadata = consumer.list_topics(topic)
            topic_metadata = metadata.topics[topic]
            
            total_messages = 0
            total_lag = 0
            
            for partition_id in topic_metadata.partitions:
                tp = TopicPartition(topic, partition_id)
                low, high = consumer.get_watermark_offsets(tp)
                partition_messages = high - low
                total_messages += partition_messages
                print(f"  Partition {partition_id}: {partition_messages:,} messages (offsets {low} to {high})")
            
            return total_messages, len(topic_metadata.partitions)
            
        finally:
            consumer.close()
    
    def benchmark_kafka_read(self, 
                           topic: str, 
                           max_messages: int = None,
                           duration_seconds: int = None,
                           from_beginning: bool = False,
                           from_offset: Optional[int] = None) -> Dict:
        """Benchmark Kafka read throughput."""
        print(f"\n📊 Benchmarking Kafka Read Performance")
        print(f"   Topic: {topic}")
        print(f"   Consumer Group: {self.consumer_group}")
        
        # Get topic info
        total_messages, num_partitions = self.get_topic_size(topic)
        print(f"   Total Messages Available: {total_messages:,}")
        print(f"   Partitions: {num_partitions}")
        
        if duration_seconds:
            print(f"   Test Duration: {duration_seconds} seconds")
            messages_to_read = total_messages  # Read as many as possible in time limit
        elif max_messages:
            messages_to_read = min(max_messages, total_messages)
            print(f"   Messages to Read: {messages_to_read:,}")
        else:
            messages_to_read = total_messages
            print(f"   Messages to Read: ALL ({messages_to_read:,})")
        
        # Configure consumer for maximum throughput
        config = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': self.consumer_group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',  # Always start from earliest available
            'fetch.min.bytes': 10 * 1024 * 1024,  # 10MB - larger batches
            'fetch.max.bytes': 50 * 1024 * 1024,  # 50MB max per fetch
            'fetch.wait.max.ms': 10,  # Don't wait long for batches
            'max.partition.fetch.bytes': 10 * 1024 * 1024,  # 10MB per partition
            'queued.min.messages': 100000,  # Large internal queue
            'queued.max.messages.kbytes': 1048576,  # 1GB internal queue
            'receive.message.max.bytes': 100 * 1024 * 1024,  # 100MB max message
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
        
        print("\n⏱️  Starting read benchmark...")
        
        messages_read = 0
        bytes_read = 0
        start_time = time.time()
        last_report_time = start_time
        batch_latencies = []
        
        try:
            while self.running:
                # Check time limit if set
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
                
                # Check message limit if set
                if messages_read >= messages_to_read:
                    break
                
                batch_start = time.time()
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue
                
                # Record metrics
                messages_read += 1
                msg_size = len(msg.value()) if msg.value() else 0
                bytes_read += msg_size
                
                batch_latency = (time.time() - batch_start) * 1000  # ms
                batch_latencies.append(batch_latency)
                
                # Progress report every second
                current_time = time.time()
                if current_time - last_report_time >= 1.0:
                    elapsed = current_time - start_time
                    rate = messages_read / elapsed
                    mb_rate = (bytes_read / (1024 * 1024)) / elapsed
                    
                    if duration_seconds:
                        remaining = duration_seconds - elapsed
                        print(f"\r  Messages: {messages_read:,} | Time: {elapsed:.0f}s/{duration_seconds}s | "
                              f"Rate: {rate:.0f} msg/s | {mb_rate:.2f} MB/s", end='', flush=True)
                    else:
                        progress = (messages_read / messages_to_read) * 100
                        print(f"\r  Progress: {messages_read:,}/{messages_to_read:,} ({progress:.1f}%) | "
                              f"Rate: {rate:.0f} msg/s | {mb_rate:.2f} MB/s", end='', flush=True)
                    
                    last_report_time = current_time
            
            # Final metrics
            total_time = time.time() - start_time
            
            metrics = {
                'messages_read': messages_read,
                'bytes_read': bytes_read,
                'duration_seconds': total_time,
                'messages_per_second': messages_read / total_time if total_time > 0 else 0,
                'mb_per_second': (bytes_read / (1024 * 1024)) / total_time if total_time > 0 else 0,
                'avg_message_size': bytes_read / messages_read if messages_read > 0 else 0,
                'latency_p50': np.percentile(batch_latencies, 50) if batch_latencies else 0,
                'latency_p95': np.percentile(batch_latencies, 95) if batch_latencies else 0,
                'latency_p99': np.percentile(batch_latencies, 99) if batch_latencies else 0,
            }
            
            print(f"\n\n✅ Kafka Read Benchmark Complete!")
            return metrics
            
        finally:
            consumer.close()
    
    def benchmark_redis_ops(self, duration_seconds: int = 10) -> Dict:
        """Benchmark Redis read/write performance."""
        print(f"\n📊 Benchmarking Redis Performance")
        print(f"   Test Duration: {duration_seconds} seconds per operation type")
        
        # Get sample keys for reading - try multiple patterns
        risk_keys = self.redis_client.keys("risk:*")
        if not risk_keys:
            risk_keys = self.redis_client.keys("stats:*")
        if not risk_keys:
            risk_keys = self.redis_client.keys("*")  # Get any keys
        
        if not risk_keys:
            print("   ⚠️  No data found in Redis. Skipping Redis benchmark.")
            return {
                'read': {'error': 'No data in Redis'},
                'write': {'operations': 0, 'duration_seconds': 0, 'ops_per_second': 0}
            }
        
        print(f"   Available Keys: {len(risk_keys)}")
        
        # Benchmark reads
        print("\n   Testing Redis Reads...")
        read_latencies = []
        read_count = 0
        read_start = time.time()
        last_report = read_start
        
        while time.time() - read_start < duration_seconds:
            key = risk_keys[read_count % len(risk_keys)]
            
            op_start = time.time()
            # Handle different key types
            if key.startswith('stats:'):
                value = self.redis_client.hgetall(key)
            else:
                value = self.redis_client.get(key)
            op_latency = (time.time() - op_start) * 1000  # ms
            read_latencies.append(op_latency)
            read_count += 1
            
            if time.time() - last_report >= 1.0:
                rate = read_count / (time.time() - read_start)
                print(f"\r     Read Operations: {read_count:,} | Rate: {rate:.0f} ops/s", 
                      end='', flush=True)
                last_report = time.time()
        
        read_duration = time.time() - read_start
        
        # Benchmark writes
        print(f"\n   Testing Redis Writes...")
        write_latencies = []
        write_count = 0
        write_start = time.time()
        last_report = write_start
        
        while time.time() - write_start < duration_seconds:
            key = f"benchmark:write_{write_count}"
            value = json.dumps({
                'timestamp': time.time(),
                'value': np.random.random(),
                'index': write_count
            })
            
            op_start = time.time()
            self.redis_client.setex(key, 60, value)  # 1 minute TTL
            op_latency = (time.time() - op_start) * 1000  # ms
            write_latencies.append(op_latency)
            write_count += 1
            
            if time.time() - last_report >= 1.0:
                rate = write_count / (time.time() - write_start)
                print(f"\r     Write Operations: {write_count:,} | Rate: {rate:.0f} ops/s", 
                      end='', flush=True)
                last_report = time.time()
        
        write_duration = time.time() - write_start
        
        # Clean up benchmark keys
        for key in self.redis_client.keys("benchmark:*"):
            self.redis_client.delete(key)
        
        metrics = {
            'read': {
                'operations': read_count,
                'duration_seconds': read_duration,
                'ops_per_second': read_count / read_duration,
                'latency_p50': np.percentile(read_latencies, 50),
                'latency_p95': np.percentile(read_latencies, 95),
                'latency_p99': np.percentile(read_latencies, 99),
                'latency_avg': np.mean(read_latencies),
            },
            'write': {
                'operations': write_count,
                'duration_seconds': write_duration,
                'ops_per_second': write_count / write_duration,
                'latency_p50': np.percentile(write_latencies, 50),
                'latency_p95': np.percentile(write_latencies, 95),
                'latency_p99': np.percentile(write_latencies, 99),
                'latency_avg': np.mean(write_latencies),
            }
        }
        
        print(f"\n\n✅ Redis Benchmark Complete!")
        return metrics
    
    def benchmark_end_to_end(self, 
                           input_topic: str = "portfolio-updates",
                           output_topic: str = "risk-updates",
                           duration_seconds: int = 30) -> Dict:
        """Benchmark end-to-end processing throughput."""
        print(f"\n📊 Benchmarking End-to-End Processing")
        print(f"   Input Topic: {input_topic}")
        print(f"   Output Topic: {output_topic}")
        print(f"   Duration: {duration_seconds} seconds")
        
        # Monitor output topic for processed messages
        output_consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f"{self.consumer_group}-output",
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest'
        })
        
        output_consumer.subscribe([output_topic])
        
        print("\n⏱️  Monitoring processing throughput...")
        
        start_time = time.time()
        messages_processed = 0
        processing_times = []
        last_report_time = start_time
        
        while time.time() - start_time < duration_seconds and self.running:
            msg = output_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                continue
            
            messages_processed += 1
            
            # Try to extract processing time from message
            try:
                data = json.loads(msg.value())
                if 'calculation_time_ms' in data:
                    processing_times.append(data['calculation_time_ms'])
            except:
                pass
            
            # Progress report
            current_time = time.time()
            if current_time - last_report_time >= 1.0:
                elapsed = current_time - start_time
                rate = messages_processed / elapsed
                print(f"\r  Processed: {messages_processed:,} | Rate: {rate:.0f} msg/s", 
                      end='', flush=True)
                last_report_time = current_time
        
        output_consumer.close()
        
        total_time = time.time() - start_time
        
        metrics = {
            'messages_processed': messages_processed,
            'duration_seconds': total_time,
            'messages_per_second': messages_processed / total_time if total_time > 0 else 0,
            'processing_latency_p50': np.percentile(processing_times, 50) if processing_times else 0,
            'processing_latency_p95': np.percentile(processing_times, 95) if processing_times else 0,
            'processing_latency_p99': np.percentile(processing_times, 99) if processing_times else 0,
        }
        
        print(f"\n\n✅ End-to-End Benchmark Complete!")
        return metrics
    
    def print_report(self, results: Dict):
        """Print a formatted benchmark report."""
        print("\n" + "="*80)
        print("PROSPECTOR THROUGHPUT BENCHMARK REPORT")
        print("="*80)
        print(f"Timestamp: {datetime.now().isoformat()}")
        print(f"Consumer Group: {self.consumer_group}")
        print("="*80)
        
        if 'kafka' in results:
            kafka = results['kafka']
            print(f"\n📥 KAFKA READ PERFORMANCE")
            print(f"  Messages Read: {kafka['messages_read']:,}")
            print(f"  Data Read: {kafka['bytes_read'] / (1024*1024):.2f} MB")
            print(f"  Duration: {kafka['duration_seconds']:.2f} seconds")
            print(f"  Throughput: {kafka['messages_per_second']:.0f} messages/second")
            print(f"  Throughput: {kafka['mb_per_second']:.2f} MB/second")
            print(f"  Avg Message Size: {kafka['avg_message_size']:.0f} bytes")
            print(f"  Read Latency P50: {kafka['latency_p50']:.2f} ms")
            print(f"  Read Latency P95: {kafka['latency_p95']:.2f} ms")
            print(f"  Read Latency P99: {kafka['latency_p99']:.2f} ms")
        
        if 'redis' in results:
            redis_metrics = results['redis']
            print(f"\n💾 REDIS PERFORMANCE")
            
            if 'error' in redis_metrics.get('read', {}):
                print(f"  Error: {redis_metrics['read']['error']}")
            else:
                print(f"\n  Read Operations:")
                read = redis_metrics.get('read', {})
                if 'operations' in read:
                    print(f"    Operations: {read['operations']:,}")
            print(f"    Throughput: {read['ops_per_second']:.0f} ops/second")
            print(f"    Latency Avg: {read['latency_avg']:.3f} ms")
            print(f"    Latency P50: {read['latency_p50']:.3f} ms")
            print(f"    Latency P95: {read['latency_p95']:.3f} ms")
            print(f"    Latency P99: {read['latency_p99']:.3f} ms")
            
            print(f"\n  Write Operations:")
            write = redis_metrics['write']
            print(f"    Operations: {write['operations']:,}")
            print(f"    Throughput: {write['ops_per_second']:.0f} ops/second")
            print(f"    Latency Avg: {write['latency_avg']:.3f} ms")
            print(f"    Latency P50: {write['latency_p50']:.3f} ms")
            print(f"    Latency P95: {write['latency_p95']:.3f} ms")
            print(f"    Latency P99: {write['latency_p99']:.3f} ms")
        
        if 'e2e' in results:
            e2e = results['e2e']
            print(f"\n⚡ END-TO-END PROCESSING")
            print(f"  Messages Processed: {e2e['messages_processed']:,}")
            print(f"  Duration: {e2e['duration_seconds']:.2f} seconds")
            print(f"  Throughput: {e2e['messages_per_second']:.0f} messages/second")
            if e2e['processing_latency_p50'] > 0:
                print(f"  Processing Latency P50: {e2e['processing_latency_p50']:.2f} ms")
                print(f"  Processing Latency P95: {e2e['processing_latency_p95']:.2f} ms")
                print(f"  Processing Latency P99: {e2e['processing_latency_p99']:.2f} ms")
        
        print("\n" + "="*80)
        
        # Save to file
        report_file = f"throughput_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'consumer_group': self.consumer_group,
                'results': results
            }, f, indent=2)
        print(f"\n📄 Report saved to: {report_file}")
    
    def run_full_benchmark(self, 
                         kafka_messages: int = 0,  # 0 = read all
                         kafka_duration: int = None,  # Time limit for Kafka
                         redis_duration: int = 10,
                         e2e_duration: int = 30,
                         from_beginning: bool = False):
        """Run complete benchmark suite."""
        results = {}
        
        try:
            # Kafka read benchmark
            results['kafka'] = self.benchmark_kafka_read(
                'portfolio-updates', 
                max_messages=kafka_messages if kafka_messages > 0 else None,
                duration_seconds=kafka_duration,
                from_beginning=from_beginning
            )
            
            # Redis benchmark
            results['redis'] = self.benchmark_redis_ops(redis_duration)
            
            # End-to-end benchmark (if risk calculator is running)
            try:
                results['e2e'] = self.benchmark_end_to_end(duration_seconds=e2e_duration)
            except Exception as e:
                print(f"\n⚠️  Could not benchmark end-to-end processing: {e}")
                print("   Make sure the risk calculator is running: uv run risk-calculator")
            
            # Print report
            self.print_report(results)
            
        except KeyboardInterrupt:
            print("\n\nBenchmark interrupted!")
            self.running = False


def main():
    parser = argparse.ArgumentParser(description='Throughput benchmark for Prospector')
    parser.add_argument('--kafka-messages', type=int, default=0,
                        help='Number of Kafka messages to read (0 = time-based)')
    parser.add_argument('--kafka-duration', type=int, default=10,
                        help='Duration for Kafka benchmark (seconds, used if messages=0)')
    parser.add_argument('--redis-duration', type=int, default=10,
                        help='Duration for Redis benchmark (seconds)')
    parser.add_argument('--e2e-duration', type=int, default=30,
                        help='Duration for end-to-end benchmark (seconds)')
    parser.add_argument('--consumer-group', type=str,
                        help='Kafka consumer group name (auto-generated if not specified)')
    parser.add_argument('--from-beginning', action='store_true',
                        help='Read Kafka topic from beginning')
    parser.add_argument('--from-offset', type=int,
                        help='Read Kafka topic from specific offset')
    parser.add_argument('--topic', type=str, default='portfolio-updates',
                        help='Kafka topic to benchmark')
    
    args = parser.parse_args()
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print('\n\nStopping benchmark...')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("🚀 Starting Prospector Throughput Benchmark")
    
    benchmark = ThroughputBenchmark(consumer_group=args.consumer_group)
    
    # Run specific benchmark or full suite
    if args.topic != 'portfolio-updates':
        # Just benchmark the specified topic
        results = {
            'kafka': benchmark.benchmark_kafka_read(
                args.topic, 
                max_messages=args.kafka_messages,
                from_beginning=args.from_beginning,
                from_offset=args.from_offset
            )
        }
        benchmark.print_report(results)
    else:
        # Run full benchmark suite
        benchmark.run_full_benchmark(
            kafka_messages=args.kafka_messages,
            kafka_duration=args.kafka_duration if args.kafka_messages == 0 else None,
            redis_duration=args.redis_duration,
            e2e_duration=args.e2e_duration,
            from_beginning=args.from_beginning
        )


if __name__ == "__main__":
    main()