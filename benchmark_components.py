#!/usr/bin/env python3
"""
Component Performance Benchmark for Prospector.

Tests individual component performance:
1. Kafka read throughput
2. NumPy calculation speed
3. Redis write throughput
"""

import time
import json
import argparse
import numpy as np
from typing import Dict, List
import statistics

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient
import redis

from models import Portfolio, Position, RiskTolerance, Sector
from prospector.core.calculations import (
    calculate_correlation_matrix,
    calculate_portfolio_metrics,
    calculate_value_at_risk,
    downside_percentage_to_risk_number
)
from prospector.config.securities import get_security_characteristics
from prospector.config.constants import Z_SCORE


class ComponentBenchmark:
    def __init__(self):
        self.kafka_brokers = "localhost:9092"
        self.redis_client = redis.Redis(
            host='localhost', 
            port=6379, 
            decode_responses=True,
            connection_pool=redis.ConnectionPool(max_connections=50)
        )
    
    def benchmark_kafka_throughput(self, topic: str = "portfolio-updates-v2", 
                                 num_messages: int = 100000) -> Dict:
        """Test pure Kafka read throughput."""
        print(f"\n📊 Kafka Read Throughput Test")
        print(f"   Messages to read: {num_messages:,}")
        
        consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f"benchmark-kafka-{int(time.time())}",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 10 * 1024 * 1024,  # 10MB
            'fetch.max.bytes': 50 * 1024 * 1024,  # 50MB
            'queued.min.messages': 100000,
        })
        
        # Subscribe to topic
        consumer.subscribe([topic])
        
        messages_read = 0
        bytes_read = 0
        start_time = time.time()
        
        while messages_read < num_messages:
            msg = consumer.poll(0.01)
            if msg and not msg.error():
                messages_read += 1
                bytes_read += len(msg.value())
                
                if messages_read % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_read / elapsed
                    print(f"   Progress: {messages_read:,} messages, {rate:,.0f} msg/s")
        
        consumer.close()
        
        total_time = time.time() - start_time
        avg_rate = messages_read / total_time
        throughput_mb = (bytes_read / (1024 * 1024)) / total_time
        
        print(f"   ✅ Complete: {avg_rate:,.0f} msg/s, {throughput_mb:.1f} MB/s")
        
        return {
            'messages': messages_read,
            'duration': total_time,
            'rate': avg_rate,
            'throughput_mb': throughput_mb,
            'avg_msg_size': bytes_read / messages_read
        }
    
    def benchmark_numpy_calculations(self, num_calculations: int = 10000) -> Dict:
        """Test NumPy calculation performance."""
        print(f"\n📊 NumPy Calculation Performance Test")
        print(f"   Calculations to perform: {num_calculations:,}")
        
        calc_times = []
        
        for i in range(num_calculations):
            # Create sample portfolio
            positions = self._create_sample_positions()
            
            start = time.time()
            
            # Perform calculations
            weights = np.array([p.weight / 100.0 for p in positions])
            returns = []
            volatilities = []
            betas = []
            
            for position in positions:
                chars = get_security_characteristics(position.symbol)
                returns.append(chars["expected_return"])
                volatilities.append(chars["volatility"])
                betas.append(chars["beta"])
            
            returns = np.array(returns)
            volatilities = np.array(volatilities)
            betas = np.array(betas)
            
            # Calculate metrics
            correlation = calculate_correlation_matrix(positions)
            portfolio_return, portfolio_volatility, sharpe_ratio = calculate_portfolio_metrics(
                weights, returns, volatilities, correlation
            )
            
            # Calculate risk metrics
            portfolio_beta = np.dot(weights, betas)
            total_value = sum(p.market_value for p in positions)
            var_95 = calculate_value_at_risk(total_value, portfolio_volatility)
            downside_percentage = -Z_SCORE * portfolio_volatility * 100
            risk_number = downside_percentage_to_risk_number(downside_percentage)
            
            calc_time = (time.time() - start) * 1000  # ms
            calc_times.append(calc_time)
            
            if (i + 1) % 1000 == 0:
                avg_time = statistics.mean(calc_times)
                rate = 1000 / avg_time
                print(f"   Progress: {i+1:,} calculations, {rate:,.0f} calc/s")
        
        avg_time = statistics.mean(calc_times)
        p50_time = statistics.median(calc_times)
        p95_time = np.percentile(calc_times, 95)
        p99_time = np.percentile(calc_times, 99)
        rate = 1000 / avg_time  # calculations per second
        
        print(f"   ✅ Complete: {rate:,.0f} calc/s, {avg_time:.3f} ms average")
        
        return {
            'calculations': num_calculations,
            'avg_time_ms': avg_time,
            'p50_time_ms': p50_time,
            'p95_time_ms': p95_time,
            'p99_time_ms': p99_time,
            'rate': rate
        }
    
    def benchmark_redis_throughput(self, num_operations: int = 100000) -> Dict:
        """Test Redis write throughput with pipelining."""
        print(f"\n📊 Redis Write Throughput Test")
        print(f"   Operations to perform: {num_operations:,}")
        
        pipeline = self.redis_client.pipeline(transaction=False)
        write_times = []
        
        start_time = time.time()
        
        for i in range(num_operations):
            op_start = time.time()
            
            # Prepare data
            data = {
                "portfolio_id": f"bench-{i}",
                "risk_number": str(50 + (i % 50)),
                "var_95": str(100000 + i * 1000),
                "timestamp": str(time.time())
            }
            
            # Add to pipeline
            pipeline.hset(f"benchmark:portfolio:{i}", mapping=data)
            pipeline.expire(f"benchmark:portfolio:{i}", 60)
            
            # Execute every 100 operations
            if (i + 1) % 100 == 0:
                pipeline.execute()
                pipeline = self.redis_client.pipeline(transaction=False)
            
            write_time = (time.time() - op_start) * 1000
            write_times.append(write_time)
            
            if (i + 1) % 10000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                print(f"   Progress: {i+1:,} operations, {rate:,.0f} ops/s")
        
        # Execute remaining
        pipeline.execute()
        
        total_time = time.time() - start_time
        avg_rate = num_operations / total_time
        avg_latency = statistics.mean(write_times)
        
        # Cleanup
        print("   Cleaning up test data...")
        keys = self.redis_client.keys("benchmark:portfolio:*")
        if keys:
            for i in range(0, len(keys), 1000):
                self.redis_client.delete(*keys[i:i+1000])
        
        print(f"   ✅ Complete: {avg_rate:,.0f} ops/s, {avg_latency:.3f} ms average")
        
        return {
            'operations': num_operations,
            'duration': total_time,
            'rate': avg_rate,
            'avg_latency_ms': avg_latency,
            'p50_latency_ms': statistics.median(write_times),
            'p95_latency_ms': np.percentile(write_times, 95),
            'p99_latency_ms': np.percentile(write_times, 99)
        }
    
    def _create_sample_positions(self) -> List[Position]:
        """Create sample portfolio positions."""
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
        positions = []
        
        for i, symbol in enumerate(symbols):
            positions.append(Position(
                symbol=symbol,
                quantity=100,
                price=100 + i * 50,
                market_value=100 * (100 + i * 50),
                weight=20.0,
                sector=Sector.TECHNOLOGY
            ))
        
        return positions
    
    def run_all_benchmarks(self):
        """Run all component benchmarks."""
        print("=" * 80)
        print("🚀 PROSPECTOR COMPONENT PERFORMANCE BENCHMARK")
        print("=" * 80)
        
        results = {}
        
        # Kafka throughput
        results['kafka'] = self.benchmark_kafka_throughput()
        
        # NumPy calculations
        results['numpy'] = self.benchmark_numpy_calculations()
        
        # Redis throughput
        results['redis'] = self.benchmark_redis_throughput()
        
        # Display summary
        print("\n" + "=" * 80)
        print("📊 PERFORMANCE SUMMARY")
        print("=" * 80)
        
        print(f"\n🔷 Kafka Read Performance:")
        print(f"   Rate: {results['kafka']['rate']:,.0f} messages/second")
        print(f"   Throughput: {results['kafka']['throughput_mb']:.1f} MB/second")
        print(f"   Avg Message Size: {results['kafka']['avg_msg_size']:.0f} bytes")
        
        print(f"\n🔷 NumPy Calculation Performance:")
        print(f"   Rate: {results['numpy']['rate']:,.0f} calculations/second")
        print(f"   Avg Latency: {results['numpy']['avg_time_ms']:.3f} ms")
        print(f"   P95 Latency: {results['numpy']['p95_time_ms']:.3f} ms")
        print(f"   P99 Latency: {results['numpy']['p99_time_ms']:.3f} ms")
        
        print(f"\n🔷 Redis Write Performance:")
        print(f"   Rate: {results['redis']['rate']:,.0f} operations/second")
        print(f"   Avg Latency: {results['redis']['avg_latency_ms']:.3f} ms")
        print(f"   P95 Latency: {results['redis']['p95_latency_ms']:.3f} ms")
        print(f"   P99 Latency: {results['redis']['p99_latency_ms']:.3f} ms")
        
        print("\n" + "=" * 80)
        
        return results


def main():
    parser = argparse.ArgumentParser(description="Component Performance Benchmark")
    parser.add_argument("--kafka-messages", type=int, default=100000,
                       help="Number of Kafka messages to read")
    parser.add_argument("--numpy-calculations", type=int, default=10000,
                       help="Number of NumPy calculations to perform")
    parser.add_argument("--redis-operations", type=int, default=100000,
                       help="Number of Redis operations to perform")
    
    args = parser.parse_args()
    
    benchmark = ComponentBenchmark()
    benchmark.run_all_benchmarks()


if __name__ == "__main__":
    main()