#!/usr/bin/env python3
"""
Kafka Sink Benchmark for Prospector.

Tests writing calculated risk results to Kafka instead of Redis
to compare throughput characteristics.
"""

import time
import json
import argparse
from typing import Dict, Tuple
import numpy as np

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic
import redis

from models import Portfolio, Position, RiskCalculation
from prospector.core.calculations import (
    calculate_correlation_matrix,
    calculate_portfolio_metrics,
    calculate_value_at_risk,
    downside_percentage_to_risk_number
)
from prospector.config.securities import get_security_characteristics
from prospector.config.constants import Z_SCORE


class KafkaSinkBenchmark:
    def __init__(self):
        self.kafka_brokers = "localhost:9092"
        self.input_topic = "portfolio-updates-v2"
        self.output_topic = "risk-calculations-benchmark"
        
        # Create output topic if needed
        self._ensure_output_topic()
    
    def _ensure_output_topic(self):
        """Create output topic if it doesn't exist."""
        admin = AdminClient({'bootstrap.servers': self.kafka_brokers})
        
        try:
            metadata = admin.list_topics(timeout=10)
            if self.output_topic not in metadata.topics:
                print(f"Creating output topic: {self.output_topic}")
                new_topic = NewTopic(self.output_topic, num_partitions=12, replication_factor=1)
                admin.create_topics([new_topic])
                time.sleep(2)  # Wait for creation
        except Exception as e:
            print(f"Error creating topic: {e}")
    
    def process_and_sink_to_kafka(self, num_messages: int = 100000) -> Dict:
        """Process portfolios and sink results to Kafka."""
        print(f"\n📊 Kafka Sink Benchmark")
        print(f"   Input Topic: {self.input_topic}")
        print(f"   Output Topic: {self.output_topic}")
        print(f"   Messages to process: {num_messages:,}")
        
        # Consumer for input
        consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f"benchmark-kafka-sink-{int(time.time())}",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 10 * 1024 * 1024,
            'queued.min.messages': 100000,
        })
        
        # Producer for output
        producer = Producer({
            'bootstrap.servers': self.kafka_brokers,
            'compression.type': 'snappy',
            'batch.size': 100000,
            'linger.ms': 10,
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 1048576,
        })
        
        consumer.subscribe([self.input_topic])
        
        messages_processed = 0
        calculation_times = []
        sink_times = []
        start_time = time.time()
        
        while messages_processed < num_messages:
            msg = consumer.poll(0.01)
            if not msg or msg.error():
                continue
            
            try:
                # Parse portfolio
                portfolio_data = json.loads(msg.value())
                portfolio = Portfolio(**portfolio_data)
                
                # Calculate risk (same as regular processor)
                calc_start = time.time()
                risk_calc = self._calculate_risk(portfolio)
                calc_time = (time.time() - calc_start) * 1000
                calculation_times.append(calc_time)
                
                # Sink to Kafka
                sink_start = time.time()
                producer.produce(
                    self.output_topic,
                    key=portfolio.id.encode(),
                    value=risk_calc.model_dump_json().encode(),
                    partition=msg.partition()  # Same partition as input
                )
                
                # Flush periodically
                if messages_processed % 1000 == 0:
                    producer.flush()
                
                sink_time = (time.time() - sink_start) * 1000
                sink_times.append(sink_time)
                
                messages_processed += 1
                
                if messages_processed % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_processed / elapsed
                    print(f"   Progress: {messages_processed:,} messages, {rate:,.0f} msg/s")
                    
            except Exception as e:
                print(f"   Error processing message: {e}")
                continue
        
        # Final flush
        producer.flush()
        consumer.close()
        
        total_time = time.time() - start_time
        avg_rate = messages_processed / total_time
        
        print(f"   ✅ Complete: {avg_rate:,.0f} msg/s")
        
        return {
            'messages_processed': messages_processed,
            'duration': total_time,
            'rate': avg_rate,
            'avg_calc_time_ms': np.mean(calculation_times),
            'avg_sink_time_ms': np.mean(sink_times),
            'total_latency_ms': np.mean(calculation_times) + np.mean(sink_times)
        }
    
    def process_and_sink_to_redis(self, num_messages: int = 100000) -> Dict:
        """Process portfolios and sink results to Redis for comparison."""
        print(f"\n📊 Redis Sink Benchmark (for comparison)")
        print(f"   Messages to process: {num_messages:,}")
        
        # Redis with connection pooling
        pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, max_connections=50)
        redis_client = redis.Redis(connection_pool=pool)
        pipeline = redis_client.pipeline(transaction=False)
        
        # Consumer
        consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f"benchmark-redis-sink-{int(time.time())}",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'fetch.min.bytes': 10 * 1024 * 1024,
            'queued.min.messages': 100000,
        })
        
        consumer.subscribe([self.input_topic])
        
        messages_processed = 0
        calculation_times = []
        sink_times = []
        start_time = time.time()
        
        while messages_processed < num_messages:
            msg = consumer.poll(0.01)
            if not msg or msg.error():
                continue
            
            try:
                # Parse portfolio
                portfolio_data = json.loads(msg.value())
                portfolio = Portfolio(**portfolio_data)
                
                # Calculate risk
                calc_start = time.time()
                risk_calc = self._calculate_risk(portfolio)
                calc_time = (time.time() - calc_start) * 1000
                calculation_times.append(calc_time)
                
                # Sink to Redis
                sink_start = time.time()
                
                risk_data = {
                    "portfolio_id": risk_calc.portfolio_id,
                    "risk_number": str(risk_calc.risk_number),
                    "var_95": str(risk_calc.var_95),
                    "expected_return": str(risk_calc.expected_return),
                    "volatility": str(risk_calc.volatility),
                    "sharpe_ratio": str(risk_calc.sharpe_ratio),
                    "timestamp": str(risk_calc.timestamp)
                }
                
                pipeline.hset(f"benchmark:redis:{portfolio.id}", mapping=risk_data)
                pipeline.expire(f"benchmark:redis:{portfolio.id}", 300)
                
                # Execute pipeline every 100 messages
                if messages_processed % 100 == 0:
                    pipeline.execute()
                    pipeline = redis_client.pipeline(transaction=False)
                
                sink_time = (time.time() - sink_start) * 1000
                sink_times.append(sink_time)
                
                messages_processed += 1
                
                if messages_processed % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_processed / elapsed
                    print(f"   Progress: {messages_processed:,} messages, {rate:,.0f} msg/s")
                    
            except Exception as e:
                print(f"   Error processing message: {e}")
                continue
        
        # Final pipeline execution
        pipeline.execute()
        consumer.close()
        
        # Cleanup
        print("   Cleaning up Redis test data...")
        keys = redis_client.keys("benchmark:redis:*")
        if keys:
            for i in range(0, len(keys), 1000):
                redis_client.delete(*keys[i:i+1000])
        
        total_time = time.time() - start_time
        avg_rate = messages_processed / total_time
        
        print(f"   ✅ Complete: {avg_rate:,.0f} msg/s")
        
        return {
            'messages_processed': messages_processed,
            'duration': total_time,
            'rate': avg_rate,
            'avg_calc_time_ms': np.mean(calculation_times),
            'avg_sink_time_ms': np.mean(sink_times),
            'total_latency_ms': np.mean(calculation_times) + np.mean(sink_times)
        }
    
    def _calculate_risk(self, portfolio: Portfolio) -> RiskCalculation:
        """Calculate risk metrics for a portfolio."""
        # Get positions data
        weights = np.array([p.weight / 100.0 for p in portfolio.positions])
        returns = []
        volatilities = []
        betas = []
        
        for position in portfolio.positions:
            chars = get_security_characteristics(position.symbol)
            returns.append(chars["expected_return"])
            volatilities.append(chars["volatility"])
            betas.append(chars["beta"])
        
        returns = np.array(returns)
        volatilities = np.array(volatilities)
        betas = np.array(betas)
        
        # Calculate metrics
        correlation = calculate_correlation_matrix(portfolio.positions)
        portfolio_return, portfolio_volatility, sharpe_ratio = calculate_portfolio_metrics(
            weights, returns, volatilities, correlation
        )
        
        # Risk metrics
        portfolio_beta = np.dot(weights, betas)
        total_value = sum(p.market_value for p in portfolio.positions)
        downside_percentage = -Z_SCORE * portfolio_volatility * 100
        var_95 = calculate_value_at_risk(total_value, portfolio_volatility)
        risk_number = downside_percentage_to_risk_number(downside_percentage)
        
        return RiskCalculation(
            portfolio_id=portfolio.id,
            advisor_id=portfolio.advisor_id,
            risk_number=risk_number,
            var_95=var_95,
            expected_return=portfolio_return,
            volatility=portfolio_volatility,
            sharpe_ratio=sharpe_ratio,
            calculation_time_ms=0  # Set separately
        )
    
    def run_comparison(self, num_messages: int = 50000):
        """Run both benchmarks and compare results."""
        print("=" * 80)
        print("🚀 KAFKA vs REDIS SINK PERFORMANCE COMPARISON")
        print("=" * 80)
        
        # Run Kafka sink benchmark
        kafka_results = self.process_and_sink_to_kafka(num_messages)
        
        # Run Redis sink benchmark
        redis_results = self.process_and_sink_to_redis(num_messages)
        
        # Display comparison
        print("\n" + "=" * 80)
        print("📊 PERFORMANCE COMPARISON")
        print("=" * 80)
        
        print(f"\n🔷 Throughput:")
        print(f"   Kafka Sink: {kafka_results['rate']:,.0f} messages/second")
        print(f"   Redis Sink: {redis_results['rate']:,.0f} messages/second")
        print(f"   Difference: {(kafka_results['rate'] / redis_results['rate'] - 1) * 100:+.1f}%")
        
        print(f"\n🔷 Latency Breakdown:")
        print(f"   Calculation Time (both): {kafka_results['avg_calc_time_ms']:.3f} ms")
        print(f"   Kafka Sink Time: {kafka_results['avg_sink_time_ms']:.3f} ms")
        print(f"   Redis Sink Time: {redis_results['avg_sink_time_ms']:.3f} ms")
        
        print(f"\n🔷 Total Processing Time per Message:")
        print(f"   Kafka: {kafka_results['total_latency_ms']:.3f} ms")
        print(f"   Redis: {redis_results['total_latency_ms']:.3f} ms")
        
        print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Kafka vs Redis Sink Benchmark")
    parser.add_argument("--messages", type=int, default=50000,
                       help="Number of messages to process")
    parser.add_argument("--mode", choices=['both', 'kafka', 'redis'], default='both',
                       help="Which benchmark to run")
    
    args = parser.parse_args()
    
    benchmark = KafkaSinkBenchmark()
    
    if args.mode == 'both':
        benchmark.run_comparison(args.messages)
    elif args.mode == 'kafka':
        benchmark.process_and_sink_to_kafka(args.messages)
    else:
        benchmark.process_and_sink_to_redis(args.messages)


if __name__ == "__main__":
    main()