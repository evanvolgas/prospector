#!/usr/bin/env python3
"""
Parallel Kafka Sink Benchmark for Prospector.

Tests writing to Kafka using 12 concurrent processes to measure
maximum achievable throughput.
"""

import time
import json
import argparse
import multiprocessing
import signal
from typing import Dict, List
import numpy as np

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

from models import Portfolio, Position, RiskCalculation
from prospector.core.calculations import (
    calculate_correlation_matrix,
    calculate_portfolio_metrics,
    calculate_value_at_risk,
    downside_percentage_to_risk_number
)
from prospector.config.securities import get_security_characteristics
from prospector.config.constants import Z_SCORE


class ParallelKafkaBenchmark:
    def __init__(self, num_processes: int = 12):
        self.num_processes = num_processes
        self.kafka_brokers = "localhost:9092"
        self.input_topic = "portfolio-updates-v2"
        self.output_topic = "risk-calculations-parallel"
        self.manager = multiprocessing.Manager()
        self.results = self.manager.dict()
        
        # Ensure output topic exists
        self._ensure_output_topic()
    
    def _ensure_output_topic(self):
        """Create output topic with 12 partitions."""
        admin = AdminClient({'bootstrap.servers': self.kafka_brokers})
        
        try:
            metadata = admin.list_topics(timeout=10)
            if self.output_topic not in metadata.topics:
                print(f"Creating output topic: {self.output_topic}")
                new_topic = NewTopic(self.output_topic, num_partitions=12, replication_factor=1)
                admin.create_topics([new_topic])
                time.sleep(2)
        except Exception as e:
            print(f"Error creating topic: {e}")
    
    def worker_process(self, worker_id: int, partition: int, num_messages: int, results_dict: dict):
        """Worker process that reads from one partition and writes to Kafka."""
        # Set up consumer for specific partition
        consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f"parallel-benchmark-{int(time.time())}-{worker_id}",
            'enable.auto.commit': False,
            'fetch.min.bytes': 1024 * 1024,  # 1MB
            'queued.min.messages': 10000,
        })
        
        # Assign specific partition
        tp = TopicPartition(self.input_topic, partition, OFFSET_BEGINNING)
        consumer.assign([tp])
        
        # Set up producer
        producer = Producer({
            'bootstrap.servers': self.kafka_brokers,
            'compression.type': 'snappy',
            'batch.size': 100000,
            'linger.ms': 10,
            'queue.buffering.max.messages': 100000,
        })
        
        messages_processed = 0
        calculation_times = []
        start_time = time.time()
        
        print(f"Worker {worker_id}: Started processing partition {partition}")
        
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
                
                # Produce to output topic
                producer.produce(
                    self.output_topic,
                    key=portfolio.id.encode(),
                    value=risk_calc.model_dump_json().encode(),
                    partition=partition  # Same partition mapping
                )
                
                messages_processed += 1
                
                # Periodic flush
                if messages_processed % 1000 == 0:
                    producer.flush()
                
                # Progress update
                if messages_processed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = messages_processed / elapsed
                    print(f"Worker {worker_id}: {messages_processed:,} messages, {rate:,.0f} msg/s")
                    
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                continue
        
        # Final flush
        producer.flush()
        consumer.close()
        
        # Store results
        total_time = time.time() - start_time
        results_dict[worker_id] = {
            'messages': messages_processed,
            'duration': total_time,
            'rate': messages_processed / total_time,
            'avg_calc_time': np.mean(calculation_times) if calculation_times else 0
        }
        
        print(f"Worker {worker_id}: Completed - {messages_processed:,} messages in {total_time:.1f}s")
    
    def _calculate_risk(self, portfolio: Portfolio) -> RiskCalculation:
        """Calculate risk metrics for a portfolio."""
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
        
        correlation = calculate_correlation_matrix(portfolio.positions)
        portfolio_return, portfolio_volatility, sharpe_ratio = calculate_portfolio_metrics(
            weights, returns, volatilities, correlation
        )
        
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
            calculation_time_ms=0
        )
    
    def run_parallel_benchmark(self, messages_per_worker: int = 10000):
        """Run parallel benchmark with multiple processes."""
        print("=" * 80)
        print("🚀 PARALLEL KAFKA SINK BENCHMARK")
        print("=" * 80)
        print(f"Processes: {self.num_processes}")
        print(f"Messages per worker: {messages_per_worker:,}")
        print(f"Total messages: {messages_per_worker * self.num_processes:,}")
        print("=" * 80)
        
        # Get partition count
        admin = AdminClient({'bootstrap.servers': self.kafka_brokers})
        metadata = admin.list_topics(timeout=10)
        num_partitions = len(metadata.topics[self.input_topic].partitions)
        
        if num_partitions < self.num_processes:
            print(f"⚠️  Warning: Only {num_partitions} partitions available, using {num_partitions} processes")
            self.num_processes = num_partitions
        
        # Start processes
        processes = []
        start_time = time.time()
        
        for i in range(self.num_processes):
            partition = i % num_partitions
            p = multiprocessing.Process(
                target=self.worker_process,
                args=(i, partition, messages_per_worker, self.results)
            )
            p.start()
            processes.append(p)
        
        # Wait for completion
        print("\n⏳ Waiting for workers to complete...")
        for p in processes:
            p.join()
        
        total_time = time.time() - start_time
        
        # Calculate aggregate results
        total_messages = sum(r['messages'] for r in self.results.values())
        aggregate_rate = total_messages / total_time
        
        # Display results
        print("\n" + "=" * 80)
        print("📊 PARALLEL BENCHMARK RESULTS")
        print("=" * 80)
        
        print(f"\n🔷 Per-Worker Performance:")
        for worker_id, result in sorted(self.results.items()):
            print(f"   Worker {worker_id}: {result['rate']:,.0f} msg/s "
                  f"({result['messages']:,} messages in {result['duration']:.1f}s)")
        
        print(f"\n🔷 Aggregate Performance:")
        print(f"   Total Messages: {total_messages:,}")
        print(f"   Total Duration: {total_time:.1f} seconds")
        print(f"   Aggregate Throughput: {aggregate_rate:,.0f} messages/second")
        
        # Calculate efficiency
        avg_worker_rate = sum(r['rate'] for r in self.results.values()) / len(self.results)
        ideal_rate = avg_worker_rate * self.num_processes
        efficiency = (aggregate_rate / ideal_rate) * 100
        
        print(f"\n🔷 Scaling Analysis:")
        print(f"   Average Worker Rate: {avg_worker_rate:,.0f} msg/s")
        print(f"   Theoretical Maximum: {ideal_rate:,.0f} msg/s")
        print(f"   Actual Aggregate: {aggregate_rate:,.0f} msg/s")
        print(f"   Scaling Efficiency: {efficiency:.1f}%")
        
        # Daily projections
        messages_per_day = aggregate_rate * 86400
        gb_per_day = (messages_per_day * 2.1) / (1024**3)  # Assuming 2.1KB avg message
        
        print(f"\n🔷 Daily Capacity Projection:")
        print(f"   Messages: {messages_per_day/1e9:.1f} billion/day")
        print(f"   Data Volume: {gb_per_day:,.0f} GB/day")
        
        print("\n" + "=" * 80)
        
        return {
            'total_messages': total_messages,
            'duration': total_time,
            'aggregate_rate': aggregate_rate,
            'efficiency': efficiency
        }


def main():
    # Set spawn method for macOS
    multiprocessing.set_start_method('spawn', force=True)
    
    parser = argparse.ArgumentParser(description="Parallel Kafka Sink Benchmark")
    parser.add_argument("--processes", type=int, default=12,
                       help="Number of parallel processes")
    parser.add_argument("--messages", type=int, default=10000,
                       help="Messages per worker")
    
    args = parser.parse_args()
    
    benchmark = ParallelKafkaBenchmark(args.processes)
    benchmark.run_parallel_benchmark(args.messages)


if __name__ == "__main__":
    main()