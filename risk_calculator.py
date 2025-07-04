#!/usr/bin/env python3
"""
Prospector Risk Calculator - Real-time portfolio risk analysis engine.

This module implements the core risk calculation pipeline using Bytewax stream processing
to analyze portfolio positions and calculate various risk metrics including:
- Value at Risk (VaR) at 95% confidence level
- Portfolio volatility and expected returns
- Sharpe ratio for risk-adjusted performance
- Risk scoring on a 1-99 scale

The calculator processes portfolio updates from Kafka, performs risk calculations
using sector-based correlations, caches results in Redis, and publishes risk
updates back to Kafka for downstream consumption.
"""

import json
import time
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from collections import deque
import threading

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSourceMessage

import redis
import logging

from models import (
    Portfolio, Position, MarketData, RiskCalculation,
    RiskTolerance, Sector
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Performance metrics
class PerformanceTracker:
    def __init__(self):
        self.messages_processed = 0
        self.total_processing_time = 0
        self.start_time = time.time()
        self.recent_latencies = deque(maxlen=1000)
        self.lock = threading.Lock()
    
    def record_message(self, latency_ms: float):
        with self.lock:
            self.messages_processed += 1
            self.total_processing_time += latency_ms
            self.recent_latencies.append(latency_ms)
    
    def get_stats(self):
        with self.lock:
            elapsed = time.time() - self.start_time
            throughput = self.messages_processed / elapsed if elapsed > 0 else 0
            avg_latency = self.total_processing_time / self.messages_processed if self.messages_processed > 0 else 0
            recent_avg = sum(self.recent_latencies) / len(self.recent_latencies) if self.recent_latencies else 0
            return {
                'messages_processed': self.messages_processed,
                'throughput_per_second': throughput,
                'avg_latency_ms': avg_latency,
                'recent_avg_latency_ms': recent_avg,
                'uptime_seconds': elapsed
            }

perf_tracker = PerformanceTracker()

# Redis connection
try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.ping()
    logger.info("✅ Redis connected")
except:
    redis_client = None
    logger.warning("⚠️ Redis not available")

# Risk calculation constants
RISK_FREE_RATE = 0.03
CONFIDENCE_LEVEL = 0.95

# Sector risk profiles
SECTOR_VOLATILITY = {
    Sector.TECHNOLOGY: 0.25,
    Sector.HEALTHCARE: 0.18,
    Sector.FINANCE: 0.20,
    Sector.CONSUMER: 0.15,
    Sector.ENERGY: 0.30,
    Sector.RETAIL: 0.22,
    Sector.TELECOM: 0.16,
    Sector.ENTERTAINMENT: 0.28,
    Sector.AUTOMOTIVE: 0.35,
    Sector.REAL_ESTATE: 0.14,
    Sector.OTHER: 0.20
}

SECTOR_RETURNS = {
    Sector.TECHNOLOGY: 0.12,
    Sector.HEALTHCARE: 0.10,
    Sector.FINANCE: 0.09,
    Sector.CONSUMER: 0.08,
    Sector.ENERGY: 0.07,
    Sector.RETAIL: 0.09,
    Sector.TELECOM: 0.06,
    Sector.ENTERTAINMENT: 0.11,
    Sector.AUTOMOTIVE: 0.15,
    Sector.REAL_ESTATE: 0.07,
    Sector.OTHER: 0.08
}


def parse_kafka_message(msg: KafkaSourceMessage) -> Optional[Tuple[str, Portfolio]]:
    """
    Parse incoming Kafka message into Portfolio model.
    
    Args:
        msg: KafkaSourceMessage containing serialized portfolio data
        
    Returns:
        Tuple of (portfolio_id, Portfolio) if parsing succeeds, None otherwise
        
    The function expects JSON-encoded portfolio data in the message value
    and validates it against the Portfolio Pydantic model.
    """
    try:
        data = json.loads(msg.value)
        portfolio = Portfolio(**data)
        return (portfolio.id, portfolio)
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None


def calculate_portfolio_risk(portfolio_tuple: Tuple[str, Portfolio]) -> Optional[Tuple[str, RiskCalculation]]:
    """
    Calculate comprehensive risk metrics for a portfolio.
    
    Args:
        portfolio_tuple: Tuple of (portfolio_id, Portfolio object)
        
    Returns:
        Tuple of (portfolio_id, RiskCalculation) with computed metrics, or None on error
        
    This function performs the following calculations:
    1. Extracts position weights and maps to sector-based returns/volatility
    2. Constructs a correlation matrix with higher correlation for same-sector assets
    3. Calculates portfolio-level metrics:
       - Expected return (weighted average of position returns)
       - Portfolio volatility using covariance matrix
       - Value at Risk (VaR) at 95% confidence level
       - Sharpe ratio for risk-adjusted returns
       - Risk score (1-99) adjusted for investor risk tolerance
    4. Caches results in Redis with 5-minute TTL for fast retrieval
    """
    if portfolio_tuple is None:
        return None
        
    key, portfolio = portfolio_tuple
    
    start_time = time.time()
    
    try:
        # Get positions data
        positions = portfolio.positions
        total_value = portfolio.total_value
        
        # Calculate weights and returns - optimized with list comprehensions
        weights = np.array([position.weight / 100.0 for position in positions])
        
        # Use sector-based returns and volatility - vectorized
        sectors = [position.sector for position in positions]
        returns = np.array([SECTOR_RETURNS.get(sector, 0.08) for sector in sectors])
        volatilities = np.array([SECTOR_VOLATILITY.get(sector, 0.20) for sector in sectors])
        
        # Portfolio metrics
        portfolio_return = np.sum(weights * returns)
        
        # Correlation matrix (simplified - using sector correlations)
        n_assets = len(weights)
        correlation = np.full((n_assets, n_assets), 0.3)  # Base correlation
        
        # Higher correlation for same sector
        for i in range(n_assets):
            for j in range(n_assets):
                if i == j:
                    correlation[i, j] = 1.0
                elif positions[i].sector == positions[j].sector:
                    correlation[i, j] = 0.7
        
        # Covariance matrix
        cov_matrix = np.outer(volatilities, volatilities) * correlation
        portfolio_volatility = np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
        
        # Value at Risk
        z_score = 1.645  # 95% confidence
        var_95 = total_value * z_score * portfolio_volatility * np.sqrt(252/365)
        
        # Sharpe ratio
        sharpe_ratio = (portfolio_return - RISK_FREE_RATE) / portfolio_volatility if portfolio_volatility > 0 else 0
        
        # Risk number (1-99 scale)
        risk_number = int(min(99, max(1, portfolio_volatility * 100)))
        
        # Adjust for risk tolerance
        if portfolio.risk_tolerance == RiskTolerance.CONSERVATIVE:
            risk_number = int(risk_number * 0.8)
        elif portfolio.risk_tolerance == RiskTolerance.AGGRESSIVE:
            risk_number = int(risk_number * 1.2)
        
        # Ensure risk number is within bounds
        risk_number = max(1, min(99, risk_number))
        
        calculation_time = (time.time() - start_time) * 1000
        
        risk_calc = RiskCalculation(
            portfolio_id=portfolio.id,
            advisor_id=portfolio.advisor_id,
            risk_number=risk_number,
            var_95=var_95,
            expected_return=portfolio_return,
            volatility=portfolio_volatility,
            sharpe_ratio=sharpe_ratio,
            calculation_time_ms=calculation_time
        )
        
        # Cache in Redis if available - using pipeline for performance
        if redis_client:
            try:
                # Use pipeline to batch all Redis operations
                pipe = redis_client.pipeline(transaction=False)
                
                # Queue all operations
                pipe.setex(
                    f"risk:{portfolio.id}",
                    300,  # 5 minute TTL
                    risk_calc.json()
                )
                
                stats_key = f"stats:{portfolio.id}"
                pipe.hincrby(stats_key, "count", 1)
                pipe.hset(stats_key, "last_update", str(time.time()))
                pipe.hset(stats_key, "processing_time_ms", str(calculation_time))
                pipe.expire(stats_key, 3600)  # 1 hour TTL
                
                # Update global performance metrics
                pipe.hincrby("global:metrics", "total_calculations", 1)
                pipe.hincrbyfloat("global:metrics", "total_processing_time_ms", calculation_time)
                pipe.hset("global:metrics", "last_calculation", str(time.time()))
                
                # Execute all operations at once
                pipe.execute()
                
            except Exception as e:
                logger.error(f"Redis error: {e}")
        
        logger.info(f"✅ Calculated risk for {portfolio.id}: "
                   f"Risk={risk_number}, VaR=${var_95:,.2f}, "
                   f"Return={portfolio_return:.2%}, Vol={portfolio_volatility:.2%}")
        
        # Track performance metrics
        perf_tracker.record_message(calculation_time)
        
        # Log performance stats every 100 messages
        if perf_tracker.messages_processed % 100 == 0:
            stats = perf_tracker.get_stats()
            logger.info(f"📊 PERFORMANCE: Processed {stats['messages_processed']} messages | "
                       f"Throughput: {stats['throughput_per_second']:.2f} msg/s | "
                       f"Avg latency: {stats['recent_avg_latency_ms']:.2f}ms")
        
        return (key, risk_calc)
        
    except Exception as e:
        logger.error(f"Error calculating risk for {key}: {e}")
        return None


def serialize_for_kafka(risk_data: Tuple[str, RiskCalculation]) -> Optional[KafkaSinkMessage]:
    """
    Serialize risk calculation for Kafka output.
    
    Args:
        risk_data: Tuple of (portfolio_id, RiskCalculation object)
        
    Returns:
        KafkaSinkMessage for Kafka sink
        
    The serialized message uses the portfolio ID as the Kafka key to ensure
    all updates for a given portfolio are routed to the same partition.
    """
    if risk_data is None:
        return None
        
    key, risk_calc = risk_data
    return KafkaSinkMessage(
        key=risk_calc.portfolio_id.encode(),
        value=risk_calc.json().encode()
    )


def build_dataflow():
    """
    Build the Prospector risk calculation dataflow pipeline.
    
    Returns:
        Dataflow object configured with the complete processing pipeline
        
    Pipeline stages:
    1. Input: Consume portfolio updates from Kafka topic 'portfolio-updates'
    2. Parse: Deserialize JSON messages into Portfolio objects
    3. Filter: Remove any malformed messages
    4. Calculate: Compute risk metrics for each portfolio
    5. Filter: Remove failed calculations
    6. Log: Record processing statistics
    7. Serialize: Convert results to Kafka messages
    8. Output: Publish to 'risk-updates' Kafka topic
    """
    flow = Dataflow("prospector-risk-calculator")
    
    # Input from Kafka with larger batch size for better throughput
    portfolio_stream = op.input(
        "portfolio-input", 
        flow, 
        KafkaSource(
            brokers=["localhost:9092"],
            topics=["portfolio-updates"],
            batch_size=1000  # Increased from 10 for better throughput
        )
    )
    
    # Parse messages into Portfolio models
    parsed = op.map("parse", portfolio_stream, parse_kafka_message)
    
    # Filter out None values
    filtered = op.filter("filter-none", parsed, lambda x: x is not None)
    
    # Calculate risk using Pydantic models
    risk_calculations = op.map("calculate-risk", filtered, calculate_portfolio_risk)
    
    # Filter out None results
    valid_risks = op.filter("filter-valid", risk_calculations, lambda x: x is not None)
    
    # Log some statistics
    op.inspect("log-stats", valid_risks, 
              lambda step_id, x: logger.info(f"📊 Risk calculation complete for {x[0]} in {x[1].calculation_time_ms:.1f}ms"))
    
    # Serialize for output
    output_messages = op.map("serialize", valid_risks, serialize_for_kafka)
    
    # Filter out None messages
    valid_messages = op.filter("filter-messages", output_messages, lambda x: x is not None)
    
    # Output to Kafka
    op.output(
        "risk-output",
        valid_messages,
        KafkaSink(
            brokers=["localhost:9092"],
            topic="risk-updates"
        )
    )
    
    return flow


def main():
    """
    Entry point for the Prospector risk calculator.
    
    Initializes logging, displays startup information, and launches the
    stream processing pipeline. The calculator will continuously process
    portfolio updates until interrupted.
    """
    logger.info("🚀 Starting Prospector Risk Calculator")
    logger.info("📝 Using structured Pydantic data validation")
    logger.info("📊 Advanced risk calculations with sector-based correlations")
    logger.info("⚡ Real-time stream processing with Bytewax for portfolio risk analysis")
    logger.info("📡 Performance tracking enabled - metrics available in Redis")
    
    # Reset global metrics in Redis
    if redis_client:
        redis_client.delete("global:metrics")
        redis_client.hset("global:metrics", "start_time", str(time.time()))
    
    from bytewax.run import cli_main
    flow = build_dataflow()
    cli_main(flow)


if __name__ == "__main__":
    main()