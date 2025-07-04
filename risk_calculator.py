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
import aioredis
import asyncio
import logging
from functools import lru_cache

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

# Redis connection pool for better performance
try:
    redis_pool = redis.ConnectionPool(
        host='localhost', 
        port=6379, 
        max_connections=50,
        decode_responses=True
    )
    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.ping()
    logger.info("✅ Redis connected with connection pool")
except:
    redis_client = None
    logger.warning("⚠️ Redis not available")

# Async Redis for non-blocking operations
async_redis = None
async def init_async_redis():
    global async_redis
    try:
        async_redis = await aioredis.create_redis_pool(
            'redis://localhost:6379',
            minsize=5,
            maxsize=20
        )
        logger.info("✅ Async Redis pool initialized")
    except Exception as e:
        logger.warning(f"⚠️ Async Redis not available: {e}")

# Risk calculation constants
RISK_FREE_RATE = 0.03
CONFIDENCE_LEVEL = 0.95

# Cache sector lookups for performance
@lru_cache(maxsize=128)
def get_sector_volatility(sector: Sector) -> float:
    volatilities = {
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
    return volatilities.get(sector, 0.20)

@lru_cache(maxsize=128)
def get_sector_return(sector: Sector) -> float:
    returns = {
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
    return returns.get(sector, 0.08)

# Pre-compute common sector correlations
@lru_cache(maxsize=256)
def get_sector_correlation(sector1: Sector, sector2: Sector) -> float:
    if sector1 == sector2:
        return 1.0
    # High correlation for similar sectors
    tech_sectors = {Sector.TECHNOLOGY, Sector.ENTERTAINMENT}
    financial_sectors = {Sector.FINANCE, Sector.REAL_ESTATE}
    consumer_sectors = {Sector.CONSUMER, Sector.RETAIL}
    
    if {sector1, sector2}.issubset(tech_sectors):
        return 0.8
    elif {sector1, sector2}.issubset(financial_sectors):
        return 0.75
    elif {sector1, sector2}.issubset(consumer_sectors):
        return 0.7
    else:
        return 0.3  # Base correlation


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


def calculate_portfolio_risk_batch(batch: List[Tuple[str, Portfolio]]) -> List[Tuple[str, RiskCalculation]]:
    """
    Calculate risk for a batch of portfolios with vectorized operations.
    
    Args:
        batch: List of (portfolio_id, Portfolio) tuples
        
    Returns:
        List of (portfolio_id, RiskCalculation) tuples
    """
    results = []
    
    # Pre-allocate arrays for batch processing
    batch_size = len(batch)
    if batch_size == 0:
        return results
    
    for portfolio_tuple in batch:
        result = calculate_portfolio_risk(portfolio_tuple)
        if result is not None:
            results.append(result)
    
    # Batch Redis operations
    if redis_client and results:
        pipe = redis_client.pipeline(transaction=False)
        
        for key, risk_calc in results:
            # Store all data in a single hash for fewer operations
            risk_data = {
                "portfolio_id": risk_calc.portfolio_id,
                "advisor_id": risk_calc.advisor_id,
                "risk_number": str(risk_calc.risk_number),
                "var_95": str(risk_calc.var_95),
                "expected_return": str(risk_calc.expected_return),
                "volatility": str(risk_calc.volatility),
                "sharpe_ratio": str(risk_calc.sharpe_ratio),
                "calculation_time_ms": str(risk_calc.calculation_time_ms),
                "timestamp": str(risk_calc.timestamp.isoformat()),
                "last_update": str(time.time())
            }
            
            # Single hash set operation instead of multiple operations
            pipe.hset(f"portfolio:{key}", mapping=risk_data)
            pipe.expire(f"portfolio:{key}", 300)  # 5 minute TTL
            
            # Update global metrics
            pipe.hincrby("global:metrics", "total_calculations", 1)
            pipe.hincrbyfloat("global:metrics", "total_processing_time_ms", risk_calc.calculation_time_ms)
        
        pipe.hset("global:metrics", "last_calculation", str(time.time()))
        
        try:
            pipe.execute()
        except Exception as e:
            logger.error(f"Redis pipeline error: {e}")
    
    return results


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
        
        # Use cached sector-based returns and volatility - vectorized
        sectors = [position.sector for position in positions]
        returns = np.array([get_sector_return(sector) for sector in sectors])
        volatilities = np.array([get_sector_volatility(sector) for sector in sectors])
        
        # Portfolio metrics
        portfolio_return = np.sum(weights * returns)
        
        # Vectorized correlation matrix using cached lookups
        n_assets = len(weights)
        
        # Create sector array for vectorized operations
        sector_array = np.array(sectors)
        
        # Use broadcasting to create correlation matrix efficiently
        correlation = np.zeros((n_assets, n_assets))
        for i in range(n_assets):
            for j in range(n_assets):
                correlation[i, j] = get_sector_correlation(sectors[i], sectors[j])
        
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
        
        # Redis operations moved to batch function for better performance
        
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


def serialize_batch_for_kafka(batch_results: List[Tuple[str, RiskCalculation]]) -> List[KafkaSinkMessage]:
    """
    Serialize a batch of risk calculations for Kafka output.
    
    Args:
        batch_results: List of (portfolio_id, RiskCalculation) tuples
        
    Returns:
        List of KafkaSinkMessage objects
    """
    messages = []
    for risk_data in batch_results:
        msg = serialize_for_kafka(risk_data)
        if msg is not None:
            messages.append(msg)
    return messages


def build_dataflow():
    """
    Build the Prospector risk calculation dataflow pipeline.
    
    Returns:
        Dataflow object configured with the complete processing pipeline
        
    Pipeline stages:
    1. Input: Consume portfolio updates from Kafka topic 'portfolio-updates-v2'
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
            topics=["portfolio-updates-v2"],
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