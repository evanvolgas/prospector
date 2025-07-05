#!/usr/bin/env python3
"""
Prospector Risk Calculator - High-Performance Portfolio Risk Analysis Engine.

This is the main entry point for the Prospector real-time risk calculation system.
It processes portfolio updates from Kafka, calculates comprehensive risk metrics,
and caches results in Redis for API consumption.

Architecture Overview:
    - Stream Processing: Bytewax-based pipeline for high-throughput processing
    - Risk Engine: Behavioral finance model with 20-100 risk scoring
    - Data Flow: Kafka (input) -> Risk Processor -> Redis (cache)
    - Performance: Capable of processing 79,000+ messages/second

Key Features:
    - Real-time portfolio risk assessment
    - Individual security analysis (50+ supported stocks)
    - Behavioral risk adjustments based on investor profile
    - Dollar-based risk metrics (Value at Risk)
    - Correlation-based portfolio analysis

For detailed architecture information, see ARCHITECTURE.md
"""

import logging
import time
import redis
from bytewax.run import cli_main

from prospector.core.risk_processor import RiskProcessor
from prospector.streaming.pipeline import build_dataflow

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_redis() -> redis.Redis:
    """
    Initialize Redis connection with connection pooling for better performance.
    
    Establishes a connection pool to the local Redis instance used for:
    - Caching calculated risk metrics for API retrieval
    - Storing global performance metrics
    - Portfolio calculation deduplication
    
    Returns:
        redis.Redis: Connected Redis client with connection pool, or None if connection fails
        
    Note:
        The system can operate without Redis, but API endpoints will
        not have access to calculated risk data.
    """
    try:
        # Use connection pool for better performance with multiple workers
        pool = redis.ConnectionPool(
            host='localhost', 
            port=6379, 
            decode_responses=True,
            max_connections=50  # Support multiple concurrent operations
        )
        client = redis.Redis(connection_pool=pool)
        client.ping()
        logger.info("✅ Redis connected with connection pooling")
        return client
    except Exception as e:
        logger.warning(f"⚠️ Redis not available: {e}")
        return None


def main():
    """
    Main entry point for the Prospector risk calculation engine.
    
    This function orchestrates the entire risk calculation pipeline:
    
    1. Initializes Redis connection for result caching
    2. Creates RiskProcessor instance with behavioral risk model
    3. Builds Bytewax dataflow pipeline with Kafka integration
    4. Launches continuous stream processing
    
    The pipeline consumes from 'portfolio-updates-v2' Kafka topic and
    publishes results to Redis for API consumption.
    
    Usage:
        Run directly: python risk_calculator.py
        With workers: python risk_calculator.py -w 4
        
    Environment Requirements:
        - Kafka broker running on localhost:9092
        - Redis server running on localhost:6379
        - 'portfolio-updates-v2' topic created in Kafka
    """
    logger.info("🚀 Starting Prospector Risk Calculator")
    logger.info("📊 Advanced behavioral risk methodology")
    logger.info("🎯 Risk scores on intuitive 20-100 scale")
    logger.info("📈 Individual security analysis with 50+ stocks")
    logger.info("💰 Dollar-based risk metrics for practical insights")
    logger.info("⚡ Real-time stream processing with Bytewax")
    
    # Initialize Redis
    redis_client = initialize_redis()
    
    # Reset global metrics
    if redis_client:
        redis_client.delete("global:metrics")
        redis_client.hset("global:metrics", "start_time", str(time.time()))
    
    # Create risk processor
    risk_processor = RiskProcessor(redis_client)
    
    # Build and run dataflow
    flow = build_dataflow(risk_processor)
    cli_main(flow)


if __name__ == "__main__":
    main()