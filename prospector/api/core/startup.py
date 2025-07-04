"""Application lifecycle management."""

import logging
import redis
from confluent_kafka import Producer
from contextlib import asynccontextmanager
from fastapi import FastAPI

from . import dependencies as deps

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle with proper resource initialization.
    
    Startup:
    - Establishes Redis connection for caching
    - Initializes Kafka producer for message publishing
    - Sets service status metrics
    
    Shutdown:
    - Flushes pending Kafka messages
    - Closes Redis connection gracefully
    """
    # Startup
    logger.info("🚀 Starting Risk Calculator API...")
    
    # Initialize Redis
    try:
        deps.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        deps.redis_client.ping()
        deps.metrics['redis_connected'] = True
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        deps.redis_client = None
    
    # Initialize Kafka Producer
    try:
        deps.kafka_producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'prospector-api-producer'
        })
        deps.metrics['kafka_connected'] = True
        logger.info("✅ Kafka connected")
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        deps.kafka_producer = None
    
    yield
    
    # Shutdown
    logger.info("👋 Shutting down Risk Calculator API...")
    if deps.kafka_producer:
        deps.kafka_producer.flush()
    if deps.redis_client:
        deps.redis_client.close()