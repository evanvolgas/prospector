"""API dependencies and shared resources."""

import time
import redis
import logging
from typing import Optional
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

# Global resources
redis_client: Optional[redis.Redis] = None
kafka_producer: Optional[Producer] = None
start_time = time.time()
metrics = {
    'total_calculations': 0,
    'total_time_ms': 0.0,
    'kafka_connected': False,
    'redis_connected': False
}


def get_redis() -> Optional[redis.Redis]:
    """Get Redis client instance."""
    return redis_client


def get_kafka_producer() -> Optional[Producer]:
    """Get Kafka producer instance."""
    return kafka_producer


def get_metrics() -> dict:
    """Get current metrics."""
    return metrics


def get_uptime() -> float:
    """Get API uptime in seconds."""
    return time.time() - start_time