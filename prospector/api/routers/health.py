"""Health and monitoring endpoints."""

import json
import logging
from fastapi import APIRouter, HTTPException
from models import SystemStatus
from ..core.dependencies import get_redis, get_uptime, get_metrics

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Monitoring"])


@router.get("/", tags=["General"])
async def root():
    """
    Root endpoint providing API navigation information.
    """
    return {
        "message": "Prospector Risk Calculator API v1.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health"
    }


@router.get("/health", response_model=SystemStatus)
async def health_check():
    """
    Comprehensive system health check with performance metrics.
    """
    uptime = get_uptime()
    metrics = get_metrics()
    redis_client = get_redis()
    
    # Get metrics from Redis if available
    total_calcs = 0
    avg_time = 0.0
    active_portfolios = 0
    
    if redis_client:
        try:
            # Count all portfolio calculations in Redis
            portfolio_keys = redis_client.keys("portfolio:*")
            total_calcs = len(portfolio_keys)
            active_portfolios = total_calcs
            
            # Calculate average time from recent calculations
            times = []
            for key in portfolio_keys[:100]:  # Sample last 100
                calc_data = redis_client.hgetall(key)
                if calc_data and 'calculation_time_ms' in calc_data:
                    times.append(float(calc_data['calculation_time_ms']))
            
            if times:
                avg_time = sum(times) / len(times)
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
    
    return SystemStatus(
        status="healthy" if metrics['redis_connected'] and metrics['kafka_connected'] else "degraded",
        uptime_seconds=uptime,
        total_calculations=total_calcs,
        avg_calculation_time_ms=avg_time,
        redis_connected=metrics['redis_connected'],
        kafka_connected=metrics['kafka_connected'],
        active_portfolios=active_portfolios
    )