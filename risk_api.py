#!/usr/bin/env python3
"""
Prospector Risk API - Real-time portfolio risk monitoring and analytics.

This FastAPI service provides comprehensive REST endpoints for:

1. Risk Monitoring:
   - Real-time risk metrics retrieval
   - High-risk portfolio identification
   - Advisor portfolio aggregation
   
2. Portfolio Management:
   - Submit portfolio updates for processing
   - Simulate test portfolios
   
3. Analytics & Metrics:
   - System health monitoring
   - Aggregate risk statistics
   - Performance metrics tracking
   
4. Real-time Streaming:
   - Server-Sent Events (SSE) for live risk updates
   - Filtered streaming by portfolio ID

The API integrates with Redis for caching and Kafka for message streaming,
providing sub-second response times for risk queries.
"""

import json
import time
from typing import Dict, List, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import redis
import uvicorn
from confluent_kafka import Producer, Consumer, KafkaError
import asyncio
import logging

from models import (
    Portfolio, Position, MarketData, RiskCalculation, PortfolioUpdate,
    RiskMetricsResponse, PortfolioStats, SystemStatus, 
    MetricsSummary, ErrorResponse, RiskTolerance, Sector, AccountType
)

# Set up logging
logging.basicConfig(level=logging.INFO)
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
    global redis_client, kafka_producer, metrics
    
    # Startup
    logger.info("🚀 Starting Risk Calculator API...")
    
    # Initialize Redis
    try:
        redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        redis_client.ping()
        metrics['redis_connected'] = True
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        redis_client = None
    
    # Initialize Kafka Producer
    try:
        kafka_producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'client.id': 'prospector-api-producer'
        })
        metrics['kafka_connected'] = True
        logger.info("✅ Kafka connected")
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        kafka_producer = None
    
    yield
    
    # Shutdown
    logger.info("👋 Shutting down Risk Calculator API...")
    if kafka_producer:
        kafka_producer.flush()
    if redis_client:
        redis_client.close()

# FastAPI app with lifespan events
app = FastAPI(
    title="Prospector Risk Calculator API", 
    version="1.0.0",
    description="Real-time portfolio risk calculation and monitoring API powered by Prospector",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=exc.detail,
            detail=str(exc)
        ).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="Internal server error",
            detail=str(exc)
        ).dict()
    )

# API Endpoints
@app.get("/", tags=["General"])
async def root():
    """
    Root endpoint providing API navigation information.
    
    Returns:
        Dictionary with links to documentation and key endpoints
    """
    return {
        "message": "Prospector Risk Calculator API v1.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health"
    }

@app.get("/health", response_model=SystemStatus, tags=["Monitoring"])
async def health_check():
    """
    Comprehensive system health check with performance metrics.
    
    Returns:
        SystemStatus object containing:
        - Service uptime in seconds
        - Total risk calculations processed
        - Average calculation time in milliseconds
        - Redis and Kafka connection status
        - Number of active portfolios in cache
        
    The endpoint samples up to 100 recent calculations to compute
    average processing time.
    """
    uptime = time.time() - start_time
    
    # Get metrics from Redis if available
    total_calcs = 0
    avg_time = 0.0
    active_portfolios = 0
    
    if redis_client:
        try:
            # Count all risk calculations in Redis
            risk_keys = redis_client.keys("risk:*")
            total_calcs = len(risk_keys)
            active_portfolios = total_calcs
            
            # Calculate average time from recent calculations
            times = []
            for key in risk_keys[:100]:  # Sample last 100
                data = redis_client.get(key)
                if data:
                    calc = json.loads(data)
                    times.append(calc.get('calculation_time_ms', 0))
            
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

@app.get("/risk/{portfolio_id}", response_model=RiskMetricsResponse, tags=["Risk"])
async def get_portfolio_risk(
    portfolio_id: str = Path(..., description="Portfolio identifier")
):
    """
    Retrieve current risk metrics for a specific portfolio.
    
    Args:
        portfolio_id: Unique portfolio identifier
        
    Returns:
        RiskMetricsResponse containing:
        - Risk number (1-99 scale)
        - Value at Risk (95% confidence)
        - Expected portfolio return
        - Portfolio volatility
        - Sharpe ratio
        - Calculation timestamp and processing time
        
    Raises:
        404: Portfolio not found in cache
        503: Redis service unavailable
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        # Try to get from Redis cache
        risk_data = redis_client.get(f"risk:{portfolio_id}")
        if not risk_data:
            raise HTTPException(status_code=404, detail=f"No risk data found for portfolio {portfolio_id}")
        
        risk_calc = RiskCalculation(**json.loads(risk_data))
        
        return RiskMetricsResponse(
            portfolio_id=risk_calc.portfolio_id,
            advisor_id=risk_calc.advisor_id,
            risk_number=risk_calc.risk_number,
            var_95=risk_calc.var_95,
            expected_return=risk_calc.expected_return,
            volatility=risk_calc.volatility,
            sharpe_ratio=risk_calc.sharpe_ratio,
            calculation_time_ms=risk_calc.calculation_time_ms,
            timestamp=risk_calc.timestamp,
            last_update=risk_calc.calculation_datetime
        )
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise HTTPException(status_code=500, detail="Invalid data format")
    except Exception as e:
        logger.error(f"Error getting risk data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/portfolios/at-risk", response_model=List[str], tags=["Risk"])
async def get_high_risk_portfolios(
    risk_threshold: int = Query(70, ge=1, le=99, description="Risk threshold (1-99)")
):
    """
    Identify portfolios exceeding specified risk threshold.
    
    Args:
        risk_threshold: Minimum risk number to filter (default: 70)
        
    Returns:
        List of portfolio IDs with risk >= threshold
        
    Useful for:
    - Risk alerts and monitoring
    - Compliance reporting
    - Portfolio rebalancing triggers
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        high_risk_portfolios = []
        
        # Scan all risk calculations
        risk_keys = redis_client.keys("risk:*")
        for key in risk_keys:
            data = redis_client.get(key)
            if data:
                risk_calc = json.loads(data)
                if risk_calc.get('risk_number', 0) >= risk_threshold:
                    high_risk_portfolios.append(risk_calc['portfolio_id'])
        
        return high_risk_portfolios
        
    except Exception as e:
        logger.error(f"Error getting high risk portfolios: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/advisor/{advisor_id}/portfolios", response_model=List[PortfolioStats], tags=["Advisor"])
async def get_advisor_portfolios(
    advisor_id: str = Path(..., description="Advisor identifier")
):
    """
    Retrieve all portfolios managed by a specific advisor.
    
    Args:
        advisor_id: Unique advisor identifier
        
    Returns:
        List of PortfolioStats containing:
        - Portfolio ID
        - Last update timestamp
        - Total risk calculations performed
        - Current risk number
        
    Useful for advisor dashboards and client reporting.
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        advisor_portfolios = []
        
        # Scan risk calculations for this advisor
        risk_keys = redis_client.keys("risk:*")
        for key in risk_keys:
            data = redis_client.get(key)
            if data:
                risk_calc = json.loads(data)
                if risk_calc.get('advisor_id') == advisor_id:
                    # Get portfolio stats
                    stats_key = f"stats:{risk_calc['portfolio_id']}"
                    stats_data = redis_client.get(stats_key)
                    
                    total_calcs = 1
                    if stats_data:
                        stats = json.loads(stats_data)
                        total_calcs = stats.get('count', 1)
                    
                    advisor_portfolios.append(PortfolioStats(
                        portfolio_id=risk_calc['portfolio_id'],
                        last_update=datetime.fromtimestamp(risk_calc['timestamp']),
                        total_calculations=total_calcs,
                        current_risk_number=risk_calc['risk_number']
                    ))
        
        return advisor_portfolios
        
    except Exception as e:
        logger.error(f"Error getting advisor portfolios: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/portfolio/update", response_model=Dict[str, str], tags=["Portfolio"])
async def update_portfolio(update: PortfolioUpdate):
    """
    Submit a portfolio update for asynchronous risk calculation.
    
    Args:
        update: PortfolioUpdate containing complete portfolio data
        
    Returns:
        Success confirmation with portfolio ID
        
    The update is published to Kafka for processing by the
    Prospector risk calculation engine. Results will be available
    via the GET /risk/{portfolio_id} endpoint after processing.
    """
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Message broker unavailable")
    
    try:
        # Send to Kafka
        kafka_producer.produce(
            'portfolio-updates',
            key=update.portfolio.id.encode(),
            value=update.portfolio.json().encode()
        )
        kafka_producer.flush()
        
        return {
            "status": "success",
            "message": f"Portfolio update sent for {update.portfolio.id}",
            "portfolio_id": update.portfolio.id
        }
        
    except Exception as e:
        logger.error(f"Error sending portfolio update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/simulate/portfolio", response_model=Dict[str, str], tags=["Testing"])
async def simulate_portfolio_update(
    portfolio_id: str = Query(..., description="Portfolio ID"),
    advisor_id: str = Query("advisor-1", description="Advisor ID"),
    risk_tolerance: RiskTolerance = Query(RiskTolerance.MODERATE, description="Risk tolerance")
):
    """
    Generate and submit a test portfolio for development/testing.
    
    Args:
        portfolio_id: Custom portfolio identifier
        advisor_id: Advisor identifier (default: advisor-1)
        risk_tolerance: Risk profile (CONSERVATIVE/MODERATE/AGGRESSIVE)
        
    Returns:
        Confirmation with portfolio ID and total value
        
    Creates a sample portfolio with:
    - 3 positions (AAPL, MSFT, JNJ)
    - Diversified across Technology and Healthcare
    - Realistic market values and weights
    """
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Message broker unavailable")
    
    try:
        # Create a sample portfolio
        positions = [
            Position(
                symbol="AAPL",
                quantity=100,
                price=185.50,
                market_value=18550.0,
                weight=40.0,
                sector=Sector.TECHNOLOGY
            ),
            Position(
                symbol="MSFT",
                quantity=50,
                price=420.25,
                market_value=21012.50,
                weight=45.0,
                sector=Sector.TECHNOLOGY
            ),
            Position(
                symbol="JNJ",
                quantity=75,
                price=155.75,
                market_value=11681.25,
                weight=15.0,
                sector=Sector.HEALTHCARE
            )
        ]
        
        portfolio = Portfolio(
            id=portfolio_id,
            advisor_id=advisor_id,
            client_id=f"client-{portfolio_id}",
            positions=positions,
            total_value=sum(p.market_value for p in positions),
            risk_tolerance=risk_tolerance,
            account_type=AccountType.INDIVIDUAL
        )
        
        # Send to Kafka
        kafka_producer.produce(
            'portfolio-updates',
            key=portfolio.id.encode(),
            value=portfolio.json().encode()
        )
        kafka_producer.flush()
        
        return {
            "status": "success",
            "message": f"Portfolio simulation sent for {portfolio_id}",
            "portfolio_value": f"${portfolio.total_value:,.2f}"
        }
        
    except Exception as e:
        logger.error(f"Error simulating portfolio update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/summary", response_model=MetricsSummary, tags=["Analytics"])
async def get_metrics_summary():
    """
    Aggregate risk metrics across all portfolios in the system.
    
    Returns:
        MetricsSummary containing:
        - Total portfolio count
        - Average risk number
        - Combined Value at Risk
        - High-risk portfolio count (risk >= 70)
        - Risk distribution (low/moderate/high percentages)
        
    Risk categories:
    - Low: risk < 30
    - Moderate: 30 <= risk < 70
    - High: risk >= 70
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        risk_keys = redis_client.keys("risk:*")
        
        if not risk_keys:
            return MetricsSummary(
                total_portfolios=0,
                avg_risk_number=0,
                total_value_at_risk=0,
                high_risk_count=0,
                risk_distribution={"low": 0, "moderate": 0, "high": 0}
            )
        
        risk_numbers = []
        total_var = 0.0
        high_risk_count = 0
        risk_distribution = {"low": 0, "moderate": 0, "high": 0}
        
        for key in risk_keys:
            data = redis_client.get(key)
            if data:
                risk_calc = json.loads(data)
                risk_num = risk_calc.get('risk_number', 0)
                risk_numbers.append(risk_num)
                total_var += risk_calc.get('var_95', 0)
                
                if risk_num >= 70:
                    high_risk_count += 1
                    risk_distribution["high"] += 1
                elif risk_num >= 30:
                    risk_distribution["moderate"] += 1
                else:
                    risk_distribution["low"] += 1
        
        return MetricsSummary(
            total_portfolios=len(risk_numbers),
            avg_risk_number=sum(risk_numbers) / len(risk_numbers) if risk_numbers else 0,
            total_value_at_risk=total_var,
            high_risk_count=high_risk_count,
            risk_distribution=risk_distribution
        )
        
    except Exception as e:
        logger.error(f"Error getting metrics summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/risk-updates", tags=["Streaming"])
async def stream_risk_updates(portfolio_id: Optional[str] = None):
    """
    Stream real-time risk updates via Server-Sent Events (SSE).
    
    Args:
        portfolio_id: Optional filter for specific portfolio updates
        
    Returns:
        SSE stream of risk calculations as they occur
        
    Usage:
        - Connect with EventSource in JavaScript
        - Or use curl: curl http://localhost:6066/stream/risk-updates
        
    Each event contains JSON-encoded RiskCalculation data.
    Connection remains open until client disconnects.
    """
    async def event_generator():
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'risk-api-stream-{time.time()}',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['risk-updates'])
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    # Filter by portfolio_id if specified
                    if portfolio_id and data.get('portfolio_id') != portfolio_id:
                        continue
                    
                    yield f"data: {json.dumps(data)}\n\n"
                    
                except json.JSONDecodeError:
                    logger.error("Failed to decode message")
                    
        finally:
            consumer.close()
    
    from fastapi.responses import StreamingResponse
    return StreamingResponse(event_generator(), media_type="text/event-stream")

def main():
    """Entry point for the risk API server."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=6066)


if __name__ == "__main__":
    main()