"""Portfolio management endpoints."""

import logging
from typing import Dict
from fastapi import APIRouter, HTTPException, Query
from models import (
    Portfolio, Position, PortfolioUpdate,
    RiskTolerance, Sector, AccountType
)
from ..core.dependencies import get_kafka_producer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolio", tags=["Portfolio"])


@router.post("/update", response_model=Dict[str, str])
async def update_portfolio(update: PortfolioUpdate):
    """
    Submit a portfolio update for asynchronous risk calculation.
    """
    kafka_producer = get_kafka_producer()
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Message broker unavailable")
    
    try:
        # Send to Kafka
        kafka_producer.produce(
            'portfolio-updates-v2',
            key=update.portfolio.id.encode(),
            value=update.portfolio.model_dump_json().encode()
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


@router.post("/simulate", response_model=Dict[str, str], tags=["Testing"])
async def simulate_portfolio_update(
    portfolio_id: str = Query(..., description="Portfolio ID"),
    advisor_id: str = Query("advisor-1", description="Advisor ID"),
    risk_tolerance: RiskTolerance = Query(RiskTolerance.MODERATE, description="Risk tolerance")
):
    """
    Generate and submit a test portfolio for development/testing.
    """
    kafka_producer = get_kafka_producer()
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
            'portfolio-updates-v2',
            key=portfolio.id.encode(),
            value=portfolio.model_dump_json().encode()
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