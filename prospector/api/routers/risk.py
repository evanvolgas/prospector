"""Risk analysis endpoints."""

import json
import logging
from typing import List
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Path
from models import RiskMetricsResponse
from ..core.dependencies import get_redis

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/risk", tags=["Risk"])


@router.get("/{portfolio_id}", response_model=RiskMetricsResponse)
async def get_portfolio_risk(
    portfolio_id: str = Path(..., description="Portfolio identifier")
):
    """
    Retrieve current risk metrics for a specific portfolio.
    """
    redis_client = get_redis()
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        # Try to get from Redis cache
        risk_data = redis_client.hgetall(f"portfolio:{portfolio_id}")
        if not risk_data:
            raise HTTPException(status_code=404, detail=f"No risk data found for portfolio {portfolio_id}")
        
        # Convert string values back to appropriate types
        risk_data['risk_number'] = int(risk_data['risk_number'])
        risk_data['var_95'] = float(risk_data['var_95'])
        risk_data['expected_return'] = float(risk_data['expected_return'])
        risk_data['volatility'] = float(risk_data['volatility'])
        risk_data['sharpe_ratio'] = float(risk_data['sharpe_ratio'])
        risk_data['calculation_time_ms'] = float(risk_data['calculation_time_ms'])
        risk_data['timestamp'] = float(risk_data['timestamp'])
        
        return RiskMetricsResponse(
            portfolio_id=risk_data['portfolio_id'],
            advisor_id=risk_data['advisor_id'],
            risk_number=risk_data['risk_number'],
            var_95=risk_data['var_95'],
            expected_return=risk_data['expected_return'],
            volatility=risk_data['volatility'],
            sharpe_ratio=risk_data['sharpe_ratio'],
            calculation_time_ms=risk_data['calculation_time_ms'],
            timestamp=risk_data['timestamp'],
            last_update=datetime.fromtimestamp(float(risk_data['timestamp']))
        )
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise HTTPException(status_code=500, detail="Invalid data format")
    except Exception as e:
        logger.error(f"Error getting risk data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


