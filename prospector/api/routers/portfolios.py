"""Portfolio collection endpoints."""

import logging
from typing import List
from fastapi import APIRouter, HTTPException, Query
from ..core.dependencies import get_redis

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolios", tags=["Risk"])


@router.get("/at-risk", response_model=List[str])
async def get_high_risk_portfolios(
    risk_threshold: int = Query(70, ge=1, le=99, description="Risk threshold (1-99)")
):
    """
    Identify portfolios exceeding specified risk threshold.
    """
    redis_client = get_redis()
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        high_risk_portfolios = []
        
        # Scan all portfolio calculations
        portfolio_keys = redis_client.keys("portfolio:*")
        for key in portfolio_keys:
            calc_data = redis_client.hgetall(key)
            if calc_data and 'risk_number' in calc_data:
                if int(calc_data['risk_number']) >= risk_threshold:
                    high_risk_portfolios.append(calc_data['portfolio_id'])
        
        return high_risk_portfolios
        
    except Exception as e:
        logger.error(f"Error getting high risk portfolios: {e}")
        raise HTTPException(status_code=500, detail=str(e))