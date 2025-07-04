"""Advisor-specific endpoints."""

import json
import logging
from typing import List
from datetime import datetime
from fastapi import APIRouter, HTTPException, Path
from models import PortfolioStats
from ..core.dependencies import get_redis

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/advisor", tags=["Advisor"])


@router.get("/{advisor_id}/portfolios", response_model=List[PortfolioStats])
async def get_advisor_portfolios(
    advisor_id: str = Path(..., description="Advisor identifier")
):
    """
    Retrieve all portfolios managed by a specific advisor.
    """
    redis_client = get_redis()
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        advisor_portfolios = []
        
        # Scan portfolio calculations for this advisor
        portfolio_keys = redis_client.keys("portfolio:*")
        for key in portfolio_keys:
            calc_data = redis_client.hgetall(key)
            if calc_data and calc_data.get('advisor_id') == advisor_id:
                # Get portfolio stats
                stats_key = f"stats:{calc_data['portfolio_id']}"
                stats_data = redis_client.get(stats_key)
                
                total_calcs = 1
                if stats_data:
                    stats = json.loads(stats_data)
                    total_calcs = stats.get('count', 1)
                
                advisor_portfolios.append(PortfolioStats(
                    portfolio_id=calc_data['portfolio_id'],
                    last_update=datetime.fromtimestamp(float(calc_data['timestamp'])),
                    total_calculations=total_calcs,
                    current_risk_number=int(calc_data['risk_number'])
                ))
        
        return advisor_portfolios
        
    except Exception as e:
        logger.error(f"Error getting advisor portfolios: {e}")
        raise HTTPException(status_code=500, detail=str(e))