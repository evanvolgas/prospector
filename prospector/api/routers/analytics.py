"""Analytics and metrics endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from models import MetricsSummary
from ..core.dependencies import get_redis

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/metrics", tags=["Analytics"])


@router.get("/summary", response_model=MetricsSummary)
async def get_metrics_summary():
    """
    Aggregate risk metrics across all portfolios in the system.
    """
    redis_client = get_redis()
    if not redis_client:
        raise HTTPException(status_code=503, detail="Cache service unavailable")
    
    try:
        portfolio_keys = redis_client.keys("portfolio:*")
        
        if not portfolio_keys:
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
        
        for key in portfolio_keys:
            calc_data = redis_client.hgetall(key)
            if calc_data and 'risk_number' in calc_data:
                risk_num = int(calc_data['risk_number'])
                risk_numbers.append(risk_num)
                total_var += float(calc_data.get('var_95', 0))
                
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