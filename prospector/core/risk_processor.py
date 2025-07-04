"""
Main risk processing logic for portfolio analysis.
"""

import time
import numpy as np
import logging
from typing import Optional, Tuple
import redis

from models import Portfolio, RiskCalculation, RiskTolerance
from ..config.constants import (
    CONSERVATIVE_ADJUSTMENT, AGGRESSIVE_ADJUSTMENT,
    REDIS_TTL, PERFORMANCE_LOG_INTERVAL, Z_SCORE
)
from ..config.securities import get_security_characteristics
from ..utils.performance import PerformanceTracker
from .calculations import (
    calculate_correlation_matrix,
    calculate_portfolio_metrics,
    calculate_value_at_risk,
    downside_percentage_to_risk_number
)

logger = logging.getLogger(__name__)


class RiskProcessor:
    """
    Main processor for portfolio risk calculations.
    
    Handles the complete risk calculation pipeline including:
    - Security characteristic retrieval
    - Portfolio metrics calculation
    - Risk score generation
    - Results caching
    - Performance tracking
    """
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        """
        Initialize risk processor.
        
        Args:
            redis_client: Optional Redis client for caching
        """
        self.redis_client = redis_client
        self.perf_tracker = PerformanceTracker()
        
    def calculate_portfolio_risk(
        self, 
        portfolio_tuple: Tuple[str, Portfolio]
    ) -> Optional[Tuple[str, RiskCalculation]]:
        """
        Calculate comprehensive portfolio risk metrics.
        
        Args:
            portfolio_tuple: Tuple of (portfolio_id, Portfolio object)
            
        Returns:
            Tuple of (portfolio_id, RiskCalculation) or None on error
        """
        if portfolio_tuple is None:
            return None
            
        key, portfolio = portfolio_tuple
        start_time = time.time()
        
        try:
            # Extract portfolio data
            positions = portfolio.positions
            total_value = portfolio.total_value
            
            # Get position weights
            weights = np.array([position.weight / 100.0 for position in positions])
            
            # Get individual security characteristics
            returns = []
            volatilities = []
            betas = []
            
            for position in positions:
                chars = get_security_characteristics(position.symbol)
                returns.append(chars["expected_return"])
                volatilities.append(chars["volatility"])
                betas.append(chars["beta"])
            
            returns = np.array(returns)
            volatilities = np.array(volatilities)
            betas = np.array(betas)
            
            # Calculate portfolio beta
            portfolio_beta = np.sum(weights * betas)
            
            # Build correlation matrix
            correlation = calculate_correlation_matrix(positions)
            
            # Calculate portfolio metrics
            portfolio_return, portfolio_volatility, sharpe_ratio = calculate_portfolio_metrics(
                weights, returns, volatilities, correlation
            )
            
            # Calculate downside risk percentage
            downside_percentage = -Z_SCORE * portfolio_volatility * 100
            
            # Calculate VaR
            var_95 = calculate_value_at_risk(total_value, portfolio_volatility)
            
            # Convert to risk number
            risk_number = downside_percentage_to_risk_number(downside_percentage)
            
            # Apply behavioral adjustments
            risk_number = self._apply_risk_tolerance_adjustment(
                risk_number, portfolio.risk_tolerance
            )
            
            # Additional metrics
            downside_capture = portfolio_beta * 100  # Simplified metric
            
            calculation_time = (time.time() - start_time) * 1000
            
            # Create risk calculation result
            risk_calc = RiskCalculation(
                portfolio_id=portfolio.id,
                advisor_id=portfolio.advisor_id,
                risk_number=risk_number,
                var_95=var_95,
                expected_return=portfolio_return,
                volatility=portfolio_volatility,
                sharpe_ratio=sharpe_ratio,
                calculation_time_ms=calculation_time
            )
            
            # Cache results
            self._cache_results(key, risk_calc, downside_percentage, 
                              portfolio_beta, downside_capture)
            
            # Track performance
            self.perf_tracker.record_message(calculation_time)
            self.perf_tracker.log_stats(PERFORMANCE_LOG_INTERVAL)
            
            # Log completion
            logger.info(
                f"✅ Calculated risk for {portfolio.id}: "
                f"Risk={risk_number}, Downside={downside_percentage:.1f}%, "
                f"VaR=${var_95:,.2f}, Beta={portfolio_beta:.2f}"
            )
            
            return (key, risk_calc)
            
        except Exception as e:
            logger.error(f"Error calculating risk for {key}: {e}")
            return None
    
    def _apply_risk_tolerance_adjustment(
        self, 
        risk_number: int, 
        risk_tolerance: RiskTolerance
    ) -> int:
        """
        Apply behavioral adjustments based on investor risk tolerance.
        
        Args:
            risk_number: Base risk number
            risk_tolerance: Investor's risk tolerance level
            
        Returns:
            Adjusted risk number
        """
        if risk_tolerance == RiskTolerance.CONSERVATIVE:
            # Conservative investors perceive more risk
            risk_number = min(100, int(risk_number * CONSERVATIVE_ADJUSTMENT))
        elif risk_tolerance == RiskTolerance.AGGRESSIVE:
            # Aggressive investors perceive less risk
            risk_number = max(20, int(risk_number * AGGRESSIVE_ADJUSTMENT))
            
        return risk_number
    
    def _cache_results(
        self,
        key: str,
        risk_calc: RiskCalculation,
        downside_percentage: float,
        portfolio_beta: float,
        downside_capture: float
    ) -> None:
        """
        Cache risk calculation results in Redis.
        
        Args:
            key: Portfolio key
            risk_calc: Calculated risk metrics
            downside_percentage: Downside risk percentage
            portfolio_beta: Portfolio beta
            downside_capture: Downside capture ratio
        """
        if not self.redis_client:
            return
            
        risk_data = {
            "portfolio_id": risk_calc.portfolio_id,
            "advisor_id": risk_calc.advisor_id,
            "risk_number": str(risk_calc.risk_number),
            "var_95": str(risk_calc.var_95),
            "expected_return": str(risk_calc.expected_return),
            "volatility": str(risk_calc.volatility),
            "sharpe_ratio": str(risk_calc.sharpe_ratio),
            "downside_percentage": str(downside_percentage),
            "portfolio_beta": str(portfolio_beta),
            "downside_capture": str(downside_capture),
            "calculation_time_ms": str(risk_calc.calculation_time_ms),
            "timestamp": str(risk_calc.timestamp),
            "methodology": "advanced_behavioral"
        }
        
        try:
            self.redis_client.hset(f"portfolio:{key}", mapping=risk_data)
            self.redis_client.expire(f"portfolio:{key}", REDIS_TTL)
            
            # Update global metrics
            self.redis_client.hincrby("global:metrics", "total_calculations", 1)
            self.redis_client.hincrbyfloat(
                "global:metrics", 
                "total_processing_time_ms", 
                risk_calc.calculation_time_ms
            )
        except Exception as e:
            logger.error(f"Redis error: {e}")