"""
Core risk calculation functions for portfolio analysis.
"""

import numpy as np
from typing import List, Tuple
import logging

from ..config.constants import (
    Z_SCORE, RISK_FREE_RATE, MIN_RISK_NUMBER, MAX_RISK_NUMBER,
    SAME_SECTOR_CORRELATION, DIFFERENT_SECTOR_CORRELATION,
    BETA_CORRELATION_ADJUSTMENT, MIN_CORRELATION, MAX_CORRELATION
)
from ..config.securities import get_security_characteristics
from models import Position

logger = logging.getLogger(__name__)


def calculate_downside_risk(returns: np.ndarray, target_return: float = 0) -> float:
    """
    Calculate downside deviation focusing only on returns below target.
    
    Args:
        returns: Array of historical returns
        target_return: Minimum acceptable return (default 0)
        
    Returns:
        Downside deviation (semi-standard deviation)
        
    This metric better captures investor risk perception as it focuses only
    on the possibility of losses, not general volatility.
    """
    downside_returns = returns[returns < target_return]
    
    if len(downside_returns) == 0:
        return 0.0
    
    downside_deviation = np.sqrt(np.mean((downside_returns - target_return) ** 2))
    return downside_deviation


def calculate_correlation_matrix(positions: List[Position]) -> np.ndarray:
    """
    Build sophisticated correlation matrix based on individual securities.
    
    Args:
        positions: List of portfolio positions
        
    Returns:
        Correlation matrix incorporating sector and beta relationships
        
    The correlation model considers:
    1. Base sector correlation (0.7 for same sector, 0.3 for different)
    2. Beta similarity adjustment (similar betas increase correlation)
    3. Bounds checking to ensure valid correlation values
    """
    n = len(positions)
    correlation = np.zeros((n, n))
    
    for i in range(n):
        for j in range(n):
            if i == j:
                correlation[i, j] = 1.0
            else:
                symbol_i = positions[i].symbol
                symbol_j = positions[j].symbol
                sector_i = positions[i].sector
                sector_j = positions[j].sector
                
                # Base correlation on sectors
                if sector_i == sector_j:
                    base_corr = SAME_SECTOR_CORRELATION
                else:
                    base_corr = DIFFERENT_SECTOR_CORRELATION
                
                # Adjust based on beta similarity
                beta_i = get_security_characteristics(symbol_i)["beta"]
                beta_j = get_security_characteristics(symbol_j)["beta"]
                beta_diff = abs(beta_i - beta_j)
                
                # Similar betas increase correlation (max adjustment ±0.1)
                beta_adjustment = -BETA_CORRELATION_ADJUSTMENT * min(beta_diff, 1.0)
                
                # Ensure correlation stays within valid bounds
                correlation[i, j] = min(MAX_CORRELATION, 
                                      max(MIN_CORRELATION, base_corr + beta_adjustment))
    
    return correlation


def downside_percentage_to_risk_number(downside_pct: float) -> int:
    """
    Convert downside risk percentage to intuitive risk score (20-100).
    
    Args:
        downside_pct: Downside risk as negative percentage
        
    Returns:
        Risk number between 20 and 100
        
    Mapping logic:
    - 0% to -2%: Linear scale from 20 to 25 (very low risk)
    - -2% to -18%: Exponential scale from 25 to 85 (increasing concern)
    - -18% to -30%: Linear scale from 85 to 100 (high risk)
    - Beyond -30%: Capped at 100
    
    This non-linear mapping better reflects how investors perceive risk,
    with accelerating concern as potential losses increase.
    """
    if downside_pct >= 0:
        return MIN_RISK_NUMBER
    
    # Convert to positive for calculation
    downside_abs = abs(downside_pct)
    
    if downside_abs <= 2:
        # Very low risk zone - linear mapping
        risk_number = MIN_RISK_NUMBER + (downside_abs / 2) * 5
    elif downside_abs <= 18:
        # Moderate to high risk - exponential mapping for intuitive scaling
        normalized = (downside_abs - 2) / 16  # 0 to 1
        risk_number = 25 + normalized * normalized * 60  # Quadratic scaling
    else:
        # Very high risk zone - linear mapping
        normalized = min((downside_abs - 18) / 12, 1)  # Cap at 30% downside
        risk_number = 85 + normalized * 15
    
    return int(min(MAX_RISK_NUMBER, max(MIN_RISK_NUMBER, risk_number)))


def calculate_portfolio_metrics(
    weights: np.ndarray,
    returns: np.ndarray,
    volatilities: np.ndarray,
    correlation: np.ndarray
) -> Tuple[float, float, float]:
    """
    Calculate core portfolio metrics.
    
    Args:
        weights: Position weights as fractions
        returns: Expected returns for each position
        volatilities: Volatility for each position
        correlation: Correlation matrix
        
    Returns:
        Tuple of (portfolio_return, portfolio_volatility, sharpe_ratio)
    """
    # Portfolio expected return (weighted average)
    portfolio_return = np.sum(weights * returns)
    
    # Covariance matrix
    cov_matrix = np.outer(volatilities, volatilities) * correlation
    
    # Portfolio variance and volatility
    portfolio_variance = np.dot(weights, np.dot(cov_matrix, weights))
    portfolio_volatility = np.sqrt(portfolio_variance)
    
    # Sharpe ratio
    sharpe_ratio = (portfolio_return - RISK_FREE_RATE) / portfolio_volatility if portfolio_volatility > 0 else 0
    
    return portfolio_return, portfolio_volatility, sharpe_ratio


def calculate_value_at_risk(
    total_value: float,
    portfolio_volatility: float,
    confidence_level: float = 0.95
) -> float:
    """
    Calculate Value at Risk (VaR) for the portfolio.
    
    Args:
        total_value: Total portfolio value in dollars
        portfolio_volatility: Portfolio volatility (standard deviation)
        confidence_level: Confidence level for VaR (default 95%)
        
    Returns:
        VaR in dollars
    """
    # Downside percentage at confidence level
    downside_percentage = -Z_SCORE * portfolio_volatility * 100
    
    # VaR in dollars
    var = abs(downside_percentage / 100 * total_value)
    
    return var