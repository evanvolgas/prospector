"""
Prospector Data Models - Comprehensive type definitions for portfolio risk analysis.

This module defines all Pydantic models used throughout the Prospector system,
providing strict type validation and serialization for:

1. Portfolio Components:
   - Position: Individual stock holdings with sector classification
   - Portfolio: Complete portfolio with positions and metadata
   - MarketData: Real-time market metrics for securities

2. Risk Analysis:
   - RiskCalculation: Computed risk metrics and scores
   - RiskMetricsResponse: API response format for risk data

3. System Types:
   - Enums for risk tolerance, account types, and sectors
   - API request/response models
   - System monitoring and statistics models

All models include validation rules to ensure data integrity
and provide automatic JSON serialization/deserialization.
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, ConfigDict
from enum import Enum


class RiskTolerance(str, Enum):
    CONSERVATIVE = "Conservative"
    MODERATE = "Moderate"
    AGGRESSIVE = "Aggressive"


class AccountType(str, Enum):
    INDIVIDUAL = "Individual"
    JOINT = "Joint"
    IRA = "IRA"
    ROTH_IRA = "Roth IRA"
    FOUR_ZERO_ONE_K = "401k"
    TRUST = "Trust"


class Sector(str, Enum):
    TECHNOLOGY = "Technology"
    HEALTHCARE = "Healthcare"
    FINANCE = "Finance"
    CONSUMER = "Consumer"
    ENERGY = "Energy"
    REAL_ESTATE = "Real Estate"
    RETAIL = "Retail"
    TELECOM = "Telecom"
    ENTERTAINMENT = "Entertainment"
    AUTOMOTIVE = "Automotive"
    OTHER = "Other"


class Position(BaseModel):
    """
    Individual security position within a portfolio.

    Represents a single stock holding with its market value,
    portfolio weight, and sector classification for correlation
    analysis in risk calculations.
    """

    symbol: str = Field(..., description="Stock ticker symbol")
    quantity: float = Field(..., gt=0, description="Number of shares")
    price: float = Field(..., gt=0, description="Current price per share")
    market_value: float = Field(..., gt=0, description="Total market value")
    weight: float = Field(..., ge=0, le=100, description="Position weight as percentage")
    sector: Sector = Field(default=Sector.OTHER, description="Industry sector")

    @field_validator("market_value")
    @classmethod
    def validate_market_value(cls, v, info):
        """Ensure market value matches quantity * price within tolerance"""
        if info.data and "quantity" in info.data and "price" in info.data:
            expected = info.data["quantity"] * info.data["price"]
            if abs(v - expected) > 0.01:  # Allow small rounding differences
                raise ValueError(f"Market value {v} doesn't match quantity * price {expected}")
        return v


class Portfolio(BaseModel):
    """
    Complete portfolio representation with positions and metadata.

    Contains all holdings for a client, along with account information
    and risk preferences. Validates that position weights sum to 100%
    and total value matches sum of individual positions.
    """

    id: str = Field(..., description="Unique portfolio identifier")
    advisor_id: str = Field(..., description="Advisor identifier")
    client_id: str = Field(..., description="Client identifier")
    positions: List[Position] = Field(..., min_items=1, description="List of positions")
    total_value: float = Field(..., gt=0, description="Total portfolio value")
    timestamp: float = Field(
        default_factory=lambda: datetime.now().timestamp(), description="Unix timestamp"
    )
    risk_tolerance: RiskTolerance = Field(
        default=RiskTolerance.MODERATE, description="Client risk tolerance"
    )
    account_type: AccountType = Field(default=AccountType.INDIVIDUAL, description="Account type")

    @field_validator("total_value")
    @classmethod
    def validate_total_value(cls, v, info):
        """Verify total portfolio value equals sum of all position values"""
        if info.data and "positions" in info.data:
            expected = sum(pos.market_value for pos in info.data["positions"])
            if abs(v - expected) > 0.01:
                raise ValueError(f"Total value {v} doesn't match sum of positions {expected}")
        return v

    @field_validator("positions")
    @classmethod
    def validate_weights(cls, v):
        """Verify position weights sum to 100% within tolerance"""
        total_weight = sum(pos.weight for pos in v)
        if abs(total_weight - 100.0) > 0.1:
            raise ValueError(f"Position weights sum to {total_weight}, expected ~100")
        return v


class MarketData(BaseModel):
    """
    Real-time market data for a security.

    Provides current pricing and risk metrics needed for portfolio
    calculations including volatility, expected returns, and beta
    coefficient for systematic risk measurement.
    """

    symbol: str = Field(..., description="Stock ticker symbol")
    price: float = Field(..., gt=0, description="Current market price")
    volatility: float = Field(..., ge=0, le=1, description="Annual volatility")
    expected_return: float = Field(..., ge=-1, le=1, description="Expected annual return")
    beta: float = Field(..., ge=0, le=3, description="Beta coefficient")
    timestamp: float = Field(
        default_factory=lambda: datetime.now().timestamp(), description="Unix timestamp"
    )


class RiskCalculation(BaseModel):
    """
    Comprehensive risk metrics for a portfolio.

    Contains all calculated risk measures including:
    - Risk score (1-100 scale)
    - Value at Risk (VaR) at 95% confidence
    - Expected returns and volatility
    - Sharpe ratio for risk-adjusted performance
    - Processing time for performance monitoring
    """

    portfolio_id: str = Field(..., description="Portfolio identifier")
    advisor_id: str = Field(..., description="Advisor identifier")
    risk_number: int = Field(..., ge=1, le=100, description="Risk score (1-100)")
    var_95: float = Field(..., ge=0, description="95% Value at Risk")
    expected_return: float = Field(..., ge=-1, le=1, description="Expected portfolio return")
    volatility: float = Field(..., ge=0, le=1, description="Portfolio volatility")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")
    calculation_time_ms: float = Field(..., ge=0, description="Calculation time in ms")
    timestamp: float = Field(
        default_factory=lambda: datetime.now().timestamp(), description="Unix timestamp"
    )

    @property
    def calculation_datetime(self) -> datetime:
        """Convert Unix timestamp to datetime object for display"""
        return datetime.fromtimestamp(self.timestamp)


class PortfolioUpdate(BaseModel):
    """
    API request model for submitting portfolio updates.

    Wraps a Portfolio object with optional flags for controlling
    the update process such as immediate recalculation triggers.
    """

    portfolio: Portfolio = Field(..., description="Portfolio to update")
    recalculate_immediately: bool = Field(
        default=True, description="Whether to trigger immediate risk calculation"
    )


class RiskMetricsResponse(BaseModel):
    """
    API response model for risk metrics endpoints.

    Provides a complete view of portfolio risk calculations
    formatted for client consumption with ISO datetime formatting.
    """

    portfolio_id: str
    advisor_id: str
    risk_number: int
    var_95: float
    expected_return: float
    volatility: float
    sharpe_ratio: float
    calculation_time_ms: float
    timestamp: float
    last_update: datetime

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class PortfolioStats(BaseModel):
    """
    Portfolio performance and calculation statistics.

    Tracks historical calculation metrics for a portfolio including
    update frequency and average processing times.
    """

    portfolio_id: str
    last_update: datetime
    total_calculations: int
    current_risk_number: int
    average_calculation_time_ms: Optional[float] = None

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class SystemStatus(BaseModel):
    """
    System health monitoring and operational metrics.

    Provides comprehensive status information including:
    - Service uptime and connection status
    - Performance metrics (calculation counts and timing)
    - Active portfolio tracking
    """

    status: str = Field(..., description="Overall system status")
    uptime_seconds: float = Field(..., ge=0)
    total_calculations: int = Field(..., ge=0)
    avg_calculation_time_ms: float = Field(..., ge=0)
    redis_connected: bool
    kafka_connected: bool
    active_portfolios: int = Field(default=0, ge=0)

    @property
    def status_emoji(self) -> str:
        """Visual status indicator - ✅ for healthy, ⚠️ for degraded"""
        return "✅" if self.status == "healthy" else "⚠️"


class MetricsSummary(BaseModel):
    """
    Aggregate risk analytics across all portfolios.

    Provides system-wide risk overview including:
    - Portfolio counts and average risk levels
    - Total Value at Risk exposure
    - Risk distribution across low/moderate/high categories
    """

    total_portfolios: int = Field(..., ge=0)
    avg_risk_number: float = Field(..., ge=0, le=100)
    total_value_at_risk: float = Field(..., ge=0)
    high_risk_count: int = Field(..., ge=0)
    risk_distribution: dict = Field(default_factory=dict)

    @field_validator("risk_distribution")
    @classmethod
    def validate_risk_distribution(cls, v):
        """Verify risk distribution contains all required categories"""
        required_keys = {"low", "moderate", "high"}
        if not all(key in v for key in required_keys):
            raise ValueError(f"Risk distribution must contain keys: {required_keys}")
        return v


class ErrorResponse(BaseModel):
    """
    Standardized error response format.

    Provides consistent error reporting across all API endpoints
    with optional detailed information and ISO-formatted timestamps.
    """

    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
