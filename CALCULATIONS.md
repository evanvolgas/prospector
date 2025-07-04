# Financial Calculations in Prospector

This document explains the financial calculations, terminology, and economic intuition behind the Prospector risk calculator. Whether you're a developer new to finance or a financial professional interested in the implementation, this guide will help you understand how portfolio risk is calculated.

## Table of Contents
1. [Core Concepts](#core-concepts)
2. [Portfolio Metrics](#portfolio-metrics)
3. [Risk Calculations](#risk-calculations)
4. [Economic Intuition](#economic-intuition)
5. [Implementation Details](#implementation-details)

## Core Concepts

### What is Portfolio Risk?
Portfolio risk measures the uncertainty of returns - how much the actual returns might deviate from expected returns. In finance, risk isn't just about losing money; it's about volatility and unpredictability.

### Key Terms

- **Portfolio**: A collection of financial assets (stocks, bonds, etc.) held by an investor
- **Position**: An individual holding within a portfolio (e.g., 100 shares of Apple)
- **Weight**: The percentage of total portfolio value that each position represents
- **Return**: The gain or loss on an investment over a period
- **Volatility**: The degree of variation in returns (standard deviation)
- **Risk-Free Rate**: The theoretical return of an investment with zero risk (typically government bonds)

## Portfolio Metrics

### 1. Portfolio Weights
```python
weight = position_value / total_portfolio_value
```

**What it means**: If you have $10,000 in Apple stock and your total portfolio is worth $100,000, Apple's weight is 10%.

**Why it matters**: Weights determine how much each asset contributes to overall portfolio performance and risk.

### 2. Expected Return
```python
portfolio_return = sum(weight[i] * expected_return[i] for all positions)
```

**What it means**: The weighted average of expected returns from all positions.

**Example**: 
- 60% in stocks (12% expected return)
- 40% in bonds (5% expected return)
- Portfolio expected return = (0.6 × 12%) + (0.4 × 5%) = 9.2%

### 3. Portfolio Volatility
```python
portfolio_volatility = sqrt(weights.T @ covariance_matrix @ weights)
```

**What it means**: How much the portfolio value is expected to fluctuate. Higher volatility = higher risk.

**Components**:
- **Covariance Matrix**: Measures how assets move together
- **Correlation**: Ranges from -1 (perfect inverse) to +1 (perfect together)

## Risk Calculations

### 1. Value at Risk (VaR)
```python
var_95 = total_value * z_score * portfolio_volatility * sqrt(time_period)
```

**What it means**: The maximum expected loss over a given time period at a certain confidence level.

**Example**: A 95% VaR of $10,000 means:
- There's a 95% chance you won't lose more than $10,000
- There's a 5% chance you could lose more than $10,000

**Components**:
- `z_score = 1.645` for 95% confidence (from normal distribution)
- `sqrt(252/365)` adjusts for trading days vs calendar days

### 2. Sharpe Ratio
```python
sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_volatility
```

**What it means**: Risk-adjusted return - how much excess return you receive for the extra volatility.

**Interpretation**:
- < 1.0: Sub-optimal risk-adjusted returns
- 1.0 - 2.0: Good risk-adjusted returns
- > 2.0: Excellent risk-adjusted returns
- > 3.0: Outstanding (but verify it's sustainable)

**Example**: 
- Portfolio return: 12%
- Risk-free rate: 3%
- Volatility: 15%
- Sharpe ratio = (12% - 3%) / 15% = 0.6

### 3. Risk Score (1-99)
```python
risk_number = min(99, max(1, portfolio_volatility * 100))
# Adjusted for risk tolerance
if risk_tolerance == "Conservative":
    risk_number *= 0.8
elif risk_tolerance == "Aggressive":
    risk_number *= 1.2
```

**What it means**: A simplified risk metric for easy understanding.
- 1-20: Very low risk
- 21-40: Low risk
- 41-60: Moderate risk
- 61-80: High risk
- 81-99: Very high risk

## Economic Intuition

### Why Diversification Matters

The correlation matrix in our calculations captures the diversification benefit:

```python
# Same sector correlation = 0.8 (high)
# Different sector correlation = 0.3 (low)
```

**Intuition**: When stocks move together (high correlation), you get less diversification benefit. When they move independently, your risk decreases without sacrificing return.

### Sector-Based Risk Model

Different sectors have different inherent risks:

```python
SECTOR_VOLATILITY = {
    'Technology': 0.25,      # High innovation, high volatility
    'Healthcare': 0.18,      # Moderate, affected by regulation
    'Finance': 0.20,         # Sensitive to interest rates
    'Consumer': 0.15,        # Stable demand
    'Energy': 0.30,          # Commodity price dependent
    'Automotive': 0.35,      # Highest - cyclical + disruption
    'Real Estate': 0.14      # Lowest - tangible assets
}
```

**Economic Reasoning**:
- **Technology**: Rapid innovation creates uncertainty
- **Energy**: Oil price swings create volatility
- **Consumer Goods**: People always need toothpaste (stable)
- **Real Estate**: Physical assets with steady cash flows

### Time Scaling of Risk

```python
annual_risk = daily_risk * sqrt(252)
```

**Why sqrt(252)?**: Risk scales with the square root of time under random walk assumption. There are ~252 trading days per year.

## Implementation Details

### Correlation Matrix Construction

```python
for i in range(n_assets):
    for j in range(n_assets):
        if i == j:
            correlation[i,j] = 1.0  # Asset perfectly correlated with itself
        elif same_sector(i, j):
            correlation[i,j] = 0.8  # High correlation within sector
        else:
            correlation[i,j] = 0.3  # Lower correlation across sectors
```

### Covariance to Risk

```python
# Covariance = Correlation × Volatility[i] × Volatility[j]
cov_matrix = np.outer(volatilities, volatilities) * correlation

# Portfolio variance = weights^T × Covariance × weights
portfolio_variance = np.dot(weights, np.dot(cov_matrix, weights))

# Portfolio volatility (std dev) = sqrt(variance)
portfolio_volatility = np.sqrt(portfolio_variance)
```

### Risk Tolerance Adjustments

The system adjusts risk calculations based on investor profile:

- **Conservative**: Reduce risk score by 20% (multiply by 0.8)
- **Moderate**: No adjustment
- **Aggressive**: Increase risk score by 20% (multiply by 1.2)

**Rationale**: Conservative investors perceive the same volatility as riskier than aggressive investors.

## Practical Examples

### Example 1: Tech-Heavy Portfolio
```
Portfolio: 80% Technology, 20% Bonds
Expected Return: (0.8 × 12%) + (0.2 × 3%) = 10.2%
Volatility: High due to tech concentration
Risk Score: ~75 (High risk)
```

### Example 2: Balanced Portfolio
```
Portfolio: 30% Tech, 30% Healthcare, 20% Consumer, 20% Bonds
Expected Return: Weighted average ~8.5%
Volatility: Lower due to diversification
Risk Score: ~45 (Moderate risk)
```

### Example 3: Conservative Portfolio
```
Portfolio: 20% Stocks, 80% Bonds
Expected Return: ~4.8%
Volatility: Very low
Risk Score: ~20 (Low risk)
```

## Key Takeaways

1. **Risk ≠ Loss**: Risk measures uncertainty, not just downside
2. **Diversification**: Reduces risk without proportionally reducing returns
3. **Risk-Adjusted Returns**: Sharpe ratio tells you if extra risk is worth it
4. **VaR Limitations**: Only tells you the minimum loss in 95% of cases, not the worst case
5. **Correlation Matters**: How assets move together is as important as individual volatility

## Further Reading

- Modern Portfolio Theory (Markowitz)
- Capital Asset Pricing Model (CAPM)
- Black-Scholes Option Pricing
- Risk Management in Financial Institutions

This implementation provides a simplified but practical approach to portfolio risk calculation, suitable for real-time processing while maintaining financial accuracy.