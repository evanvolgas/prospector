"""
Security characteristics database for risk calculations.

This module contains pre-configured characteristics for 50+ securities including
volatility, expected returns, and beta values. In production, these would be
derived from historical market data.
"""

from typing import Dict

# Individual security characteristics
# Format: symbol -> {volatility, expected_return, beta}
SECURITY_CHARACTERISTICS: Dict[str, Dict[str, float]] = {
    # Technology stocks - higher volatility, higher expected returns
    "AAPL": {"volatility": 0.22, "expected_return": 0.15, "beta": 1.2},
    "GOOGL": {"volatility": 0.24, "expected_return": 0.14, "beta": 1.1},
    "MSFT": {"volatility": 0.20, "expected_return": 0.13, "beta": 1.0},
    "META": {"volatility": 0.32, "expected_return": 0.16, "beta": 1.4},
    "NVDA": {"volatility": 0.40, "expected_return": 0.20, "beta": 1.8},
    "AMD": {"volatility": 0.45, "expected_return": 0.18, "beta": 2.0},
    "INTC": {"volatility": 0.28, "expected_return": 0.10, "beta": 1.1},
    "CRM": {"volatility": 0.30, "expected_return": 0.15, "beta": 1.3},
    "ORCL": {"volatility": 0.26, "expected_return": 0.11, "beta": 0.9},
    "ADBE": {"volatility": 0.28, "expected_return": 0.14, "beta": 1.2},
    
    # Financial stocks - moderate volatility
    "JPM": {"volatility": 0.20, "expected_return": 0.10, "beta": 1.1},
    "BAC": {"volatility": 0.25, "expected_return": 0.09, "beta": 1.3},
    "WFC": {"volatility": 0.23, "expected_return": 0.09, "beta": 1.2},
    "GS": {"volatility": 0.26, "expected_return": 0.11, "beta": 1.4},
    "MS": {"volatility": 0.28, "expected_return": 0.11, "beta": 1.5},
    "V": {"volatility": 0.18, "expected_return": 0.12, "beta": 0.9},
    "MA": {"volatility": 0.19, "expected_return": 0.12, "beta": 1.0},
    "PYPL": {"volatility": 0.35, "expected_return": 0.08, "beta": 1.6},
    "BRK.B": {"volatility": 0.16, "expected_return": 0.10, "beta": 0.8},
    
    # Healthcare - lower volatility, stable returns
    "JNJ": {"volatility": 0.14, "expected_return": 0.08, "beta": 0.7},
    "PFE": {"volatility": 0.18, "expected_return": 0.07, "beta": 0.8},
    "UNH": {"volatility": 0.16, "expected_return": 0.11, "beta": 0.8},
    "CVS": {"volatility": 0.20, "expected_return": 0.08, "beta": 0.9},
    "MRK": {"volatility": 0.17, "expected_return": 0.08, "beta": 0.7},
    "ABBV": {"volatility": 0.19, "expected_return": 0.09, "beta": 0.8},
    "LLY": {"volatility": 0.18, "expected_return": 0.10, "beta": 0.7},
    "TMO": {"volatility": 0.19, "expected_return": 0.11, "beta": 0.9},
    
    # Consumer stocks - mixed characteristics
    "AMZN": {"volatility": 0.28, "expected_return": 0.15, "beta": 1.3},
    "WMT": {"volatility": 0.16, "expected_return": 0.08, "beta": 0.6},
    "HD": {"volatility": 0.18, "expected_return": 0.10, "beta": 0.9},
    "NKE": {"volatility": 0.22, "expected_return": 0.11, "beta": 1.0},
    "MCD": {"volatility": 0.15, "expected_return": 0.08, "beta": 0.6},
    "SBUX": {"volatility": 0.24, "expected_return": 0.10, "beta": 1.0},
    "KO": {"volatility": 0.14, "expected_return": 0.07, "beta": 0.6},
    "PEP": {"volatility": 0.13, "expected_return": 0.07, "beta": 0.5},
    "PG": {"volatility": 0.15, "expected_return": 0.08, "beta": 0.6},
    
    # Energy stocks - high volatility, cyclical
    "XOM": {"volatility": 0.28, "expected_return": 0.08, "beta": 1.1},
    "CVX": {"volatility": 0.30, "expected_return": 0.08, "beta": 1.2},
    "COP": {"volatility": 0.35, "expected_return": 0.09, "beta": 1.4},
    
    # Entertainment/Media - growth characteristics
    "DIS": {"volatility": 0.22, "expected_return": 0.09, "beta": 1.1},
    "NFLX": {"volatility": 0.38, "expected_return": 0.15, "beta": 1.5},
    
    # Automotive - high volatility, transformation risk
    "TSLA": {"volatility": 0.50, "expected_return": 0.20, "beta": 2.2},
    "F": {"volatility": 0.35, "expected_return": 0.06, "beta": 1.5},
    "GM": {"volatility": 0.32, "expected_return": 0.07, "beta": 1.4},
    
    # Telecom - defensive characteristics
    "T": {"volatility": 0.18, "expected_return": 0.06, "beta": 0.7},
    "VZ": {"volatility": 0.16, "expected_return": 0.06, "beta": 0.6},
    "CMCSA": {"volatility": 0.20, "expected_return": 0.08, "beta": 0.9},
    
    # Other technology and industrial stocks
    "CSCO": {"volatility": 0.22, "expected_return": 0.08, "beta": 1.0},
    "IBM": {"volatility": 0.20, "expected_return": 0.06, "beta": 0.9},
    "TXN": {"volatility": 0.22, "expected_return": 0.10, "beta": 1.1},
    "AVGO": {"volatility": 0.26, "expected_return": 0.12, "beta": 1.3},
}

def get_security_characteristics(symbol: str) -> Dict[str, float]:
    """
    Retrieve characteristics for a security with intelligent defaults.
    
    Args:
        symbol: Stock ticker symbol
        
    Returns:
        Dictionary containing volatility, expected_return, and beta
        
    For unknown symbols, returns sector-appropriate defaults based on
    common patterns in symbol naming conventions.
    """
    if symbol in SECURITY_CHARACTERISTICS:
        return SECURITY_CHARACTERISTICS[symbol]
    else:
        # Intelligent defaults based on symbol patterns
        if any(tech in symbol.upper() for tech in ['TECH', 'SOFT', 'CYBER', 'CLOUD', 'AI']):
            return {"volatility": 0.30, "expected_return": 0.12, "beta": 1.3}
        elif any(fin in symbol.upper() for fin in ['BANK', 'CAPITAL', 'FINANCIAL', 'FUND']):
            return {"volatility": 0.22, "expected_return": 0.09, "beta": 1.1}
        elif any(health in symbol.upper() for health in ['HEALTH', 'BIO', 'PHARMA', 'MED']):
            return {"volatility": 0.20, "expected_return": 0.09, "beta": 0.8}
        elif any(energy in symbol.upper() for energy in ['ENERGY', 'OIL', 'GAS', 'SOLAR']):
            return {"volatility": 0.32, "expected_return": 0.08, "beta": 1.3}
        else:
            # Generic default for unknown symbols
            return {"volatility": 0.20, "expected_return": 0.08, "beta": 1.0}