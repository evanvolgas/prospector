"""
Risk calculation constants and configuration parameters.
"""

# Risk calculation parameters
CONFIDENCE_LEVEL = 0.95
Z_SCORE = 1.64  # For 95% one-tailed confidence (downside risk)
RISK_FREE_RATE = 0.03

# Risk score boundaries
MIN_RISK_NUMBER = 20
MAX_RISK_NUMBER = 100

# Behavioral adjustments
CONSERVATIVE_ADJUSTMENT = 1.1  # +10% risk perception
AGGRESSIVE_ADJUSTMENT = 0.9    # -10% risk perception

# Correlation parameters
SAME_SECTOR_CORRELATION = 0.7
DIFFERENT_SECTOR_CORRELATION = 0.3
BETA_CORRELATION_ADJUSTMENT = 0.1
MIN_CORRELATION = 0.1
MAX_CORRELATION = 0.95

# Cache settings
REDIS_TTL = 300  # 5 minutes

# Performance tracking
PERFORMANCE_LOG_INTERVAL = 100  # Log stats every N messages

# Kafka settings
KAFKA_BATCH_SIZE = 1000
INPUT_TOPIC = "portfolio-updates-v2"
OUTPUT_TOPIC = "risk-updates"