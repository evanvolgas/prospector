# Prospector Architecture

## Overview

The Prospector codebase has been reorganized into a modular architecture for better maintainability and clarity. The main risk calculator file has been reduced from ~600 lines to ~65 lines by extracting functionality into focused modules.

## Package Structure

### `prospector/` - Main Package

#### `core/` - Core Business Logic
- **`calculations.py`** (~150 lines)
  - Downside risk calculations
  - Correlation matrix generation
  - Portfolio metrics (return, volatility, Sharpe ratio)
  - Risk score mapping functions
  - Value at Risk (VaR) calculations

- **`risk_processor.py`** (~200 lines)
  - Main `RiskProcessor` class
  - Portfolio risk calculation orchestration
  - Redis caching logic
  - Performance tracking integration
  - Behavioral adjustments

#### `config/` - Configuration
- **`securities.py`** (~120 lines)
  - Database of 50+ security characteristics
  - Volatility, return, and beta values
  - Intelligent defaults for unknown symbols

- **`constants.py`** (~40 lines)
  - Risk calculation parameters
  - Behavioral adjustment factors
  - System configuration (Redis TTL, Kafka settings)

#### `streaming/` - Stream Processing
- **`pipeline.py`** (~120 lines)
  - Bytewax dataflow definition
  - Kafka input/output configuration
  - Message parsing and serialization
  - Pipeline stage orchestration

#### `utils/` - Utilities
- **`performance.py`** (~100 lines)
  - `PerformanceTracker` class
  - Thread-safe metrics collection
  - Throughput and latency tracking

## Key Benefits

### 1. **Separation of Concerns**
- Configuration separated from logic
- Calculations isolated from I/O
- Clear boundaries between components

### 2. **Maintainability**
- Smaller, focused files (100-200 lines each)
- Single responsibility per module
- Easy to locate specific functionality

### 3. **Testability**
- Pure functions in `calculations.py`
- Mockable dependencies
- Isolated components

### 4. **Extensibility**
- Easy to add new securities to database
- Simple to modify risk parameters
- Clear extension points

## Module Dependencies

```
risk_calculator.py
    ├── prospector.core.risk_processor
    │   ├── prospector.config.constants
    │   ├── prospector.config.securities
    │   ├── prospector.core.calculations
    │   └── prospector.utils.performance
    └── prospector.streaming.pipeline
        └── prospector.config.constants
```

## Data Flow

1. **Input**: Kafka message → `pipeline.parse_kafka_message()`
2. **Processing**: Portfolio → `RiskProcessor.calculate_portfolio_risk()`
   - Get security characteristics
   - Calculate correlations
   - Compute portfolio metrics
   - Generate risk score
3. **Caching**: Results → Redis via `RiskProcessor._cache_results()`
4. **Output**: RiskCalculation → `pipeline.serialize_for_kafka()`

## Configuration Points

### Risk Parameters (`config/constants.py`)
- Confidence levels
- Risk score boundaries
- Behavioral adjustments
- Correlation parameters

### Security Data (`config/securities.py`)
- Add/modify individual securities
- Adjust sector defaults
- Update volatility/return assumptions

### Performance (`utils/performance.py`)
- Metrics window size
- Logging intervals
- Statistics calculations

## Future Enhancements

1. **Dynamic Security Loading**
   - Load characteristics from database/API
   - Real-time updates

2. **Advanced Calculations**
   - Monte Carlo simulations
   - Factor models
   - Stress testing

3. **Additional Utilities**
   - Backtesting framework
   - Risk reporting
   - Alert system

The modular structure makes these enhancements straightforward to implement without affecting existing functionality.