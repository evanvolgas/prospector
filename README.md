# Prospector Real-Time Risk Calculator

A high-performance, real-time portfolio risk calculation system built with Bytewax, Kafka, and Redis. Prospector processes portfolio updates and market data streams to calculate risk metrics including Value at Risk (VaR), Sharpe ratios, and custom risk scores.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Data Generator  │────▶│     Kafka       │────▶│ Prospector      │
│                 │     │                 │     │ Calculator      │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                          │
                        ┌─────────────────┐               │
                        │     Redis       │◀──────────────┘
                        │    (Cache)      │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │   FastAPI       │
                        │  Monitoring     │
                        └─────────────────┘
```

## Features

- **Real-time Stream Processing**: Bytewax-based pipeline for continuous portfolio risk analysis
- **Advanced Risk Methodology**: 
  - Individual security analysis with 50+ pre-configured stocks
  - Behavioral finance risk scoring (20-100 scale)
  - Value at Risk (VaR) calculations at 95% confidence
  - Correlation-based portfolio analysis
- **Production-Ready Performance**:
  - **Kafka**: 83,320 msg/s (173.3 MB/s) average, 148,135 msg/s (308.1 MB/s) peak
  - **NumPy**: 51,172 calculations/second (0.020 ms average latency)
  - **Redis**: 36,173 writes/second with pipelining
  - **End-to-End**: 14,260 msg/s (29.6 MB/s) single worker throughput
- **Horizontal Scalability**: 
  - Kafka partitioning enables parallel processing
  - Best scaling with distributed deployment (1 worker per machine)
  - Alternative: Kafka sink for 180K+ msg/s on single machine (12 processes)
- **RESTful API**: FastAPI service for real-time risk queries and monitoring
- **High-Performance Caching**: Redis for instant risk metric retrieval
- **One-Command Setup**: Complete infrastructure via Docker Compose

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local development)
- uv (Python package manager) - Install with: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Quick Start

### Using Prospector CLI (Recommended)

```bash
# Install dependencies
uv sync

# Start everything with one command
uv run python prospector.py start all

# Or run demo mode
uv run python prospector.py demo
```

That's it! The Prospector CLI handles all the complexity of starting services in the right order.

### Available Commands

```bash
# Start/stop components
uv run python prospector.py start all      # Start everything
uv run python prospector.py start infra    # Start only Kafka/Redis
uv run python prospector.py stop all       # Stop everything

# Status and monitoring
uv run python prospector.py status         # Show component status

# Testing and benchmarks
uv run python prospector.py generate       # Generate test data
uv run python prospector.py benchmark      # Run performance benchmark
```

### Manual Setup (Alternative)

#### 1. Clone the Repository

```bash
git clone <repository-url>
cd prospector
```

#### 2. Start Infrastructure

```bash
# Using make
make up

# Or manually with docker-compose
docker-compose up -d

# Check status
make status
```

#### 3. Install Python Dependencies

```bash
# Using make
make install

# Or manually with uv
uv sync
```

#### 4. Generate Test Data

```bash
# Using make
make run-generator

# Or manually
uv run python data_generator.py

# Batch mode
uv run python data_generator.py --mode batch --num-portfolios 50
```

#### 5. Run the Risk Calculator

```bash
# Using make
make run-calculator

# Or manually
uv run python risk_calculator.py

# With multiple workers
uv run python risk_calculator.py -w 4
```

#### 6. Start the Monitoring API

```bash
# Using make
make run-api

# Or manually
uv run python risk_api.py
```

## Access the Services

- **Kafka UI**: http://localhost:8080 - Monitor topics and messages
- **Risk API**: http://localhost:6066 - REST API and documentation
- **API Docs**: http://localhost:6066/docs - Interactive API documentation
- **RedisInsight**: http://localhost:8001 - Redis web UI

## API Endpoints

### Health Check
```bash
curl http://localhost:6066/health
```

### Get Portfolio Risk
```bash
curl http://localhost:6066/risk/{portfolio_id}
```

### Get High-Risk Portfolios
```bash
curl http://localhost:6066/portfolios/at-risk?risk_threshold=70
```

### Get Advisor Portfolios
```bash
curl http://localhost:6066/advisor/{advisor_id}/portfolios
```

### Simulate Portfolio Update
```bash
curl -X POST "http://localhost:6066/portfolio/simulate?portfolio_id=test-123"
```

### Get Metrics Summary
```bash
curl http://localhost:6066/metrics/summary
```

## Configuration

### Data Generator Options

```bash
# Continuous mode with custom intervals
uv run python data_generator.py \
  --portfolio-interval 5.0 \
  --market-interval 2.0

# Batch mode with custom settings
uv run python data_generator.py \
  --mode batch \
  --num-portfolios 200 \
  --updates-per-portfolio 10
```

### Risk Calculator Options

```bash
# Run with multiple workers
uv run python risk_calculator.py -w 4

# Enable recovery (requires persistent storage)
uv run python risk_calculator.py \
  -r ./recovery \
  -s 10
```

## Architecture Components

### Prospector CLI

The `prospector` command is your main interface for managing all components:

```bash
uv run python prospector.py --help
```

It automatically:
- Checks if Docker services are already running
- Starts only what's needed
- Manages component lifecycle
- Provides unified status monitoring

## Development

### Running Individual Components

While the Prospector CLI is recommended, you can still run components individually:

```bash
# Start infrastructure only
docker-compose up -d

# Run specific components
uv run python risk_calculator.py -w 4
uv run python data_generator.py --mode continuous
uv run python risk_api.py
```

### Performance Testing

```bash
# Quick benchmark (1000 portfolios)
uv run python prospector.py benchmark

# Large benchmark
uv run python prospector.py benchmark --portfolios 10000 --updates 10

# Throughput testing
uv run python benchmark_throughput.py
```

### Project Structure

```
.
├── prospector/                     # Main package directory
│   ├── __init__.py
│   ├── api/                        # REST API modules
│   │   ├── __init__.py
│   │   ├── core/                   # API infrastructure
│   │   │   ├── __init__.py
│   │   │   ├── dependencies.py    # Shared resources (Redis, Kafka)
│   │   │   ├── exceptions.py      # Exception handlers
│   │   │   └── startup.py         # Lifecycle management
│   │   └── routers/                # API endpoints
│   │       ├── __init__.py
│   │       ├── advisor.py         # Advisor portfolio endpoints
│   │       ├── analytics.py       # Metrics aggregation
│   │       ├── health.py          # Health checks
│   │       ├── portfolio.py       # Portfolio management
│   │       ├── portfolios.py      # Portfolio collections
│   │       ├── risk.py            # Risk data retrieval
│   │       └── streaming.py       # SSE streaming
│   ├── config/                     # Configuration and constants
│   │   ├── __init__.py
│   │   ├── constants.py            # Risk parameters and settings
│   │   └── securities.py           # Security characteristics database
│   ├── core/                       # Core business logic
│   │   ├── __init__.py
│   │   ├── calculations.py         # Risk calculation functions
│   │   └── risk_processor.py       # Main risk processing logic
│   ├── streaming/                  # Bytewax streaming components
│   │   ├── __init__.py
│   │   └── pipeline.py             # Dataflow pipeline definition
│   └── utils/                      # Utility modules
│       ├── __init__.py
│       └── performance.py          # Performance tracking
├── benchmark_throughput.py         # Throughput testing tool
├── data_generator.py               # Test data generator
├── models.py                       # Pydantic data models
├── prospector.py                   # Master control CLI
├── risk_api.py                     # FastAPI REST service (main entry)
├── risk_calculator.py              # Bytewax streaming entry point
├── docker-compose.yml              # Infrastructure setup
├── pyproject.toml                  # Project configuration
├── ARCHITECTURE.md                 # Detailed architecture documentation
├── CALCULATIONS.md                 # Financial calculations explained
└── README.md                       # This file
```

## Risk Calculation Methodology

The Prospector risk calculator uses an advanced methodology that focuses on downside risk and behavioral finance:

### Key Components

1. **Individual Security Analysis**
   - 50+ pre-configured securities with unique volatility, return, and beta profiles
   - Intelligent defaults for unknown symbols based on naming patterns
   - More accurate than simple sector-based approaches

2. **Downside Risk Focus**
   - 95% confidence intervals using 1.64 standard deviations
   - Emphasis on potential losses rather than general volatility
   - Dollar-based Value at Risk (VaR) calculations

3. **Risk Score Mapping**
   - Intuitive 20-100 scale (higher = more risk)
   - Non-linear mapping that reflects investor psychology:
     - 20-25: Very low risk (0-2% downside)
     - 25-85: Moderate to high risk (2-18% downside)
     - 85-100: Very high risk (18%+ downside)

4. **Behavioral Adjustments**
   - Conservative investors: +10% risk perception
   - Aggressive investors: -10% risk perception
   - Aligns risk scores with investor psychology

### Running Tests

```bash
# Install dev dependencies
uv sync --dev

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov

# Format code
uv run black .
uv run ruff --fix .

# Type checking
uv run mypy .
```

### Debugging

1. **Check System Status**:
   ```bash
   uv run prospector status
   ```

2. **Check Kafka Topics**:
   - Open Kafka UI at http://localhost:8080
   - Verify topics exist: `portfolio-updates-v2`, `market-data`, `risk-updates`
   - Check message flow

3. **View Logs**:
   ```bash
   # All services
   docker-compose logs -f

   # Specific service
   docker-compose logs -f kafka
   ```

4. **Redis Debugging**:
   - Use RedisInsight UI at http://localhost:8001
   - Or use CLI:
   ```bash
   docker exec -it redis redis-cli
   KEYS risk:*
   ```

## Performance Benchmarking

### Benchmark Suite

#### Component Performance Testing
Test individual components in isolation:
```bash
# Test Kafka, NumPy, and Redis separately
uv run python benchmark_components.py

# Customize test sizes
uv run python benchmark_components.py --kafka-messages 100000 --numpy-calculations 10000
```

#### Kafka vs Redis Sink Comparison
Compare throughput when sinking to Kafka vs Redis:
```bash
# Run both benchmarks
uv run python benchmark_kafka_sink.py --messages 50000

# Run only Kafka sink test
uv run python benchmark_kafka_sink.py --mode kafka --messages 100000

# Run only Redis sink test  
uv run python benchmark_kafka_sink.py --mode redis --messages 100000
```

#### Parallel Processing Benchmark
Test maximum throughput with 12 concurrent processes:
```bash
# Run with 12 processes
uv run python benchmark_kafka_parallel.py --processes 12 --messages 10000

# Test with different process counts
uv run python benchmark_kafka_parallel.py --processes 4 --messages 25000
```

#### Enhanced Throughput Testing
Detailed per-second metrics for Kafka consumption:
```bash
# Run complete throughput benchmark
uv run python benchmark_throughput.py

# Test with specific duration
uv run python benchmark_throughput.py --duration 60

# Test with specific message count
uv run python benchmark_throughput.py --messages 100000
```

### Latest Performance Metrics

**Test Environment:** Apple M3 Max (14 cores, 36GB RAM), macOS, Local Docker

#### Component Performance (Isolated)
- **Kafka Read**: 83,320 msg/s (173.3 MB/s) average, 148,135 msg/s (308.1 MB/s) peak
- **NumPy Calculations**: 51,172 calculations/second
  - P50: 0.020 ms, P95: 0.022 ms, P99: 0.032 ms per calculation
- **Redis Writes**: 36,173 operations/second (with pipelining)
  - P50: 0.028 ms, P95: 0.002 ms, P99: 0.087 ms per operation

#### End-to-End Performance (Single Machine)
- **Single Worker**: 14,260 msg/s (29.6 MB/s) complete pipeline
- **Multi-Worker Scaling**: Limited by Redis contention
  - 2 Workers: ~500 msg/s (1.0 MB/s) total - severe degradation
  - 4+ Workers: <400 msg/s (0.8 MB/s) total - Redis bottleneck

#### Sink Performance Comparison
- **Kafka Sink**: ~25,000 msg/s (52.0 MB/s) single process
- **Redis Sink**: ~18,000 msg/s (37.4 MB/s) with pipelining
- **Parallel Kafka Sink**: ~180,000 msg/s (374.3 MB/s) with 12 processes

#### Daily Capacity Estimates

**Single Worker (Redis Sink):**
- Messages: 1.2 billion/day
- Data Volume: 2.5 TB/day (29.6 MB/s × 86,400s)

**12 Parallel Processes (Kafka Sink):**
- Messages: 15.6 billion/day  
- Data Volume: 31.5 TB/day (374.3 MB/s × 86,400s)

#### Distributed Deployment Projections

With proper distributed architecture (12 machines, 1 worker each):
- Expected: ~171,000 msg/s (355.6 MB/s) aggregate
- Daily: 14.8 billion messages (29.9 TB/day)
- No Redis contention between workers

#### Architecture Efficiency

The Prospector system achieves this performance through:

1. **Streaming Architecture**: Bytewax processes messages without batching delays
2. **Zero-Copy Design**: Direct Kafka-to-Redis pipeline minimizes data movement
3. **Optimized Risk Engine**: Vectorized NumPy calculations for portfolio metrics
4. **Efficient Caching**: Redis hash structures for O(1) lookups
5. **Parallel Processing**: Each partition processed independently

Run your own benchmark:
```bash
# Quick performance test
uv run python benchmark_throughput.py --topic portfolio-updates-v2 --duration 30

# Test with specific message count
uv run python benchmark_throughput.py --messages 100000 --from-beginning
```

##  Deployment

### Environment Variables

Create `.env` file:
```env
KAFKA_BROKERS=kafka1:9092,kafka2:9092,kafka3:9092
REDIS_URL=redis://redis-cluster:6379
LOG_LEVEL=INFO
```

### Monitoring

- Use Prometheus metrics exposed by Bytewax
- Set up Grafana dashboards for visualization
- Configure alerts for high-risk portfolios

## Troubleshooting

### Common Issues

1. **Services Won't Start**
   ```bash
   # Check what's already running
   uv run prospector status

   # Force restart everything
   uv run prospector stop all
   uv run prospector start all
   ```

2. **Kafka Connection Failed**
   ```bash
   # Infrastructure may not be ready
   uv run prospector start infra
   # Wait 10 seconds, then try again
   ```

3. **No Risk Calculations Appearing**
   ```bash
   # Generate test data first
   uv run prospector generate --portfolios 100

   # Check if calculator is processing
   uv run prospector status
   ```

4. **Performance Issues**
   ```bash
   # Run benchmark to diagnose
   uv run python prospector.py benchmark --portfolios 100

   # Run throughput test
   uv run python benchmark_throughput.py

   # Increase workers if needed
   uv run python risk_calculator.py -w 4
   ```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Built with [Bytewax](https://github.com/bytewax/bytewax) - Modern Python stream processing
- Inspired by real-time financial risk management systems
- Uses Apache Kafka for reliable message streaming