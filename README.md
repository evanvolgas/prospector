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

- **Real-time Processing**: Processes portfolio updates and market data in real-time using Bytewax
- **Stateful Computations**: Maintains portfolio state across updates for accurate risk calculations
- **Ultra-High Performance**:
  - Processes **79,196 messages/second** average (93,658 peak)
  - Handles **6.84 billion messages per day**
  - Ingests **13.9 TB per day** with single consumer
  - Sub-millisecond latency (< 0.01 ms)
- **Scalable Architecture**: Easily scales horizontally with Kafka partitions and Bytewax workers
- **REST API**: FastAPI-based monitoring and querying interface
- **Caching Layer**: Redis caching for fast risk metric retrieval
- **Docker Compose**: Complete infrastructure setup with one command

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
uv run prospector start all

# Or run demo mode
uv run prospector demo
```

That's it! The Prospector CLI handles all the complexity of starting services in the right order.

### Available Commands

```bash
# Start/stop components
uv run prospector start all      # Start everything
uv run prospector start infra    # Start only Kafka/Redis
uv run prospector stop all       # Stop everything

# Status and monitoring
uv run prospector status         # Show component status

# Testing and benchmarks
uv run prospector generate       # Generate test data
uv run prospector benchmark      # Run performance benchmark
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
uv run data-generator

# Batch mode
uv run data-generator --mode batch --num-portfolios 50
```

#### 5. Run the Risk Calculator

```bash
# Using make
make run-calculator

# Or manually
uv run risk-calculator

# With multiple workers
uv run risk-calculator --workers 4
```

#### 6. Start the Monitoring API

```bash
# Using make
make run-api

# Or manually
uv run risk-api
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
curl -X POST http://localhost:6066/simulate/portfolio-update?portfolio_id=test-123
```

### Get Metrics Summary
```bash
curl http://localhost:6066/metrics/summary
```

## Configuration

### Data Generator Options

```bash
# Continuous mode with custom intervals
uv run data-generator \
  --portfolio-interval 5.0 \
  --market-interval 2.0

# Batch mode with custom settings
uv run data-generator \
  --mode batch \
  --num-portfolios 200 \
  --updates-per-portfolio 10
```

### Risk Calculator Options

```bash
# Run with multiple workers
uv run risk-calculator --workers 4

# Enable recovery (requires persistent storage)
uv run risk-calculator \
  --recovery-directory ./recovery \
  --epoch-interval 10
```

## Architecture Components

### Prospector CLI

The `prospector` command is your main interface for managing all components:

```bash
uv run prospector --help
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
uv run risk-calculator --workers 4
uv run data-generator --mode continuous
uv run risk-api
```

### Performance Testing

```bash
# Quick benchmark (1000 portfolios)
uv run prospector benchmark

# Large benchmark
uv run prospector benchmark --portfolios 10000 --updates 10

# Throughput testing
uv run benchmark-throughput
```

### Project Structure

```
.
├── prospector.py               # Master control CLI
├── risk_calculator.py          # Main risk calculation engine
├── data_generator.py           # Test data generator
├── risk_api.py                 # FastAPI REST service
├── models.py                   # Pydantic data models
├── benchmark_throughput.py     # Throughput testing tool
├── docker-compose.yml          # Infrastructure setup
├── pyproject.toml              # Project configuration
├── PERFORMANCE_OPTIMIZATIONS.md # Performance optimization guide
├── THROUGHPUT_TESTING.md       # Throughput testing guide
└── README.md                   # This file
```

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

### Throughput Testing

The `benchmark-throughput` command tests actual read and processing speeds without clearing your data:

```bash
# Run complete throughput benchmark
uv run benchmark-throughput

# Test with more messages
uv run benchmark-throughput --kafka-messages 50000 --redis-operations 20000

# Reprocess topic from beginning
uv run benchmark-throughput --from-beginning

# Use specific consumer group
uv run benchmark-throughput --consumer-group my-test-group
```

#### What It Measures

1. **Kafka Read Performance**: Messages/second and MB/second consumption rates
2. **Redis Performance**: Read and write operations per second
3. **End-to-End Processing**: Actual throughput with the risk calculator running

### Latest Performance Metrics

Based on benchmarks with 12 partitions on a topic with 5 million messages totalling 10 GB of data:

#### Single Consumer Performance
- **Average Throughput**: 79,196 messages/second (164.54 MB/s)
- **Peak Throughput**: 93,658 messages/second (193.73 MB/s)
- **Latency**: < 0.01 ms average (ultra-low)
- **Daily Capacity**:
  - **6.84 billion messages per day**
  - **13.9 TB per day** (sustained average)
  - **16.0 TB per day** (at peak rates)

#### System Specifications
- **Topic Configuration**: 12 partitions with even distribution
- **Message Size**: ~2,179 bytes average
- **Partition Balance**: ~413k messages per partition

#### Scaling Potential
- **Kafka Level**: 12 partitions allow up to 12 consumers in a consumer group
- **Bytewax Level**: Can run multiple workers per consumer for parallel processing
  - Example: `uv run risk-calculator --workers 4`
- **Combined Scaling**: Multiple Bytewax instances + multiple workers per instance
- Total cluster capacity could exceed **150+ TB/day** with proper scaling

Run your own benchmark:
```bash
# Quick performance test
uv run benchmark-throughput --topic portfolio-updates-v2 --duration 30

# Test with specific message count
uv run benchmark-throughput --messages 100000 --from-beginning
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
   uv run prospector benchmark --portfolios 100

   # Run throughput test
   uv run benchmark-throughput

   # Increase workers if needed
   uv run risk-calculator --workers 4
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