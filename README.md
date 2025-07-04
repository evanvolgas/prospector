# Bytewax Real-Time Portfolio Risk Calculator

A high-performance, real-time portfolio risk calculation system built with Bytewax, Kafka, and Redis. This system processes portfolio updates and market data streams to calculate risk metrics including Value at Risk (VaR), Sharpe ratios, and custom risk scores.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Data Generator  │────▶│     Kafka       │────▶│ Bytewax Risk    │
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
- **High Performance**: Rust-based Bytewax engine provides superior performance for numerical calculations
- **Scalable Architecture**: Easily scales horizontally with Kafka partitions and Bytewax workers
- **REST API**: FastAPI-based monitoring and querying interface
- **Caching Layer**: Redis caching for fast risk metric retrieval
- **Docker Compose**: Complete infrastructure setup with one command

## Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)
- uv (Python package manager) - Install with: `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Quick Start

### Using Make (Recommended)

```bash
# One-command setup
make quickstart

# Then in separate terminals:
make run-generator  # Terminal 1
make run-calculator # Terminal 2
make run-api       # Terminal 3
```

### Manual Setup

#### 1. Clone the Repository

```bash
git clone <repository-url>
cd faust
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

### 7. Access the Services

- **Kafka UI**: http://localhost:8080 - Monitor topics and messages
- **Risk API**: http://localhost:6066 - REST API and documentation
- **API Docs**: http://localhost:6066/docs - Interactive API documentation

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
uv run python bytewax_data_generator.py \
  --portfolio-interval 5.0 \
  --market-interval 2.0

# Batch mode with custom settings
uv run python bytewax_data_generator.py \
  --mode batch \
  --num-portfolios 200 \
  --updates-per-portfolio 10
```

### Bytewax Worker Options

```bash
# Run with multiple workers
uv run python run_risk_calculator.py --workers 4

# Enable recovery (requires persistent storage)
uv run python run_risk_calculator.py \
  --recovery-directory ./recovery \
  --epoch-interval 10
```

## Development

### Project Structure

```
.
├── docker-compose.yml          # Infrastructure setup
├── pyproject.toml              # Project configuration and dependencies
├── README.md                   # This file
├── Makefile                    # Convenience commands
├── bytewax_risk_calculator.py  # Main risk calculation logic
├── bytewax_data_generator.py   # Test data generator
├── run_risk_calculator.py      # Bytewax runner script
├── risk_api.py                 # FastAPI monitoring service
└── old/                        # Legacy Faust implementation (deprecated)
    ├── risk_calculator.py      
    ├── data_generator.py       
    └── performance_test.py     
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

1. **Check Kafka Topics**:
   - Open Kafka UI at http://localhost:8080
   - Verify topics exist: `portfolio-updates`, `market-data`, `risk-updates`
   - Check message flow

2. **View Logs**:
   ```bash
   # All services
   docker-compose logs -f
   
   # Specific service
   docker-compose logs -f kafka
   ```

3. **Redis CLI**:
   ```bash
   # Connect to Redis
   docker exec -it redis redis-cli
   
   # List all keys
   KEYS *
   
   # Get specific risk calculation
   GET risk:portfolio_123
   ```

## Performance Tuning

### Kafka Configuration

Edit `docker-compose.yml` to adjust:
- Partition count for topics (more partitions = more parallelism)
- Replication factor (for production use)
- Message retention settings

### Bytewax Optimization

```python
# In bytewax_risk_calculator.py, adjust:
batch_size=100  # Increase for higher throughput
```

### Redis Optimization

```yaml
# In docker-compose.yml, add Redis configuration:
command: redis-server --maxmemory 2gb --maxmemory-policy allkeys-lru
```

## Production Deployment

### Using Kubernetes

```bash
# Deploy with waxctl (Bytewax CLI)
waxctl create deployment risk-calculator \
  --image your-registry/risk-calculator:latest \
  --replicas 3
```

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

1. **Kafka Connection Failed**
   ```bash
   # Ensure Kafka is running
   docker-compose ps
   
   # Check Kafka logs
   docker-compose logs kafka
   ```

2. **Redis Connection Failed**
   ```bash
   # Ensure Redis is running
   docker-compose ps redis
   
   # Test connection
   docker exec -it redis redis-cli ping
   ```

3. **No Risk Calculations Appearing**
   - Check data generator is running
   - Verify Kafka topics have messages
   - Check Bytewax worker logs for errors

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