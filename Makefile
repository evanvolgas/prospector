.PHONY: help install install-dev up down start stop logs clean test format lint type-check run-calculator run-generator run-api

# Default target
help:
	@echo "Prospector Risk Calculator - Available Commands:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install        - Install dependencies with uv"
	@echo "  make install-dev    - Install with dev dependencies"
	@echo ""
	@echo "Docker Infrastructure:"
	@echo "  make up            - Start all Docker services"
	@echo "  make down          - Stop all Docker services"
	@echo "  make start         - Start stopped services"
	@echo "  make stop          - Stop running services"
	@echo "  make logs          - Follow all service logs"
	@echo "  make clean         - Stop services and remove volumes"
	@echo ""
	@echo "Development:"
	@echo "  make test          - Run tests"
	@echo "  make format        - Format code with black and ruff"
	@echo "  make lint          - Check code with ruff"
	@echo "  make type-check    - Run mypy type checking"
	@echo ""
	@echo "Running Services:"
	@echo "  make run-calculator - Run the risk calculator"
	@echo "  make run-generator  - Run the data generator"
	@echo "  make run-api       - Run the monitoring API"
	@echo "  make run-all       - Run all services in separate terminals"
	@echo ""
	@echo "Monitoring:"
	@echo "  make status        - Check service status"
	@echo "  make topics        - List Kafka topics"
	@echo "  make redis-cli     - Connect to Redis CLI"

# Installation
install:
	uv sync

install-dev:
	uv sync --dev

# Docker commands
up:
	docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	docker-compose ps

down:
	docker-compose down

start:
	docker-compose start

stop:
	docker-compose stop

logs:
	docker-compose logs -f

clean:
	docker-compose down -v
	rm -rf .venv
	rm -rf __pycache__ .pytest_cache .mypy_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Development
test:
	uv run pytest -v

format:
	uv run black .
	uv run ruff --fix .

lint:
	uv run ruff check .

type-check:
	uv run mypy .

# Run services
run-calculator:
	uv run risk-calculator

run-generator:
	uv run data-generator

run-api:
	uv run risk-api

# New unified commands using prospector CLI
run:
	uv run prospector start all

demo:
	uv run prospector demo

benchmark:
	uv run prospector benchmark

monitor:
	uv run prospector start monitor

dashboard:
	uv run prospector start dashboard

# Monitoring
status:
	uv run prospector status

topics:
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

redis-cli:
	docker exec -it redis redis-cli

# Quick start commands
quickstart: install
	@echo "🚀 Starting Prospector in demo mode..."
	uv run prospector demo

# Development shortcuts
dev: install-dev format lint type-check test
	@echo "Development checks complete!"