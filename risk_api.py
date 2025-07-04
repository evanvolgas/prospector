#!/usr/bin/env python3
"""
Prospector Risk API - Real-time Portfolio Risk Analytics Service.

This FastAPI application provides a comprehensive REST API for accessing
real-time portfolio risk calculations and analytics. It serves as the
client-facing interface to the Prospector risk calculation engine.

Architecture:
    The API follows a modular architecture with clear separation of concerns:
    
    - prospector.api.core: Infrastructure components
        - dependencies.py: Shared resources (Redis, Kafka clients)
        - startup.py: Application lifecycle management
        - exceptions.py: Centralized error handling
        
    - prospector.api.routers: Business logic endpoints
        - health.py: System health and monitoring
        - risk.py: Individual portfolio risk retrieval
        - portfolios.py: Portfolio collection operations
        - portfolio.py: Portfolio management and updates
        - advisor.py: Advisor-specific operations
        - analytics.py: Aggregate metrics and analytics
        - streaming.py: Real-time SSE streaming

Key Features:
    - RESTful API with comprehensive OpenAPI documentation
    - Real-time risk metrics retrieval from Redis cache
    - Portfolio submission to Kafka processing pipeline
    - Server-Sent Events for live risk updates
    - Aggregate analytics across all portfolios
    - High-performance with sub-millisecond response times

Usage:
    Run directly: python risk_api.py
    With uvicorn: uvicorn risk_api:app --reload
    
API Documentation:
    - Interactive docs: http://localhost:6066/docs
    - ReDoc: http://localhost:6066/redoc
"""

import logging
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Import core modules
from prospector.api.core.startup import lifespan
from prospector.api.core.exceptions import http_exception_handler, general_exception_handler

# Import routers
from prospector.api.routers import (
    health,
    risk,
    portfolio,
    portfolios,
    advisor,
    analytics,
    streaming
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Prospector Risk Calculator API",
    version="2.0.0",
    description="Real-time portfolio risk calculation and monitoring API - Modular Architecture",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register exception handlers
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# Include routers
app.include_router(health.router)
app.include_router(risk.router)
app.include_router(portfolio.router)
app.include_router(portfolios.router)
app.include_router(advisor.router)
app.include_router(analytics.router)
app.include_router(streaming.router)


def main():
    """
    Launch the Prospector Risk API server.
    
    Starts the Uvicorn ASGI server with the FastAPI application,
    listening on all interfaces (0.0.0.0) on port 6066.
    
    The server will:
    - Initialize Redis and Kafka connections on startup
    - Serve API endpoints for risk data retrieval and portfolio management
    - Provide real-time streaming capabilities
    - Gracefully shutdown connections on termination
    
    Configuration:
        Host: 0.0.0.0 (all interfaces)
        Port: 6066
        
    To run with custom settings, use uvicorn directly:
        uvicorn risk_api:app --host 127.0.0.1 --port 8000 --workers 4
    """
    uvicorn.run(app, host="0.0.0.0", port=6066)


if __name__ == "__main__":
    main()