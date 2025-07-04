#!/usr/bin/env python3
"""
Prospector Data Generator - Realistic portfolio and market data simulation.

This module generates synthetic but realistic financial data for testing the
Prospector risk calculation system. It produces:

1. Portfolio Data:
   - Client portfolios with 5-20 stock positions
   - Realistic position weights using exponential distribution
   - Portfolio values ranging from $100k to $5M
   - Various risk tolerance levels and account types

2. Market Data:
   - Real-time price updates for all tracked securities
   - Sector-based volatility and expected returns
   - Beta values for systematic risk measurement

The generator can run in two modes:
- Continuous: Streams data at configurable intervals
- Batch: Generates a fixed amount of data and exits
"""

import json
import time
import random
import logging
from typing import Dict, List
from faker import Faker
import numpy as np
from confluent_kafka import Producer
import argparse

from models import (
    Portfolio, Position, MarketData, 
    RiskTolerance, AccountType, Sector
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

# Stock symbols with their sectors
STOCK_SECTORS = {
    'AAPL': Sector.TECHNOLOGY, 'MSFT': Sector.TECHNOLOGY, 'GOOGL': Sector.TECHNOLOGY,
    'AMZN': Sector.CONSUMER, 'TSLA': Sector.AUTOMOTIVE, 'META': Sector.TECHNOLOGY,
    'NVDA': Sector.TECHNOLOGY, 'NFLX': Sector.ENTERTAINMENT, 'BRK.B': Sector.FINANCE,
    'JNJ': Sector.HEALTHCARE, 'JPM': Sector.FINANCE, 'V': Sector.FINANCE,
    'PG': Sector.CONSUMER, 'HD': Sector.RETAIL, 'UNH': Sector.HEALTHCARE,
    'MA': Sector.FINANCE, 'DIS': Sector.ENTERTAINMENT, 'PYPL': Sector.FINANCE,
    'ADBE': Sector.TECHNOLOGY, 'CRM': Sector.TECHNOLOGY, 'CMCSA': Sector.TELECOM,
    'INTC': Sector.TECHNOLOGY, 'VZ': Sector.TELECOM, 'T': Sector.TELECOM,
    'PFE': Sector.HEALTHCARE, 'WMT': Sector.RETAIL, 'BAC': Sector.FINANCE,
    'KO': Sector.CONSUMER, 'ABBV': Sector.HEALTHCARE, 'CVX': Sector.ENERGY,
    'LLY': Sector.HEALTHCARE, 'TMO': Sector.HEALTHCARE, 'AVGO': Sector.TECHNOLOGY,
    'ORCL': Sector.TECHNOLOGY, 'MRK': Sector.HEALTHCARE, 'AMD': Sector.TECHNOLOGY,
    'CSCO': Sector.TECHNOLOGY, 'TXN': Sector.TECHNOLOGY
}

# Base prices for stocks
BASE_PRICES = {
    'AAPL': 185.0, 'MSFT': 420.0, 'GOOGL': 140.0, 'AMZN': 170.0,
    'TSLA': 250.0, 'META': 500.0, 'NVDA': 870.0, 'NFLX': 460.0,
    'BRK.B': 380.0, 'JNJ': 155.0, 'JPM': 195.0, 'V': 275.0,
    'PG': 160.0, 'HD': 380.0, 'UNH': 540.0, 'MA': 460.0
}

# Sector characteristics
SECTOR_VOLATILITY = {
    Sector.TECHNOLOGY: 0.25,
    Sector.HEALTHCARE: 0.18,
    Sector.FINANCE: 0.20,
    Sector.CONSUMER: 0.15,
    Sector.ENERGY: 0.30,
    Sector.RETAIL: 0.22,
    Sector.TELECOM: 0.16,
    Sector.ENTERTAINMENT: 0.28,
    Sector.AUTOMOTIVE: 0.35,
    Sector.REAL_ESTATE: 0.14,
    Sector.OTHER: 0.20
}

SECTOR_RETURNS = {
    Sector.TECHNOLOGY: 0.12,
    Sector.HEALTHCARE: 0.10,
    Sector.FINANCE: 0.09,
    Sector.CONSUMER: 0.08,
    Sector.ENERGY: 0.07,
    Sector.RETAIL: 0.09,
    Sector.TELECOM: 0.06,
    Sector.ENTERTAINMENT: 0.11,
    Sector.AUTOMOTIVE: 0.15,
    Sector.REAL_ESTATE: 0.07,
    Sector.OTHER: 0.08
}


class PortfolioGenerator:
    def __init__(self, broker: str = 'localhost:9092'):
        """
        Initialize the generator with Kafka producer configuration.
        
        Args:
            broker: Kafka broker address (default: localhost:9092)
            
        Sets up a Kafka producer with compression and batching for efficient
        message delivery. Maintains an internal registry of active portfolios
        for realistic update simulation.
        """
        self.producer_config = {
            'bootstrap.servers': broker,
            'client.id': 'prospector-portfolio-generator',
            'compression.type': 'gzip',
            'batch.size': 16384,
            'linger.ms': 10
        }
        self.producer = Producer(self.producer_config)
        self.portfolios: Dict[str, Portfolio] = {}
        
    def delivery_report(self, err, msg):
        """
        Kafka producer delivery callback for tracking message status.
        
        Args:
            err: Error object if delivery failed, None on success
            msg: The message that was delivered or failed
            
        Logs errors for failed deliveries and debug info for successful ones.
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def get_current_price(self, symbol: str) -> float:
        """
        Generate realistic stock price with random market fluctuation.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Float price based on real base prices with ±5% random variation
            
        Uses predefined base prices for known symbols or generates a random
        price between $50-$200 for unknown symbols.
        """
        base = BASE_PRICES.get(symbol, random.uniform(50, 200))
        # Add some random variation (±5%)
        return base * random.uniform(0.95, 1.05)
    
    def generate_portfolio(self) -> Portfolio:
        """
        Generate a complete portfolio with realistic characteristics.
        
        Returns:
            Portfolio object with positions, risk tolerance, and account info
            
        Creates portfolios with:
        - 5-20 randomly selected stock positions
        - Weights following exponential distribution (mimics real portfolios)
        - Normalized weights summing to 100%
        - Random risk tolerance and account type
        - Unique IDs for portfolio, advisor, and client
        """
        portfolio_id = f"portfolio_{fake.uuid4()}"
        advisor_id = f"advisor_{random.randint(1, 20)}"
        client_id = f"client_{fake.uuid4()}"
        
        # Random number of positions (5-20 stocks)
        num_positions = random.randint(5, 20)
        selected_symbols = random.sample(list(STOCK_SECTORS.keys()), num_positions)
        
        # Generate positions with realistic weights
        raw_weights = np.random.exponential(scale=2.0, size=num_positions)
        weights = (raw_weights / raw_weights.sum()) * 100
        
        # Target portfolio value ($100k - $5M)
        portfolio_target = random.uniform(100000, 5000000)
        
        positions = []
        for i, symbol in enumerate(selected_symbols):
            price = self.get_current_price(symbol)
            weight = weights[i]
            position_value = portfolio_target * (weight / 100)
            quantity = int(position_value / price)
            market_value = quantity * price
            
            position = Position(
                symbol=symbol,
                quantity=float(quantity),
                price=price,
                market_value=market_value,
                weight=weight,
                sector=STOCK_SECTORS[symbol]
            )
            positions.append(position)
        
        # Normalize weights to ensure they sum to 100
        total_value = sum(p.market_value for p in positions)
        for position in positions:
            position.weight = (position.market_value / total_value) * 100
        
        portfolio = Portfolio(
            id=portfolio_id,
            advisor_id=advisor_id,
            client_id=client_id,
            positions=positions,
            total_value=total_value,
            risk_tolerance=random.choice(list(RiskTolerance)),
            account_type=random.choice(list(AccountType))
        )
        
        # Store for tracking
        self.portfolios[portfolio_id] = portfolio
        
        return portfolio
    
    def generate_market_data(self, symbol: str) -> MarketData:
        """
        Generate market data metrics for a given symbol.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            MarketData object with price, volatility, returns, and beta
            
        Market data includes:
        - Current price with market fluctuation
        - Sector-based volatility with ±20% variation
        - Expected returns based on sector performance
        - Beta coefficient (0.7-1.5) for systematic risk
        """
        sector = STOCK_SECTORS.get(symbol, Sector.OTHER)
        
        return MarketData(
            symbol=symbol,
            price=self.get_current_price(symbol),
            volatility=SECTOR_VOLATILITY[sector] * random.uniform(0.8, 1.2),
            expected_return=SECTOR_RETURNS[sector] * random.uniform(0.9, 1.1),
            beta=random.uniform(0.7, 1.5)
        )
    
    def send_portfolio_update(self, portfolio: Portfolio):
        """
        Publish portfolio update to Kafka topic.
        
        Args:
            portfolio: Portfolio object to send
            
        Sends to 'portfolio-updates' topic with portfolio ID as key
        for consistent partitioning. Message value is JSON-serialized
        portfolio data.
        """
        self.producer.produce(
            'portfolio-updates',
            key=portfolio.id.encode(),
            value=portfolio.json().encode(),
            callback=self.delivery_report
        )
    
    def send_market_data(self, market_data: MarketData):
        """
        Publish market data update to Kafka topic.
        
        Args:
            market_data: MarketData object to send
            
        Sends to 'market-data' topic with symbol as key. This ensures
        all updates for a given symbol go to the same partition.
        """
        self.producer.produce(
            'market-data',
            key=market_data.symbol.encode(),
            value=market_data.json().encode(),
            callback=self.delivery_report
        )
    
    def update_portfolio(self, portfolio: Portfolio) -> Portfolio:
        """
        Simulate market movements by updating portfolio positions.
        
        Args:
            portfolio: Existing portfolio to update
            
        Returns:
            Updated portfolio with new prices and quantities
            
        Simulates:
        - Price changes: ±2% random movement
        - Volume changes: ±5% position size adjustment
        - Automatic recalculation of market values and weights
        - Updated timestamp for tracking
        """
        # Simulate market movements
        for position in portfolio.positions:
            # Price changes (±2%)
            position.price *= random.uniform(0.98, 1.02)
            
            # Volume changes (±5%)
            position.quantity *= random.uniform(0.95, 1.05)
            
            # Recalculate market value
            position.market_value = position.quantity * position.price
        
        # Recalculate total value and weights
        portfolio.total_value = sum(p.market_value for p in portfolio.positions)
        for position in portfolio.positions:
            position.weight = (position.market_value / portfolio.total_value) * 100
        
        portfolio.timestamp = time.time()
        
        return portfolio
    
    def run_continuous(self, portfolio_interval: float = 2.0, market_interval: float = 1.0):
        """
        Run generator in continuous streaming mode.
        
        Args:
            portfolio_interval: Seconds between portfolio updates (default: 2.0)
            market_interval: Seconds between market data updates (default: 1.0)
            
        Continuously generates:
        - Portfolio updates (70% existing updates, 30% new portfolios)
        - Market data for all active symbols plus random selections
        - Runs until interrupted with Ctrl+C
        
        Flushes producer periodically to ensure timely delivery.
        """
        logger.info("🚀 Starting continuous data generation...")
        logger.info(f"Portfolio updates every {portfolio_interval}s")
        logger.info(f"Market data updates every {market_interval}s")
        
        last_portfolio_time = 0
        last_market_time = 0
        portfolio_count = 0
        market_count = 0
        
        try:
            while True:
                current_time = time.time()
                
                # Generate portfolio updates
                if current_time - last_portfolio_time >= portfolio_interval:
                    # 70% chance to update existing, 30% to create new
                    if self.portfolios and random.random() < 0.7:
                        # Update existing portfolio
                        portfolio_id = random.choice(list(self.portfolios.keys()))
                        portfolio = self.update_portfolio(self.portfolios[portfolio_id])
                    else:
                        # Create new portfolio
                        portfolio = self.generate_portfolio()
                    
                    self.send_portfolio_update(portfolio)
                    portfolio_count += 1
                    last_portfolio_time = current_time
                    logger.info(f"📊 Sent portfolio update {portfolio_count}: {portfolio.id} "
                              f"(${portfolio.total_value:,.2f})")
                
                # Generate market data updates
                if current_time - last_market_time >= market_interval:
                    # Update market data for all symbols in active portfolios
                    symbols_to_update = set()
                    for portfolio in self.portfolios.values():
                        for position in portfolio.positions:
                            symbols_to_update.add(position.symbol)
                    
                    # Also add some random symbols
                    symbols_to_update.update(random.sample(list(STOCK_SECTORS.keys()), 5))
                    
                    for symbol in symbols_to_update:
                        market_data = self.generate_market_data(symbol)
                        self.send_market_data(market_data)
                        market_count += 1
                    
                    last_market_time = current_time
                    logger.info(f"📈 Sent market data for {len(symbols_to_update)} symbols "
                              f"(total: {market_count})")
                
                # Small sleep to prevent tight loop
                time.sleep(0.1)
                
                # Flush producer periodically
                self.producer.poll(0)
                
        except KeyboardInterrupt:
            logger.info("\n⏹️  Stopping data generation...")
        finally:
            self.producer.flush()
            logger.info(f"✅ Generated {portfolio_count} portfolios and {market_count} market updates")
    
    def run_batch(self, num_portfolios: int = 100, num_updates_per_portfolio: int = 5):
        """
        Generate a fixed batch of data and exit.
        
        Args:
            num_portfolios: Number of initial portfolios to create
            num_updates_per_portfolio: Number of updates per portfolio
            
        Useful for:
        - Testing with predictable data volumes
        - Populating initial system state
        - Performance benchmarking
        
        Generates all portfolios first, then cycles through updates.
        """
        logger.info(f"🚀 Generating batch data: {num_portfolios} portfolios")
        
        # First, generate initial portfolios
        for i in range(num_portfolios):
            portfolio = self.generate_portfolio()
            self.send_portfolio_update(portfolio)
            
            # Generate market data for all positions
            for position in portfolio.positions:
                market_data = self.generate_market_data(position.symbol)
                self.send_market_data(market_data)
            
            if (i + 1) % 10 == 0:
                logger.info(f"Generated {i + 1}/{num_portfolios} portfolios...")
        
        # Then generate updates for existing portfolios
        logger.info(f"Generating {num_updates_per_portfolio} updates per portfolio...")
        
        for i in range(num_updates_per_portfolio):
            for portfolio_id in list(self.portfolios.keys()):
                portfolio = self.update_portfolio(self.portfolios[portfolio_id])
                self.send_portfolio_update(portfolio)
            
            # Also update market data
            for symbol in STOCK_SECTORS.keys():
                market_data = self.generate_market_data(symbol)
                self.send_market_data(market_data)
            
            logger.info(f"Completed update round {i + 1}/{num_updates_per_portfolio}")
            time.sleep(1)
        
        self.producer.flush()
        logger.info(f"✅ Batch generation complete!")


def main():
    """
    Command-line entry point for the Prospector data generator.
    
    Supports multiple configuration options:
    - Broker address for Kafka connection
    - Generation mode (continuous vs batch)
    - Update intervals for continuous mode
    - Data volumes for batch mode
    
    Run with --help for full option list.
    """
    parser = argparse.ArgumentParser(description='Generate portfolio and market data for Prospector risk calculator')
    parser.add_argument('--broker', default='localhost:9092', help='Kafka broker address')
    parser.add_argument('--mode', choices=['continuous', 'batch'], default='continuous',
                       help='Generation mode')
    parser.add_argument('--portfolio-interval', type=float, default=2.0,
                       help='Seconds between portfolio updates (continuous mode)')
    parser.add_argument('--market-interval', type=float, default=1.0,
                       help='Seconds between market data updates (continuous mode)')
    parser.add_argument('--num-portfolios', type=int, default=100,
                       help='Number of portfolios to generate (batch mode)')
    parser.add_argument('--updates-per-portfolio', type=int, default=5,
                       help='Number of updates per portfolio (batch mode)')
    
    args = parser.parse_args()
    
    generator = PortfolioGenerator(broker=args.broker)
    
    if args.mode == 'continuous':
        generator.run_continuous(
            portfolio_interval=args.portfolio_interval,
            market_interval=args.market_interval
        )
    else:
        generator.run_batch(
            num_portfolios=args.num_portfolios,
            num_updates_per_portfolio=args.updates_per_portfolio
        )


if __name__ == '__main__':
    main()