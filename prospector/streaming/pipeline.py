"""
Bytewax streaming pipeline for real-time risk calculation.
"""

import json
import logging
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSourceMessage

from models import Portfolio, RiskCalculation
from ..config.constants import KAFKA_BATCH_SIZE, INPUT_TOPIC, OUTPUT_TOPIC

logger = logging.getLogger(__name__)


def parse_kafka_message(msg: KafkaSourceMessage) -> Optional[Tuple[str, Portfolio]]:
    """
    Parse incoming Kafka message into Portfolio model.
    
    Args:
        msg: Raw Kafka message
        
    Returns:
        Tuple of (portfolio_id, Portfolio) or None if parsing fails
    """
    try:
        data = json.loads(msg.value)
        portfolio = Portfolio(**data)
        return (portfolio.id, portfolio)
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None


def serialize_for_kafka(risk_data: Tuple[str, RiskCalculation]) -> Optional[KafkaSinkMessage]:
    """
    Serialize risk calculation results for Kafka output.
    
    Args:
        risk_data: Tuple of (portfolio_id, RiskCalculation)
        
    Returns:
        KafkaSinkMessage ready for Kafka sink
    """
    if risk_data is None:
        return None
        
    key, risk_calc = risk_data
    return KafkaSinkMessage(
        key=risk_calc.portfolio_id.encode(),
        value=risk_calc.json().encode()
    )


def build_dataflow(risk_processor) -> Dataflow:
    """
    Construct the Bytewax dataflow pipeline for risk calculation.
    
    Args:
        risk_processor: RiskProcessor instance for calculations
        
    Returns:
        Configured Dataflow object ready for execution
        
    Pipeline stages:
    1. Input: Consume portfolio updates from Kafka
    2. Parse: Deserialize JSON to Portfolio models
    3. Filter: Remove malformed messages
    4. Calculate: Compute advanced risk metrics
    5. Filter: Remove failed calculations
    6. Log: Record processing statistics
    7. Serialize: Prepare results for output
    8. Output: Publish risk updates to Kafka
    """
    flow = Dataflow("prospector-risk-calculator")
    
    # Input from Kafka with optimized batch size
    portfolio_stream = op.input(
        "portfolio-input", 
        flow, 
        KafkaSource(
            brokers=["localhost:9092"],
            topics=[INPUT_TOPIC],
            batch_size=KAFKA_BATCH_SIZE
        )
    )
    
    # Parse messages into Portfolio models
    parsed = op.map("parse", portfolio_stream, parse_kafka_message)
    
    # Filter out parsing failures
    filtered = op.filter("filter-none", parsed, lambda x: x is not None)
    
    # Calculate risk using the processor
    risk_calculations = op.map(
        "calculate-risk", 
        filtered, 
        risk_processor.calculate_portfolio_risk
    )
    
    # Filter out calculation failures
    valid_risks = op.filter("filter-valid", risk_calculations, lambda x: x is not None)
    
    # Log statistics
    op.inspect(
        "log-stats", 
        valid_risks, 
        lambda step_id, x: logger.info(
            f"📊 Risk calculation complete for {x[0]} in {x[1].calculation_time_ms:.1f}ms"
        )
    )
    
    # Serialize for output
    output_messages = op.map("serialize", valid_risks, serialize_for_kafka)
    
    # Filter out serialization failures
    valid_messages = op.filter("filter-messages", output_messages, lambda x: x is not None)
    
    # Output to Kafka
    op.output(
        "risk-output",
        valid_messages,
        KafkaSink(
            brokers=["localhost:9092"],
            topic=OUTPUT_TOPIC
        )
    )
    
    return flow