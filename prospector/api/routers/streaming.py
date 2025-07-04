"""Real-time streaming endpoints."""

import json
import time
import asyncio
import logging
from typing import Optional
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stream", tags=["Streaming"])


@router.get("/risk-updates")
async def stream_risk_updates(portfolio_id: Optional[str] = None):
    """
    Stream real-time risk updates via Server-Sent Events (SSE).
    
    Args:
        portfolio_id: Optional filter for specific portfolio updates
        
    Returns:
        SSE stream of risk calculations as they occur
    """
    async def event_generator():
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'risk-api-stream-{time.time()}',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['risk-updates'])
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    # Filter by portfolio_id if specified
                    if portfolio_id and data.get('portfolio_id') != portfolio_id:
                        continue
                    
                    yield f"data: {json.dumps(data)}\n\n"
                    
                except json.JSONDecodeError:
                    logger.error("Failed to decode message")
                    
        finally:
            consumer.close()
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")