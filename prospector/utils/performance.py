"""
Performance tracking utilities for monitoring system throughput and latency.
"""

import time
import threading
from collections import deque
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class PerformanceTracker:
    """
    Thread-safe performance metrics tracker for real-time monitoring.
    
    Tracks:
    - Message processing count
    - Processing time and latency
    - Throughput (messages/second)
    - Recent performance trends
    """
    
    def __init__(self, window_size: int = 1000):
        """
        Initialize performance tracker.
        
        Args:
            window_size: Size of rolling window for recent metrics
        """
        self.messages_processed = 0
        self.total_processing_time = 0
        self.start_time = time.time()
        self.recent_latencies = deque(maxlen=window_size)
        self.lock = threading.Lock()
        self.window_size = window_size
    
    def record_message(self, latency_ms: float) -> None:
        """
        Record processing of a single message.
        
        Args:
            latency_ms: Processing latency in milliseconds
        """
        with self.lock:
            self.messages_processed += 1
            self.total_processing_time += latency_ms
            self.recent_latencies.append(latency_ms)
    
    def get_stats(self) -> Dict[str, float]:
        """
        Get current performance statistics.
        
        Returns:
            Dictionary containing:
            - messages_processed: Total count
            - throughput_per_second: Overall throughput
            - avg_latency_ms: Overall average latency
            - recent_avg_latency_ms: Recent window average
            - uptime_seconds: Total runtime
        """
        with self.lock:
            elapsed = time.time() - self.start_time
            
            if elapsed <= 0:
                return {
                    'messages_processed': 0,
                    'throughput_per_second': 0,
                    'avg_latency_ms': 0,
                    'recent_avg_latency_ms': 0,
                    'uptime_seconds': 0
                }
            
            throughput = self.messages_processed / elapsed if elapsed > 0 else 0
            avg_latency = (self.total_processing_time / self.messages_processed 
                          if self.messages_processed > 0 else 0)
            recent_avg = (sum(self.recent_latencies) / len(self.recent_latencies) 
                         if self.recent_latencies else 0)
            
            return {
                'messages_processed': self.messages_processed,
                'throughput_per_second': throughput,
                'avg_latency_ms': avg_latency,
                'recent_avg_latency_ms': recent_avg,
                'uptime_seconds': elapsed
            }
    
    def log_stats(self, interval: int = 100) -> None:
        """
        Log performance statistics if interval threshold is met.
        
        Args:
            interval: Log every N messages
        """
        if self.messages_processed % interval == 0 and self.messages_processed > 0:
            stats = self.get_stats()
            logger.info(
                f"📊 PERFORMANCE: Processed {stats['messages_processed']} messages | "
                f"Throughput: {stats['throughput_per_second']:.2f} msg/s | "
                f"Avg latency: {stats['recent_avg_latency_ms']:.2f}ms"
            )
    
    def reset(self) -> None:
        """Reset all performance metrics."""
        with self.lock:
            self.messages_processed = 0
            self.total_processing_time = 0
            self.start_time = time.time()
            self.recent_latencies.clear()