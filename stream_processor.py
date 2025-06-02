#!/usr/bin/env python3
"""
Stream Processing Engine
Real-time data stream processing with Apache Kafka-like functionality.
Built with Python, asyncio, and modern streaming technologies.
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Callable, Any
import uuid
import threading
from collections import defaultdict, deque

class StreamProcessor:
    def __init__(self):
        self.topics = defaultdict(list)
        self.consumers = defaultdict(list)
        self.producers = {}
        self.running = False
        self.metrics = {
            'messages_processed': 0,
            'topics_count': 0,
            'consumers_count': 0,
            'producers_count': 0,
            'throughput': 0,
            'latency': []
        }
        
    def create_topic(self, topic_name: str, partitions: int = 1):
        """Create a new topic with specified partitions"""
        if topic_name not in self.topics:
            self.topics[topic_name] = [deque() for _ in range(partitions)]
            self.metrics['topics_count'] += 1
            print(f"Topic '{topic_name}' created with {partitions} partitions")
        
    def produce(self, topic: str, message: Dict[str, Any], partition: int = 0):
        """Produce a message to a topic"""
        if topic not in self.topics:
            self.create_topic(topic)
            
        timestamp = datetime.now().isoformat()
        enriched_message = {
            'id': str(uuid.uuid4()),
            'timestamp': timestamp,
            'topic': topic,
            'partition': partition,
            'data': message
        }
        
        self.topics[topic][partition].append(enriched_message)
        self.metrics['messages_processed'] += 1
        
        # Notify consumers
        for consumer in self.consumers[topic]:
            consumer['callback'](enriched_message)
            
        return enriched_message['id']
    
    def consume(self, topic: str, consumer_group: str, callback: Callable):
        """Register a consumer for a topic"""
        consumer = {
            'id': str(uuid.uuid4()),
            'group': consumer_group,
            'topic': topic,
            'callback': callback,
            'created_at': datetime.now().isoformat()
        }
        
        self.consumers[topic].append(consumer)
        self.metrics['consumers_count'] += 1
        print(f"Consumer registered for topic '{topic}' in group '{consumer_group}'")
        return consumer['id']
    
    def get_metrics(self):
        """Get current streaming metrics"""
        return self.metrics.copy()
    
    def list_topics(self):
        """List all available topics"""
        return list(self.topics.keys())

class DataTransformer:
    """Data transformation utilities for stream processing"""
    
    @staticmethod
    def filter_data(data: Dict, condition: Callable) -> bool:
        """Filter data based on condition"""
        return condition(data)
    
    @staticmethod
    def transform_data(data: Dict, transformer: Callable) -> Dict:
        """Transform data using provided function"""
        return transformer(data)
    
    @staticmethod
    def aggregate_data(data_stream: List[Dict], aggregator: Callable) -> Any:
        """Aggregate data from stream"""
        return aggregator(data_stream)

class StreamAnalytics:
    """Real-time analytics for stream processing"""
    
    def __init__(self):
        self.window_data = defaultdict(deque)
        self.window_size = 60  # seconds
        
    def add_data_point(self, metric_name: str, value: float):
        """Add data point to analytics window"""
        timestamp = time.time()
        self.window_data[metric_name].append((timestamp, value))
        
        # Remove old data points
        cutoff = timestamp - self.window_size
        while (self.window_data[metric_name] and 
               self.window_data[metric_name][0][0] < cutoff):
            self.window_data[metric_name].popleft()
    
    def get_average(self, metric_name: str) -> float:
        """Get average value for metric in current window"""
        if not self.window_data[metric_name]:
            return 0.0
        
        values = [point[1] for point in self.window_data[metric_name]]
        return sum(values) / len(values)
    
    def get_throughput(self, metric_name: str) -> float:
        """Get throughput (events per second) for metric"""
        if len(self.window_data[metric_name]) < 2:
            return 0.0
        
        return len(self.window_data[metric_name]) / self.window_size

# Example usage and demo
def demo_stream_processing():
    """Demonstrate stream processing capabilities"""
    
    # Initialize stream processor
    processor = StreamProcessor()
    analytics = StreamAnalytics()
    transformer = DataTransformer()
    
    # Create topics
    processor.create_topic('user_events', partitions=3)
    processor.create_topic('system_metrics', partitions=2)
    processor.create_topic('processed_data', partitions=1)
    
    # Define consumer callbacks
    def user_event_consumer(message):
        print(f"Processing user event: {message['data']['event_type']}")
        analytics.add_data_point('user_events', 1)
        
        # Transform and forward to processed_data topic
        if message['data']['event_type'] == 'purchase':
            transformed = transformer.transform_data(
                message['data'], 
                lambda x: {**x, 'processed': True, 'value_category': 'high' if x.get('amount', 0) > 100 else 'low'}
            )
            processor.produce('processed_data', transformed)
    
    def metrics_consumer(message):
        print(f"Processing system metric: {message['data']['metric_name']}")
        analytics.add_data_point('system_metrics', message['data']['value'])
    
    def processed_data_consumer(message):
        print(f"Final processed data: {message['data']}")
    
    # Register consumers
    processor.consume('user_events', 'analytics_group', user_event_consumer)
    processor.consume('system_metrics', 'monitoring_group', metrics_consumer)
    processor.consume('processed_data', 'output_group', processed_data_consumer)
    
    # Simulate data production
    print("\\n=== Starting Stream Processing Demo ===\\n")
    
    # Produce user events
    user_events = [
        {'event_type': 'login', 'user_id': 'user_123', 'timestamp': time.time()},
        {'event_type': 'purchase', 'user_id': 'user_123', 'amount': 150, 'product': 'laptop'},
        {'event_type': 'logout', 'user_id': 'user_123'},
        {'event_type': 'purchase', 'user_id': 'user_456', 'amount': 50, 'product': 'book'}
    ]
    
    for event in user_events:
        processor.produce('user_events', event, partition=hash(event['user_id']) % 3)
        time.sleep(0.1)
    
    # Produce system metrics
    system_metrics = [
        {'metric_name': 'cpu_usage', 'value': 75.5, 'host': 'server-01'},
        {'metric_name': 'memory_usage', 'value': 82.3, 'host': 'server-01'},
        {'metric_name': 'disk_io', 'value': 45.2, 'host': 'server-02'}
    ]
    
    for metric in system_metrics:
        processor.produce('system_metrics', metric)
        time.sleep(0.1)
    
    # Display analytics
    time.sleep(1)
    print("\\n=== Stream Analytics ===")
    print(f"User events throughput: {analytics.get_throughput('user_events'):.2f} events/sec")
    print(f"System metrics throughput: {analytics.get_throughput('system_metrics'):.2f} events/sec")
    print(f"Average system metric value: {analytics.get_average('system_metrics'):.2f}")
    
    # Display processor metrics
    print("\\n=== Processor Metrics ===")
    metrics = processor.get_metrics()
    for key, value in metrics.items():
        if key != 'latency':
            print(f"{key}: {value}")
    
    print(f"\\nAvailable topics: {processor.list_topics()}")

if __name__ == '__main__':
    demo_stream_processing()

