import json
import time
import sys
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDERS_TOPIC,
    DLQ_TOPIC,
    CONSUMER_GROUP_ID,
    MAX_RETRY_ATTEMPTS,
    RETRY_BACKOFF_MS,
    SCHEMA_PATH
)
from logger import setup_logger

logger = setup_logger('OrderConsumer')


class PriceAggregator:
    
    def __init__(self):
        self.total_price = 0.0
        self.count = 0
        self.running_average = 0.0
    
    def update(self, price: float) -> float:
        self.total_price += price
        self.count += 1
        self.running_average = self.total_price / self.count
        return self.running_average
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            'total_orders': self.count,
            'total_revenue': round(self.total_price, 2),
            'running_average': round(self.running_average, 2)
        }

class OrderConsumer:
    def __init__(self):
        self.orders_topic = ORDERS_TOPIC
        self.dlq_topic = DLQ_TOPIC
        
        with open(SCHEMA_PATH, 'r') as f:
            self.schema_str = f.read()
        
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            self.schema_str,
            lambda obj, ctx: obj
        )
        
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for better control
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000
        }
        
        self.consumer = Consumer(consumer_conf)
        
        dlq_producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'order-consumer-dlq-producer'
        }
        self.dlq_producer = Producer(dlq_producer_conf)
        
        self.aggregator = PriceAggregator()
        
        self.retry_counts: Dict[str, int] = {}
        
        logger.info(f"Consumer initialized. Group ID: {CONSUMER_GROUP_ID}")
        logger.info(f"Subscribing to topic: {self.orders_topic}")
    
    