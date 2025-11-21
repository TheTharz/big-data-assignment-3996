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


