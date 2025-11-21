import json
import random
import time
import sys
from typing import Dict, Any
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    ORDERS_TOPIC,
    SCHEMA_PATH
)
from logger import setup_logger

logger = setup_logger('OrderProducer')


class OrderProducer:
    
    def __init__(self):
        self.topic = ORDERS_TOPIC
        
        with open(SCHEMA_PATH, 'r') as f:
            self.schema_str = f.read()
        
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            self.schema_str,
            lambda obj, ctx: obj  
        )

        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'order-producer',
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'compression.type': 'snappy'
        }
        
        self.producer = Producer(producer_conf)
        logger.info(f"Producer initialized. Broker: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
    
    