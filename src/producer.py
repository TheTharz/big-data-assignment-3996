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
    
    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(
                f'Message delivered to {msg.topic()} '
                f'[partition {msg.partition()}] at offset {msg.offset()}'
            )
    
    def generate_order(self, order_id: int) -> Dict[str, Any]:
        products = ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Monitor', 
                   'Keyboard', 'Mouse', 'Webcam', 'Smartwatch', 'Speaker']
        
        order = {
            'orderId': str(order_id),
            'product': random.choice(products),
            'price': round(random.uniform(10.0, 2000.0), 2)
        }
        
        return order
    
    def produce_order(self, order: Dict[str, Any]):
        try:
            serialization_context = SerializationContext(
                self.topic,
                MessageField.VALUE
            )
            
            serialized_value = self.avro_serializer(
                order,
                serialization_context
            )
            
            self.producer.produce(
                topic=self.topic,
                key=order['orderId'].encode('utf-8'),
                value=serialized_value,
                on_delivery=self.delivery_report
            )
            
            self.producer.poll(0)
            
            logger.debug(f"Produced order: {order}")
            
        except Exception as e:
            logger.error(f"Failed to produce order {order['orderId']}: {e}")
            raise
    
    def run(self, num_messages: int = 100, delay_seconds: float = 1.0):
        logger.info(f"Starting to produce {num_messages} orders to topic '{self.topic}'")
        
        try:
            for i in range(1, num_messages + 1):
                order = self.generate_order(i)
                self.produce_order(order)
                
                if i % 10 == 0:
                    logger.info(f"Produced {i}/{num_messages} orders")
                
                time.sleep(delay_seconds)
            
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            logger.info(f"Successfully produced {num_messages} orders")
            
        except KeyboardInterrupt:
            logger.warning("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
            raise
        finally:
            self.producer.flush()
