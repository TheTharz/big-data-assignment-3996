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
from price_aggregator import PriceAggregator

logger = setup_logger('OrderConsumer')

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
    
    def send_to_dlq(self, message, error_reason: str):
        try:
            dlq_payload = {
                'original_topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
                'key': message.key().decode('utf-8') if message.key() else None,
                'value': message.value(),
                'error_reason': error_reason,
                'timestamp': time.time(),
                'retry_count': self.retry_counts.get(message.key().decode('utf-8'), 0)
            }
            
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=message.key(),
                value=json.dumps(dlq_payload).encode('utf-8')
            )
            
            self.dlq_producer.poll(0)
            logger.warning(f"Message sent to DLQ: {error_reason}")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    def process_message(self, order: Dict[str, Any]) -> bool:
        try:
            order_id = order['orderId']
            product = order['product']
            price = order['price']
            
            if price < 0:
                raise ValueError(f"Invalid price: {price}")
            
            avg_price = self.aggregator.update(price)
            
            logger.info(
                f"Processed Order {order_id}: {product} @ ${price:.2f} | "
                f"Running Avg: ${avg_price:.2f} | "
                f"Total Orders: {self.aggregator.count}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing order: {e}")
            return False
    
    def handle_message_with_retry(self, msg) -> bool:
        message_key = msg.key().decode('utf-8') if msg.key() else 'unknown'
        
        if message_key not in self.retry_counts:
            self.retry_counts[message_key] = 0
        
        try:
            serialization_context = SerializationContext(
                msg.topic(),
                MessageField.VALUE
            )
            order = self.avro_deserializer(msg.value(), serialization_context)
            
            success = self.process_message(order)
            
            if success:
                self.retry_counts.pop(message_key, None)
                return True
            else:
                self.retry_counts[message_key] += 1
                
                if self.retry_counts[message_key] < MAX_RETRY_ATTEMPTS:
                    backoff = RETRY_BACKOFF_MS * (2 ** (self.retry_counts[message_key] - 1))
                    logger.warning(
                        f"Retry {self.retry_counts[message_key]}/{MAX_RETRY_ATTEMPTS} "
                        f"for message {message_key}. Waiting {backoff}ms"
                    )
                    time.sleep(backoff / 1000.0)
                    return False
                else:
                    self.send_to_dlq(msg, "Max retry attempts exceeded")
                    self.retry_counts.pop(message_key, None)
                    return True
        
        except Exception as e:
            logger.error(f"Error handling message {message_key}: {e}")
            self.retry_counts[message_key] += 1
            
            if self.retry_counts[message_key] >= MAX_RETRY_ATTEMPTS:
                self.send_to_dlq(msg, f"Deserialization/Processing error: {str(e)}")
                self.retry_counts.pop(message_key, None)
                return True
            
            return False
    
    def run(self):
        try:
            self.consumer.subscribe([self.orders_topic])
            logger.info("Consumer started. Waiting for messages...")
            
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                success = self.handle_message_with_retry(msg)
                
                if success:
                    self.consumer.commit(asynchronous=False)
                
                if self.aggregator.count % 10 == 0:
                    stats = self.aggregator.get_stats()
                    logger.info(f"=== Stats: {stats} ===")
        
        except KeyboardInterrupt:
            logger.warning("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            logger.info("Closing consumer...")
            self.consumer.close()
            self.dlq_producer.flush()
            
            final_stats = self.aggregator.get_stats()
            logger.info(f"=== Final Statistics ===")
            logger.info(f"Total Orders Processed: {final_stats['total_orders']}")
            logger.info(f"Total Revenue: ${final_stats['total_revenue']}")
            logger.info(f"Average Order Price: ${final_stats['running_average']}")


def main():
    try:
        consumer = OrderConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
