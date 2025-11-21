import json
import sys
from confluent_kafka import Consumer, KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    DLQ_TOPIC,
    CONSUMER_GROUP_ID
)
from logger import setup_logger

logger = setup_logger('DLQConsumer')


class DLQConsumer:    
    def __init__(self):
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': f'{CONSUMER_GROUP_ID}-dlq-monitor',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }
        
        self.consumer = Consumer(consumer_conf)
        self.dlq_topic = DLQ_TOPIC
        logger.info(f"DLQ Consumer initialized for topic: {self.dlq_topic}")
    
    def run(self):
        try:
            self.consumer.subscribe([self.dlq_topic])
            logger.info("Monitoring DLQ. Press Ctrl+C to stop...")
            
            message_count = 0
            
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    dlq_data = json.loads(msg.value().decode('utf-8'))
                    message_count += 1
                    
                    logger.warning("=" * 60)
                    logger.warning(f"DLQ Message #{message_count}")
                    logger.warning(f"Original Topic: {dlq_data.get('original_topic')}")
                    logger.warning(f"Partition: {dlq_data.get('partition')}")
                    logger.warning(f"Offset: {dlq_data.get('offset')}")
                    logger.warning(f"Key: {dlq_data.get('key')}")
                    logger.warning(f"Error Reason: {dlq_data.get('error_reason')}")
                    logger.warning(f"Retry Count: {dlq_data.get('retry_count')}")
                    logger.warning(f"Timestamp: {dlq_data.get('timestamp')}")
                    logger.warning("=" * 60)
                    
                except Exception as e:
                    logger.error(f"Error parsing DLQ message: {e}")
        
        except KeyboardInterrupt:
            logger.info("DLQ monitoring stopped by user")
        finally:
            self.consumer.close()


def main():
    try:
        dlq_consumer = DLQConsumer()
        dlq_consumer.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
