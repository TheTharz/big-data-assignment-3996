import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')

ORDERS_TOPIC = os.getenv('ORDERS_TOPIC', 'orders')
DLQ_TOPIC = os.getenv('DLQ_TOPIC', 'orders-dlq')

CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'order-consumer-group')
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
RETRY_BACKOFF_MS = int(os.getenv('RETRY_BACKOFF_MS', '1000'))

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'order.avsc')
