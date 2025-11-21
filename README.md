# Kafka Order Processing System

A simple Kafka-based order processing system with Avro serialization, multi-partition support, and dead letter queue handling.

## Setup

1. **Create and activate virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install confluent-kafka[avro] python-dotenv
```

3. **Start Kafka cluster:**
```bash
docker-compose up -d
```

4. **Create topic with partitions:**
```bash
docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --create --topic orders \
  --partitions 3 \
  --replication-factor 3

docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --create --topic orders-dlq --partitions 3 --replication-factor 3
```

## Run

5. **Run producer:**
```bash
# Produce 100 orders with 1 second delay
python src/producer.py 100 1
```

6. **Run consumer:**
```bash
python src/consumer.py
```

7. **View Kafka UI:** http://localhost:8080

## Services
- **Kafka Brokers**: localhost:9092, 9093, 9094
- **Schema Registry**: localhost:8081
- **Kafka UI**: localhost:8080

## Components
- `producer.py` - Generates and sends orders to Kafka
- `consumer.py` - Processes orders with retry logic
- `dlq_consumer.py` - Handles failed messages
- `price_aggregator.py` - Aggregates price statistics

## Stop
```bash
docker-compose down -v
deactivate  # Exit virtual environment
```