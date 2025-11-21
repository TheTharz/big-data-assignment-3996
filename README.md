after docker up run below

docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --create --topic orders \
  --partitions 3 \
  --replication-factor 3

docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 \
  --describe --topic orders