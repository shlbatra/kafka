# kafka

# Instructions to set up kafka

Docker images required

1. Start Zookeper Container and expose PORT 2181
docker run -p 2181:2181 zookeeper

2. Start Kafka Container, expose PORT 9092 and setup ENV variables.
docker run -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=10.0.0.224:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://10.0.0.224:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:6.2.0

3. Instructions to run code

npm install kafkajs

- Producer
node producer.js

- Run multiple consumers
node consumer.js <GROUP_NAME>

Ex. data for producer
sahil north
sahil south
sahil north