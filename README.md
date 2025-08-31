# kafka

Video reference
https://www.youtube.com/watch?v=ZJJHm_bd9Zo


# Topics

Topics are categories or feeds where messages are published
Think of them as channels or queues where data is organized
Topics are split into partitions for scalability and parallelism

# Producers

Applications that publish (send) messages to Kafka topics
Can send data to specific partitions or let Kafka distribute automatically

# Consumers

Applications that subscribe to topics and process messages
Can be organized into consumer groups for load balancing
Each message in a partition is consumed by only one consumer in a group

# Brokers

Kafka servers that store and serve data
A Kafka cluster consists of multiple brokers for redundancy
Each broker handles multiple topic partitions

# Key Features

Durability: Messages are persisted to disk and replicated
Scalability: Horizontal scaling through partitioning
High Throughput: Handles millions of messages per second
Fault Tolerance: Data replication across multiple brokers
Real-time Processing: Low-latency message delivery

# Basic Workflow

1. Producer sends messages to a Topic
2. Messages are stored in Partitions across Brokers
3. Consumers read messages from topics
4. Messages are retained for a configurable time period

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