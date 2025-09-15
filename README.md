# Kafka

Apache Kafka is a distributed streaming platform that handles real-time data feeds by publishing and subscribing to streams of records, similar to a message queue. It's designed for high-throughput, fault-tolerant processing of continuous data streams across distributed systems, commonly used for building real-time analytics and data pipelines.

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
