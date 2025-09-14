# Kafka Basic Next.js Example

This project demonstrates basic Kafka producer and consumer functionality using Node.js and KafkaJS.
Video Reference - https://www.youtube.com/watch?v=ZJJHm_bd9Zo

## Local Kafka Setup Steps


### 1. Install KafkaJS
```bash
npm install kafkajs
```

### 2. Start Zookeeper Container and expose PORT 2181
```bash
# Start Zookeeper (required for Kafka) 
docker run -p 2181:2181 zookeeper
```

### 3. Start Kafka
```bash
# Get IP address for your machine
docker run -p 9092:9092 \
-e KAFKA_ZOOKEEPER_CONNECT=<PRIVATE_IP>:2181 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://<PRIVATE_IP>:9092 \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
confluentinc/cp-kafka
```

## Usage

### Create Kafka Topic
```bash
node admin.js
```
This creates the `rider-updates1` topic with 2 partitions.

### Start Kafka Producer
```bash
node producer.js
```

### Run multiple consumers
Update the broker address in `client.js` to match your local setup:
```bash
node consumer.js <GROUP_NAME>
```

### Running the Consumer
Open a terminal and run:
```bash
node consumer.js <consumer-group-name>
```

Examples:
```bash
# Terminal 1 - Consumer Group 1
node consumer.js group1

# Terminal 2 - Consumer Group 2  
node consumer.js group2
```

### Running the Producer
In another terminal:
```bash
node producer.js
```

The producer accepts input in the format: `<riderName> <location>`
- Location should be either "north" or "south"
- Messages with "north" go to partition 0
- Messages with "south" go to partition 1

Example inputs:
```
> sahil north
> lisa south
> andrew north
```

## File Structure

- `client.js` - Kafka client configuration
- `admin.js` - Creates topics and manages Kafka admin operations
- `producer.js` - Interactive producer that sends messages based on user input
- `consumer.js` - Consumer that reads messages from the topic
- `package.json` - Project dependencies (KafkaJS)

## Key Features

- **Partitioned Messages**: Messages are routed to different partitions based on location (north/south)
- **Consumer Groups**: Multiple consumers can be run with different group IDs
- **Interactive Producer**: Real-time message sending via command line input
- **JSON Message Format**: Messages are stored as JSON with rider name and location

## Troubleshooting

1. **Connection Issues**: Ensure Kafka and Zookeeper are running before starting the application
2. **Port Conflicts**: Default Kafka port is 9092, Zookeeper is 2181
3. **Topic Not Found**: Run `admin.js` first to create the required topic
4. **Network Issues**: Update the broker address in `client.js` to match your Kafka server