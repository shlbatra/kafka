# Kafka Basic Next.js Example

This project demonstrates basic Kafka producer and consumer functionality using Node.js and KafkaJS.

## Prerequisites

1. **Apache Kafka Setup**
   - Download and install Apache Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
   - Or use Docker: `docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 spotify/kafka`

2. **Node.js**
   - Node.js version 12 or higher

## Local Kafka Setup Steps

### 1. Start Zookeeper
```bash
# Navigate to your Kafka directory
cd /path/to/kafka

# Start Zookeeper (required for Kafka)
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 2. Start Kafka Server
```bash
# In a new terminal, start Kafka server
bin/kafka-server-start.sh config/server.properties
```

### 3. Update Broker Configuration
Update the broker address in `client.js` to match your local setup:
```javascript
brokers: ["localhost:9092"]  // Change from current IP to localhost
```

## Project Setup

### 1. Install Dependencies
```bash
npm install
```

### 2. Create Kafka Topic
```bash
node admin.js
```
This creates the `rider-updates1` topic with 2 partitions.

## Usage

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
> Alice north
> Bob south
> Charlie north
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