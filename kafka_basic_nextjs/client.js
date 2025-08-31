const { Kafka } = require("kafkajs");

exports.kafka = new Kafka({
  clientId: "my-first-kafka-app",
  brokers: ["10.0.0.224:9092"],
});