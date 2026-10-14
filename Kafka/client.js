require("dotenv").config();

const { Kafka } = require("kafkajs");

const brokers = (process.env.KAFKA_BROKERS || "localhost:9092")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  throw new Error("Configure at least one Kafka broker.");
}

exports.kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "rider-tracking-app",
  brokers,
});