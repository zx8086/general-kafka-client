const { Kafka } = require("kafkajs");

const { KAFKA_USERNAME: username, KAFKA_PASSWORD: password } = process.env;
const sasl = username && password ? { username, password, mechanism: "plain" } : null;
const ssl = !!sasl;

const kafka = new Kafka({
  clientId: process.env.CLIENT_ID,
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVER],
  ssl,
  sasl,
  connectionTimeout: 10000,
  retry: { retries: 3 },
});

module.exports = kafka;
