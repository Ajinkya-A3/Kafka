require("dotenv").config();

const TOPIC = (process.env.KAFKA_TOPIC || "rider-updates").trim();
const PARTITION_COUNT = Number.parseInt(
  process.env.PARTITION_COUNT || "2",
  10
);

if (!TOPIC) {
  throw new Error("KAFKA_TOPIC cannot be empty.");
}

if (!Number.isSafeInteger(PARTITION_COUNT) || PARTITION_COUNT < 1) {
  throw new Error("PARTITION_COUNT must be a positive integer.");
}

module.exports = {
  TOPIC,
  PARTITION_COUNT,
};