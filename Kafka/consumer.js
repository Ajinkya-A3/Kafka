const { kafka } = require("./client");
const { TOPIC } = require("./topic-config");

const groupId = process.argv[2];
const topic = process.argv[3] || TOPIC;

if (!groupId) {
  console.error("Usage: node consumer.js <group-id> [topic]");
  process.exit(1);
}

async function main() {
  const consumer = kafka.consumer({ groupId });
  let shuttingDown = false;

  process.once("SIGINT", async () => {
    if (shuttingDown) return;
    shuttingDown = true;

    console.log("\nShutting down consumer...");
    await consumer.disconnect();
    process.exit(0);
  });

  process.once("SIGTERM", async () => {
    if (shuttingDown) return;
    shuttingDown = true;

    await consumer.disconnect();
    process.exit(0);
  });

  await consumer.connect();

  await consumer.subscribe({
    topic,
    fromBeginning: true,
  });

  console.log(`Group "${groupId}" consuming topic "${topic}"`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `[group=${groupId}] ` +
        `topic=${topic} ` +
        `partition=${partition} ` +
        `offset=${message.offset} ` +
        `key=${message.key?.toString() ?? "null"} ` +
        `value=${message.value?.toString() ?? ""}`
      );
    },
  });
}

main().catch((error) => {
  console.error("Consumer error:", error);
  process.exitCode = 1;
});