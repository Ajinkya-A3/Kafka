const inquirer = require("inquirer");

const { kafka } = require("./client");
const { TOPIC, PARTITION_COUNT } = require("./topic-config");

const UPDATE_OPTIONS = [
  "RIDE_ACCEPTED",
  "RIDER_ARRIVED",
  "RIDE_STARTED",
  "RIDE_COMPLETED",
  "RIDE_CANCELLED",
];

function selectPartition(riderId, partitionCount) {
  let hash = 0;

  for (const char of String(riderId)) {
    hash =
      (Math.imul(hash, 31) + char.charCodeAt(0)) >>> 0;
  }

  return hash % partitionCount;
}

async function getActualPartitionCount() {
  const admin = kafka.admin();
  let connected = false;

  try {
    await admin.connect();
    connected = true;

    const metadata = await admin.fetchTopicMetadata({
      topics: [TOPIC],
    });

    const topicMetadata = metadata.topics.find(
      (topic) => topic.name === TOPIC
    );

    if (!topicMetadata || topicMetadata.partitions.length === 0) {
      throw new Error(
        `Topic "${TOPIC}" does not exist. Run "npm run setup" first.`
      );
    }

    return topicMetadata.partitions.length;
  } finally {
    if (connected) {
      await admin.disconnect();
    }
  }
}

async function main() {
  const producer = kafka.producer({
    allowAutoTopicCreation: false,
  });

  let connected = false;

  try {
    // Discover the actual partition count from Kafka.
    const partitionCount = await getActualPartitionCount();

    console.log(`Topic: ${TOPIC}`);
    console.log(`Actual partition count: ${partitionCount}`);
    console.log(`Configured partition count: ${PARTITION_COUNT}`);

    await producer.connect();
    connected = true;

    console.log("\nProducer ready. Type /exit at the Rider ID prompt to quit.");

    while (true) {
      const { riderIdInput } = await inquirer.prompt([
        {
          type: "input",
          name: "riderIdInput",
          message: "Enter numeric Rider ID:",
          validate: (value) => {
            const input = value.trim();

            if (input.toLowerCase() === "/exit") {
              return true;
            }

            if (!/^[1-9]\d*$/.test(input)) {
              return "Enter a positive integer Rider ID.";
            }

            if (!Number.isSafeInteger(Number(input))) {
              return "Rider ID is too large.";
            }

            return true;
          },
        },
      ]);

      if (riderIdInput.trim().toLowerCase() === "/exit") {
        break;
      }

      const riderId = Number(riderIdInput.trim());

      const { status } = await inquirer.prompt([
        {
          type: "list",
          name: "status",
          message: `Select update for Rider ${riderId}:`,
          choices: UPDATE_OPTIONS,
          loop: false,
        },
      ]);

      const event = {
        riderId,
        status,
        updatedAt: new Date().toISOString(),
      };

      // Same rider ID and partition count produce the same partition.
      const partition = selectPartition(riderId, partitionCount);

      const result = await producer.send({
        topic: TOPIC,
        messages: [
          {
            key: String(riderId),
            partition,
            value: JSON.stringify(event),
          },
        ],
      });

      console.log("\nEvent published successfully:");
      console.log(
        JSON.stringify(
          {
            topic: TOPIC,
            partition: result[0].partition,
            offset: result[0].baseOffset,
            event,
          },
          null,
          2
        )
      );
    }
  } finally {
    if (connected) {
      await producer.disconnect();
    }

    console.log("\nProducer disconnected.");
  }
}

main().catch((error) => {
  console.error("Producer error:", error);
  process.exitCode = 1;
});