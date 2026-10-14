const { kafka } = require("./client");
const { TOPIC, PARTITION_COUNT } = require("./topic-config");

async function init() {
  const admin = kafka.admin();
  let connected = false;

  try {
    console.log("Admin connecting...");
    await admin.connect();
    connected = true;

    const existingTopics = await admin.listTopics();

    if (!existingTopics.includes(TOPIC)) {
      console.log(`Creating topic: ${TOPIC}`);

      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: TOPIC,
            numPartitions: PARTITION_COUNT,
            replicationFactor: 1,
          },
        ],
      });

      console.log(
        `Topic created: ${TOPIC}, partitions: ${PARTITION_COUNT}`
      );
      return;
    }

    const metadata = await admin.fetchTopicMetadata({
      topics: [TOPIC],
    });

    const topicMetadata = metadata.topics.find(
      (topic) => topic.name === TOPIC
    );

    if (!topicMetadata) {
      throw new Error(`Unable to retrieve metadata for ${TOPIC}`);
    }

    const currentCount = topicMetadata.partitions.length;

    if (currentCount < PARTITION_COUNT) {
      await admin.createPartitions({
        topicPartitions: [
          {
            topic: TOPIC,
            count: PARTITION_COUNT,
          },
        ],
      });

      console.log(
        `Increased ${TOPIC} partitions from ${currentCount} to ${PARTITION_COUNT}`
      );
    } else {
      console.log(
        `Topic ${TOPIC} already exists with ${currentCount} partitions`
      );
    }
  } finally {
    if (connected) {
      console.log("Disconnecting admin...");
      await admin.disconnect();
    }
  }
}

init().catch((error) => {
  console.error("Admin error:", error);
  process.exitCode = 1;
});