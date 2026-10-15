# Kafka Demo

A small, hands-on Kafka learning project (Node.js + [KafkaJS](https://kafka.js.org/) + a single-broker KRaft Kafka in Docker) that shows how **topics, partitions, consumers and consumer groups** behave.

A producer publishes ride-status events (`RIDE_ACCEPTED`, `RIDER_ARRIVED`, `RIDE_STARTED`, `RIDE_COMPLETED`, `RIDE_CANCELLED`) keyed by rider ID. Consumers read them in different groups so you can watch work being shared, or duplicated, in real time.

---

## Contents

1. [Project layout](#project-layout)
2. [Core concepts](#core-concepts)
3. [How this project uses them](#how-this-project-uses-them)
4. [Consumer group behaviour](#consumer-group-behaviour)
5. [Setup and run](#setup-and-run)
6. [Experiments to try](#experiments-to-try)
7. [Inspecting groups and offsets](#inspecting-groups-and-offsets)
8. [Configuration](#configuration)
9. [Limitations](#limitations)

---

## Project layout

```
.
├── docker-compose.yaml     # single-node Kafka 4.3.1 (KRaft, broker + controller)
└── Kafka/
    ├── client.js           # shared KafkaJS client (brokers, clientId)
    ├── topic-config.js     # TOPIC and PARTITION_COUNT from env
    ├── admin.js            # creates the topic / grows partitions  (npm run setup)
    ├── producer.js         # interactive producer                  (npm run producer)
    ├── consumer.js         # consumer, group ID from CLI           (npm run consumer)
    └── .env.example
```

---

## Core concepts

### Topic
A named, append-only log of events. Here: `rider-updates`.

### Partition
A topic is split into **partitions**. Each partition is an independent, ordered, append-only log.

- Every record in a partition gets a sequential **offset** (0, 1, 2, ...).
- **Ordering is guaranteed only within a single partition**, never across partitions.
- Partitions are the unit of **parallelism**: they decide how many consumers can work on a topic at the same time.

```
Topic: rider-updates

Partition 0:  [0][1][2][3][4] -> newest
Partition 1:  [0][1][2][3]    -> newest
```

### Key
Each message can carry a **key**. Records with the same key should always land in the same partition, which is how you keep all events for one entity (one rider) in order.

### Consumer
A process that reads records from partitions and tracks how far it has read.

### Consumer group
A set of consumers that share a `groupId` and cooperate to read a topic **as one logical subscriber**.

- Within a group, **each partition is assigned to exactly one consumer** at a time. This is what splits the work.
- **Different groups are independent.** Each group gets its own full copy of the stream and keeps its own offsets.

### Offset and committed offset
Kafka stores, per (group, topic, partition), the **committed offset**: the position where that group will resume. It lives in Kafka's internal `__consumer_offsets` topic. Offsets are per group, so one group reading a record does not remove it for any other group.

---

## How this project uses them

### Partitioning: same rider, same partition
`producer.js` chooses the partition itself with a simple string hash of the rider ID:

```js
function selectPartition(riderId, partitionCount) {
  let hash = 0;
  for (const char of String(riderId)) {
    hash = (Math.imul(hash, 31) + char.charCodeAt(0)) >>> 0;
  }
  return hash % partitionCount;
}
```

and sends the message with both `key: String(riderId)` and that explicit `partition`.

With the default **2 partitions**:

| Rider ID | Partition |
|---------:|:---------:|
| 101      | 0         |
| 102      | 1         |
| 103      | 0         |
| 104      | 1         |
| 105      | 0         |
| 201      | 1         |
| 202      | 0         |

Consequences:

- All events for rider `101` always go to partition 0, so they are consumed **in the order they were produced**. `RIDE_ACCEPTED` is never processed after `RIDE_COMPLETED` for the same rider.
- Events for different riders can be processed in parallel by different consumers.
- The mapping depends on the partition count. If you change `PARTITION_COUNT`, a rider's partition can change (see [Limitations](#limitations)).

> The producer reads the **actual** partition count from the broker at startup (it does not trust the env value), so the hash always matches the real topic.

### Topic setup
`npm run setup` (`admin.js`) creates `rider-updates` with `PARTITION_COUNT` partitions and replication factor 1. If the topic already exists with fewer partitions, it increases them. It never decreases partitions, because Kafka does not support that.

### Consumers
`consumer.js` takes the group ID from the command line:

```bash
npm run consumer -- <group-id> [topic]
```

Each message is logged with its group, partition and offset, so you can see exactly who got what.

---

## Consumer group behaviour

The demo runs three consumers in two groups against a topic with two partitions:

```
                      Topic: rider-updates
                  +------------------------------+
Producer -------> |  Partition 0   Partition 1   |
                  +------+--------------+--------+
                         |              |
        group: payment-group            |
          Consumer 1 <---+              |
          Consumer 2 <------------------+

        group: analytics-group
          Consumer 3 <--- reads BOTH partitions
```

### 1. Within a group: work is shared
In `payment-group`, the two partitions are split between the two consumers: one owns partition 0, the other owns partition 1. A given event is delivered to **only one** of them. This is horizontal scaling.

### 2. Across groups: everyone gets everything
`analytics-group` is independent. It receives **every** event, regardless of what `payment-group` does, and tracks its own offsets. This is the publish/subscribe behaviour: payments, analytics, notifications and so on can each consume the full stream without affecting each other.

### 3. Assignment rules at a glance

| Consumers in a group | Partitions | Result |
|:--:|:--:|---|
| 1 | 2 | One consumer reads both partitions |
| 2 | 2 | One partition each (ideal split) |
| 3 | 2 | Two consumers get one each, **the third is idle** |

**The partition count is the ceiling on useful parallelism for a group.** Extra consumers beyond that number sit idle as hot standbys.

### 4. Rebalancing
Whenever group membership changes, the group **rebalances** and partitions are reassigned:

- A consumer **joins**: partitions are redistributed to include it.
- A consumer **leaves or crashes** (Ctrl+C, process dies, or it stops heartbeating): its partitions move to the remaining consumers, which resume from the last committed offset.
- The topic's partition count changes.

During a rebalance, consumption in that group pauses briefly. You will see this when you start or stop a consumer terminal.

`consumer.js` handles `SIGINT`/`SIGTERM` by calling `consumer.disconnect()`, so the group leaves cleanly and rebalances right away instead of waiting for a session timeout.

### 5. Offsets and where a consumer starts
KafkaJS commits offsets automatically as messages are processed. Where a consumer starts depends on whether its group already has a committed offset:

| Situation | Starting position |
|---|---|
| New group, no committed offset, `fromBeginning: true` (this project) | Earliest retained record |
| Existing group with a committed offset | Resumes from the saved position (`fromBeginning` is ignored) |
| Consumer restarts in the same group | Continues where the group left off |

So a brand-new group name replays history, while restarting `payment-group` does not. To replay for an existing group, use a new group ID or reset its offsets (see below).

### 6. Ordering guarantee, precisely
- Per partition: strict order.
- Per rider: strict order, because one rider maps to one partition and one partition is read by one consumer in a group.
- Across riders or across partitions: no ordering guarantee. Consumer 1's output can interleave arbitrarily with Consumer 2's.

---

## Setup and run

**Prerequisites:** Docker with Compose, Node.js 18+.

```bash
cd Kafka
npm install
cp .env.example .env      # optional; defaults are the same
cd ..
```

### 1. Start Kafka

```bash
docker compose up -d --wait
docker compose ps
docker compose logs kafka
```

### 2. Create the topic and partitions

```bash
cd Kafka
npm run setup
```

### 3. Start the consumers and producer
Use four terminals in the `Kafka/` directory.

| Terminal | Role | Command |
|:--:|---|---|
| 1 | Consumer 1, group `payment-group` | `npm run consumer -- payment-group` |
| 2 | Consumer 2, same group | `npm run consumer -- payment-group` |
| 3 | Independent group | `npm run consumer -- analytics-group` |
| 4 | Interactive producer | `npm run producer` |

In the producer terminal, enter a numeric rider ID such as `101`, pick a status with the arrow keys and press Enter. Type `/exit` at the rider ID prompt to quit.

Expected result: **both groups receive the event**, while the two consumers in `payment-group` share the work across the topic's partitions.

### 4. Shut down

```bash
docker compose down
```

---

## Experiments to try

**A. See the key-to-partition mapping.** Publish riders `101`, `103`, `105`, then `102`, `104`. The first three always show `partition=0` and the others `partition=1`, in both groups.

**B. Watch work sharing.** Publish `101` and `102`. In `payment-group`, one terminal prints rider 101 and the other prints rider 102. `analytics-group` prints both.

**C. Per-rider ordering.** Publish several statuses for the same rider, one after another. They always appear on the same consumer, with increasing offsets, in the order sent.

**D. Failover.** Stop Consumer 2 with Ctrl+C. After a brief rebalance, Consumer 1 owns both partitions and receives rider 102 as well. Restart Consumer 2 and the partitions are split again.

**E. Idle consumer.** Start a third `payment-group` consumer. With 2 partitions it receives nothing. Stop one of the other two and it takes over.

**F. Replay with a new group.** Start `npm run consumer -- audit-group`. Because it is new and `fromBeginning: true`, it prints every retained event so far.

**G. Scale out partitions.** Set `PARTITION_COUNT=3` in `.env` and run `npm run setup`. A third consumer in `payment-group` can now be useful (after a rebalance). Note that rider-to-partition mapping changes, see below.

---

## Inspecting groups and offsets

Run these inside the broker container:

```bash
# list groups
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# members, assigned partitions, current offset and lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group payment-group

# describe the topic's partitions
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic rider-updates

# reset a group to the beginning (stop its consumers first)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group payment-group \
  --topic rider-updates --reset-offsets --to-earliest --execute
```

`--describe` is the best way to see the behaviour: it shows which consumer owns which partition, plus `LAG` (records not yet consumed).

---

## Configuration

Set in `Kafka/.env` (see `.env.example`):

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_CLIENT_ID` | `rider-tracking-app` | Client identifier |
| `KAFKA_TOPIC` | `rider-updates` | Topic name |
| `PARTITION_COUNT` | `2` | Partitions created by `npm run setup` |

The broker advertises `localhost:9092` to host applications (`EXTERNAL` listener) and `kafka:19092` to other containers (`INTERNAL`). Topic auto-creation is disabled, so you must run `npm run setup` first.

---

## Limitations

- **Single broker, replication factor 1.** This is a learning setup. It is not highly available, and losing the broker or its volume loses the data.
- **Changing the partition count changes the mapping.** Because `hash % partitionCount` depends on the count, a rider's events may move to a different partition after you add partitions. Ordering is then only guaranteed within each period of constant partition count. Kafka can only increase partitions, never reduce them.
- **Custom partitioner.** The producer picks the partition explicitly with its own hash, which overrides KafkaJS's default key-based partitioner. The key is still sent with the record so consumers can see it.
- **`fromBeginning: true`** only applies when a group has no committed offset. An existing group normally resumes from its saved position.
- **Data location.** Logs are stored in `/tmp/kraft-combined-logs` inside the container, so `docker compose down` followed by a fresh `up` may start with an empty topic.
- **At-least-once delivery.** With automatic commits, a crash between processing and commit can cause a record to be redelivered after a rebalance, so real consumers should be idempotent.
