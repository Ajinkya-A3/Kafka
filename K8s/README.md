# Kafka on Minikube with Strimzi (Node.js demo)

Runs a single-node Kafka (KRaft) on Minikube using the Strimzi operator, with ephemeral storage, and connects the Node.js (KafkaJS) rider-updates demo to it. Part 1 is the no-auth setup. Part 2 adds a SCRAM user with ACLs and the code changes it needs.

> **Lab setup only.** One broker, replication factor 1, ephemeral storage (deleting the broker pod wipes all data), and no TLS.

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Start Minikube](#2-start-minikube)
3. [Install the Strimzi operator](#3-install-the-strimzi-operator)
4. [Deploy the Kafka cluster](#4-deploy-the-kafka-cluster)
5. [Create the topic](#5-create-the-topic)
6. [Verify](#6-verify)
7. [Connect the Node.js scripts (no auth)](#7-connect-the-nodejs-scripts-no-auth)
8. [Change partitions](#8-change-partitions)
9. [Add a user with credentials (SCRAM)](#9-add-a-user-with-credentials-scram)
10. [Code changes for the user](#10-code-changes-for-the-user)
11. [Troubleshooting](#11-troubleshooting)
12. [Clean up](#12-clean-up)

---

## 1. Prerequisites

- Linux host with Docker, `minikube`, `kubectl`, Node.js 18+ and `nc` (netcat)
- Run the Node.js scripts **on the same machine as Minikube**. With the Docker driver the Minikube IP (for example `192.168.49.2`) is a private bridge address that other machines cannot reach.

---

## 2. Start Minikube

```bash
minikube start --driver=docker --memory=4096 --cpus=2
minikube ip          # note this, for example 192.168.49.2
```

Everything below uses `192.168.49.2`. If your IP differs, replace it in section 4 and in `.env`.

---

## 3. Install the Strimzi operator

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available -n kafka --timeout=120s
```

Check which API version this release serves, then use that version in every manifest below (this README uses `v1`):

```bash
kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}{"\n"}'
```

If the output only shows `v1beta2`, replace `kafka.strimzi.io/v1` with `kafka.strimzi.io/v1beta2` in the manifests.

---

## 4. Deploy the Kafka cluster

Two resources:

- **`KafkaNodePool`** describes the nodes: how many, their roles and their storage. Here: one node that is both controller and broker, with `ephemeral` storage (an `emptyDir`, so no PVC and no StorageClass).
- **`Kafka`** describes the cluster: listeners, broker config, and the Topic Operator. It links to the pool through the label `strimzi.io/cluster: my-cluster`.

Listeners:

| Name | Port | Use |
|---|---|---|
| `plain` | 9092 | Pods inside the cluster (`my-cluster-kafka-bootstrap:9092`) |
| `external` | 9094 (NodePort 32100 bootstrap, 32000 broker) | Node.js scripts on the host, through the Minikube IP |

The broker advertises `192.168.49.2:32000`. Clients reach the bootstrap address first, then are told to connect to the advertised address, so that address must be reachable from where the scripts run.

```bash
kubectl apply -n kafka -f - <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: my-cluster
spec:
  replicas: 1
  roles: [controller, broker]
  storage:
    type: ephemeral
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: external
        port: 9094
        type: nodeport
        tls: false
        configuration:
          bootstrap:
            nodePort: 32100
          brokers:
            - broker: 0
              nodePort: 32000
              advertisedHost: 192.168.49.2
              advertisedPort: 32000
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      auto.create.topics.enable: false
  entityOperator:
    topicOperator: {}
EOF

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

Check that the ports are reachable from the host:

```bash
kubectl get pods -n kafka
nc -zv 192.168.49.2 32100
nc -zv 192.168.49.2 32000
```

---

## 5. Create the topic

The operator owns the topic, so **do not run `npm run setup`** against this cluster. Topic auto-creation is disabled.

```bash
kubectl apply -n kafka -f - <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: rider-updates
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 2
  replicas: 1
EOF
```

---

## 6. Verify

```bash
# Kubernetes view: READY should be True
kubectl get kafkatopic -n kafka

# Kafka's view
kubectl exec -n kafka my-cluster-dual-role-0 -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

kubectl exec -n kafka my-cluster-dual-role-0 -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic rider-updates
```

Expect `PartitionCount: 2` and `ReplicationFactor: 1`.

Inspect consumer groups (once consumers are running):

```bash
kubectl exec -n kafka my-cluster-dual-role-0 -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group payment-group
```

---

## 7. Connect the Node.js scripts (no auth)

```bash
cd Kafka
npm install
cat > .env <<'EOF'
KAFKA_BROKERS=192.168.49.2:32100
KAFKA_TOPIC=rider-updates
PARTITION_COUNT=2
EOF
```

Run in four terminals (from `Kafka/`):

```bash
npm run consumer -- payment-group
npm run consumer -- payment-group
npm run consumer -- analytics-group
npm run producer
```

For an app running **inside** the cluster, use `KAFKA_BROKERS=my-cluster-kafka-bootstrap:9092` (or `my-cluster-kafka-bootstrap.kafka.svc:9092` from another namespace).

---

## 8. Change partitions

Edit the `KafkaTopic` (or patch it). If you manage the manifests in Git, change `spec.partitions` and apply.

```bash
kubectl patch kafkatopic rider-updates -n kafka --type merge -p '{"spec":{"partitions":4}}'
kubectl get kafkatopic rider-updates -n kafka -w
```

- **Increase:** live, no downtime. Existing records stay where they are, and new partitions start empty.
- **Decrease:** not supported by Kafka. The operator marks the topic as not ready with an error and the topic keeps running unchanged. To shrink, delete and recreate the topic (data is lost).
- **Restart consumers** after an increase so the group picks up the new partitions and rebalances.
- **Restart the producer.** It reads the partition count at startup, so until then it keeps hashing with the old count.
- **Ordering:** `hash % partitionCount` changes with the count, so a rider's events can move to a different partition after the change. Per-rider ordering holds only within each period of constant partition count.
- Do not leave a different `PARTITION_COUNT` in `.env` and run `npm run setup`. The operator is the source of truth for the topic.

---

## 9. Add a user with credentials (SCRAM)

A `KafkaUser` is a client identity. The User Operator creates a Kubernetes Secret with its generated password, and applies its ACLs.

This turns on SCRAM-SHA-512 authentication and ACL authorization, so it **rolls the broker pod**. Ephemeral storage means topic data is lost on the restart, and the Topic Operator recreates the empty topic.

### 9.1 Update the `Kafka` resource

Both listeners get authentication, and `authorization: simple` enables ACLs. Authorization applies to the whole cluster, so an unauthenticated listener would be treated as an anonymous user and denied everything. That is why `plain` is also authenticated. The User Operator is enabled as well.

```bash
kubectl apply -n kafka -f - <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    authorization:
      type: simple
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
        authentication:
          type: scram-sha-512
      - name: external
        port: 9094
        type: nodeport
        tls: false
        authentication:
          type: scram-sha-512
        configuration:
          bootstrap:
            nodePort: 32100
          brokers:
            - broker: 0
              nodePort: 32000
              advertisedHost: 192.168.49.2
              advertisedPort: 32000
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
      auto.create.topics.enable: false
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF

kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

### 9.2 Create the user

ACLs follow the least-privilege needs of the demo:

| Role | Needs |
|---|---|
| Producer | `Write` and `Describe` on the topic (it reads topic metadata for the partition count) |
| Consumer | `Read` and `Describe` on the topic, and `Read` on its consumer groups |

```bash
kubectl apply -n kafka -f - <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: demo-user
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: rider-updates
          patternType: literal
        operations: [Read, Write, Describe]
      - resource:
          type: group
          name: payment-group
          patternType: literal
        operations: [Read]
      - resource:
          type: group
          name: analytics-group
          patternType: literal
        operations: [Read]
      - resource:
          type: group
          name: audit-group
          patternType: literal
        operations: [Read]
EOF

kubectl wait kafkauser/demo-user --for=condition=Ready -n kafka --timeout=120s
```

Add more `group` ACL entries for any other group names you use, or use `name: "*"` to allow any group.

### 9.3 Get the credentials

The Secret is named after the user and holds the generated password:

```bash
kubectl get secret demo-user -n kafka -o jsonpath='{.data.password}' | base64 -d; echo
```

The username is `demo-user`, and the mechanism is `scram-sha-512`.

For a real setup, do not store this password in Git. Let the operator generate it, or supply your own through `spec.authentication.password.valueFrom.secretKeyRef` backed by your secret manager.

### 9.4 Update `.env`

```bash
cd Kafka
cat > .env <<EOF
KAFKA_BROKERS=192.168.49.2:32100
KAFKA_TOPIC=rider-updates
PARTITION_COUNT=2
KAFKA_USERNAME=demo-user
KAFKA_PASSWORD=$(kubectl get secret demo-user -n kafka -o jsonpath='{.data.password}' | base64 -d)
EOF
```

Add `.env` to `.gitignore` so the password is not committed.

---

## 10. Code changes for the user

Only the shared client needs to change, since producer and consumer both import it. Update `Kafka/client.js` so SASL is used when credentials are set, and plain connections still work without them:

```js
const { Kafka } = require('kafkajs');
require('dotenv').config(); // keep this if your existing client.js already loads .env

const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

const sasl = process.env.KAFKA_USERNAME
  ? {
      mechanism: 'scram-sha-512',
      username: process.env.KAFKA_USERNAME,
      password: process.env.KAFKA_PASSWORD,
    }
  : undefined;

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'rider-tracking-app',
  brokers,
  sasl,
  // ssl: true,  // only if the listener is configured with tls: true
});

module.exports = kafka;
```

Notes:

- Keep your existing export style. If `client.js` currently exports something other than `kafka`, or uses ES modules, apply the same `sasl` addition to your existing `new Kafka({...})` call.
- **`admin.js` / `npm run setup`** now fails with an authorization error, because `demo-user` has no permission to create topics or alter partitions. This is intended, since the `KafkaTopic` resource owns the topic.
- The producer's startup lookup of the real partition count only needs `Describe` on the topic, which the ACL grants.
- SCRAM over a `tls: false` listener sends credentials without encryption. That is acceptable for a local lab. For anything real, set `tls: true` on the listener, read the cluster CA from the `my-cluster-cluster-ca-cert` Secret and pass it to KafkaJS as `ssl: { ca: [...] }`.
- For an app running in the cluster, inject the credentials from the Secret instead of copying them:

```yaml
env:
  - name: KAFKA_BROKERS
    value: my-cluster-kafka-bootstrap:9092
  - name: KAFKA_USERNAME
    value: demo-user
  - name: KAFKA_PASSWORD
    valueFrom:
      secretKeyRef:
        name: demo-user
        key: password
```

Then run the consumers and producer as in section 7.

---

## 11. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `no matches for kind "Kafka" in version ".../v1beta2"` | The installed Strimzi serves a different API version. Check with the CRD command in section 3 and use that version. |
| Connects, then hangs or times out | The advertised address is not reachable. Check `kubectl get kafka my-cluster -n kafka -o jsonpath='{.status.listeners}'` and that `advertisedHost` matches `minikube ip`. |
| `nc` to the Minikube IP fails | Scripts are running on a machine that cannot reach the Minikube bridge. Run them on the Minikube host, or use an SSH tunnel and set `advertisedHost: localhost`. |
| `Authentication failed` | Wrong username or password, or the password in `.env` is stale after the user was recreated. Re-read the Secret. |
| `Not authorized to access topics` / `group authorization failed` | A missing ACL. Add the topic or group to the `KafkaUser` and reapply. |
| Topic missing after broker restart | Ephemeral storage. The operator recreates it empty from the `KafkaTopic`. |
| Minikube IP changed | Update `advertisedHost` in the `Kafka` resource and `KAFKA_BROKERS` in `.env`, then reapply. |
| Topic not ready after a partition decrease | Expected. Kafka cannot reduce partitions. Restore the previous number. |

Useful logs:

```bash
kubectl logs deploy/strimzi-cluster-operator -n kafka --tail=100
kubectl logs my-cluster-dual-role-0 -n kafka --tail=100
kubectl describe kafkatopic rider-updates -n kafka
kubectl describe kafkauser demo-user -n kafka
```

---

## 12. Clean up

```bash
kubectl delete kafkauser demo-user -n kafka
kubectl delete kafkatopic rider-updates -n kafka
kubectl delete kafka my-cluster -n kafka
kubectl delete kafkanodepool dual-role -n kafka
kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl delete namespace kafka
minikube stop        # or: minikube delete
```
