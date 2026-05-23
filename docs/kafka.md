# Kafka — Distributed Event Streaming

## What is Kafka

Apache Kafka is a distributed event streaming platform. In the Nexus project it acts as the durable buffer between the mock producer and the PySpark streaming job. The producer writes events to Kafka at whatever rate it can; PySpark reads them in micro-batches at its own pace. Neither service needs to know the other exists.

Kafka was chosen over alternatives (RabbitMQ, AWS SQS, Redis Streams) because it is the industry standard for high-throughput event pipelines, it integrates natively with PySpark Structured Streaming, and it pairs naturally with the Confluent Schema Registry that enforces Avro schema compatibility.

---

## Core concepts

### Topics

A topic is a named, ordered, durable log of messages. In Nexus there is one topic: `github-events`. Every event produced by the mock producer is appended to this topic. Topics are append-only — messages are never updated or deleted (until the retention period expires).

```
github-events topic:
  offset 0:  {event_id: "abc", event_type: "PushEvent", ...}
  offset 1:  {event_id: "def", event_type: "WatchEvent", ...}
  offset 2:  {event_id: "ghi", event_type: "ForkEvent", ...}
  ...
  offset N:  {latest event}
```

### Partitions

Topics are split into partitions for parallelism. Nexus uses 1 partition (the default for a learning project). In production you would use more partitions to allow multiple Spark tasks to read in parallel.

Messages are assigned to partitions by key. The producer keys each message by `repo_name`, so all events for the same repository always go to the same partition — preserving ordering within a repo.

### Offsets

Every message in a partition has a unique, monotonically increasing offset. Consumers track which offset they have processed. PySpark records the last processed offset in the checkpoint directory after each successful micro-batch. On restart, Spark resumes from that offset — this is the foundation of exactly-once semantics.

### Producers and consumers

A producer appends messages to a topic. A consumer reads messages from a topic at its own pace. Kafka decouples them completely — the producer does not wait for the consumer, and the consumer can replay old messages by resetting its offset.

### Retention

Messages stay in Kafka for the configured retention period (default 7 days), regardless of whether they have been consumed. This means:

- If PySpark is down for 6 days, it can still catch up and process all missed events
- You can add a new consumer (e.g. a second analytics pipeline) that reads from offset 0 and processes the full history without affecting the existing PySpark consumer

---

## KRaft mode — no ZooKeeper

Nexus uses `apache/kafka:4.0.0` which runs in KRaft mode (Kafka Raft) — ZooKeeper is not required. KRaft embeds the cluster metadata management directly into Kafka brokers, simplifying the deployment from two services to one.

```yaml
kafka:
  environment:
    KAFKA_PROCESS_ROLES: broker,controller   # single node acts as both
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

In KRaft mode the same Kafka process handles both message storage (broker role) and cluster coordination (controller role). For a single-node learning setup this is ideal.

---

## Listener configuration

Nexus exposes two Kafka listeners:

```yaml
KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:19092
```

| Listener | Port | Used by |
|---|---|---|
| `INTERNAL` | 9092 | Other Docker containers (PySpark, Schema Registry) |
| `EXTERNAL` | 19092 | Your Mac host machine (Python producer) |
| `CONTROLLER` | 9093 | Internal KRaft coordination |

This is why the producer uses `localhost:19092` when running on your Mac, but PySpark uses `kafka:9092` when running inside Docker — they are connecting to the same broker via different listeners.

---

## Schema Registry integration

The Confluent Schema Registry stores Avro schemas separately from Kafka. When the producer sends a message:

1. Schema is registered (or fetched if already registered) from the registry
2. The schema ID (an integer) is embedded in the message header
3. The Avro payload is written using that schema
4. Message lands in Kafka

When PySpark consumes the message:

1. The 5-byte header (magic byte + schema ID) is stripped
2. The Avro schema string is fetched from the registry at job startup
3. `from_avro()` deserialises the payload using that schema

The schema never travels with the message — only the schema ID does. This keeps messages compact and ensures schema changes are centralised.

---

## Kafka in the Nexus pipeline

```
mock_producer.py
  └── KafkaProducer(bootstrap_servers="localhost:19092")
      └── serialise to Avro (Confluent wire format)
          └── send to topic: github-events (keyed by repo_name)

PySpark streaming_job.py
  └── spark.readStream.format("kafka")
      └── .option("kafka.bootstrap.servers", "kafka:9092")
          └── .option("subscribe", "github-events")
              └── reads new messages every 30 seconds
                  └── deserialises Avro → writes to Iceberg
```

---

## Verifying Kafka is working

```bash
# List topics
docker exec -it iceberg-lakehouse-ecosystem-kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Check message count in topic
docker exec -it iceberg-lakehouse-ecosystem-kafka-1 \
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic github-events
# → github-events:0:1532  (partition 0 has 1532 messages)

# Consumer group lag (how far behind is PySpark?)
docker exec -it iceberg-lakehouse-ecosystem-kafka-1 \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group spark-streaming
```

Or use the Kafka UI at `http://localhost:8090` for a visual view of topics, messages, consumer groups, and lag.

---

## Key concepts learned

**Decoupling** — Kafka separates the rate of production from the rate of consumption. The producer can send 1000 messages/second while PySpark processes them in 30-second batches. Neither blocks the other.

**Durability** — messages are written to disk and replicated (in a multi-broker setup). A broker restart does not lose messages.

**Consumer offset tracking** — consumers control their own position in the log. Multiple independent consumers can read the same topic at different speeds without interfering.

**Partitioning for ordering** — messages with the same key always go to the same partition, preserving ordering within a key (repo in this project).

**Retention vs deletion** — Kafka is not a queue where messages disappear after consumption. They stay for the retention period. This makes Kafka suitable as a short-term event store and enables replay.

**KRaft vs ZooKeeper** — modern Kafka (2.8+) no longer requires ZooKeeper. KRaft mode embeds cluster coordination into the brokers themselves, simplifying operations significantly.