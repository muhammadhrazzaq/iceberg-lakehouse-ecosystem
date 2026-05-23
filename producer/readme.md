# Producer — Mock GitHub Events → Kafka

## What this does

This folder contains the mock event producer that simulates a live GitHub API feed. It generates realistic fake GitHub events (push, pull request, star, fork, issue) and streams them into a Kafka topic in Avro format, with each message validated against a schema registered in the Confluent-compatible Schema Registry.

```
Fake GitHub events (random, every 0.5s)
        ↓
Avro serialisation (schema from registry)
        ↓
Confluent wire format (magic byte + schema_id + avro payload)
        ↓
Kafka topic: github-events
```

---

## Files

```
producer/
  mock_producer.py   ← the producer script
  Dockerfile         ← container definition
  README.md          ← this file
```

---

## Why mock data first

The real GitHub API has rate limits (5,000 requests/hour for authenticated users, 60/hour unauthenticated). For building and testing a streaming pipeline, rate limits get in the way — you want a continuous, predictable stream to validate that every downstream component (PySpark, Iceberg, ClickHouse, Neo4j) is working correctly.

Once the pipeline is proven end-to-end with mock data, swapping in the real GitHub API is just changing the producer — nothing downstream changes because the schema stays the same.

---

## How it works — step by step

### 1. Fetch schema from registry at startup

```python
resp = requests.get(f"{REGISTRY_URL}/subjects/github-events-value/versions/latest")
schema_id   = resp.json()["id"]
avro_schema = fastavro.parse_schema(json.loads(resp.json()["schema"]))
```

The producer fetches the registered Avro schema before sending any messages. This means:

- If the schema doesn't exist yet the producer fails immediately with a clear error — not a silent serialisation failure downstream.
- The `schema_id` (an integer like `1`) is embedded in every message header so consumers know exactly which schema version to use for deserialisation.
- Schema changes are validated by the registry before they reach Kafka — a breaking change is caught at the producer, not discovered hours later when PySpark fails.

### 2. Avro serialisation with Confluent wire format

```python
def serialise(record: dict) -> bytes:
    buf = io.BytesIO()
    buf.write(b'\x00')                       # magic byte — signals Confluent format
    buf.write(schema_id.to_bytes(4, 'big'))  # 4-byte big-endian schema ID
    fastavro.schemaless_writer(buf, avro_schema, record)
    return buf.getvalue()
```

Every message is structured as:

```
┌──────────┬─────────────────┬──────────────────────┐
│  0x00    │   schema_id     │    avro payload       │
│ (1 byte) │   (4 bytes)     │    (variable)         │
└──────────┴─────────────────┴──────────────────────┘
```

This is the **Confluent wire format** — the industry standard for Kafka + Schema Registry. PySpark's `from_avro()` function expects this exact format, which is why the streaming job strips the first 5 bytes before deserialising.

Using binary Avro instead of JSON gives roughly 3-5x compression on the wire, which matters at scale — 1 million JSON events vs 1 million Avro events is the difference between gigabytes and hundreds of megabytes.

### 3. Event generation

```python
REPOS  = ["apache/iceberg","ClickHouse/ClickHouse","apache/kafka","neo4j/neo4j","dbt-labs/dbt-core"]
TYPES  = ["PushEvent","PullRequestEvent","IssuesEvent","WatchEvent","ForkEvent"]

event = {
    "event_id":    str(uuid.uuid4()),   # unique ID for deduplication
    "event_type":  random.choice(TYPES),
    "repo_name":   random.choice(REPOS),
    "actor_login": f"user_{random.randint(1, 500)}",
    "created_at":  datetime.now(timezone.utc).isoformat(),
    "stars":       random.randint(100, 40000),
}
```

Each event gets a UUID `event_id` — this is important for deduplication. If the producer restarts and re-sends messages, or if a Kafka consumer processes a message twice, the `event_id` can be used to deduplicate downstream.

The 500 unique users create a realistic contributor network in Neo4j — enough variety to make graph analytics meaningful without being so large that the graph is slow to load.

### 4. Kafka partitioning by repo

```python
producer.send(TOPIC, key=event["repo_name"], value=serialise(event))
```

Messages are keyed by `repo_name`. Kafka uses the key to determine which partition a message goes to — all events for the same repo always land in the same partition. This preserves **ordering within a repo** — if PushEvent for `apache/iceberg` is sent before a PullRequestEvent for the same repo, they'll be consumed in that order.

This matters for event sourcing patterns where you need to reconstruct state by replaying events in order.

---

## Running locally

```bash
# Prerequisites
# - Kafka running on localhost:19092
# - Schema registry on localhost:8081 with schema registered
#   (run ./register_schema.sh first)

# Install
pip install kafka-python-ng fastavro requests python-dotenv

# Run
python producer/mock_producer.py
```

Output:
```
Producing Avro events — Ctrl+C to stop
  → PushEvent on apache/iceberg
  → WatchEvent on neo4j/neo4j
  → ForkEvent on ClickHouse/ClickHouse
  → PullRequestEvent on apache/kafka
```

---

## Running in Docker

The producer runs as a long-lived Docker service:

```bash
docker compose up -d producer
docker compose logs -f producer
```

It will restart automatically if it crashes (`restart: unless-stopped`).

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:19092` | Kafka bootstrap server |
| `REGISTRY_URL` | `http://localhost:8081` | Schema registry base URL |

When running inside Docker use internal hostnames:
- `KAFKA_BROKER=kafka:9092`
- `REGISTRY_URL=http://schema-registry:8081`

---

## Swapping in real GitHub API

When you're ready to use real data, replace the event generation loop with a GitHub API poller. Everything else — serialisation, schema, Kafka send — stays identical:

```python
import requests as gh_requests

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
session = gh_requests.Session()
session.headers.update({
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
})

def fetch_real_events(repo: str) -> list[dict]:
    resp = session.get(f"https://api.github.com/repos/{repo}/events?per_page=100")
    poll_interval = int(resp.headers.get("X-Poll-Interval", 60))
    events = []
    for raw in resp.json():
        events.append({
            "event_id":    raw["id"],
            "event_type":  raw["type"],
            "repo_name":   raw["repo"]["name"],
            "actor_login": raw["actor"]["login"],
            "created_at":  raw["created_at"],
            "stars":       None,
        })
    return events, poll_interval
```

The schema stays the same so no changes to PySpark, ClickHouse, or Neo4j.

---

## Schema registry integration

The producer enforces **BACKWARD compatibility** — meaning any new schema version must be readable by the existing PySpark consumer. This is enforced automatically:

```bash
# Adding an optional field — ACCEPTED
{"name": "language", "type": ["null", "string"], "default": null}

# Adding a required field with no default — REJECTED by registry
{"name": "language", "type": "string"}
# → {"error_code":409,"message":"Schema being registered is incompatible"}
```

The registry catches breaking changes at the producer before they ever reach Kafka. Without this, a schema change would silently corrupt data in the pipeline and only surface as null values or parse errors in PySpark hours later.

---

## Key concepts learned

**Producer-consumer decoupling** — the producer and PySpark consumer are completely independent processes. The producer doesn't know or care whether PySpark is running. Kafka acts as the durable buffer between them — messages stay in the topic for the configured retention period (default 7 days) regardless of whether anyone is consuming them.

**Idempotent production** — each event has a unique `event_id`. Combined with Kafka's idempotent producer setting, duplicate messages are prevented even if the producer retries a failed send.

**Schema as a contract** — the Avro schema registered in the Schema Registry is the formal contract between producer and consumer. Neither side can change it unilaterally — the registry enforces compatibility rules on every new version.

**Binary serialisation** — Avro is significantly more compact than JSON. A typical GitHub event as JSON is ~400 bytes; the same event as Avro is ~80 bytes. At 2 events/second that's 3GB/month vs 600MB/month — a meaningful difference when paying for Kafka storage and S3 transfer.