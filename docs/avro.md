# Avro — Binary Serialisation Format

## What is Avro

Avro is a binary data serialisation format developed by the Apache Software Foundation. In the Nexus project it is the wire format used to encode GitHub events as they travel from the producer into Kafka. Every message in the `github-events` topic is an Avro-encoded binary payload.

Avro was chosen over JSON and Protobuf for three reasons: it is compact on the wire, it requires a schema (enforcing structure), and it has first-class support in the Confluent Schema Registry ecosystem that PySpark, Kafka, and most data tools expect.

---

## Avro vs JSON vs Parquet

| | JSON | Avro | Parquet |
|---|---|---|---|
| Format | Text | Binary | Binary |
| Schema required | No | Yes | Yes |
| Compression | Poor | Good | Excellent |
| Best for | APIs, debugging | Kafka messages | Analytical queries |
| Human readable | Yes | No | No |
| Row vs column | Row | Row | Column |

A single GitHub event as JSON is roughly 400 bytes. The same event as Avro is roughly 80 bytes — a 5x reduction. At 2 events per second that is the difference between 3 GB/month and 600 MB/month in Kafka storage.

Avro is row-oriented (one record at a time) which is why it suits Kafka — messages are produced and consumed one at a time. Parquet is column-oriented which is why it suits S3 analytical storage — queries read only the columns they need.

---

## The Schema

The Avro schema for the `github-events` topic:

```json
{
  "type": "record",
  "name": "GitHubEvent",
  "namespace": "com.nexus.events",
  "fields": [
    {"name": "event_id",    "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "repo_name",   "type": "string"},
    {"name": "actor_login", "type": "string"},
    {"name": "created_at",  "type": "string"},
    {"name": "stars",       "type": ["null", "long"], "default": null}
  ]
}
```

Key decisions in this schema:

**`stars` is nullable** — `["null", "long"]` is an Avro union type. The first element is the default type. Because `"default": null` is set, old messages that lack this field will deserialise as `null` rather than throwing an error. This is what makes BACKWARD compatibility possible.

**`created_at` is a string** — storing timestamps as ISO 8601 strings avoids timezone serialisation complexity in Avro. PySpark casts it to `TIMESTAMP` during the streaming job using `to_timestamp()`.

**`namespace`** — `com.nexus.events` namespaces the schema to avoid collisions if multiple schemas share the same record name across different topics.

---

## The Confluent Wire Format

Every Kafka message produced by the Nexus producer follows the Confluent wire format:

```
┌──────────────┬───────────────────┬────────────────────────┐
│  Magic byte  │    Schema ID      │     Avro payload       │
│   0x00       │  4 bytes big-end  │    variable length     │
│   1 byte     │                   │                        │
└──────────────┴───────────────────┴────────────────────────┘
     Total header = 5 bytes
```

The magic byte `0x00` signals to consumers that this message uses the Confluent wire format. The 4-byte schema ID tells consumers exactly which schema version to fetch from the registry for deserialisation — even if the schema has changed since the message was written.

This is why the PySpark streaming job strips the first 5 bytes before calling `from_avro()`:

```python
.withColumn("avro_bytes", expr("substring(value, 6, length(value) - 5)"))
.withColumn("data", from_avro(col("avro_bytes"), avro_schema_str))
```

---

## Serialisation in the producer

```python
import fastavro, io

def serialise(record: dict, schema_id: int, parsed_schema: dict) -> bytes:
    buf = io.BytesIO()
    buf.write(b'\x00')                        # magic byte
    buf.write(schema_id.to_bytes(4, 'big'))   # schema id
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()
```

`fastavro.schemaless_writer` writes the Avro binary without the Avro file header — just the raw record bytes. The Schema Registry replaces the Avro file header as the schema source of truth.

## Deserialisation in PySpark

```python
from pyspark.sql.avro.functions import from_avro

events = (raw
    .withColumn("avro_bytes", expr("substring(value, 6, length(value) - 5)"))
    .withColumn("data", from_avro(col("avro_bytes"), avro_schema_str))
    .select("data.*"))
```

PySpark's `from_avro()` takes the raw Avro bytes (without the 5-byte header) and the schema JSON string, and returns a struct column that gets exploded into individual columns via `select("data.*")`.

---

## Schema Evolution

Avro schemas are versioned in the Schema Registry. When you need to add a field:

```json
{"name": "language", "type": ["null", "string"], "default": null}
```

The registry validates the new schema against the compatibility rules before accepting it:

```bash
curl -X POST http://localhost:8081/compatibility/subjects/github-events-value/versions/latest \
  -d '{"schema": "...new schema..."}'
# → {"is_compatible": true}
```

BACKWARD compatibility (the rule used in Nexus) means:

- Adding an optional field with a default → allowed
- Removing a field → not allowed
- Renaming a field → not allowed
- Adding a required field with no default → not allowed

This protects the PySpark consumer — it can always read old messages with the new schema because missing fields will be filled with the declared default.

---

## Key concepts learned

**Schema as a contract** — Avro enforces that every message conforms to a known structure. Without a schema, a producer could send `{"stars": "many"}` and the consumer would only discover the type error at runtime.

**Binary efficiency** — field names are not repeated in every message (unlike JSON). The schema describes the structure once; the binary payload contains only values in order.

**Union types for nullability** — `["null", "string"]` explicitly declares that a field can be absent. This is more honest than JSON where any field can silently be missing.

**fastavro vs Apache Avro** — `fastavro` is a pure-Python Avro implementation that is significantly faster than the official `avro-python3` package. For high-throughput producers the difference is meaningful.