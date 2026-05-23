# Spark Streaming — Kafka → Iceberg

## What this does

This folder contains the PySpark Structured Streaming job that sits at the heart of the Nexus pipeline. It continuously reads Avro-encoded GitHub events from a Kafka topic, deserialises them using the schema from the Confluent-compatible Schema Registry, and writes them as partitioned Parquet files into an Apache Iceberg table on S3 — with all metadata tracked by Lakekeeper.

```
Kafka topic (github-events)
        ↓
PySpark Structured Streaming (30s micro-batch)
        ↓
Avro deserialisation (schema from registry)
        ↓
Iceberg table on S3 (partitioned by month)
        ↓
Lakekeeper catalog (metadata + snapshots)
```

---

## Files

```
spark/
  streaming_job.py   ← the streaming job
  README.md          ← this file
```

---

## How it works — step by step

### 1. Bootstrap (before Spark starts)

```python
WAREHOUSE_PREFIX = get_warehouse_prefix(CATALOG_URI, WAREHOUSE)
avro_schema_str  = get_avro_schema(REGISTRY_URL)
```

Before creating the Spark session, the script makes two HTTP calls:

- **Lakekeeper config endpoint** — fetches the warehouse UUID prefix (`6914dcac-...`). This is Lakekeeper's internal routing ID used in all API paths. We pass it to Spark as the `warehouse` config so Spark can find the right catalog.
- **Schema Registry** — fetches the Avro schema for `github-events-value`. PySpark's `from_avro()` function needs the schema as a JSON string at job startup.

### 2. Spark session config

```python
spark = (SparkSession.builder
    .config("spark.sql.catalog.nexus.type", "rest")
    .config("spark.sql.catalog.nexus.uri",  CATALOG_URI)
    .config("spark.sql.catalog.nexus.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.nexus.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    ...)
```

Key decisions:

- **`io-impl = S3FileIO`** — tells Iceberg to use the AWS SDK v2 S3 client (from `iceberg-aws-bundle`) instead of the Hadoop filesystem. Without this, Iceberg falls back to `HadoopFileIO` which doesn't understand `s3://` URIs.
- **`fs.s3a.impl = S3AFileSystem`** — tells Hadoop to use the S3A filesystem driver for `s3a://` URIs. This is what actually reads and writes the Parquet files.
- **`rest` catalog type** — connects to Lakekeeper's Iceberg REST catalog API for all metadata operations (create table, commit snapshot, list namespaces).

### 3. Table creation

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.github")
spark.sql("""
    CREATE TABLE IF NOT EXISTS nexus.github.events (...)
    USING iceberg
    PARTITIONED BY (months(created_at))
    TBLPROPERTIES ('format-version' = '2')
""")
```

`IF NOT EXISTS` means this is idempotent — safe to run on every restart. The table is partitioned by month using Iceberg's `months()` transform, which means Spark writes data into separate S3 prefixes per month. ClickHouse and PyIceberg can then prune partitions during reads for better performance.

### 4. Kafka read stream

```python
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "github-events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load())
```

`startingOffsets = earliest` means on the first run Spark reads all historical messages from the beginning of the topic. On subsequent restarts it picks up from the checkpoint — the last committed Kafka offset.

`failOnDataLoss = false` prevents the job from crashing if Kafka deletes old messages before Spark reads them (common when Kafka's retention period is shorter than the job's downtime).

### 5. Avro deserialisation

```python
events = (raw
    .withColumn("avro_bytes", expr("substring(value, 6, length(value) - 5)"))
    .withColumn("data", from_avro(col("avro_bytes"), avro_schema_str))
    .select("data.*")
    ...)
```

Kafka messages produced with the Confluent Schema Registry use a 5-byte wire format header:

```
byte[0]   = 0x00 (magic byte)
byte[1-4] = schema_id (4-byte big-endian int)
byte[5..] = avro payload
```

The `substring(value, 6, ...)` strips that header before passing to `from_avro()`. The schema ID in the header tells consumers which schema version was used — we already fetched the schema at startup so we just strip the header and deserialise.

### 6. Micro-batch write to Iceberg

```python
def log_batch(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        batch_df.writeTo("nexus.github.events").append()

query = (events.writeStream
    .trigger(processingTime="30 seconds")
    .foreachBatch(log_batch)
    .start())
```

`foreachBatch` gives us a regular DataFrame every 30 seconds containing only the new rows from that batch window. We then use `.writeTo().append()` which is the Iceberg-native write path — it commits a new snapshot to the Lakekeeper catalog after each batch.

Every successful batch creates an immutable Iceberg snapshot. This is what enables time travel — you can query any past snapshot to see what the data looked like at that point.

### 7. Checkpoint

```
/tmp/nexus-checkpoint/
  offsets/        ← which Kafka offsets were processed
  commits/        ← which batches were successfully committed
```

The checkpoint directory records exactly which Kafka offsets each batch processed. If the job crashes mid-batch, on restart Spark re-reads from the last committed offset — guaranteeing exactly-once semantics when combined with Iceberg's atomic snapshot commits.

**Never delete the checkpoint while the job is running** — you'll get duplicate data.

---

## Running locally

```bash
# Prerequisites
# - Kafka running on localhost:19092
# - Schema registry on localhost:8081
# - Lakekeeper on localhost:8181
# - Schema registered (run register_schema.sh first)

# Install
pip install pyspark==3.5.3 pyiceberg requests python-dotenv fastavro

# Run (producer must be running in another terminal first)
python spark/streaming_job.py
```

First run downloads ~500MB of JARs. Subsequent runs use the cache.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:19092` | Kafka bootstrap server |
| `CATALOG_URI` | `http://localhost:8181/catalog` | Lakekeeper REST catalog |
| `REGISTRY_URL` | `http://localhost:8081` | Schema registry |
| `WAREHOUSE_NAME` | `nexus` | Lakekeeper warehouse name |
| `CHECKPOINT_DIR` | `/tmp/nexus-checkpoint` | Spark checkpoint location |
| `AWS_ACCESS_KEY_ID` | — | AWS credentials (from `.env`) |
| `AWS_SECRET_ACCESS_KEY` | — | AWS credentials (from `.env`) |
| `AWS_REGION` | `eu-west-2` | S3 bucket region |

When running inside Docker, `KAFKA_BROKER=kafka:9092` and `CATALOG_URI=http://lakekeeper:8181/catalog` (internal hostnames).

---

## Key concepts learned

**Micro-batch streaming** — Spark doesn't process each event individually. It accumulates events over a trigger interval (30 seconds) and processes them as a batch. This is more efficient than true record-at-a-time streaming and gives exactly-once guarantees.

**Iceberg snapshots** — every batch write creates an immutable snapshot. The table always has a consistent view even while new data is being written. This is fundamentally different from writing raw Parquet where a partial write leaves the table in an inconsistent state.

**Schema evolution** — because the Avro schema is fetched from the registry at startup and `mergeSchema=true` is set, the job handles new fields being added to the schema without requiring a restart or data migration.

**Partition pruning** — partitioning by `months(created_at)` means a query for "last week's data" only scans the current month's S3 files, not the entire table.