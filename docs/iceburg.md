# Apache Iceberg — Open Table Format

## What is Iceberg

Apache Iceberg is an open table format for large analytic datasets. In the Nexus project it sits between PySpark (which writes data) and ClickHouse, PyIceberg, and Neo4j (which read data). Iceberg is not a file format and not a storage system — it is a metadata layer that sits on top of Parquet files in S3 and manages them as a proper table with schema, partitioning, versioning, and transactions.

Iceberg was chosen because it solves the fundamental problems with raw Parquet files on S3: no atomic writes, no schema evolution, no time travel, and no partition management. With Iceberg, S3 becomes a proper data lakehouse — a storage layer with database-like guarantees.

---

## The problem Iceberg solves

Without Iceberg, writing Parquet to S3 has serious limitations:

```
Raw Parquet on S3 (without Iceberg):
  ✗ No atomic writes — readers see partial data during writes
  ✗ No schema evolution — adding a column breaks old readers
  ✗ No time travel — can't query historical state
  ✗ No partition management — you manage folder structure manually
  ✗ No transactions — concurrent writers corrupt data

Parquet on S3 with Iceberg:
  ✓ Atomic snapshot commits — readers always see consistent data
  ✓ Schema evolution — add/rename/drop columns without rewriting files
  ✓ Time travel — query any past snapshot by ID or timestamp
  ✓ Hidden partitioning — Iceberg manages partition structure for you
  ✓ Optimistic concurrency — multiple writers coordinate via snapshots
```

---

## How Iceberg works

Iceberg separates the table into three layers:

```
┌─────────────────────────────────────────────────┐
│  Catalog (Lakekeeper)                           │
│  Stores: current metadata file pointer          │
│  API: REST (Iceberg REST catalog spec)          │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│  Metadata layer (S3: metadata/*.json)           │
│  Stores: schema, partition spec, snapshot list  │
│  Each snapshot → manifest list → manifest files │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│  Data layer (S3: data/*.parquet)                │
│  Stores: actual row data in Parquet files       │
│  Never modified — only new files are added      │
└─────────────────────────────────────────────────┘
```

### Snapshots

Every write operation creates a new immutable snapshot. A snapshot is a complete, consistent view of the table at a point in time. Old snapshots are never modified — they accumulate until the expiry period (configurable, default 5 days).

```
Snapshot 1 (batch 0): files [A.parquet]
Snapshot 2 (batch 1): files [A.parquet, B.parquet]
Snapshot 3 (batch 2): files [A.parquet, B.parquet, C.parquet]
```

The catalog always points to the latest snapshot. Readers that started before a new snapshot is committed continue reading the old snapshot — they are never interrupted by concurrent writes.

### Manifest files

Between the metadata file and the data files sit manifest files. A manifest lists a subset of data files with their partition information and column statistics:

```
metadata.json
  └── snap-003.avro (manifest list)
       ├── manifest-001.avro → [A.parquet, B.parquet]
       └── manifest-002.avro → [C.parquet]
```

This hierarchy allows Iceberg to prune at multiple levels — skip manifests that cover irrelevant partitions before even looking at individual files.

---

## Iceberg in Nexus — table config

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS nexus.github.events (
        event_id    STRING,
        event_type  STRING,
        repo_name   STRING,
        actor_login STRING,
        created_at  TIMESTAMP,
        stars       BIGINT
    ) USING iceberg
    PARTITIONED BY (months(created_at))
    TBLPROPERTIES (
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes'    = '67108864',
        'format-version'                  = '2'
    )
""")
```

**`PARTITIONED BY (months(created_at))`** — Iceberg's hidden partitioning. The partition column is derived from `created_at` using the `months()` transform. Data files are organised by month in S3 but the partition column does not appear in the table schema — queries filter on `created_at` and Iceberg automatically skips irrelevant month partitions.

**`format-version = 2`** — Iceberg v2 adds row-level deletes and merge-on-read, enabling UPDATE and DELETE operations without rewriting entire files.

**`write.target-file-size-bytes = 67108864`** — targets 64MB Parquet files. Small files (under 1MB) hurt query performance; this setting guides Spark toward producing appropriately-sized files.

---

## Time travel

Because every write creates an immutable snapshot, you can query any past state:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("nexus", **{
    "type": "rest", "uri": "http://localhost:8181/catalog",
    "warehouse": "nexus", "token": "dummy"
})

table = catalog.load_table("github.events")

# See all snapshots
for snap in table.history():
    print(f"snapshot {snap.snapshot_id} at {snap.timestamp_ms}")

# Query as of a specific snapshot
df = table.scan(snapshot_id=snap.snapshot_id).to_arrow().to_pandas()

# Query as of a timestamp
yesterday_ms = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
snap = next(s for s in reversed(table.history()) if s.timestamp_ms <= yesterday_ms)
df   = table.scan(snapshot_id=snap.snapshot_id).to_arrow().to_pandas()
```

In PySpark:

```python
# Time travel by snapshot ID
spark.read \
    .option("snapshot-id", "8270633197359469822") \
    .format("iceberg") \
    .load("nexus.github.events")

# Time travel by timestamp
spark.read \
    .option("as-of-timestamp", "1736899200000") \
    .format("iceberg") \
    .load("nexus.github.events")
```

---

## Schema evolution

Iceberg tracks column identity by internal field IDs, not by name. This means renaming a column does not break readers — old files that used the old name are still readable because Iceberg maps old field IDs to new names:

```python
from pyiceberg.types import StringType

with table.update_schema() as update:
    update.add_column("language", StringType(), doc="Repo primary language")
    # Old files return null for language — they predate this field
    # New files return the actual value

with table.update_schema() as update:
    update.rename_column("actor_login", "contributor_login")
    # Iceberg maps field ID → new name transparently
    # Old Parquet files still readable — field ID unchanged
```

---

## Lakekeeper — the catalog

Lakekeeper is the Iceberg REST catalog used in Nexus. It implements the [Apache Iceberg REST catalog specification](https://iceberg.apache.org/rest-catalog) and stores metadata in Postgres. Every engine that reads or writes Iceberg tables in Nexus goes through Lakekeeper:

```
PySpark   → Lakekeeper → S3 (writes Parquet, commits snapshot)
PyIceberg → Lakekeeper → S3 (reads metadata, scans Parquet)
ClickHouse → S3 directly  (reads Parquet, bypasses Lakekeeper)
```

ClickHouse bypasses Lakekeeper and reads Parquet files directly using the S3 table function. This means ClickHouse always reads the latest files but doesn't participate in the Iceberg snapshot protocol — it won't respect time travel or see in-progress writes atomically. For this project that is acceptable.

---

## S3 structure after several batches

```
s3://nexus-iceberg/
  {warehouse-uuid}/
    {table-uuid}/
      data/
        created_at_month=2026-05/
          00000-0-abc123-0001.parquet   ← batch 0, task 0
          00000-1-abc123-0002.parquet   ← batch 0, task 1
          00001-0-def456-0001.parquet   ← batch 1, task 0
      metadata/
        00000.gz.metadata.json          ← snapshot 0 (table creation)
        00001.gz.metadata.json          ← snapshot 1 (batch 0 committed)
        00002.gz.metadata.json          ← snapshot 2 (batch 1 committed)
        snap-{id}.avro                  ← manifest list per snapshot
        {uuid}-m0.avro                  ← manifest file
```

---

## Write-Audit-Publish pattern

Iceberg supports branching — like git branches but for data. This enables safe data validation before publishing to the main table:

```python
# 1. Create audit branch
table.manage_snapshots().create_branch("audit").commit()

# 2. Write new batch to audit branch only
table.append(new_data, branch="audit")

# 3. Validate
df = table.scan(branch="audit").to_arrow().to_pandas()
assert df["event_id"].is_unique, "Duplicate IDs found"
assert df["created_at"].notna().all(), "Null timestamps"

# 4. Promote to main if valid
table.manage_snapshots().fast_forward("main", "audit").commit()

# 4b. Or rollback if invalid
table.manage_snapshots().remove_branch("audit").commit()
```

This is the production-grade approach for data pipelines where data quality must be verified before becoming visible to downstream consumers.

---

## Key concepts learned

**Table format vs file format** — Iceberg is a table format (metadata layer). Parquet is a file format (data storage). They work together: Iceberg tracks which Parquet files belong to the table and in what state.

**Immutability** — data files are never modified after writing. Schema changes, compaction, and deletes all work by writing new files and updating metadata to reference them. Old files remain readable via time travel until they expire.

**Hidden partitioning** — partition columns are derived from data columns via transforms (`months()`, `days()`, `bucket()`, `truncate()`). The partition structure is managed by Iceberg; queries filter on the original column and Iceberg prunes partitions automatically.

**Snapshot isolation** — readers always see a consistent snapshot. A write that is in progress is invisible to readers until it commits. This is what makes Iceberg suitable for concurrent reads and writes.

**Catalog as the source of truth** — the catalog (Lakekeeper) holds the pointer to the current metadata file. All engines go through the catalog to find the table — this is what makes the table accessible to multiple engines simultaneously (PySpark, PyIceberg, ClickHouse, dbt).