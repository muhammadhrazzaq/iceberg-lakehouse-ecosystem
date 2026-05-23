# Parquet — Columnar Storage Format

## What is Parquet

Parquet is a columnar binary file format designed for analytical workloads. In the Nexus project, every batch of GitHub events written by PySpark to S3 is stored as Parquet files, organised and tracked by Apache Iceberg. ClickHouse reads these files directly from S3 for OLAP queries.

Parquet was not chosen — it is the default output format for Iceberg tables, and for good reason. It is the most widely supported analytical file format in the data ecosystem and compresses tabular data better than any row-oriented format.

---

## Why columnar storage matters

Row-oriented storage (JSON, CSV, Avro) stores all fields of a record together:

```
Row 1: [event_id, event_type, repo_name, actor_login, created_at, stars]
Row 2: [event_id, event_type, repo_name, actor_login, created_at, stars]
Row 3: [event_id, event_type, repo_name, actor_login, created_at, stars]
```

Columnar storage (Parquet) stores each field together across all rows:

```
event_id column:    [id1, id2, id3, ...]
event_type column:  [PushEvent, WatchEvent, PushEvent, ...]
repo_name column:   [apache/iceberg, neo4j/neo4j, ...]
stars column:       [6200, 35100, 6200, ...]
```

When ClickHouse runs `SELECT event_type, count() FROM github_events GROUP BY event_type`, it only needs to read the `event_type` column — it skips `event_id`, `repo_name`, `actor_login`, `created_at`, and `stars` entirely. On a table with 10 million rows and 6 columns, this means reading roughly 17% of the data instead of 100%.

---

## Parquet file structure

Each Parquet file is divided into row groups, which are divided into column chunks, which store data pages:

```
Parquet file
├── Row Group 1 (e.g. 128 MB of rows)
│   ├── Column chunk: event_id
│   │   ├── Data page 1
│   │   └── Data page 2
│   ├── Column chunk: event_type
│   └── ...
├── Row Group 2
│   └── ...
└── Footer (schema + row group metadata + statistics)
```

The footer contains **column statistics** — min/max values for each column in each row group. Query engines use these to skip entire row groups without reading them. If you query `WHERE stars > 30000` and a row group's max stars is 15000, the entire row group is skipped.

---

## Compression in Nexus

Iceberg tables in Nexus are configured to use ZSTD compression:

```python
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')
```

ZSTD (Zstandard) provides an excellent balance of compression ratio and decompression speed. Compared to the alternatives:

| Codec | Compression ratio | Speed | CPU usage |
|---|---|---|---|
| None | 1x | Fastest | None |
| Snappy | ~2x | Fast | Low |
| GZIP | ~3x | Slow | High |
| ZSTD | ~3x | Fast | Medium |

ZSTD gives near-GZIP compression ratios at near-Snappy speeds, making it the best choice for a data lake where files are written once and read many times.

---

## Parquet files on S3

After a few batches, the S3 structure looks like:

```
s3://nexus-iceberg/
  {warehouse-id}/
    {table-id}/
      data/
        created_at_month=2026-05/          ← Iceberg partition
          00000-0-abc123.parquet
          00000-1-def456.parquet
      metadata/
        00000.metadata.json                ← snapshot 1
        00001.metadata.json                ← snapshot 2
        snap-001.avro                      ← manifest list
        manifest-001.avro                  ← manifest file
```

Each Parquet file corresponds to one Spark task output. With `local[2]` (2 cores) and a 30-second trigger, you get roughly 2 Parquet files per batch — one per partition.

The target file size is configured to 64MB:

```python
TBLPROPERTIES ('write.target-file-size-bytes' = '67108864')
```

Small files (under 1MB) hurt query performance because each file requires a separate S3 API call to open. Iceberg's compaction process (run separately) merges small files into larger ones. For this learning project compaction is skipped — at small data volumes the performance difference is negligible.

---

## How ClickHouse reads Parquet from S3

```sql
SELECT event_type, count() AS n
FROM s3(
    'https://nexus-iceberg.s3.eu-west-2.amazonaws.com/.../data/*/*.parquet',
    'Parquet'
)
GROUP BY event_type
ORDER BY n DESC;
```

ClickHouse uses its native S3 table function to read Parquet files directly. The wildcard `*/*.parquet` matches all partition directories and all files within them. ClickHouse reads only the `event_type` column from each file (column pruning) and uses the Parquet footer statistics to skip row groups that can't contribute to the result (predicate pushdown).

---

## Parquet vs Iceberg

An important distinction: Parquet is the file format; Iceberg is the table format. They are separate layers:

```
Iceberg (table format)
  → tracks which Parquet files belong to the table
  → manages snapshots, schema, partitioning
  → stored as JSON metadata files in S3

Parquet (file format)
  → the actual data bytes on disk
  → columnar, compressed, with statistics
  → stored as .parquet files in S3
```

You could have Iceberg without Parquet (using ORC or Avro as the file format), and you could have Parquet without Iceberg (raw files with no metadata layer). Nexus uses both together: Iceberg manages the table metadata, Parquet stores the data.

---

## Key concepts learned

**Column pruning** — query engines only read the columns they need. A 6-column table queried on 1 column reads ~17% of the data.

**Predicate pushdown** — column statistics in the Parquet footer let query engines skip row groups that can't match a WHERE clause, without reading the data.

**Row groups** — the unit of parallelism in Parquet. Multiple threads can read different row groups from the same file simultaneously.

**ZSTD compression** — the best general-purpose codec for a data lake where data is written once and read many times.

**Small file problem** — many small Parquet files hurt query performance. In production, Iceberg's compaction process merges them. For learning projects at small data volumes this is not a concern.

**Parquet is not queryable without a schema** — unlike CSV or JSON, Parquet embeds the schema in the file footer. This means the schema is always available to the reader, but it also means Parquet files from incompatible schemas cannot be read together without schema evolution handling (which Iceberg provides).