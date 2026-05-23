import sys
sys.stdout.reconfigure(line_buffering=True)  # real-time logs in Docker

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.avro.functions import from_avro
import requests
import os

load_dotenv()

AWS_KEY      = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET   = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION   = os.environ.get("AWS_REGION", "eu-west-2")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER",  "localhost:19092")
CATALOG_URI  = os.environ.get("CATALOG_URI",   "http://localhost:8181/catalog")
REGISTRY_URL = os.environ.get("REGISTRY_URL",  "http://localhost:8081")
CHECKPOINT   = os.environ.get("CHECKPOINT_DIR","/tmp/nexus-checkpoint")
WAREHOUSE    = os.environ.get("WAREHOUSE_NAME","nexus")

def get_warehouse_prefix(catalog_uri: str, warehouse_name: str) -> str:
    resp = requests.get(
        f"{catalog_uri}/v1/config",
        params={"warehouse": warehouse_name}
    )
    resp.raise_for_status()
    prefix = resp.json()["defaults"]["prefix"]
    print(f"  ✓ Warehouse '{warehouse_name}' → prefix: {prefix}")
    return prefix

def get_avro_schema(registry_url: str) -> str:
    resp = requests.get(
        f"{registry_url}/subjects/github-events-value/versions/latest"
    )
    resp.raise_for_status()
    data = resp.json()
    print(f"  ✓ Avro schema id={data['id']} fetched")
    return data["schema"]

print("Starting nexus streaming job...")
print(f"  Kafka        : {KAFKA_BROKER}")
print(f"  Catalog URI  : {CATALOG_URI}")
print(f"  Registry     : {REGISTRY_URL}")
print(f"  Warehouse    : {WAREHOUSE}")

WAREHOUSE_PREFIX = get_warehouse_prefix(CATALOG_URI, WAREHOUSE)
avro_schema_str  = get_avro_schema(REGISTRY_URL)

spark = (SparkSession.builder
    .appName("nexus-streaming")
    .config("spark.jars.packages",
        ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "org.apache.spark:spark-avro_2.12:3.5.3",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]))
    .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.nexus",
        "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nexus.type",      "rest")
    .config("spark.sql.catalog.nexus.uri",       CATALOG_URI)
    .config("spark.sql.catalog.nexus.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.nexus.prefix",    WAREHOUSE_PREFIX)
    .config("spark.sql.catalog.nexus.token",     "dummy")
    .config("spark.sql.catalog.nexus.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.access.key",    AWS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",    AWS_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint",      f"s3.{AWS_REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism",    "4")
    .master("local[2]")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("Spark session started")
print(f"  Warehouse prefix: {WAREHOUSE_PREFIX}")

spark.sql("CREATE NAMESPACE IF NOT EXISTS nexus.github")
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
print("Table ready: nexus.github.events")

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe",               "github-events")
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .load())

events = (raw
    .withColumn("avro_bytes", expr("substring(value, 6, length(value) - 5)"))
    .withColumn("data", from_avro(col("avro_bytes"), avro_schema_str))
    .select("data.*")
    .withColumn("created_at", to_timestamp(col("created_at")))
    .filter(col("event_id").isNotNull())
    .filter(col("created_at").isNotNull())
)

def log_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"  [BATCH {batch_id}] {count} rows → nexus.github.events", flush=True)
    if count > 0:
        batch_df.writeTo("nexus.github.events").append()

print("Starting streaming query — writing to Iceberg every 30s\n")

query = (events.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .foreachBatch(log_batch)
    .start())

query.awaitTermination()