from dotenv import load_dotenv
import os, requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.avro.functions import from_avro

load_dotenv()

AWS_KEY    = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")
os.environ["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "C:/hadoop")
os.environ["PATH"] = os.environ["HADOOP_HOME"] + "/bin;" + os.environ.get("PATH", "")
REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP    = "kafka:9092"

resp = requests.get(f"{REGISTRY_URL}/subjects/github-events-value/versions/latest")
avro_schema_str = resp.json()["schema"]

spark = (SparkSession.builder
    .appName("nexus-streaming")
    .config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.apache.spark:spark-avro_2.12:3.5.3,"
        "software.amazon.awssdk:bundle:2.20.18,"
        "org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.nexus",           "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nexus.type",      "rest")
    .config("spark.sql.catalog.nexus.uri",       "http://localhost:8181/catalog")
    .config("spark.sql.catalog.nexus.warehouse", "nexus")
    .config("spark.sql.catalog.nexus.token",     "dummy")
    .config("spark.hadoop.fs.s3a.access.key",    AWS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",    AWS_SECRET)
    .config("spark.hadoop.fs.s3a.endpoint",      f"s3.{AWS_REGION}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .getOrCreate())


spark.sparkContext.setLogLevel("WARN")

# Create table if not exists
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
        'write.parquet.compression-codec' = 'zstd'
    )
""")

# Read from Kafka
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", "github-events")
    .option("startingOffsets", "earliest")
    .load())

# Strip 5-byte Confluent header then deserialise Avro
events = (raw
    .withColumn("avro_bytes", expr("substring(value, 6, length(value)-5)"))
    .withColumn("data", from_avro(col("avro_bytes"), avro_schema_str))
    .select("data.*")
    .withColumn("created_at", to_timestamp(col("created_at"))))

# Write to Iceberg
(events.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/nexus-checkpoint")
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")
    .toTable("nexus.github.events")
    .awaitTermination())
