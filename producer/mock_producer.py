import json, uuid, random, time, io, requests, os, sys
from datetime import datetime, timezone
from kafka import KafkaProducer
import fastavro

# Force unbuffered output for Docker logs
sys.stdout.reconfigure(line_buffering=True)
print(f"Starting producer...")
print(f"  Broker  : {os.environ.get('KAFKA_BROKER', 'not set')}")
print(f"  Registry: {os.environ.get('REGISTRY_URL', 'not set')}")
# ── Config from env vars ──────────────────────────────────
REGISTRY_URL     = os.environ.get("REGISTRY_URL",  "http://localhost:8081")
BOOTSTRAP_SERVER = os.environ.get("KAFKA_BROKER",  "localhost:19092")
TOPIC            = "github-events"
INTERVAL         = float(os.environ.get("PRODUCE_INTERVAL", "0.5"))

print(f"Starting producer...")
print(f"  Broker  : {BOOTSTRAP_SERVER}")
print(f"  Registry: {REGISTRY_URL}")

# Fetch schema from registry at startup
resp = requests.get(f"{REGISTRY_URL}/subjects/{TOPIC}-value/versions/latest")
resp.raise_for_status()
data        = resp.json()
schema_id   = data["id"]
avro_schema = fastavro.parse_schema(json.loads(data["schema"]))

def serialise(record: dict) -> bytes:
    buf = io.BytesIO()
    buf.write(b'\x00')                       # magic byte
    buf.write(schema_id.to_bytes(4, 'big'))  # schema id
    fastavro.schemaless_writer(buf, avro_schema, record)
    return buf.getvalue()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVER,
    key_serializer=lambda k: k.encode("utf-8"),
)

REPOS = ["apache/iceberg","ClickHouse/ClickHouse",
         "apache/kafka","neo4j/neo4j","dbt-labs/dbt-core"]
TYPES = ["PushEvent","PullRequestEvent","IssuesEvent",
         "WatchEvent","ForkEvent"]

print(f"Producing to {BOOTSTRAP_SERVER} — Ctrl+C to stop")
while True:
    event = {
        "event_id":    str(uuid.uuid4()),
        "event_type":  random.choice(TYPES),
        "repo_name":   random.choice(REPOS),
        "actor_login": f"user_{random.randint(1, 500)}",
        "created_at":  datetime.now(timezone.utc).isoformat(),
        "stars":       random.randint(100, 40000),
    }
    producer.send(TOPIC, key=event["repo_name"], value=serialise(event))
    print(f"  → {event['event_type']} on {event['repo_name']}")
    time.sleep(0.5)