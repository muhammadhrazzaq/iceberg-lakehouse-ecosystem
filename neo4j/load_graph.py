import sys
sys.stdout.reconfigure(line_buffering=True)

from pyiceberg.catalog import load_catalog
from neo4j import GraphDatabase
import os

from dotenv import load_dotenv
load_dotenv()

catalog = load_catalog("nexus", **{
    "type":      "rest",
    "uri":       "http://localhost:8181/catalog",
    "warehouse": "nexus",
    "token":     "dummy",
})

df = catalog.load_table("github.events").scan().to_arrow().to_pandas()
print(f"Loaded {len(df)} rows from Iceberg")

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "devpassword")
)

with driver.session() as session:
    # Indexes first
    session.run("CREATE INDEX IF NOT EXISTS FOR (c:Contributor) ON (c.login)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (r:Repo) ON (r.name)")

    # Clear old graph
    session.run("MATCH (n) DETACH DELETE n")

    # Batch load
    events = df.to_dict("records")
    session.run("""
        UNWIND $events AS event
        MERGE (c:Contributor {login: event.actor_login})
        MERGE (r:Repo {name: event.repo_name})
          ON CREATE SET r.stars = event.stars
        MERGE (c)-[rel:CONTRIBUTED_TO]->(r)
          ON CREATE SET rel.count = 1
          ON MATCH  SET rel.count = rel.count + 1
    """, events=events)

print(f"Graph loaded successfully")
driver.close()