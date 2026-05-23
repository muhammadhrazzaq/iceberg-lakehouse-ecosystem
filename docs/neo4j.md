# Neo4j — Graph Database

## What is Neo4j

Neo4j is a native graph database. In the Nexus project it models the open source developer ecosystem as a property graph — repositories, contributors, and the relationships between them. It answers relationship-first questions that are either impossible or prohibitively expensive in SQL: blast radius of a vulnerable dependency, contributor influence scoring, community detection across the ecosystem, and shortest paths between any two nodes.

Neo4j was chosen because graph traversal is its native operation — it does not translate graph queries into table joins. A 5-hop path traversal in Cypher takes the same time regardless of whether the graph has 1,000 nodes or 1,000,000 nodes, because Neo4j follows pointer-linked node records rather than scanning tables.

---

## Where Neo4j sits in Nexus

```
Iceberg on S3 (source of truth)
        ↓
PyIceberg scan → pandas DataFrame
        ↓
Neo4j load (MERGE nodes and edges)
        ↓
        ├── Neo4j Browser (http://localhost:7474) — visual exploration
        ├── Cypher queries — relationship analytics
        ├── GDS algorithms — PageRank, Louvain, BFS
        └── Claude tool router — natural language → Cypher
```

---

## Architecture

### Native graph storage

Neo4j uses a native graph storage engine — nodes and relationships are stored as fixed-size records with direct pointers to adjacent records. This is fundamentally different from a relational database where relationships are represented as foreign keys requiring index lookups.

```
Relational (finding all repos a contributor touches):
  SELECT repo_name FROM events WHERE actor_login = 'user_1'
  → full index scan on actor_login
  → O(log n) per lookup, O(k log n) for k hops

Neo4j (same query):
  MATCH (c:Contributor {login: 'user_1'})-[:CONTRIBUTED_TO]->(r:Repo)
  RETURN r.name
  → follows pointers from the Contributor node directly to adjacent Repo nodes
  → O(degree) — proportional to number of connections, not table size
```

At 1 hop the difference is small. At 5 hops the difference is the gap between milliseconds and hours.

### Property graph model

Neo4j uses the **Labeled Property Graph** model:

```
Nodes       — entities with labels and properties
Edges       — directed relationships with a type and properties
Labels      — categorise nodes (e.g. :Contributor, :Repo)
Properties  — key-value pairs on nodes and edges
```

In Nexus:

```
(:Contributor {login: "user_42"})
    -[:CONTRIBUTED_TO {count: 15, event_type: "PushEvent"}]->
(:Repo {name: "apache/iceberg", stars: 6200})
```

### In-memory graph projection (GDS)

Graph Data Science (GDS) algorithms do not run on the stored graph directly. They require a **graph projection** — an in-memory copy of a subgraph optimised for algorithm execution:

```cypher
CALL gds.graph.project(
    'repo-graph',        -- projection name
    'Repo',              -- node label to include
    {CONTRIBUTED_TO: {orientation: 'REVERSE'}}  -- edge type + direction
)
```

The projection loads the relevant nodes and edges into native memory structures optimised for the algorithm. This separation means you can project different views of the same data for different algorithms without modifying the stored graph.

---

## Core concepts

### Nodes

Nodes are the entities in your graph. In Nexus there are two node labels:

```cypher
(:Contributor {login: "user_42"})
(:Repo {name: "apache/iceberg", stars: 6200})
```

Labels categorise nodes and are used in queries to filter which nodes to traverse. A node can have multiple labels.

### Relationships (edges)

Relationships are directed connections between nodes with a mandatory type:

```cypher
(:Contributor)-[:CONTRIBUTED_TO {count: 15}]->(:Repo)
```

Direction matters in storage but Cypher can traverse in either direction:

```cypher
// Traverse with the direction
MATCH (c:Contributor)-[:CONTRIBUTED_TO]->(r:Repo)

// Traverse against the direction
MATCH (r:Repo)<-[:CONTRIBUTED_TO]-(c:Contributor)

// Traverse either direction (undirected)
MATCH (c:Contributor)-[:CONTRIBUTED_TO]-(r:Repo)
```

### Indexes

Indexes speed up node lookups by property. Always create indexes before loading data:

```cypher
CREATE INDEX contrib_login IF NOT EXISTS FOR (c:Contributor) ON (c.login);
CREATE INDEX repo_name     IF NOT EXISTS FOR (r:Repo)        ON (r.name);
```

Without indexes, finding a node by property requires scanning every node in the database.

### MERGE vs CREATE

`MERGE` is the idempotent write operation — it creates the node or relationship only if it does not already exist:

```cypher
MERGE (c:Contributor {login: $login})    -- create if not exists
MERGE (r:Repo {name: $repo})             -- create if not exists
MERGE (c)-[rel:CONTRIBUTED_TO]->(r)
  ON CREATE SET rel.count = 1            -- first time
  ON MATCH  SET rel.count = rel.count + 1 -- subsequent times
```

Using `CREATE` instead of `MERGE` would create duplicate nodes on every load. In Nexus, the Neo4j load job runs every time Dagster triggers — `MERGE` ensures the graph stays consistent across repeated loads.

### Variable-length paths

Cypher's `*1..n` syntax traverses variable-length paths:

```cypher
// Find all repos connected to apache/iceberg within 3 hops
MATCH (seed:Repo {name: "apache/iceberg"})-[:CONTRIBUTED_TO*1..3]-(connected:Repo)
RETURN DISTINCT connected.name
```

This is the query that would require 3 self-joins in SQL — and in Neo4j it is a single pattern match.

---

## Cypher — the query language

Cypher is a declarative, pattern-based query language. Patterns are written visually using ASCII art:

```
(node)-[:RELATIONSHIP]->(node)
```

### Basic MATCH

```cypher
-- All contributors and their repos
MATCH (c:Contributor)-[:CONTRIBUTED_TO]->(r:Repo)
RETURN c.login, r.name, r.stars
ORDER BY r.stars DESC
LIMIT 20;
```

### Aggregation

```cypher
-- Most active contributors (total events across all repos)
MATCH (c:Contributor)-[rel:CONTRIBUTED_TO]->()
RETURN c.login AS contributor, sum(rel.count) AS total_events
ORDER BY total_events DESC
LIMIT 10;
```

### Pattern matching for shared connections

```cypher
-- Repos that share contributors (collaboration clusters)
MATCH (c:Contributor)-[:CONTRIBUTED_TO]->(r1:Repo),
      (c)-[:CONTRIBUTED_TO]->(r2:Repo)
WHERE r1.name < r2.name
RETURN r1.name, r2.name, count(DISTINCT c) AS shared_contributors
ORDER BY shared_contributors DESC
LIMIT 10;
```

### Variable-length traversal

```cypher
-- Blast radius: all repos affected if apache/iceberg has a vulnerability
-- (repos that share contributors up to 3 hops away)
MATCH path = (vuln:Repo {name: "apache/iceberg"})
             -[:CONTRIBUTED_TO*1..3]-
             (affected:Repo)
WHERE vuln <> affected
RETURN DISTINCT affected.name,
       length(path) AS hops,
       [n IN nodes(path) | n.name] AS chain
ORDER BY hops;
```

### Shortest path

```cypher
-- Shortest connection between two repos via shared contributors
MATCH p = shortestPath(
    (r1:Repo {name: "apache/iceberg"})-[*]-(r2:Repo {name: "neo4j/neo4j"})
)
RETURN [n IN nodes(p) | coalesce(n.name, n.login)] AS path,
       length(p) AS hops;
```

### APOC subgraph expansion

APOC (A Package Of Components) extends Cypher with procedures for subgraph operations:

```cypher
-- Expand from a seed node up to 3 hops
MATCH (seed:Repo {name: "apache/iceberg"})
CALL apoc.path.subgraphNodes(seed, {
    maxLevel: 3,
    relationshipFilter: "CONTRIBUTED_TO"
}) YIELD node
RETURN node.name AS connected_entity
LIMIT 50;
```

---

## Advanced — Graph Data Science (GDS)

GDS is Neo4j's graph analytics library. It runs algorithms directly in the database without exporting data.

### Step 1: Project the graph

```cypher
-- Project all Repo nodes and CONTRIBUTED_TO relationships
-- REVERSE orientation: edges point from Repo to Contributor
-- (algorithms typically flow in the "natural" direction)
CALL gds.graph.project(
    'repo-graph',
    ['Repo', 'Contributor'],
    {CONTRIBUTED_TO: {orientation: 'UNDIRECTED'}}
);
```

### Step 2: PageRank — influence scoring

PageRank scores nodes by how many other important nodes point to them. A repo contributed to by many highly-connected contributors scores higher:

```cypher
CALL gds.pageRank.stream('repo-graph', {
    maxIterations: 20,
    dampingFactor: 0.85
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS node, score
WHERE node:Repo
RETURN node.name AS repo,
       round(score, 4) AS influence_score
ORDER BY influence_score DESC
LIMIT 10;
```

Write scores back to node properties for use in other queries:

```cypher
CALL gds.pageRank.write('repo-graph', {
    writeProperty: 'pagerank'
});

-- Now query by pagerank directly
MATCH (r:Repo)
RETURN r.name, r.pagerank
ORDER BY r.pagerank DESC LIMIT 10;
```

### Step 3: Louvain — community detection

Louvain detects densely connected clusters of nodes — repos that tend to share the same contributors form a community:

```cypher
CALL gds.louvain.stream('repo-graph')
YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS node, communityId
WHERE node:Repo
RETURN communityId,
       collect(node.name) AS repos,
       count(*)           AS size
ORDER BY size DESC;
```

In the Nexus dataset you would expect to see clusters like:
- Apache ecosystem: iceberg, kafka, spark, flink
- Database ecosystem: neo4j, clickhouse
- dbt ecosystem: dbt-core, dbt-clickhouse

### Step 4: BFS — blast radius

Breadth-First Search finds all nodes reachable from a source within a maximum number of hops:

```cypher
-- Find the blast radius of a vulnerability in apache/iceberg
MATCH (vuln:Repo {name: "apache/iceberg"})
CALL gds.bfs.stream('repo-graph', {
    sourceNode: id(vuln),
    maxDepth: 3
})
YIELD path
WITH last(nodes(path)) AS affected
WHERE affected:Repo AND affected.name <> "apache/iceberg"
RETURN affected.name AS repo,
       affected.stars AS stars,
       affected.pagerank AS influence
ORDER BY influence DESC;
```

### Step 5: Node similarity

Find repos that are similar based on shared contributors:

```cypher
CALL gds.nodeSimilarity.stream('repo-graph')
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS r1,
     gds.util.asNode(node2) AS r2,
     similarity
WHERE r1:Repo AND r2:Repo
RETURN r1.name AS repo1,
       r2.name AS repo2,
       round(similarity, 4) AS jaccard_similarity
ORDER BY similarity DESC
LIMIT 10;
```

Jaccard similarity = shared contributors / total unique contributors. A score of 1.0 means the repos have identical contributor sets.

### Clean up projection

```cypher
-- Always drop projections when done — they consume memory
CALL gds.graph.drop('repo-graph');
```

---

## Loading Neo4j from Iceberg

```python
from pyiceberg.catalog import load_catalog
from neo4j import GraphDatabase

catalog = load_catalog("nexus", **{
    "type": "rest", "uri": "http://localhost:8181/catalog",
    "warehouse": "nexus", "token": "dummy"
})

df = catalog.load_table("github.events") \
            .scan().to_arrow().to_pandas()

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","devpassword"))

with driver.session() as session:
    # Create indexes first
    session.run("CREATE INDEX IF NOT EXISTS FOR (c:Contributor) ON (c.login)")
    session.run("CREATE INDEX IF NOT EXISTS FOR (r:Repo) ON (r.name)")

    # Batch load using UNWIND for performance
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

print(f"Loaded {len(df)} events into Neo4j")
driver.close()
```

`UNWIND $events` is critical for performance — it sends the entire batch as a single query with a list parameter, rather than one query per row. The difference is 1 network round-trip vs 100,000 network round-trips.

---

## Claude tool router integration

The Claude router decides when to send a query to Neo4j based on the question type:

```python
TOOLS = [
    {
        "name": "query_neo4j",
        "description": (
            "Relationship questions, contributor networks, shared connections, "
            "graph paths, blast radius, influence scoring, community detection. "
            "Use for: who contributes to both X and Y, most influential repos, "
            "connected contributors, shortest path between repos."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cypher": {"type": "string"},
                "params": {"type": "object"}
            },
            "required": ["cypher"]
        }
    }
]

def run_neo4j(cypher: str, params: dict = {}) -> str:
    with driver.session() as session:
        result = [dict(r) for r in session.run(cypher, params)]
        return json.dumps(result[:50])
```

Example routing:

```
"Which repos have the most shared contributors?"
→ Claude picks query_neo4j
→ Generates: MATCH (c:Contributor)-[:CONTRIBUTED_TO]->(r1:Repo),
             (c)-[:CONTRIBUTED_TO]->(r2:Repo)
             WHERE r1 <> r2
             RETURN r1.name, r2.name, count(c) AS shared ORDER BY shared DESC
→ Returns ranked list
→ Claude synthesises natural language answer
```

---

## Graph-Augmented RAG

The most advanced pattern — combine vector similarity with graph expansion for richer LLM context:

```python
from sentence_transformers import SentenceTransformer

encoder = SentenceTransformer("all-MiniLM-L6-v2")

def graph_rag(question: str) -> str:
    # 1. Embed the question
    embedding = encoder.encode(question).tolist()

    with driver.session() as session:
        # 2. Vector search — find seed repos by semantic similarity
        seeds = [r["name"] for r in session.run("""
            CALL db.index.vector.queryNodes('repo-embeddings', 5, $emb)
            YIELD node, score WHERE score > 0.7
            RETURN node.name AS name
        """, emb=embedding)]

        # 3. Expand k hops in the graph from seed nodes
        context_rows = [dict(r) for r in session.run("""
            MATCH (seed:Repo) WHERE seed.name IN $seeds
            CALL apoc.path.subgraphNodes(seed, {maxLevel: 2})
            YIELD node
            WITH collect(DISTINCT node) AS nodes
            UNWIND nodes AS n
            OPTIONAL MATCH (n)<-[:CONTRIBUTED_TO]-(c:Contributor)
            RETURN n.name        AS repo,
                   n.stars       AS stars,
                   n.pagerank    AS influence,
                   collect(DISTINCT c.login)[..5] AS top_contributors
            ORDER BY n.pagerank DESC LIMIT 20
        """, seeds=seeds)]

    # 4. Format as context for LLM
    context = "\n".join([
        f"**{r['repo']}** (stars: {r['stars']}, influence: {r['influence']:.3f})\n"
        f"  Top contributors: {', '.join(r['top_contributors'])}"
        for r in context_rows
    ])

    # 5. Ask Claude with graph context
    resp = anthropic.Anthropic().messages.create(
        model="claude-opus-4-5", max_tokens=1024,
        system="You are a developer ecosystem expert. Answer using the graph context.",
        messages=[{"role": "user", "content": f"{context}\n\nQuestion: {question}"}]
    )
    return resp.content[0].text
```

Standard RAG finds semantically similar documents. Graph-augmented RAG additionally traverses the relationship network around those documents — surfacing connected repos, shared contributors, and dependency chains that pure vector search would miss.

---

## Neo4j Browser queries — quick reference

Open `http://localhost:7474` → login `neo4j/devpassword`

```cypher
-- See the full graph (limit nodes for performance)
MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 100;

-- Count nodes by label
MATCH (n) RETURN labels(n) AS label, count(n) AS count;

-- Count relationships by type
MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count;

-- Schema overview
CALL db.schema.visualization();

-- Most connected nodes
MATCH (n)
RETURN labels(n)[0] AS label, n.name AS name, count{(n)-[]->()} AS out_degree
ORDER BY out_degree DESC LIMIT 10;
```

---

## Key concepts learned

**Graph thinking vs tabular thinking** — graph databases shine when the relationships between entities are as important as the entities themselves. The question "who contributes to both X and Y" is a join in SQL but a pattern match in Cypher.

**Index-free adjacency** — Neo4j nodes store direct pointers to adjacent nodes. Traversal cost is proportional to the number of relationships traversed, not the size of the database. This is fundamentally different from SQL where a join cost grows with table size.

**MERGE idempotency** — MERGE is the cornerstone of safe repeated loads. It finds or creates, and the ON CREATE / ON MATCH clauses let you handle both cases differently in one operation.

**UNWIND for batch operations** — sending a list parameter with UNWIND is orders of magnitude faster than running individual queries in a loop. One network round-trip instead of N.

**GDS projection** — algorithms run on an in-memory projection of the graph, not the stored graph. This allows algorithm-specific optimisations (e.g. undirected vs directed) without modifying the data model.

**PageRank in graphs** — a repo scored highly by PageRank is not necessarily the most starred — it is the repo that the most influential contributors work on. It captures indirect importance through the network structure.

**Community detection** — Louvain finds groups of nodes that are more densely connected to each other than to the rest of the graph. In a contributor network this reveals natural ecosystems (Apache, CNCF, database tools) without any labelling.

**Graph-augmented RAG** — standard vector search finds semantic similarity. Graph expansion finds structural similarity — the repos, contributors, and dependencies that exist in relationship with the semantically similar nodes. Together they give an LLM richer, more accurate context.