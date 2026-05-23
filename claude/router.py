import sys
sys.stdout.reconfigure(line_buffering=True)

import os
from dotenv import load_dotenv


load_dotenv()

api_key = os.environ.get("ANTHROPIC_API_KEY")
if not api_key:
    raise ValueError("ANTHROPIC_API_KEY not found in .env file")
print(f"  ✓ API key loaded: {api_key[:8]}...")

import anthropic
import json
import os
from neo4j import GraphDatabase
import clickhouse_connect
from dotenv import load_dotenv

client = anthropic.Anthropic()

neo4j = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "devpassword")
)

ch = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="nexus123",
)

# ── Tools ─────────────────────────────────────────────────
TOOLS = [
    {
        "name": "query_clickhouse",
        "description": (
            "Use for analytics, counts, trends, time-series, aggregations, anomalies. "
            "Examples: how many events, which repo is most active, activity over time, "
            "z-score anomalies, events per hour, contributor daily stats. "
            "Tables available: marts.mart_repo_daily, marts.mart_contributor_stats, marts.mart_event_hourly"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "Valid ClickHouse SQL query"
                },
                "explanation": {
                    "type": "string",
                    "description": "Why you chose ClickHouse for this question"
                }
            },
            "required": ["sql", "explanation"]
        }
    },
    {
        "name": "query_neo4j",
        "description": (
            "Use for relationships, networks, shared connections, graph paths, "
            "influence scoring, community detection, blast radius. "
            "Examples: who contributes to both X and Y, most influential repos, "
            "shortest path between repos, contributors active on multiple repos. "
            "Nodes: Contributor(login), Repo(name, stars). "
            "Relationships: CONTRIBUTED_TO(count)"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "cypher": {
                    "type": "string",
                    "description": "Valid Cypher query for Neo4j"
                },
                "explanation": {
                    "type": "string",
                    "description": "Why you chose Neo4j for this question"
                }
            },
            "required": ["cypher", "explanation"]
        }
    },
]

# ── Tool execution ────────────────────────────────────────
def run_tool(name: str, inp: dict) -> str:
    print(f"\n  → Running {name}")
    print(f"    Reason: {inp.get('explanation', 'n/a')}")

    if name == "query_clickhouse":
        print(f"    SQL: {inp['sql'][:100]}...")
        try:
            result = ch.query(inp["sql"])
            rows   = result.result_rows[:20]
            cols   = result.column_names
            data   = [dict(zip(cols, row)) for row in rows]
            print(f"    Returned {len(data)} rows")
            return json.dumps(data, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    if name == "query_neo4j":
        print(f"    Cypher: {inp['cypher'][:100]}...")
        try:
            with neo4j.session() as session:
                result = [dict(r) for r in session.run(inp["cypher"])]
            print(f"    Returned {len(result)} rows")
            return json.dumps(result[:20], default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return json.dumps({"error": f"Unknown tool: {name}"})

# ── Agentic loop ──────────────────────────────────────────
def ask(question: str) -> str:
    print(f"\n{'='*60}")
    print(f"Question: {question}")
    print(f"{'='*60}")

    messages = [{"role": "user", "content": question}]

    while True:
        resp = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=2048,
            system=(
                "You are a developer ecosystem analyst for the Nexus project. "
                "You have access to two data systems:\n"
                "1. ClickHouse — for analytics, aggregations, time-series, anomaly detection\n"
                "2. Neo4j — for graph relationships, contributor networks, influence scoring\n\n"
                "Always pick the right tool based on the question type. "
                "For relationship questions use Neo4j. "
                "For metrics and trends use ClickHouse. "
                "Give clear, concise answers with specific numbers."
            ),
            tools=TOOLS,
            messages=messages
        )

        # End of conversation
        if resp.stop_reason == "end_turn":
            answer = next(
                (b.text for b in resp.content if b.type == "text"),
                "No answer generated"
            )
            print(f"\nAnswer: {answer}")
            return answer

        # Execute tool calls
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = run_tool(block.name, block.input)
                tool_results.append({
                    "type":        "tool_result",
                    "tool_use_id": block.id,
                    "content":     result,
                })

        # Continue conversation with tool results
        messages += [
            {"role": "assistant", "content": resp.content},
            {"role": "user",      "content": tool_results},
        ]

# ── Test questions ────────────────────────────────────────
if __name__ == "__main__":
    questions = [
        # ClickHouse questions
        "Which repo had the most push events?",
        "Are there any contributor anomalies in the last 24 hours?",
        "What is the hourly event trend today?",

        # Neo4j questions
        "Which contributors work across multiple repos?",
        "Which repo is most influential based on contributor network?",
        "Which two repos share the most contributors?",

        # Ambiguous — let Claude decide
        "Who are the top 5 most active contributors and which repos do they work on?",
    ]

    for q in questions:
        ask(q)
        print()