#!/bin/bash
set -e

SCHEMA_FILE="schema/github_events.json"
SUBJECT="github-events-value"
REGISTRY="http://localhost:8081"

echo "--- Registering Avro Schema ---"
echo "Schema file: ${SCHEMA_FILE}"
echo "Subject:     ${SUBJECT}"
echo ""

# Check schema file exists
[ -f "$SCHEMA_FILE" ] \
  && echo "  ✓ Schema file found" \
  || { echo "  ✗ Schema file not found: ${SCHEMA_FILE}"; exit 1; }

# Check schema registry is up
curl -sf "${REGISTRY}/subjects" > /dev/null \
  && echo "  ✓ Schema registry is up" \
  || { echo "  ✗ Schema registry not reachable"; exit 1; }

echo ""

# Wrap the raw Avro JSON into the registry payload format
# Registry expects: {"schema": "<escaped json string>"}
SCHEMA_STRING=$(cat "$SCHEMA_FILE" | python3 -c "
import sys, json
raw = sys.stdin.read()
# Validate it's valid JSON first
parsed = json.loads(raw)
# Wrap as escaped string inside registry envelope
print(json.dumps({'schema': json.dumps(parsed)}))
")

# Register schema
echo "Registering schema..."
echo "$SCHEMA_STRING" | curl -s -X POST \
  "${REGISTRY}/subjects/${SUBJECT}/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @-
echo ""

# Set BACKWARD compatibility
echo "Setting BACKWARD compatibility..."
curl -s -X PUT "${REGISTRY}/config/${SUBJECT}" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility":"BACKWARD"}'
echo ""

# Verify
echo "Verifying registration..."
curl -s "${REGISTRY}/subjects/${SUBJECT}/versions/latest" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
schema = json.loads(d['schema'])
print(f'  ✓ Subject  : {d[\"subject\"]}')
print(f'  ✓ Schema ID: {d[\"id\"]}')
print(f'  ✓ Version  : {d[\"version\"]}')
print(f'  ✓ Fields   : {[f[\"name\"] for f in schema[\"fields\"]]}')
"

curl -s "${REGISTRY}/config/${SUBJECT}" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'  ✓ Compatibility: {d[\"compatibilityLevel\"]}')
"

echo ""
echo "--- Done ---"