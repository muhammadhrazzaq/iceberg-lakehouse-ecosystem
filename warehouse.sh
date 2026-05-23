#!/bin/bash
set -e

export $(cat .env | grep -v '#' | grep -v '^$' | xargs)

echo "--- Bootstrapping Lakekeeper ---"

# Health check
curl -sf http://localhost:8181/health > /dev/null \
  && echo "  ✓ Lakekeeper is up" \
  || { echo "  ✗ Lakekeeper not reachable"; exit 1; }

# Create project
echo "Creating project..."
curl -s -X POST http://localhost:8181/management/v1/project \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "project-id": "00000000-0000-0000-0000-000000000000",
  "project-name": "nexus"
}
EOF
echo ""

# Create warehouse
echo "Creating warehouse..."
curl -s -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "warehouse-name": "nexus",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3",
    "bucket": "${S3_BUCKET}",
    "region": "${AWS_REGION}",
    "flavor": "aws",
    "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "${AWS_ACCESS_KEY_ID}",
    "aws-secret-access-key": "${AWS_SECRET_ACCESS_KEY}"
  }
}
EOF
echo ""

# Get warehouse prefix dynamically
echo "Fetching warehouse prefix..."
WAREHOUSE_PREFIX=$(curl -s "http://localhost:8181/catalog/v1/config?warehouse=nexus" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['defaults']['prefix'])")
echo "  ✓ Prefix: ${WAREHOUSE_PREFIX}"

# Create github namespace
echo "Creating github namespace..."
curl -s -X POST \
  "http://localhost:8181/catalog/v1/${WAREHOUSE_PREFIX}/namespaces" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["github"], "properties": {}}' \
  | python3 -m json.tool
echo ""

# Verify
echo "Verifying..."
curl -s "http://localhost:8181/catalog/v1/${WAREHOUSE_PREFIX}/namespaces" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('  ✓ Namespaces:', d.get('namespaces', []))
"

echo ""
echo "--- Done ---"