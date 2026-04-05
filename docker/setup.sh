#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== Setting up S3 buckets (RustFS) ==="
# Wait for RustFS health
for i in $(seq 1 30); do
  if curl -sf http://localhost:9000/health >/dev/null 2>&1; then
    echo "RustFS is ready"
    break
  fi
  echo "  waiting for RustFS... (attempt $i)"
  sleep 2
done

# Create S3 buckets via AWS CLI (using RustFS endpoint)
docker run --rm --network=acdc_default \
  -e AWS_ACCESS_KEY_ID=acdc_root \
  -e AWS_SECRET_ACCESS_KEY=acdc_password \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.34.19 \
  --endpoint-url http://rustfs:9000 s3 mb s3://acdc-warehouse 2>/dev/null || true

docker run --rm --network=acdc_default \
  -e AWS_ACCESS_KEY_ID=acdc_root \
  -e AWS_SECRET_ACCESS_KEY=acdc_password \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli:2.34.19 \
  --endpoint-url http://rustfs:9000 s3 mb s3://acdc-state 2>/dev/null || true

echo "S3 buckets created"

echo ""
echo "=== Setting up Polaris catalog ==="

# Wait for Polaris health
for i in $(seq 1 30); do
  if curl -sf http://localhost:8182/q/health >/dev/null 2>&1; then
    echo "Polaris is ready"
    break
  fi
  echo "  waiting for Polaris... (attempt $i)"
  sleep 3
done

# Fixed credentials from POLARIS_BOOTSTRAP_CREDENTIALS
CLIENT_ID="root"
CLIENT_SECRET="s3cr3t"

# Get OAuth token
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "Got OAuth token"

# Create catalog
echo "Creating catalog..."
curl -sf -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "acdc",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://acdc-warehouse/iceberg"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://acdc-warehouse"],
        "endpoint": "http://localhost:9000",
        "endpointInternal": "http://rustfs:9000",
        "pathStyleAccess": true
      }
    }
  }' >/dev/null
echo "  catalog created"

# Create catalog role
curl -sf -X POST http://localhost:8181/api/management/v1/catalogs/acdc/catalog-roles \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "admin"}}' >/dev/null

# Grant role
curl -sf -X PUT http://localhost:8181/api/management/v1/principal-roles/service_admin/catalog-roles/acdc \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "admin"}}' >/dev/null

# Grant privileges
curl -sf -X PUT http://localhost:8181/api/management/v1/catalogs/acdc/catalog-roles/admin/grants \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' >/dev/null
echo "  roles configured"

# Create namespace
curl -sf -X POST http://localhost:8181/api/catalog/v1/acdc/namespaces \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["cdc"]}' >/dev/null
echo "  namespace 'cdc' created"

# Write env file
cat > .env <<EOF
POLARIS_CLIENT_ID=$CLIENT_ID
POLARIS_CLIENT_SECRET=$CLIENT_SECRET
S3_ACCESS_KEY=acdc_root
S3_SECRET_KEY=acdc_password
S3_ENDPOINT=http://localhost:9000
EOF

echo ""
echo "=== Setup complete ==="
echo "RustFS S3:            http://localhost:9000"
echo "RustFS Console:       http://localhost:9001"
echo "Polaris REST catalog: http://localhost:8181/api/catalog"
echo "Polaris credential:   $CLIENT_ID:$CLIENT_SECRET"
echo "MySQL:                localhost:3306 (cdc_user/cdc_password)"
