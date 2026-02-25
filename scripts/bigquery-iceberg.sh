#!/bin/bash
# register_iceberg_api.sh
# Automates Iceberg table registration using pure direct BigLake REST API calls (curl).
# No dependencies on PySpark or gcloud for resource lifecycle.

PROJECT_ID=${GCP_PROJECT:-"848136119615"}
LOCATION=${LOCATION:-"us-central1"}
METADATA_PATH=$1

if [ -z "$METADATA_PATH" ]; then
    echo "Usage: LOCATION=region $0 gs://path/to/metadata.json"
    exit 1
fi

# 1. Derive names from GCS path
# Logic: gs://bucket/namespace/table/metadata/v.json
BUCKET=$(echo $METADATA_PATH | cut -d'/' -f3)
CATALOG_NAME=$BUCKET
NAMESPACE=$(echo $METADATA_PATH | cut -d'/' -f4)
TABLE_NAME=$(echo $METADATA_PATH | cut -d'/' -f5)

# API Base URLs
BIGLAKE_API="https://biglake.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION/catalogs"
# Note: Iceberg REST API uses a global endpoint with prefix-based routing
ICEBERG_REST_API="https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/projects/$PROJECT_ID/catalogs/$CATALOG_NAME"

echo "--- Registration Info ---"
echo "Project:   $PROJECT_ID"
echo "Location:  $LOCATION"
echo "Catalog:   $CATALOG_NAME"
echo "Namespace: $NAMESPACE"
echo "Table:     $TABLE_NAME"
echo "--------------------------"

TOKEN=$(gcloud auth print-access-token)
if [ $? -ne 0 ]; then echo "Error: Failed to get auth token."; exit 1; fi

# 2. Ensure Catalog exists via direct curl
echo "Step 1: Ensuring Catalog '$CATALOG_NAME' exists..."
CHECK_CAT_URL="$BIGLAKE_API/$CATALOG_NAME"
CAT_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $TOKEN" "$CHECK_CAT_URL")

if [ "$CAT_HTTP_CODE" == "200" ]; then
    echo "Response: HTTP 200 (Catalog exists). No-op."
else
    echo "Response: HTTP $CAT_HTTP_CODE (Catalog not found). Creating..."
    CREATE_CAT_URL="$BIGLAKE_API?catalog_id=$CATALOG_NAME"
    curl -s -X POST \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "{}" \
        "$CREATE_CAT_URL" | grep -v "^$"
fi

# 3. Handle Namespace via direct curl
echo -e "\nStep 2: Ensuring Namespace '$NAMESPACE' exists..."
CHECK_NS_URL="$ICEBERG_REST_API/namespaces/$NAMESPACE"
NS_HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer $TOKEN" \
    -H "x-goog-user-project: $PROJECT_ID" \
    "$CHECK_NS_URL")

if [ "$NS_HTTP_CODE" == "200" ]; then
    echo "Response: HTTP 200 (Namespace exists). No-op."
else
    echo "Response: HTTP $NS_HTTP_CODE (Namespace not found). Creating..."
    CREATE_NS_URL="$ICEBERG_REST_API/namespaces"
    # Iceberg spec: {"namespace": ["name"]}
    CREATE_NS_DATA="{\"namespace\": [\"$NAMESPACE\"]}"
    curl -s -X POST \
        -H "Authorization: Bearer $TOKEN" \
        -H "x-goog-user-project: $PROJECT_ID" \
        -H "Content-Type: application/json" \
        -d "$CREATE_NS_DATA" \
        "$CREATE_NS_URL" | grep -v "^$"
fi

# 4. Register Table via direct curl
echo -e "\nStep 3: Registering Table '$TABLE_NAME'..."
# The correct endpoint for BigLake Iceberg registration
REG_URL="$ICEBERG_REST_API/namespaces/$NAMESPACE/register"
REG_DATA="{\"name\": \"$TABLE_NAME\", \"metadata-location\": \"$METADATA_PATH\"}"

REG_RESPONSE=$(curl -s -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "x-goog-user-project: $PROJECT_ID" \
    -H "Content-Type: application/json" \
    -d "$REG_DATA" \
    "$REG_URL")

echo "Response: $REG_RESPONSE"

# 5. Fetch Final Details
echo -e "\nStep 4: Fetching Final Table Metadata..."
GET_TABLE_URL="$ICEBERG_REST_API/namespaces/$NAMESPACE/tables/$TABLE_NAME"
curl -s -H "Authorization: Bearer $TOKEN" -H "x-goog-user-project: $PROJECT_ID" "$GET_TABLE_URL"