# Iceberg Table Registration Tool (Pure API)

This directory contains a lightweight script to automate the registration of existing Iceberg tables into the BigLake Iceberg REST Catalog using direct REST API calls.

## Contents

- `bigquery-iceberg.sh`: A bash script that uses `curl` to interact with the BigLake REST API. It handles:
    - Automatic derivation of Catalog and Table names from the provided GCS metadata path.
    - Creation of the Catalog (GCS-bucket type) if it does not exist via BigLake regional REST API.
    - Verification and creation of the Namespace via Iceberg REST Catalog endpoint.
    - Registration of the Table via the Iceberg `/register` REST endpoint.

## Prerequisites

- `gcloud` CLI installed and authenticated (used only for the access token).
- `curl` installed.
- Appropriate permissions on the GCP project and GCS buckets.

## Usage

```bash
$ curl -O https://raw.githubusercontent.com/lowiq3/iceberg/main/scripts/bigquery-iceberg.sh
$ chmod +x bigquery-iceberg.sh
$ gcloud auth login
$ gcloud config set project [YOUR GCP PROJECT]
```

Provide the full GCS path to an Iceberg `metadata.json` file:

```bash
NAMESPACE=my-ns LOCATION=us-central1 ./bigquery-iceberg.sh gs://my-bucket/[foo/../bar]/my-table/metadata/v1.metadata.json
```

## Implementation Details

The script is designed for maximum efficiency by avoiding Spark:
1. **Catalog & Namespace management:** Uses direct REST calls to the BigLake service.
2. **Table Registration:** Uses the Iceberg REST standard `/register` endpoint to ingest existing metadata into the catalog.
3. **Automatic Naming:** Derives Catalog, and Table names from the provided GCS metadata uri.
