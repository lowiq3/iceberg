# Benchmark Queries Tool

## Build

* Optional:
  - ```python3 -m venv venv```
  - ```source venv/bin/activate```
* ```pip install -e .```

## Authentication

* ```gcloud init```
* ```gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/drive```

## Query Directory

Create a directory with files with a `.sql` extension, which would represent each query to be executed. This directory when provided to the tool, will execute queries in the sorted order of the file names.

## Run

```
benchmark-queries --project_id=<project_id> --default_dataset=<dataset_id> --query_dir=./queries
```

By default the queries will execute a single warmup run for each query, followed by 5 test iterations, which would be executed in an interleaved manner, i.e. `q1-itr1`, `q2-itr1`, `q1-itr2`, `q2-itr2`, etc...

Each run of the tool will generate a unique `run_id` in order to identify a specific run, which would be used in the reports directories, and in the name of the Google spreadsheet.

Additionally, each query executed would contain an SQL comment header with the following format:

```sql
/* run_id={run_id}, run_mode={run_mode}, iter={iteration_index}, query={query_name} */
```

The above information can be used in case we want to search past executed queries and group them by their run_id etc...

The `run_mode` above specifies if its a `warmup` or `test` execution that has happened.

## Tool Usage

```benchmark-queries -h
usage: benchmark-queries [-h] --project_id PROJECT_ID [--default_dataset DEFAULT_DATASET] --query_dir QUERY_DIR [--warmup_iters WARMUP_ITERS] [--test_iters TEST_ITERS]
```

The resultant reports will contain the following information:

* Query name
* Start time
* Total slot time (milliseconds)
* Job ID
* SQL

```
$ benchmark-queries -h
usage: benchmark-queries [-h] --project_id PROJECT_ID [--default_dataset DEFAULT_DATASET] --query_dir QUERY_DIR [--warmup {true,false}] [--test_iters TEST_ITERS]

Run BigQuery queries.

options:
  -h, --help            show this help message and exit
  --project_id PROJECT_ID
                        BigQuery project id used to run queries in.
  --default_dataset DEFAULT_DATASET
                        Default dataset to use for the queries. This dataset would be used if the table name is not fully qualified in the query to specify the dataset name.
  --query_dir QUERY_DIR
                        Directory containing SQL query files (.sql). The queries will be executed in the order of their file names.
  --warmup {true,false}
                        Run warmup iterations [default=true].
  --test_iters TEST_ITERS
                        Number of test iterations to execute [default=5].
```
