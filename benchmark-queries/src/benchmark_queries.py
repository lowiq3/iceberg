"""Benchmark queries in BigQuery."""

import argparse
import collections
from collections.abc import Sequence
import csv
import dataclasses
import datetime
import json
import logging
import os
import time

import google.auth
from google.auth import exceptions as google_auth_exceptions
from google.cloud import bigquery
import gspread
from gspread import exceptions as gspread_exceptions


@dataclasses.dataclass(frozen=True)
class Query:
  name: str
  sql: str


@dataclasses.dataclass
class QueryExecution:
  """Represents a single execution of a query."""

  query: Query
  start_time: datetime.datetime
  duration_ms: int
  iteration_index: int
  run_mode: str
  job_id: str
  total_slot_millis: int


def _load_queries(query_dir: str) -> Sequence[Query]:
  """Loads queries from a directory."""
  queries = []
  for query_file in os.listdir(query_dir):
    if not query_file.endswith(".sql"):
      continue
    with open(os.path.join(query_dir, query_file), "r") as f:
      query_sql = f.read()
      query_name = query_file.split(".")[0]
      queries.append(Query(name=query_name, sql=query_sql))
  # Sort queries by name to ensure consistent execution order.
  return sorted(queries, key=lambda q: q.name)


def _add_query_identification_comment(
    run_id: str,
    sql: str,
    query_name: str,
    iteration_index: int,
    run_mode: str,
) -> str:
  """Generates a query identification comment for the given query."""
  header_comment = (
      f"/* run_id={run_id}, run_mode={run_mode}, iter={iteration_index},"
      f" query={query_name} */"
  )
  return f"{header_comment}\n{sql}"


def _execute_query(
    run_id: str,
    client: bigquery.Client,
    query_execution: QueryExecution,
) -> None:
  """Executes a query and returns the results."""
  start_time = datetime.datetime.now()
  start_time_monotonic = time.monotonic()
  result = client.query_and_wait(
      _add_query_identification_comment(
          run_id,
          query_execution.query.sql,
          query_execution.query.name,
          query_execution.iteration_index,
          query_execution.run_mode,
      ),
      max_results=0,
  )
  end_time_monotonic = time.monotonic()
  query_execution.start_time = start_time
  query_execution.duration_ms = round(
      (end_time_monotonic - start_time_monotonic) * 1000.0
  )
  query_execution.job_id = f"{result.project}:{result.location}.{result.job_id}"
  query_execution.total_slot_millis = result.slot_millis


def _generate_query_executions(
    run_mode: str,
    iteration_count: int,
    queries: Sequence[Query],
) -> Sequence[QueryExecution]:
  """Generates query executions for the given query/strategy."""
  query_executions = []
  for iteration_index in range(1, iteration_count + 1):
    for query in queries:
      query_execution = QueryExecution(
          query=query,
          start_time=None,
          duration_ms=0,
          iteration_index=iteration_index,
          run_mode=run_mode,
          job_id=None,
          total_slot_millis=0,
      )
      query_executions.append(query_execution)
  return query_executions


def _execute_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference | None,
    queries: Sequence[Query],
    iteration_count: int,
    run_mode: str,
) -> Sequence[QueryExecution]:
  """Executes queries and returns the results."""
  query_config = bigquery.job.QueryJobConfig(
      default_dataset=default_dataset,
      use_legacy_sql=False,
      use_query_cache=False,
  )
  client = bigquery.Client(
      project=project_id, default_query_job_config=query_config
  )
  query_executions = _generate_query_executions(
      run_mode, iteration_count, queries
  )
  for query_execution in query_executions:
    _execute_query(
        run_id,
        client,
        query_execution,
    )
    logging.info(
        "Executed query: %s, iteration: %d, run mode: %s, client time: %dms,"
        " total slot time: %dms",
        query_execution.query.name,
        query_execution.iteration_index,
        query_execution.run_mode,
        query_execution.duration_ms,
        query_execution.total_slot_millis,
    )
  return query_executions


def _spreadsheet_row_from_execution(
    qe: QueryExecution, include_sql: bool = False
) -> dict[str, object]:
  """Converts a QueryExecution object to a dictionary for spreadsheet export."""
  return {
      "query": qe.query.name,
      "start_time": qe.start_time.isoformat(),
      "total_slot_millis": qe.total_slot_millis,
      "job_id": qe.job_id,
  } | ({"sql": qe.query.sql} if include_sql else {})


def _export_query_execution_details_to_csv(
    query_executions: Sequence[QueryExecution], output_file: str
):
  """Exports query executions to a CSV file."""
  if not query_executions:
    return
  with open(output_file, "w", newline="") as csvfile:
    fieldnames = list(
        _spreadsheet_row_from_execution(query_executions[0]).keys()
    )
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for query_execution in query_executions:
      writer.writerow(_spreadsheet_row_from_execution(query_execution))


def _select_median_query_executions(
    query_executions: Sequence[QueryExecution],
) -> Sequence[QueryExecution]:
  """Selects the median query executions for each query."""
  query_executions_by_name = collections.defaultdict(list)
  for qe in query_executions:
    query_executions_by_name[qe.query.name].append(qe)
  median_query_executions = []
  for query_name in sorted(query_executions_by_name.keys()):
    query_execution_list = query_executions_by_name[query_name]
    query_execution_list.sort(key=lambda qe: qe.total_slot_millis)
    median_query_executions.append(
        query_execution_list[len(query_execution_list) // 2]
    )
  return median_query_executions


def _update_worksheet_with_executions(
    worksheet: gspread.Worksheet,
    query_executions: Sequence[QueryExecution],
    include_sql: bool,
):
  """Updates a worksheet with query execution data."""
  rows = [
      _spreadsheet_row_from_execution(qe, include_sql=include_sql)
      for qe in query_executions
  ]
  header = list(rows[0].keys())
  data = [header] + [[row[col] for col in header] for row in rows]
  worksheet.update(data)
  worksheet.freeze(rows=1)


def _export_to_google_sheet(
    run_id: str,
    query_executions: Sequence[QueryExecution],
) -> None:
  """Exports query executions to a new Google Sheet."""
  try:
    creds, _ = google.auth.default()
    gc = gspread.authorize(creds)
    spreadsheet = gc.create(f"Benchmark Queries Report: {run_id}")
    logging.info("Exporting to Google Sheet: %s", spreadsheet.url)
    median_ws = spreadsheet.add_worksheet(
        title="Query Executions", rows=1, cols=1
    )
    _update_worksheet_with_executions(median_ws, query_executions, True)
    spreadsheet.del_worksheet(spreadsheet.sheet1)
  except (
      gspread_exceptions.APIError,
      google_auth_exceptions.DefaultCredentialsError,
  ) as e:
    logging.error("Failed to export to Google Sheet: %s", e)


def _export_csv_reports(
    run_id: str,
    query_executions: Sequence[QueryExecution],
) -> None:
  """Exports query executions to a CSV file."""
  reports_base_dir = "./reports"
  run_dir = os.path.join(reports_base_dir, run_id)
  if not os.path.exists(run_dir):
    os.makedirs(run_dir)
  logging.info("Exporting reports to: %s", run_dir)
  _export_query_execution_details_to_csv(
      query_executions,
      os.path.join(run_dir, "query_executions.csv"),
  )


def _execute_warmup_iters(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference | None,
    queries: Sequence[Query],
) -> Sequence[QueryExecution]:
  """Executes warmup runs."""
  query_executions = _execute_queries(
      run_id,
      project_id,
      default_dataset,
      queries,
      1,
      "warmup",
  )
  return query_executions


def _execute_test_iters(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference | None,
    queries: Sequence[Query],
    test_iters: int,
) -> Sequence[QueryExecution]:
  """Executes test runs."""
  query_executions = _execute_queries(
      run_id,
      project_id,
      default_dataset,
      queries,
      test_iters,
      "test",
  )
  return query_executions


def _process_results(
    run_id: str,
    test_query_executions: Sequence[QueryExecution],
):
  """Processes the results of the query executions."""
  median_query_executions = _select_median_query_executions(
      test_query_executions
  )
  median_time = sum(qe.duration_ms for qe in median_query_executions) / 1000.0
  logging.info("Median run total client-time: %.02fs", median_time)
  logging.info("Run ID: %s", run_id)
  _export_csv_reports(run_id, median_query_executions)
  _export_to_google_sheet(run_id, median_query_executions)


def _run_queries(
    run_id: str,
    project_id: str,
    default_dataset: bigquery.dataset.DatasetReference | None,
    query_dir: str,
    warmup: bool,
    test_iters: int = 5,
) -> None:
  """Runs queries and exports results to a CSV file."""
  logging.info(
      "test project id: '%s'; default dataset: '%s'; query directory: '%s';",
      project_id,
      default_dataset,
      query_dir,
  )
  queries = _load_queries(query_dir)
  if warmup:
    _execute_warmup_iters(
        run_id,
        project_id,
        default_dataset,
        queries,
    )
  test_query_executions = _execute_test_iters(
      run_id,
      project_id,
      default_dataset,
      queries,
      test_iters,
  )
  _process_results(
      run_id,
      test_query_executions,
  )


def main() -> None:
  parser = argparse.ArgumentParser(description="Run BigQuery queries.")
  parser.add_argument(
      "--project_id",
      required=True,
      help="BigQuery project id used to run queries in.",
  )
  parser.add_argument(
      "--default_dataset",
      required=False,
      default=None,
      help=(
          "Default dataset to use for the queries. This dataset would be used"
          " if the table name is not fully qualified in the query to specify"
          " the dataset name."
      ),
  )
  parser.add_argument(
      "--query_dir",
      required=True,
      help=(
          "Directory containing SQL query files (.sql). The queries will be"
          " executed in the order of their file names."
      ),
  )
  parser.add_argument(
      "--warmup",
      type=str,
      default="true",
      choices=["true", "false"],
      help="Run warmup iterations [default=true].",
  )
  parser.add_argument(
      "--test_iters",
      type=int,
      default=5,
      help="Number of test iterations to execute [default=5].",
  )
  args = parser.parse_args()

  logging.basicConfig(
      level="INFO", format="%(asctime)s - %(levelname)s - %(message)s"
  )

  project_id = args.project_id
  default_dataset = None
  if args.default_dataset:
    default_dataset = bigquery.dataset.DatasetReference(
        project=project_id,
        dataset_id=args.default_dataset,
    )
  run_id = f"{project_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
  logging.info("Starting benchmark-queries...")
  _run_queries(
      run_id,
      project_id,
      default_dataset,
      args.query_dir,
      json.loads(args.warmup),
      args.test_iters,
  )
  logging.info("Finished.")


if __name__ == "__main__":
  main()
