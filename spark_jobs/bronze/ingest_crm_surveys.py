"""Load daily CRM events and employee survey responses into Bronze Delta."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_jobs.bronze._ingestion import batch_day, raw_strings, read_batch, write_bronze
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
)

# Retain nested objects and arrays as JSON text so Silver can choose how to parse them.
CRM_EVENTS_RAW_SCHEMA = raw_strings("event_id", "client_id", "event_type", "timestamp", "payload")
EMPLOYEE_SURVEYS_RAW_SCHEMA = raw_strings(
    "survey_id", "emp_id", "submitted_at", "enps_score", "overall_comment", "answers"
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Replace the requested day, allowing days without a survey delivery."""
    day = batch_day(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)
    events = read_batch(
        spark.read.format("json").option("mode", "FAILFAST").schema(CRM_EVENTS_RAW_SCHEMA),
        landing,
        "crm_events",
        day,
        "part-000.jsonl",
    )
    surveys = read_batch(
        spark.read.format("json").option("mode", "FAILFAST").schema(EMPLOYEE_SURVEYS_RAW_SCHEMA),
        landing,
        "employee_surveys",
        day,
        "part-000.jsonl",
        optional=True,
    )
    write_bronze(events, lakehouse, "crm_events", day)
    write_bronze(surveys, lakehouse, "employee_surveys", day)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest CRM and survey datasets into Bronze Delta."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-crm-surveys")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
