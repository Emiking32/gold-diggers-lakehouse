"""Load daily employee snapshots and timesheet batches into Bronze Delta."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_jobs.bronze._ingestion import batch_day, raw_strings, read_batch, write_bronze
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
)

EMPLOYEES_RAW_SCHEMA = raw_strings(
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "hourly_rate_eur",
    "hire_date",
    "last_promotion_date",
    "work_mode",
    "distance_km",
    "manager_id",
    "is_active",
    "termination_date",
    "churn_type",
    "snapshot_date",
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Replace one day's HR partitions, preserving duplicates and raw values."""
    day = batch_day(batch_date)
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)
    employees = read_batch(
        spark.read.format("csv")
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .schema(EMPLOYEES_RAW_SCHEMA),
        landing,
        "employees",
        day,
        "part-000.csv",
    )
    timesheets = read_batch(
        spark.read.format("parquet"), landing, "timesheets", day, "part-000.parquet"
    )
    write_bronze(employees, lakehouse, "employees", day)
    write_bronze(timesheets, lakehouse, "timesheets", day)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
