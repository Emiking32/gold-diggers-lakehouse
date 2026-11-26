"""Bronze ingestion for the full-load reference datasets `projects` and `survey_questions`.

Both datasets are full loads (one file, no daily partitions), so each run overwrites the
whole Bronze table. That makes re-runs idempotent: running the same batch_date twice
leaves exactly the same table.

Raw values are kept as delivered (no casting, no cleaning); Silver does the typing.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `ingest_clients.py`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

# All CSV columns are read as strings so nothing is silently cast in Bronze.
PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)

# JSON keeps its own types (the scale bounds are whole numbers in the source file).
SURVEY_QUESTIONS_RAW_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("scale_min", LongType(), nullable=True),
        StructField("scale_max", LongType(), nullable=True),
        StructField("prompt_text", StringType(), nullable=True),
    ]
)


def add_bronze_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    """Append the three standard Bronze technical columns."""
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def write_full_load(df: DataFrame, target_path: str) -> None:
    """Overwrite the whole Delta table (idempotent for full-load datasets)."""
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path))


def ingest_projects(
    spark: SparkSession, batch_date: str, landing: str, lakehouse: str
) -> DataFrame:
    source_path = join_path(landing, "projects", "projects.csv")
    target_path = join_path(lakehouse, "bronze", "projects")

    raw_df = spark.read.option("header", "true").schema(PROJECTS_RAW_SCHEMA).csv(source_path)
    bronze_df = add_bronze_metadata(raw_df, batch_date)
    write_full_load(bronze_df, target_path)
    return bronze_df


def ingest_survey_questions(
    spark: SparkSession, batch_date: str, landing: str, lakehouse: str
) -> DataFrame:
    source_path = join_path(landing, "survey_questions", "survey_questions.json")
    target_path = join_path(lakehouse, "bronze", "survey_questions")

    # The file is one JSON array spread over many lines, so multiLine is required.
    raw_df = (
        spark.read.option("multiLine", "true").schema(SURVEY_QUESTIONS_RAW_SCHEMA).json(source_path)
    )
    bronze_df = add_bronze_metadata(raw_df, batch_date)
    write_full_load(bronze_df, target_path)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest `projects` and `survey_questions` into Bronze; return row counts per table."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    projects_df = ingest_projects(spark, batch_date, landing, lakehouse)
    questions_df = ingest_survey_questions(spark, batch_date, landing, lakehouse)

    counts = {"projects": projects_df.count(), "survey_questions": questions_df.count()}
    for table, rows in counts.items():
        print(f"Ingested {rows} rows into bronze/{table} for batch_date={batch_date}")
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
