"""Bronze ingestion for the Finance domain: `fx_rates`, `invoices`, `expenses`.

All three are daily batches stored as Hive-style folders (`date=YYYY-MM-DD/part-000.*`).
Each run reads only the folder for `batch_date` and replaces only that day's partition
in Bronze (`replaceWhere`), so re-running a date never duplicates rows.

`fx_rates` is delivered on weekdays only: a missing weekend file is expected and skipped.
A missing file for any other dataset is logged and skipped too; the Bronze DQ gate
(`check_non_empty_partition`) is what decides whether that should fail the pipeline.

Raw values are kept as delivered: invoice dates stay strings in both formats
(`YYYY-MM-DD` and `DD.MM.YYYY`), negative invoice amounts (credit notes) are kept.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 4.2.A) and `ingest_clients.py`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), nullable=True),
        StructField("currency_from", StringType(), nullable=True),
        StructField("currency_to", StringType(), nullable=True),
        StructField("rate", DoubleType(), nullable=True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        # Mixed formats (ISO and DD.MM.YYYY): kept as raw strings, parsed in Silver.
        StructField("issued_at", StringType(), nullable=True),
        StructField("paid_at", StringType(), nullable=True),
    ]
)

# CSV columns are read as strings so nothing is silently cast in Bronze.
EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField("exp_id", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("expense_date", StringType(), nullable=True),
    ]
)


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check a path through Hadoop's FileSystem (works for local disk and S3/MinIO)."""
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(jvm_path)


def add_bronze_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    """Append the three standard Bronze technical columns."""
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def write_daily_partition(df: DataFrame, target_path: str, batch_date: str) -> None:
    """Replace only the `_batch_date` partition being loaded (idempotent re-runs)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .partitionBy("_batch_date")
        .save(target_path)
    )


def ingest_daily(
    spark: SparkSession,
    dataset: str,
    file_name: str,
    reader,
    batch_date: str,
    landing: str,
    lakehouse: str,
) -> int:
    """Read one dataset's file for `batch_date` and write it to Bronze; return the row count."""
    source_path = join_path(landing, dataset, f"date={batch_date}", file_name)
    target_path = join_path(lakehouse, "bronze", dataset)

    if not path_exists(spark, source_path):
        print(f"No {dataset} file for batch_date={batch_date} ({source_path}); skipping.")
        return 0

    bronze_df = add_bronze_metadata(reader(source_path), batch_date)
    write_daily_partition(bronze_df, target_path, batch_date)
    rows = bronze_df.count()
    print(f"Ingested {rows} rows into bronze/{dataset} for batch_date={batch_date}")
    return rows


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest `fx_rates`, `invoices` and `expenses` for one day; return row counts per table."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    # Each file is one JSON array over many lines, so multiLine is required.
    def read_fx_rates(path: str) -> DataFrame:
        return spark.read.option("multiLine", "true").schema(FX_RATES_RAW_SCHEMA).json(path)

    # JSON Lines: one invoice object per line.
    def read_invoices(path: str) -> DataFrame:
        return spark.read.schema(INVOICES_RAW_SCHEMA).json(path)

    def read_expenses(path: str) -> DataFrame:
        return spark.read.option("header", "true").schema(EXPENSES_RAW_SCHEMA).csv(path)

    return {
        "fx_rates": ingest_daily(
            spark, "fx_rates", "part-000.json", read_fx_rates, batch_date, landing, lakehouse
        ),
        "invoices": ingest_daily(
            spark, "invoices", "part-000.jsonl", read_invoices, batch_date, landing, lakehouse
        ),
        "expenses": ingest_daily(
            spark, "expenses", "part-000.csv", read_expenses, batch_date, landing, lakehouse
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
