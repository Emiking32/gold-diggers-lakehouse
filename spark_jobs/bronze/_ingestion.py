"""Shared read and Delta write operations for the Bronze jobs."""

from datetime import date

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, DataFrameReader
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from spark_jobs.common.spark_session import join_path


def batch_day(value: str) -> date:
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("batch_date must use YYYY-MM-DD")
    return day


def raw_strings(*columns: str) -> StructType:
    """Keep source tokens intact until Silver applies business types."""
    schema = StructType()
    for column in columns:
        schema.add(column, StringType())
    return schema


def read_batch(
    reader: DataFrameReader,
    landing_root: str,
    table: str,
    day: date,
    filename: str,
    *,
    optional: bool = False,
) -> DataFrame | None:
    source = join_path(landing_root, table, f"date={day}", filename)
    try:
        return reader.load(source)
    except AnalysisException as error:
        if not optional or error.getErrorClass() != "PATH_NOT_FOUND":
            raise
        print(f"No {table} delivery for {day}; skipping.")
        return None


def write_bronze(
    raw: DataFrame | None,
    lakehouse_root: str,
    table: str,
    day: date,
    *,
    daily: bool = True,
) -> None:
    if raw is None:
        return

    bronze = (
        raw.withColumn("_batch_date", F.lit(day))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    writer = bronze.write.format("delta").mode("overwrite")
    if daily:
        writer = writer.partitionBy("_batch_date").option(
            "replaceWhere", f"_batch_date = DATE '{day}'"
        )
    writer.save(join_path(lakehouse_root, "bronze", table))
