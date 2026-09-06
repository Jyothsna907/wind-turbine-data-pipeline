"""Input/output helpers for the PoC pipeline."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wind_pipeline.schema import RAW_SCHEMA


def read_raw_csvs(spark: SparkSession, input_path: str) -> DataFrame:
    """Read all turbine CSVs with an explicit schema."""
    return (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(RAW_SCHEMA)
        .csv(input_path)
        .withColumn("source_file", F.input_file_name())
    )


def validate_required_keys(df: DataFrame) -> DataFrame:
    """Reject rows that cannot be uniquely associated with a turbine/time."""
    return df.where(F.col("timestamp").isNotNull() & F.col("turbine_id").isNotNull())


def write_managed_tables(
    spark: SparkSession,
    cleaned_df: DataFrame,
    summary_df: DataFrame,
    database_name: str,
) -> None:
    """Persist PoC outputs into Spark SQL managed tables.

    Full overwrite is intentional for the one-month take-home dataset and makes
    reruns idempotent. Productionisation notes in the README describe replacing
    this with partition-level MERGE/upserts.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")

    (
        cleaned_df.write.mode("overwrite")
        .partitionBy("event_date")
        .saveAsTable(f"{database_name}.cleaned_turbine_readings")
    )
    (
        summary_df.write.mode("overwrite")
        .partitionBy("event_date")
        .saveAsTable(f"{database_name}.daily_turbine_summary")
    )
