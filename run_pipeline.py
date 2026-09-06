"""CLI entry point for the wind turbine take-home assessment."""

import argparse

from pyspark.sql import SparkSession

from wind_pipeline.io import read_raw_csvs, validate_required_keys, write_managed_tables
from wind_pipeline.transformations import (
    add_expected_hourly_grid,
    calculate_daily_summary,
    deduplicate_readings,
    flag_and_null_outliers,
    identify_fleet_anomalies,
    impute_missing_values,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process wind turbine sensor data")
    parser.add_argument(
        "--input",
        default="data/*.csv",
        help="Input CSV glob/path (default: data/*.csv)",
    )
    parser.add_argument(
        "--database",
        default="wind_turbine",
        help="Spark SQL database for processed tables",
    )
    parser.add_argument(
        "--anomaly-stddev",
        type=float,
        default=2.0,
        help="Fleet anomaly threshold in standard deviations",
    )
    parser.add_argument(
        "--iqr-multiplier",
        type=float,
        default=1.5,
        help="IQR multiplier used for bad-reading outlier cleaning",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("wind-turbine-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = validate_required_keys(read_raw_csvs(spark, args.input))
    deduped = deduplicate_readings(raw)
    complete_grid = add_expected_hourly_grid(deduped)
    cleaned_bad_values = flag_and_null_outliers(complete_grid, args.iqr_multiplier)
    cleaned = impute_missing_values(cleaned_bad_values)
    daily_summary = calculate_daily_summary(cleaned)
    summary_with_anomalies = identify_fleet_anomalies(
        daily_summary, args.anomaly_stddev
    )

    write_managed_tables(spark, cleaned, summary_with_anomalies, args.database)

    print("\nPipeline complete")
    print(f"Database: {args.database}")
    print(f"Cleaned table: {args.database}.cleaned_turbine_readings")
    print(f"Summary table: {args.database}.daily_turbine_summary")
    print("\nDetected anomalies:")
    (
        summary_with_anomalies.where("is_anomaly = true")
        .select(
            "event_date",
            "turbine_id",
            "avg_power_output_mw",
            "fleet_mean_power_mw",
            "z_score",
            "completeness_pct",
        )
        .orderBy("event_date", "turbine_id")
        .show(100, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
