"""Pure PySpark transformations for the wind-turbine pipeline.

The functions in this module intentionally accept and return DataFrames rather
than performing I/O. This keeps the business logic easy to unit test.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate_readings(df: DataFrame) -> DataFrame:
    """Keep one record per turbine/timestamp.

    The source files are appended over time, so reprocessing must not create
    duplicate measurements. Conflicting duplicate values would be quarantined
    in a production implementation; for this PoC, exact keys are deduplicated.
    """
    return df.dropDuplicates(["turbine_id", "timestamp"])


def add_expected_hourly_grid(df: DataFrame) -> DataFrame:
    """Create the expected hourly timeline for every turbine.

    Missing sensor entries appear as rows with null measurements and
    ``is_missing_record=True``. The global min/max timestamp is suitable for
    this fixed monthly PoC where all turbines are expected to report hourly.
    """
    bounds = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts"),
    )

    expected_times = bounds.select(
        F.explode(
            F.sequence(
                F.col("min_ts"),
                F.col("max_ts"),
                F.expr("INTERVAL 1 HOUR"),
            )
        ).alias("timestamp")
    )

    turbines = df.select("turbine_id").where(F.col("turbine_id").isNotNull()).distinct()
    expected = turbines.crossJoin(expected_times)

    # A presence marker distinguishes an entirely absent sensor row from a
    # row that arrived but happened to contain null measurement values.
    actual = df.withColumn("_record_present", F.lit(True))
    joined = expected.join(actual, ["turbine_id", "timestamp"], "left")

    return joined.withColumn(
        "is_missing_record", F.col("_record_present").isNull()
    ).drop("_record_present")


def flag_and_null_outliers(df: DataFrame, iqr_multiplier: float = 1.5) -> DataFrame:
    """Flag implausible readings and replace them with null before imputation.

    * wind_direction uses its known physical domain [0, 360).
    * wind_speed and power_output use per-turbine IQR thresholds, avoiding a
      hard-coded turbine capacity that the assessment did not provide.
    * negative wind speed/power are always invalid.

    This stage handles bad *individual sensor readings*. It is intentionally
    separate from fleet-level performance anomaly detection.
    """
    stats = df.groupBy("turbine_id").agg(
        F.percentile_approx("wind_speed", 0.25).alias("ws_q1"),
        F.percentile_approx("wind_speed", 0.75).alias("ws_q3"),
        F.percentile_approx("power_output", 0.25).alias("po_q1"),
        F.percentile_approx("power_output", 0.75).alias("po_q3"),
    )

    with_stats = (
        df.join(stats, "turbine_id", "left")
        .withColumn("ws_iqr", F.col("ws_q3") - F.col("ws_q1"))
        .withColumn("po_iqr", F.col("po_q3") - F.col("po_q1"))
        .withColumn("ws_lower", F.col("ws_q1") - F.lit(iqr_multiplier) * F.col("ws_iqr"))
        .withColumn("ws_upper", F.col("ws_q3") + F.lit(iqr_multiplier) * F.col("ws_iqr"))
        .withColumn("po_lower", F.col("po_q1") - F.lit(iqr_multiplier) * F.col("po_iqr"))
        .withColumn("po_upper", F.col("po_q3") + F.lit(iqr_multiplier) * F.col("po_iqr"))
    )

    with_flags = (
        with_stats.withColumn(
            "wind_speed_outlier",
            F.col("wind_speed").isNotNull()
            & (
                (F.col("wind_speed") < 0)
                | (F.col("wind_speed") < F.col("ws_lower"))
                | (F.col("wind_speed") > F.col("ws_upper"))
            ),
        )
        .withColumn(
            "power_output_outlier",
            F.col("power_output").isNotNull()
            & (
                (F.col("power_output") < 0)
                | (F.col("power_output") < F.col("po_lower"))
                | (F.col("power_output") > F.col("po_upper"))
            ),
        )
        .withColumn(
            "wind_direction_outlier",
            F.col("wind_direction").isNotNull()
            & ((F.col("wind_direction") < 0) | (F.col("wind_direction") >= 360)),
        )
    )

    cleaned = (
        with_flags.withColumn(
            "wind_speed_clean_raw",
            F.when(F.col("wind_speed_outlier"), F.lit(None).cast("double")).otherwise(
                F.col("wind_speed")
            ),
        )
        .withColumn(
            "power_output_clean_raw",
            F.when(F.col("power_output_outlier"), F.lit(None).cast("double")).otherwise(
                F.col("power_output")
            ),
        )
        .withColumn(
            "wind_direction_clean_raw",
            F.when(F.col("wind_direction_outlier"), F.lit(None).cast("int")).otherwise(
                F.col("wind_direction")
            ),
        )
    )

    return cleaned.drop(
        "ws_q1",
        "ws_q3",
        "po_q1",
        "po_q3",
        "ws_iqr",
        "po_iqr",
        "ws_lower",
        "ws_upper",
        "po_lower",
        "po_upper",
    )


def impute_missing_values(df: DataFrame) -> DataFrame:
    """Impute null readings while preserving data-quality flags.

    Continuous values use a robust turbine/day median, then a turbine-level
    median fallback. Wind direction is circular, so a numeric median is not
    appropriate; instead the nearest available previous/next reading is used.
    """
    df = df.withColumn("event_date", F.to_date("timestamp"))

    daily_medians = df.groupBy("turbine_id", "event_date").agg(
        F.percentile_approx("wind_speed_clean_raw", 0.5).alias("daily_ws_median"),
        F.percentile_approx("power_output_clean_raw", 0.5).alias("daily_po_median"),
    )
    turbine_medians = df.groupBy("turbine_id").agg(
        F.percentile_approx("wind_speed_clean_raw", 0.5).alias("turbine_ws_median"),
        F.percentile_approx("power_output_clean_raw", 0.5).alias("turbine_po_median"),
    )

    enriched = df.join(daily_medians, ["turbine_id", "event_date"], "left").join(
        turbine_medians, "turbine_id", "left"
    )

    previous_window = (
        Window.partitionBy("turbine_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    next_window = (
        Window.partitionBy("turbine_id")
        .orderBy("timestamp")
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    enriched = (
        enriched.withColumn(
            "previous_direction",
            F.last("wind_direction_clean_raw", ignorenulls=True).over(previous_window),
        )
        .withColumn(
            "next_direction",
            F.first("wind_direction_clean_raw", ignorenulls=True).over(next_window),
        )
        .withColumn(
            "wind_speed_clean",
            F.coalesce("wind_speed_clean_raw", "daily_ws_median", "turbine_ws_median"),
        )
        .withColumn(
            "power_output_clean",
            F.coalesce("power_output_clean_raw", "daily_po_median", "turbine_po_median"),
        )
        .withColumn(
            "wind_direction_clean",
            F.coalesce("wind_direction_clean_raw", "previous_direction", "next_direction"),
        )
        .withColumn(
            "wind_speed_imputed",
            F.col("wind_speed_clean_raw").isNull() & F.col("wind_speed_clean").isNotNull(),
        )
        .withColumn(
            "power_output_imputed",
            F.col("power_output_clean_raw").isNull() & F.col("power_output_clean").isNotNull(),
        )
        .withColumn(
            "wind_direction_imputed",
            F.col("wind_direction_clean_raw").isNull()
            & F.col("wind_direction_clean").isNotNull(),
        )
    )

    return enriched.drop(
        "daily_ws_median",
        "daily_po_median",
        "turbine_ws_median",
        "turbine_po_median",
        "previous_direction",
        "next_direction",
    )


def calculate_daily_summary(cleaned_df: DataFrame) -> DataFrame:
    """Calculate 24-hour summary and data quality statistics per turbine."""
    return (
        cleaned_df.groupBy("event_date", "turbine_id")
        .agg(
            F.min("power_output_clean").alias("min_power_output_mw"),
            F.max("power_output_clean").alias("max_power_output_mw"),
            F.avg("power_output_clean").alias("avg_power_output_mw"),
            F.stddev_samp("power_output_clean").alias("stddev_power_output_mw"),
            F.count(F.lit(1)).alias("expected_reading_count"),
            F.sum(F.when(~F.col("is_missing_record"), 1).otherwise(0)).alias(
                "observed_reading_count"
            ),
            F.sum(F.col("power_output_imputed").cast("int")).alias(
                "imputed_power_reading_count"
            ),
            F.sum(F.col("power_output_outlier").cast("int")).alias(
                "power_outlier_reading_count"
            ),
        )
        .withColumn(
            "completeness_pct",
            F.round(
                F.col("observed_reading_count")
                / F.col("expected_reading_count")
                * F.lit(100.0),
                2,
            ),
        )
    )


def identify_fleet_anomalies(summary_df: DataFrame, stddev_threshold: float = 2.0) -> DataFrame:
    """Flag turbine/day averages outside fleet mean ± N standard deviations.

    This interprets "expected power output over the same time period" as the
    peer fleet's distribution of turbine 24-hour average outputs for that day.
    """
    daily_fleet = summary_df.groupBy("event_date").agg(
        F.avg("avg_power_output_mw").alias("fleet_mean_power_mw"),
        F.stddev_samp("avg_power_output_mw").alias("fleet_stddev_power_mw"),
    )

    with_baseline = summary_df.join(daily_fleet, "event_date", "left")
    lower = F.col("fleet_mean_power_mw") - F.lit(stddev_threshold) * F.col(
        "fleet_stddev_power_mw"
    )
    upper = F.col("fleet_mean_power_mw") + F.lit(stddev_threshold) * F.col(
        "fleet_stddev_power_mw"
    )

    return (
        with_baseline.withColumn("anomaly_lower_bound_mw", lower)
        .withColumn("anomaly_upper_bound_mw", upper)
        .withColumn(
            "z_score",
            F.when(
                F.col("fleet_stddev_power_mw") > 0,
                (F.col("avg_power_output_mw") - F.col("fleet_mean_power_mw"))
                / F.col("fleet_stddev_power_mw"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "is_anomaly",
            F.when(
                F.col("fleet_stddev_power_mw").isNull()
                | (F.col("fleet_stddev_power_mw") == 0),
                F.lit(False),
            ).otherwise(
                (F.col("avg_power_output_mw") < lower)
                | (F.col("avg_power_output_mw") > upper)
            ),
        )
    )
