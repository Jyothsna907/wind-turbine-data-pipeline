from datetime import date, datetime, timedelta

from pyspark.sql import Row

from wind_pipeline.transformations import (
    add_expected_hourly_grid,
    identify_fleet_anomalies,
)


def test_expected_grid_detects_missing_sensor_entry(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 100, 2.0, "file1"),
        (datetime(2022, 3, 1, 1), 1, 11.0, 110, 2.2, "file1"),
        (datetime(2022, 3, 1, 2), 1, 12.0, 120, 2.4, "file1"),
        (datetime(2022, 3, 1, 0), 2, 10.0, 100, 2.0, "file1"),
        # turbine 2 is deliberately missing 01:00
        (datetime(2022, 3, 1, 2), 2, 12.0, 120, 2.4, "file1"),
    ]
    df = spark.createDataFrame(
        rows,
        [
            "timestamp",
            "turbine_id",
            "wind_speed",
            "wind_direction",
            "power_output",
            "source_file",
        ],
    )

    result = add_expected_hourly_grid(df)

    assert result.count() == 6
    missing = result.where("turbine_id = 2 AND hour(timestamp) = 1").collect()[0]
    assert missing.is_missing_record is True


def test_fleet_anomaly_uses_two_standard_deviations(spark):
    # 14 turbines at 10 MW average and one turbine at 0 MW average.
    # The single low turbine is >2 standard deviations from the fleet mean.
    rows = [
        Row(
            event_date=date(2022, 3, 1),
            turbine_id=turbine_id,
            min_power_output_mw=avg,
            max_power_output_mw=avg,
            avg_power_output_mw=avg,
            stddev_power_output_mw=0.0,
            expected_reading_count=24,
            observed_reading_count=24,
            imputed_power_reading_count=0,
            power_outlier_reading_count=0,
            completeness_pct=100.0,
        )
        for turbine_id, avg in [(i, 10.0) for i in range(1, 15)] + [(15, 0.0)]
    ]
    summary = spark.createDataFrame(rows)

    result = identify_fleet_anomalies(summary, stddev_threshold=2.0)
    anomalies = result.where("is_anomaly = true").select("turbine_id").collect()

    assert [r.turbine_id for r in anomalies] == [15]
