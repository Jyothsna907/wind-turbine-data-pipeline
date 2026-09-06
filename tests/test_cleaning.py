from datetime import datetime, timedelta

from wind_pipeline.transformations import flag_and_null_outliers, impute_missing_values


def test_invalid_and_extreme_readings_are_flagged_then_imputed(spark):
    rows = []
    start = datetime(2022, 3, 1, 0)
    for i in range(12):
        rows.append(
            (
                start + timedelta(hours=i),
                1,
                10.0 + (i % 3),
                90 + i,
                2.0 + (i % 3) * 0.2,
                False,
            )
        )

    # Deliberately dirty readings.
    rows[5] = (start + timedelta(hours=5), 1, 100.0, 95, 50.0, False)
    rows[6] = (start + timedelta(hours=6), 1, None, 999, None, False)

    df = spark.createDataFrame(
        rows,
        [
            "timestamp",
            "turbine_id",
            "wind_speed",
            "wind_direction",
            "power_output",
            "is_missing_record",
        ],
    )

    flagged = flag_and_null_outliers(df)
    cleaned = impute_missing_values(flagged)

    row5 = cleaned.where("hour(timestamp) = 5").collect()[0]
    assert row5.wind_speed_outlier is True
    assert row5.power_output_outlier is True
    assert row5.wind_speed_imputed is True
    assert row5.power_output_imputed is True

    row6 = cleaned.where("hour(timestamp) = 6").collect()[0]
    assert row6.wind_direction_outlier is True
    assert row6.wind_speed_imputed is True
    assert row6.power_output_imputed is True
    assert row6.wind_direction_imputed is True
