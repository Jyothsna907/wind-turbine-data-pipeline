from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

RAW_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)
