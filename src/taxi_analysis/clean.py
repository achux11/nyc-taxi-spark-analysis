"""Clean raw trip records before any analysis.

The raw TLC data has exact duplicate rows, missing values, trips outside the month they
were filed under, zero or negative fares, and impossible distances, durations and speeds.
Each rule below removes one of those problems.
"""
from pyspark.sql import DataFrame, functions as F

MAX_DISTANCE_MILES = 100
MIN_DURATION_MIN, MAX_DURATION_MIN = 1, 180
MAX_SPEED_MPH = 80


def clean_trips(trips: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """Return cleaned trips with pickup date, duration and speed added.

    start_date is inclusive and end_date is exclusive (YYYY-MM-DD).
    """
    df = trips.dropDuplicates()
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
                           "PULocationID", "fare_amount", "total_amount"])

    df = df.withColumn(
        "duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(start_date).cast("timestamp"))
        & (F.col("tpep_pickup_datetime") < F.lit(end_date).cast("timestamp"))
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_DISTANCE_MILES)
        & (F.col("duration_min") >= MIN_DURATION_MIN) & (F.col("duration_min") <= MAX_DURATION_MIN)
        & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0)
        & (F.coalesce(F.col("tip_amount"), F.lit(0.0)) >= 0)
    )

    df = df.withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_min") / 60))
    df = df.filter(F.col("speed_mph") <= MAX_SPEED_MPH)

    return df.select(
        F.to_date("tpep_pickup_datetime").alias("trip_date"),
        F.col("PULocationID").alias("pu_location_id"),
        "payment_type", "trip_distance", "duration_min", "speed_mph",
        "fare_amount", F.coalesce(F.col("tip_amount"), F.lit(0.0)).alias("tip_amount"),
        "total_amount",
    )
