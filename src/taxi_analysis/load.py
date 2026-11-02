"""Read the three raw datasets into Spark DataFrames.

1. NYC TLC yellow taxi trips (monthly Parquet files)
2. NYC TLC taxi zone lookup (CSV) - maps a location ID to a zone and borough
3. NOAA GHCN-Daily weather for Central Park (station USW00094728)
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

# Only the columns the analysis needs. Older and newer TLC files store some of these with
# different types (e.g. passenger_count as long vs double), so each file is cast to this
# schema before the files are combined.
TRIP_COLUMNS = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "trip_distance": "double",
    "PULocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}

WEATHER_ELEMENTS = ["TMAX", "TMIN", "PRCP", "SNOW", "AWND"]


def read_trips(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read one or more monthly trip files and stack them into one DataFrame."""
    frames = []
    for path in paths:
        df = spark.read.parquet(path)
        frames.append(df.select([F.col(c).cast(t).alias(c) for c, t in TRIP_COLUMNS.items()]))
    out = frames[0]
    for df in frames[1:]:
        out = out.unionByName(df)
    return out


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    """Zone lookup: LocationID, Borough, Zone, service_zone."""
    return spark.read.csv(path, header=True, inferSchema=True)


def read_weather(spark: SparkSession, path: str) -> DataFrame:
    """Turn the long GHCN-Daily file (one row per day per measurement) into one row per day.

    GHCN-Daily units: TMAX/TMIN in tenths of a degree C, PRCP in tenths of a mm,
    SNOW in mm, AWND in tenths of a metre per second. Rows with a quality flag are dropped.
    """
    schema = ("station STRING, date STRING, element STRING, value INT, "
              "mflag STRING, qflag STRING, sflag STRING, obs_time STRING")
    raw = (spark.read.csv(path, schema=schema)
           .filter(F.col("element").isin(WEATHER_ELEMENTS))
           .filter(F.col("qflag").isNull()))
    daily = (raw.withColumn("date", F.to_date("date", "yyyyMMdd"))
             .groupBy("date")
             .pivot("element", WEATHER_ELEMENTS)
             .agg(F.first("value")))
    return daily.select(
        "date",
        (F.col("TMAX") / 10).alias("tmax_c"),
        (F.col("TMIN") / 10).alias("tmin_c"),
        (F.col("PRCP") / 10).alias("prcp_mm"),
        F.col("SNOW").cast("double").alias("snow_mm"),
        (F.col("AWND") / 10).alias("wind_ms"),
    )
