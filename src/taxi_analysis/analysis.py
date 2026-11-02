"""The three business questions, written as Spark SQL over temporary views.

Q1. Does rain change taxi demand?
Q2. Does trip speed change what a rider pays, borough by borough?
Q3. Does the day's temperature change how much riders tip?
"""
from pyspark.sql import DataFrame, SparkSession

# Daily rainfall buckets in mm. A simple split for daily totals, chosen for this analysis.
RAIN_BUCKETS_SQL = """
    CASE WHEN prcp_mm = 0    THEN '1. No rain'
         WHEN prcp_mm < 2.5  THEN '2. Light rain'
         WHEN prcp_mm < 10   THEN '3. Moderate rain'
         ELSE                     '4. Heavy rain' END
"""


def register_views(spark: SparkSession, trips: DataFrame, zones: DataFrame, weather: DataFrame) -> None:
    trips.createOrReplaceTempView("trips")
    zones.createOrReplaceTempView("zones")
    weather.createOrReplaceTempView("weather")


def daily_demand(spark: SparkSession) -> DataFrame:
    """One row per day: trip count joined to that day's weather."""
    return spark.sql("""
        SELECT d.trip_date, d.trips, w.prcp_mm, w.tmax_c,
               dayofweek(d.trip_date) IN (1, 7) AS is_weekend
        FROM (SELECT trip_date, COUNT(*) AS trips FROM trips GROUP BY trip_date) d
        JOIN weather w ON d.trip_date = w.date
        WHERE w.prcp_mm IS NOT NULL
    """)


def rain_vs_demand(spark: SparkSession) -> tuple[DataFrame, float]:
    """Q1. Average daily trips per rain bucket, and the trips-vs-rainfall correlation."""
    daily_demand(spark).createOrReplaceTempView("daily_demand")
    by_bucket = spark.sql(f"""
        SELECT {RAIN_BUCKETS_SQL} AS rain_level,
               COUNT(*) AS days,
               ROUND(AVG(trips)) AS avg_daily_trips,
               ROUND(AVG(CASE WHEN NOT is_weekend THEN trips END)) AS avg_weekday_trips
        FROM daily_demand
        GROUP BY 1 ORDER BY 1
    """)
    corr = spark.sql("SELECT corr(trips, prcp_mm) AS c FROM daily_demand").first()["c"]
    return by_bucket, corr


def speed_vs_cost_by_borough(spark: SparkSession) -> DataFrame:
    """Q2. Per pickup borough: correlation between trip speed and total amount paid."""
    return spark.sql("""
        SELECT z.Borough AS borough,
               COUNT(*) AS trips,
               ROUND(AVG(t.speed_mph), 1) AS avg_speed_mph,
               ROUND(AVG(t.total_amount), 2) AS avg_total_usd,
               ROUND(corr(t.speed_mph, t.total_amount), 3) AS speed_cost_corr,
               ROUND(corr(t.trip_distance, t.total_amount), 3) AS distance_cost_corr
        FROM trips t JOIN zones z ON t.pu_location_id = z.LocationID
        WHERE z.Borough NOT IN ('Unknown', 'N/A')
        GROUP BY z.Borough
        ORDER BY trips DESC
    """)


def tip_vs_temperature(spark: SparkSession) -> tuple[DataFrame, float]:
    """Q3. Daily average tip % on card trips vs. the day's high temperature.

    Only card trips (payment_type = 1) are used, because cash tips are not recorded in TLC data.
    """
    daily = spark.sql("""
        SELECT t.trip_date, w.tmax_c,
               ROUND(AVG(t.tip_amount / t.fare_amount) * 100, 2) AS avg_tip_pct,
               COUNT(*) AS card_trips
        FROM trips t JOIN weather w ON t.trip_date = w.date
        WHERE t.payment_type = 1 AND w.tmax_c IS NOT NULL
        GROUP BY t.trip_date, w.tmax_c
    """)
    daily.createOrReplaceTempView("daily_tips")
    corr = spark.sql("SELECT corr(avg_tip_pct, tmax_c) AS c FROM daily_tips").first()["c"]
    return daily, corr
