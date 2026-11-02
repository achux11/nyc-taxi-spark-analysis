"""Tests on small hand-made data, so they run in seconds without downloading anything."""
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from taxi_analysis.analysis import (register_views, rain_vs_demand,  # noqa: E402
                                    speed_vs_cost_by_borough, tip_vs_temperature)
from taxi_analysis.clean import clean_trips  # noqa: E402

COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID",
        "payment_type", "fare_amount", "tip_amount", "total_amount"]


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def trip(day, minutes, miles, zone=1, pay=1, fare=10.0, tip=2.0, total=15.0, hour=10):
    start = datetime(2023, 1, day, hour, 0)
    end = datetime(2023, 1, day, hour + minutes // 60, minutes % 60)
    return (start, end, miles, zone, pay, fare, tip, total)


def test_cleaning_removes_bad_rows(spark):
    good = trip(2, 20, 5.0)
    rows = [
        good, good,                    # exact duplicate -> keep one
        trip(2, 20, 0.0),              # zero distance
        trip(2, 20, 5.0, fare=-3.0),   # negative fare
        trip(2, 0, 5.0),               # under 1 minute
        trip(2, 5, 50.0),              # 600 mph
        (datetime(2022, 12, 31, 10), datetime(2022, 12, 31, 10, 20), 5.0, 1, 1, 10.0, 2.0, 15.0),  # outside range
        (None, datetime(2023, 1, 2, 10, 20), 5.0, 1, 1, 10.0, 2.0, 15.0),                          # missing pickup
    ]
    raw = spark.createDataFrame(rows, COLS)
    cleaned = clean_trips(raw, "2023-01-01", "2023-02-01").collect()
    assert len(cleaned) == 1
    assert cleaned[0]["speed_mph"] == pytest.approx(15.0)


def test_three_questions(spark):
    # Day 2 is dry with 2 trips, day 3 is heavy rain with 4 trips.
    rows = [trip(2, 30, 3.0, zone=1, total=12.0), trip(2, 30, 6.0, zone=2, total=20.0, pay=2, tip=0.0)]
    rows += [trip(3, 30, 3.0 + i, zone=1, total=12.0 + i) for i in range(4)]
    trips = clean_trips(spark.createDataFrame(rows, COLS), "2023-01-01", "2023-02-01")
    zones = spark.createDataFrame([(1, "Manhattan", "A", "Yellow Zone"), (2, "Queens", "B", "Boro Zone")],
                                  ["LocationID", "Borough", "Zone", "service_zone"])
    weather = spark.createDataFrame(
        [(datetime(2023, 1, 2).date(), 5.0, 0.0), (datetime(2023, 1, 3).date(), 8.0, 20.0)],
        ["date", "tmax_c", "prcp_mm"])
    register_views(spark, trips, zones, weather)

    rain, rain_corr = rain_vs_demand(spark)
    levels = {r["rain_level"]: r["avg_daily_trips"] for r in rain.collect()}
    assert levels == {"1. No rain": 2, "4. Heavy rain": 4}
    assert rain_corr == pytest.approx(1.0)

    boroughs = {r["borough"]: r["trips"] for r in speed_vs_cost_by_borough(spark).collect()}
    assert boroughs == {"Manhattan": 5, "Queens": 1}

    tips, _ = tip_vs_temperature(spark)
    assert sum(r["card_trips"] for r in tips.collect()) == 5   # the cash trip is excluded
