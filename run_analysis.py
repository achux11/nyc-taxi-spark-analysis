"""Run the full analysis and write the results to ./results.

    python run_analysis.py --start 2023-01-01 --end 2023-07-01
"""
import argparse
import glob
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent / "src"))
from taxi_analysis.analysis import (register_views, rain_vs_demand,  # noqa: E402
                                    speed_vs_cost_by_borough, tip_vs_temperature)
from taxi_analysis.charts import borough_chart, rain_chart, tip_chart  # noqa: E402
from taxi_analysis.clean import clean_trips  # noqa: E402
from taxi_analysis.load import read_trips, read_weather, read_zones  # noqa: E402


def get_spark(memory: str) -> SparkSession:
    return (SparkSession.builder.appName("nyc-taxi-analysis")
            .config("spark.driver.memory", memory)
            .config("spark.sql.session.timeZone", "America/New_York")
            .config("spark.sql.shuffle.partitions", "64")
            .getOrCreate())


def fmt(x) -> str:
    """Correlation is undefined (None) when a column has no variation."""
    return "n/a" if x is None else f"{x:.3f}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="data")
    p.add_argument("--start", default="2023-01-01", help="first pickup date (inclusive)")
    p.add_argument("--end", default="2023-07-01", help="last pickup date (exclusive)")
    p.add_argument("--out", default="results")
    p.add_argument("--memory", default="6g", help="Spark driver memory")
    args = p.parse_args()

    spark = get_spark(args.memory)
    data, out = Path(args.data), Path(args.out)
    out.mkdir(exist_ok=True)

    trip_files = sorted(glob.glob(str(data / "trips" / "*.parquet")))
    if not trip_files:
        sys.exit("No trip files in data/trips. Run download_data.py first.")

    raw = read_trips(spark, trip_files)
    zones = read_zones(spark, str(data / "taxi_zone_lookup.csv"))
    weather = read_weather(spark, str(data / "weather_central_park.csv.gz"))

    raw_count = raw.count()
    trips = clean_trips(raw, args.start, args.end).cache()
    clean_count = trips.count()
    register_views(spark, trips, zones, weather)

    rain, rain_corr = rain_vs_demand(spark)
    speed = speed_vs_cost_by_borough(spark)
    tips, tip_corr = tip_vs_temperature(spark)

    rain_pd, speed_pd, tips_pd = rain.toPandas(), speed.toPandas(), tips.toPandas()
    rain_pd.to_csv(out / "q1_rain_vs_demand.csv", index=False)
    speed_pd.to_csv(out / "q2_speed_vs_cost_by_borough.csv", index=False)
    tips_pd.sort_values("trip_date").to_csv(out / "q3_daily_tip_vs_temperature.csv", index=False)

    rain_chart(rain_pd, out / "rain_vs_demand.png")
    borough_chart(speed_pd, out / "speed_vs_cost_by_borough.png")
    tip_chart(tips_pd, out / "tip_vs_temperature.png")

    summary = f"""# Results ({args.start} to {args.end}, end exclusive)

Trips: {raw_count:,} raw -> {clean_count:,} after cleaning ({1 - clean_count / raw_count:.1%} removed)

## Q1. Rain vs. demand
Correlation between daily trips and daily rainfall: {fmt(rain_corr)}

{rain_pd.to_markdown(index=False)}

## Q2. Trip speed vs. total cost, by pickup borough

{speed_pd.to_markdown(index=False)}

## Q3. Temperature vs. tip % (card trips only)
Correlation between daily high temperature and average tip %: {fmt(tip_corr)}
"""
    (out / "summary.md").write_text(summary)
    print(summary)
    spark.stop()


if __name__ == "__main__":
    main()
