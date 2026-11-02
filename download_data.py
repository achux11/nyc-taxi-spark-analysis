"""Download the raw data into ./data.

    python download_data.py --year 2023 --months 1-6

Sources:
  Trips:   https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
  Zones:   TLC taxi zone lookup CSV
  Weather: NOAA GHCN-Daily, Central Park station USW00094728
"""
import argparse
import urllib.request
from pathlib import Path

TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
WEATHER_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/USW00094728.csv.gz"


def parse_months(text: str) -> list[int]:
    if "-" in text:
        a, b = text.split("-")
        return list(range(int(a), int(b) + 1))
    return [int(m) for m in text.split(",")]


def fetch(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"skip  {dest} (already downloaded)")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"get   {url}")
    urllib.request.urlretrieve(url, dest)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2023)
    p.add_argument("--months", default="1-6", help="e.g. 1-6 or 1,2,3")
    p.add_argument("--out", default="data")
    args = p.parse_args()

    out = Path(args.out)
    for m in parse_months(args.months):
        fetch(TRIPS_URL.format(year=args.year, month=m), out / "trips" / f"yellow_{args.year}-{m:02d}.parquet")
    fetch(ZONES_URL, out / "taxi_zone_lookup.csv")
    fetch(WEATHER_URL, out / "weather_central_park.csv.gz")
    print("done")


if __name__ == "__main__":
    main()
