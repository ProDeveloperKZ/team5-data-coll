"""
Stage 1 - Collect earthquake data from the USGS Earthquake Catalog API.

API:    https://earthquake.usgs.gov/fdsnws/event/1/query  (public, no API key)
Output: data/raw/pages/*.json        unchanged API responses (original data)
        data/raw/records.json        flat records -> Kafka producer
        data/raw_data.csv            same records as a table

Run from the project root:
    python -m src.api_client
"""

import argparse
import json
import time
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Fixed period so every run returns the same dataset (reproducible results)
START_DATE = "2023-01-01T00:00:00"
END_DATE = "2026-11-18T14:19:58"
MIN_MAGNITUDE = 4.5
PAGE_SIZE = 2000      # records per request
MAX_RETRIES = 3

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PAGES_DIR = PROJECT_ROOT / "data" / "raw" / "pages"
RECORDS_PATH = PROJECT_ROOT / "data" / "raw" / "records.json"
RAW_CSV_PATH = PROJECT_ROOT / "data" / "raw_data.csv"

FIELDS = [
    "id", "mag", "magType", "net", "place", "time", "updated", "type", "status",
    "tsunami", "sig", "felt", "cdi", "mmi", "alert", "nst", "gap", "dmin",
    "longitude", "latitude", "depth_km",
]


def fetch_page(params):
    """Send one GET request. Retry on network errors, stop on bad responses."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=60)
            response.raise_for_status()          # HTTP 4xx / 5xx -> error
            data = response.json()               # invalid JSON -> error
            if "features" not in data:
                raise ValueError("Response has no 'features' key")
            return data
        except (requests.Timeout, requests.ConnectionError) as err:
            print(f"  Network error (attempt {attempt}/{MAX_RETRIES}): {err}")
            time.sleep(2 * attempt)
        except requests.HTTPError as err:
            print(f"  HTTP error {err.response.status_code}: {err.response.text[:200]}")
            if err.response.status_code < 500:   # client error -> retrying won't help
                break
            time.sleep(2 * attempt)
    raise RuntimeError(f"Request failed after {MAX_RETRIES} attempts: {params}")


def flatten(feature):
    """Turn one GeoJSON feature into one flat record."""
    props = feature["properties"]
    longitude, latitude, depth_km = feature["geometry"]["coordinates"]
    record = {field: props.get(field) for field in FIELDS}
    record.update(id=feature["id"], longitude=longitude, latitude=latitude, depth_km=depth_km)
    return record


def yearly_periods(start, end):
    """Split the date range by year (one USGS query may return at most 20,000 events)."""
    periods = []
    for year in range(int(start[:4]), int(end[:4]) + 1):
        period_start = max(start, f"{year}-01-01T00:00:00")
        period_end = min(end, f"{year + 1}-01-01T00:00:00")
        periods.append((str(year), period_start, period_end))
    return periods


def collect(start=START_DATE, end=END_DATE, min_mag=MIN_MAGNITUDE):
    PAGES_DIR.mkdir(parents=True, exist_ok=True)
    records = []

    for label, period_start, period_end in yearly_periods(start, end):
        page = 1
        while True:
            params = {
                "format": "geojson",
                "starttime": period_start,
                "endtime": period_end,
                "minmagnitude": min_mag,
                "eventtype": "earthquake",
                "orderby": "time-asc",
                "limit": PAGE_SIZE,
                "offset": 1 + (page - 1) * PAGE_SIZE,   # USGS offset starts at 1
            }
            data = fetch_page(params)
            features = data["features"]

            # Save the original response unchanged
            page_file = PAGES_DIR / f"usgs_{label}_page_{page:02d}.json"
            page_file.write_text(json.dumps(data), encoding="utf-8")

            records.extend(flatten(f) for f in features)
            print(f"{label} page {page}: {len(features)} records")

            if len(features) < PAGE_SIZE:   # last page of this year
                break
            page += 1

    if len(records) < 200:
        raise ValueError(f"Only {len(records)} records collected, need at least 200")

    RECORDS_PATH.write_text(json.dumps(records, indent=2), encoding="utf-8")
    pd.DataFrame(records, columns=FIELDS).to_csv(RAW_CSV_PATH, index=False)
    return records


def load_records():
    """Used by the producer."""
    return json.loads(RECORDS_PATH.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Collect USGS earthquake data")
    parser.add_argument("--start", default=START_DATE, help="start time, e.g. 2023-01-01T00:00:00")
    parser.add_argument("--end", default=END_DATE, help="end time, e.g. 2026-11-18T14:19:58")
    parser.add_argument("--min-mag", type=float, default=MIN_MAGNITUDE)
    args = parser.parse_args()

    records = collect(args.start, args.end, args.min_mag)
    print(f"\nTotal collected records: {len(records)}")
    print(f"Saved raw pages -> {PAGES_DIR.relative_to(PROJECT_ROOT)}")
    print(f"Saved records   -> {RECORDS_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Saved CSV       -> {RAW_CSV_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
