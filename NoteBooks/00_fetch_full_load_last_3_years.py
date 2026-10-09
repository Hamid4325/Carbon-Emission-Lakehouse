# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 - Fetch last 3 years of air-quality data -> raw JSON for Bronze
# MAGIC Writes `/Volumes/workspace/default/bronze/full_load_last_3_years.json`
# MAGIC (a JSON array with one object per city, shaped like the Bronze `open_meteo_schema`).

# COMMAND ----------

import json
import time
from datetime import date, timedelta

import requests
from dateutil.relativedelta import relativedelta

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze")

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
RAW_FOLDER = "/Volumes/workspace/default/bronze"
SAMPLE_FILE = f"{RAW_FOLDER}/incremental_load_sample.json"
OUTPUT_FILE = f"{RAW_FOLDER}/full_load_last_3_years.json"

HOURLY_VARS = ["carbon_monoxide", "carbon_dioxide", "pm2_5", "pm10", "nitrogen_dioxide"]
CHUNK_DAYS = 180          # request in chunks so no single call is oversized
PAUSE_SECONDS = 1         # be polite to the free API

# History only: end yesterday, start exactly 3 years before that
END_DATE = date.today() - timedelta(days=1)
START_DATE = END_DATE - relativedelta(years=3)
print(f"Window: {START_DATE} -> {END_DATE}")

# COMMAND ----------

# Cities: reuse exactly the cities / coordinates / timezone / source_system from your existing sample file,
# so the full load matches what Bronze and Silver have already seen.
# If the sample can't be read, fill in FALLBACK_CITIES by hand.

FALLBACK_CITIES = {
    # "CityName": (latitude, longitude),
}
FALLBACK_TIMEZONE = "GMT"
FALLBACK_SOURCE = "open-meteo-air-quality-api"

try:
    rows = (spark.read.option("multiline", "true").json(SAMPLE_FILE)
            .select("city", "latitude", "longitude", "timezone", "source_system")
            .collect())
    CITIES = {}
    for r in rows:
        if r["city"] and r["city"] not in CITIES:
            CITIES[r["city"]] = (r["latitude"], r["longitude"])
    TIMEZONE = next((r["timezone"] for r in rows if r["timezone"]), FALLBACK_TIMEZONE)
    SOURCE_SYSTEM = next((r["source_system"] for r in rows if r["source_system"]), FALLBACK_SOURCE)
    print("Cities loaded from sample file.")
except Exception as e:
    print(f"Could not read sample file ({e}); using FALLBACK_CITIES.")
    CITIES, TIMEZONE, SOURCE_SYSTEM = FALLBACK_CITIES, FALLBACK_TIMEZONE, FALLBACK_SOURCE

assert CITIES, "No cities found: fill in FALLBACK_CITIES"
print("TIMEZONE:", TIMEZONE, "| SOURCE_SYSTEM:", SOURCE_SYSTEM)
for c, (la, lo) in CITIES.items():
    print(f"  {c}: {la}, {lo}")

# COMMAND ----------

def get_json(params, retries=5):
    """GET with exponential backoff on rate limits / transient server errors."""
    for attempt in range(retries):
        resp = requests.get(API_URL, params=params, timeout=90)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt * 5
            print(f"  HTTP {resp.status_code}, retrying in {wait}s...")
            time.sleep(wait)
            continue
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    raise RuntimeError("Giving up after repeated failures")


def date_chunks(start, end, days):
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def fetch_city(city, lat, lon):
    merged = None
    for chunk_start, chunk_end in date_chunks(START_DATE, END_DATE, CHUNK_DAYS):
        data = get_json({
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(HOURLY_VARS),
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "timezone": TIMEZONE,
        })
        if merged is None:
            merged = data                       # keeps the top-level fields + hourly_units from the first chunk
        else:
            for key in ["time"] + HOURLY_VARS:  # append the hourly arrays in order
                merged["hourly"][key].extend(data["hourly"][key])
        time.sleep(PAUSE_SECONDS)

    merged["city"] = city
    merged["source_system"] = SOURCE_SYSTEM
    n = len(merged["hourly"]["time"])
    print(f"{city}: {n} hourly readings")
    return merged


results = [fetch_city(city, la, lo) for city, (la, lo) in CITIES.items()]

# COMMAND ----------

# Save as a JSON array (one object per city). Overwrites the file if it already exists.
with open(OUTPUT_FILE, "w") as f:
    json.dump(results, f)

print(f"Saved {len(results)} cities to {OUTPUT_FILE}")

# COMMAND ----------

# Quick check: file is readable and has the expected shape
check = spark.read.option("multiline", "true").json(OUTPUT_FILE)
check.selectExpr("city", "size(hourly.time) AS readings",
                 "hourly.time[0] AS first_hour",
                 "hourly.time[size(hourly.time)-1] AS last_hour").show(truncate=False)

print("Use these dates in the Silver full-load cell:")
print(f'  process_bronze_to_silver("{START_DATE}", "{END_DATE}", ...)')