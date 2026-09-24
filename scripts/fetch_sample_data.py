import json
import os
from datetime import date, timedelta

import requests

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

CITIES = {
    "Lahore":     (31.5497, 74.3436),
    "Delhi":      (28.6139, 77.2090),
    "Beijing":    (39.9042, 116.4074),
    "London":     (51.5074, -0.1278),
    "New_York":   (40.7128, -74.0060),
}

HOURLY_VARS = "carbon_monoxide,carbon_dioxide,pm2_5,pm10,nitrogen_dioxide"

OUT_DIR = "sample_data"
os.makedirs(OUT_DIR, exist_ok=True)


def fetch(lat, lon, extra_params):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARS,
        "timezone": "auto",
        **extra_params,
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def build_full_load_sample():
    """~30 days of hourly history per city -- a small stand-in for the
    real full load, which will span Aug 2022 -> today across 100 cities."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=30)
    records = []
    for city, (lat, lon) in CITIES.items():
        payload = fetch(lat, lon, {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        })
        payload["city"] = city
        payload["source_system"] = "open-meteo-air-quality-api"
        records.append(payload)
    with open(f"{OUT_DIR}/full_load_sample.json", "w") as f:
        json.dump(records, f, indent=2)
    print(f"Wrote {OUT_DIR}/full_load_sample.json ({len(records)} cities, "
          f"{start} -> {end})")


def build_incremental_load_sample():
    """Last 24h per city -- stand-in for the daily incremental job."""
    records = []
    for city, (lat, lon) in CITIES.items():
        payload = fetch(lat, lon, {"past_days": 1, "forecast_days": 0})
        payload["city"] = city
        payload["source_system"] = "open-meteo-air-quality-api"
        records.append(payload)
    with open(f"{OUT_DIR}/incremental_load_sample.json", "w") as f:
        json.dump(records, f, indent=2)
    print(f"Wrote {OUT_DIR}/incremental_load_sample.json ({len(records)} cities)")


if __name__ == "__main__":
    build_full_load_sample()
    build_incremental_load_sample()