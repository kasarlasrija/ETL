"""
Transform step for AtmosTrack Air Quality ETL (Open-Meteo Format Fixed).

- Reads all raw JSON files from data/raw/
- Detects city by lat/lon mapping
- Cleans pollutant arrays (removes trailing nulls)
- Ensures equal-length arrays
- Flattens hourly records
- Adds AQI category, severity, risk, hour
- Saves to data/staged/air_quality_transformed.csv
"""

import os
import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
STAGED_DIR = Path(__file__).resolve().parents[1] / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------------
# City detection using coordinates
# -----------------------------------
CITY_MAP = {
    (28.7041, 77.1025): "Delhi",
    (19.0760, 72.8777): "Mumbai",
    (12.9716, 77.5946): "Bengaluru",
    (17.3850, 78.4867): "Hyderabad",
    (22.5726, 88.3639): "Kolkata",
}

def detect_city(lat, lon):
    for (clat, clon), cname in CITY_MAP.items():
        if abs(clat - lat) < 0.2 and abs(clon - lon) < 0.2:
            return cname
    return "Unknown"

# -----------------------------------
# AQI Category from PM2.5
# -----------------------------------
def compute_aqi(pm25):
    if pd.isna(pm25):
        return None
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# -----------------------------------
# Severity Score
# -----------------------------------
def compute_severity(row):
    return (
        (row["pm2_5"] * 5)
        + (row["pm10"] * 3)
        + (row["nitrogen_dioxide"] * 4)
        + (row["sulphur_dioxide"] * 4)
        + (row["carbon_monoxide"] * 2)
        + (row["ozone"] * 3)
    )

# -----------------------------------
# Risk Flag
# -----------------------------------
def compute_risk(sev):
    if sev > 400:
        return "High Risk"
    elif sev > 200:
        return "Moderate Risk"
    return "Low Risk"

# -----------------------------------
# Load raw
# -----------------------------------
def load_raw_files():
    files = sorted(RAW_DIR.glob("*_raw*.json"))
    if not files:
        print("❌ No raw JSON files found.")
    return files

# -----------------------------------
# Parse a single raw file
# -----------------------------------
def parse_raw_json(path: Path):

    with open(path, "r") as f:
        data = json.load(f)

    lat = data.get("latitude")
    lon = data.get("longitude")
    city = detect_city(lat, lon)

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        print(f"⚠ Empty hourly data in {path.name}")
        return pd.DataFrame()

    # Fix for trailing nulls → trim arrays to match times length
    def trim(arr):
        if arr is None:
            return [None] * len(times)
        return (arr + [None] * len(times))[: len(times)]

    df = pd.DataFrame({
        "time": times,
        "pm10": trim(hourly.get("pm10")),
        "pm2_5": trim(hourly.get("pm2_5")),
        "carbon_monoxide": trim(hourly.get("carbon_monoxide")),
        "nitrogen_dioxide": trim(hourly.get("nitrogen_dioxide")),
        "sulphur_dioxide": trim(hourly.get("sulphur_dioxide")),
        "ozone": trim(hourly.get("ozone")),
        "uv_index": trim(hourly.get("uv_index")),
    })

    df["city"] = city

    return df

# -----------------------------------
# Main transform
# -----------------------------------
def run_transform():

    raw_files = load_raw_files()
    rows = []

    for f in raw_files:
        print(f"Processing {f.name}")
        df = parse_raw_json(f)
        if not df.empty:
            rows.append(df)

    if not rows:
        print("❌ No valid data found.")
        return

    df = pd.concat(rows, ignore_index=True)

    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # numeric conversion
    poll_cols = ["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone","uv_index"]
    for col in poll_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=poll_cols, how="all")

    df["aqi_category"] = df["pm2_5"].apply(compute_aqi)
    df["severity_score"] = df.apply(compute_severity, axis=1)
    df["risk_flag"] = df["severity_score"].apply(compute_risk)
    df["hour"] = df["time"].dt.hour

    out_path = STAGED_DIR / "air_quality_transformed.csv"
    df.to_csv(out_path, index=False)

    print(f"✅ Transform Saved → {out_path}")
    print(f"Rows: {len(df)}")

if __name__ == "__main__":
    run_transform()
