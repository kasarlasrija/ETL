# transform.py
"""
Transform module for SwiftShip Express ETL pipeline (mock-safe)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# --- File paths ---
RAW_DIR = Path(__file__).resolve().parent / "data" / "raw_mock"
STAGED_DIR = Path(__file__).resolve().parent / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

DELIVERIES_RAW = RAW_DIR / "deliveries_raw_20251211T083053Z.json"
TRAFFIC_RAW = RAW_DIR / "traffic_routes_raw_20251211T083053Z.json"

TRANSFORMED_CSV = STAGED_DIR / "air_quality_transformed_mock.csv"

# --- Load raw JSON ---
deliveries_df = pd.read_json(DELIVERIES_RAW)
traffic_df = pd.read_json(TRAFFIC_RAW)

# --- Ensure traffic_df has required columns ---
if "source_city" not in traffic_df.columns:
    traffic_df["source_city"] = traffic_df.get("city_name", "MockCity")
if "destination_city" not in traffic_df.columns:
    traffic_df["destination_city"] = traffic_df.get("city_name", "MockCity")
if "congestion_score" not in traffic_df.columns:
    traffic_df["congestion_score"] = 5
if "avg_route_speed" not in traffic_df.columns:
    traffic_df["avg_route_speed"] = 40
if "weather_warnings" not in traffic_df.columns:
    traffic_df["weather_warnings"] = None

# --- Clean deliveries data ---
deliveries_df["dispatch_time"] = pd.to_datetime(deliveries_df["dispatch_time"], errors='coerce')
deliveries_df["expected_delivery_time"] = pd.to_datetime(deliveries_df["expected_delivery_time"], errors='coerce')
deliveries_df["actual_delivery_time"] = pd.to_datetime(deliveries_df["actual_delivery_time"], errors='coerce')

# Remove invalid rows
deliveries_df = deliveries_df.dropna(subset=["dispatch_time", "expected_delivery_time", "actual_delivery_time"])
deliveries_df = deliveries_df[deliveries_df["actual_delivery_time"] >= deliveries_df["expected_delivery_time"]]

# Convert weight to kg if needed
if deliveries_df["package_weight"].max() > 1000:
    deliveries_df["package_weight"] = deliveries_df["package_weight"] / 1000

# --- Compute delay ---
deliveries_df["delay_minutes"] = (deliveries_df["actual_delivery_time"] - deliveries_df["expected_delivery_time"]).dt.total_seconds() / 60

# --- Delay classification ---
def classify_delay(x):
    if x <= 0:
        return "On-Time"
    elif x <= 60:
        return "Slight Delay"
    elif x <= 180:
        return "Major Delay"
    else:
        return "Critical Delay"

deliveries_df["delay_class"] = deliveries_df["delay_minutes"].apply(classify_delay)

# --- Agent performance score ---
def agent_score(x):
    if x <= 0:
        return 5
    elif x <= 30:
        return 4
    elif x <= 60:
        return 3
    elif x <= 180:
        return 2
    else:
        return 1

deliveries_df["agent_score"] = deliveries_df["delay_minutes"].apply(agent_score)

# --- Merge with traffic data ---
merged_df = deliveries_df.merge(
    traffic_df[["source_city", "destination_city", "congestion_score", "avg_route_speed", "weather_warnings"]],
    on=["source_city", "destination_city"],
    how="left"
)

# --- Feature engineering ---
merged_df["traffic_impact_score"] = merged_df["congestion_score"] * (1 / merged_df["avg_route_speed"]) * 10

def risk_level(x):
    if x > 15:
        return "High Risk"
    elif x > 7:
        return "Moderate Risk"
    else:
        return "Low Risk"

merged_df["predicted_delay_risk_level"] = merged_df["traffic_impact_score"].apply(risk_level)
merged_df["delivery_efficiency_index"] = (merged_df["package_weight"] / (merged_df["delay_minutes"] + 1)) * merged_df["agent_score"]

# --- Save transformed CSV ---
merged_df.to_csv(TRANSFORMED_CSV, index=False)
print(f"✅ Transformed data saved to {TRANSFORMED_CSV}")
