"""
etl_analysis.py (Refactored)

- Reads loaded data from Supabase table air_quality_data
- Computes KPIs:
    * City with highest average PM2.5
    * City with highest average severity_score
    * Percentage distribution of risk_flag (High/Moderate/Low)
    * Hour of day with worst AQI (highest avg pm2_5)
- Dynamic risk categorization based on severity percentiles
- Saves CSVs to data/processed/
- Saves PNG visualizations:
    - pm2_5_histogram.png
    - risk_flags_bar.png
    - pm2_5_trends.png
    - severity_vs_pm2_5.png
"""

import os
import logging
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client

# --------------------------
# Setup
# --------------------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "data/processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Please set SUPABASE_URL and SUPABASE_KEY in .env")

# --------------------------
# Fetch Data from Supabase
# --------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
resp = supabase.table("air_quality_data").select("*").execute()
data = resp.data

if not data:
    logging.warning("No data fetched from Supabase table 'air_quality_data'. Exiting.")
    exit(0)

df = pd.DataFrame(data)

# Ensure correct dtypes
df["time"] = pd.to_datetime(df["time"], errors="coerce")
df = df.dropna(subset=["time"])

numeric_cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
                "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"]
for c in numeric_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# --------------------------
# Dynamic Risk Categorization
# --------------------------
# Compute percentiles
sev = df["severity_score"].dropna()
low_thresh = sev.quantile(0.33)
med_thresh = sev.quantile(0.66)

def dynamic_risk(sev_val):
    if pd.isna(sev_val):
        return None
    if sev_val <= low_thresh:
        return "Low Risk"
    elif sev_val <= med_thresh:
        return "Moderate Risk"
    return "High Risk"

df["risk_flag"] = df["severity_score"].apply(dynamic_risk)

# --------------------------
# KPI Metrics
# --------------------------
city_pm25 = df.groupby("city")["pm2_5"].mean().dropna()
city_highest_pm2_5 = city_pm25.idxmax() if not city_pm25.empty else None

city_severity = df.groupby("city")["severity_score"].mean().dropna()
city_highest_severity = city_severity.idxmax() if not city_severity.empty else None

risk_counts = df["risk_flag"].value_counts(dropna=True)
risk_pct = (risk_counts / risk_counts.sum() * 100).round(2)

df["hour_of_day"] = df["time"].dt.hour
hourly_pm25 = df.groupby("hour_of_day")["pm2_5"].mean().dropna()
worst_hour_aqi = int(hourly_pm25.idxmax()) if not hourly_pm25.empty else None

# Save summary CSV
summary = {
    "city_highest_pm2_5": [city_highest_pm2_5],
    "city_highest_severity": [city_highest_severity],
    "worst_hour_aqi": [worst_hour_aqi]
}
summary_df = pd.DataFrame(summary)
for k, v in risk_pct.items():
    summary_df[f"risk_pct_{k}"] = v
summary_df.to_csv(os.path.join(PROCESSED_DIR, "summary_metrics.csv"), index=False)
logging.info("Saved summary_metrics.csv")

# --------------------------
# City Pollution Trend
# --------------------------
trend_cols = ["time", "pm2_5", "pm10", "ozone"]
trend_df = df[["city"] + trend_cols].sort_values(["city", "time"])
trend_df.to_csv(os.path.join(PROCESSED_DIR, "pollution_trends.csv"), index=False)
logging.info("Saved pollution_trends.csv")

# --------------------------
# City Risk Distribution
# --------------------------
risk_dist = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
risk_dist.to_csv(os.path.join(PROCESSED_DIR, "city_risk_distribution.csv"))
logging.info("Saved city_risk_distribution.csv")

# --------------------------
# Visualizations
# --------------------------
plt.rcParams.update({'figure.max_open_warning': 0})

# 1) Histogram of PM2.5
plt.figure(figsize=(8,5))
plt.hist(df["pm2_5"].dropna(), bins=30, color="skyblue", edgecolor="black")
plt.title("Histogram of PM2.5")
plt.xlabel("PM2.5 (µg/m³)")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "pm2_5_histogram.png"))
plt.close()

# 2) Stacked Bar of Risk Flags per City
risk_dist_plot = risk_dist.copy()
risk_dist_plot.plot(kind="bar", stacked=True, figsize=(10,6), colormap="Set2")
plt.title("Risk Flags per City")
plt.xlabel("City")
plt.ylabel("Number of Hours")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "risk_flags_bar.png"))
plt.close()

# 3) Line Chart: Hourly PM2.5 Trends
plt.figure(figsize=(12,6))
for city in df["city"].unique():
    city_df = df[df["city"] == city].set_index("time").resample("1h")["pm2_5"].mean()
    if city_df.dropna().empty:
        continue
    plt.plot(city_df.index, city_df.values, label=city)
plt.title("Hourly PM2.5 Trends by City")
plt.xlabel("Time")
plt.ylabel("PM2.5 (µg/m³)")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "pm2_5_trends.png"))
plt.close()

# 4) Scatter: severity_score vs PM2.5
plt.figure(figsize=(8,5))
scatter_df = df[["pm2_5", "severity_score"]].dropna()
plt.scatter(scatter_df["pm2_5"], scatter_df["severity_score"], s=10, color="crimson")
plt.title("Severity Score vs PM2.5")
plt.xlabel("PM2.5 (µg/m³)")
plt.ylabel("Severity Score")
plt.tight_layout()
plt.savefig(os.path.join(PROCESSED_DIR, "severity_vs_pm2_5.png"))
plt.close()

# --------------------------
# Summary Logs
# --------------------------
logging.info(f"Analysis complete. Outputs saved to: {PROCESSED_DIR}")
logging.info(f"Summary metrics: {summary_df.to_dict(orient='records')[0]}")
logging.info(f"Risk distribution (%): {risk_pct.to_dict()}")
