import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials missing. Set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Paths to transformed CSVs
DELIVERIES_CSV = "data/staged/deliveries_transformed_mock.csv"
TRAFFIC_CSV = "data/staged/traffic_routes_transformed_mock.csv"

# --- Helper to clean invalid floats ---
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(0)  # Replace NaN
    df = df.replace([float("inf"), float("-inf")], 0)  # Replace Inf
    return df

# --- Load deliveries ---
if os.path.exists(DELIVERIES_CSV):
    deliveries_df = pd.read_csv(DELIVERIES_CSV)
    deliveries_df = clean_df(deliveries_df)
    # Insert into Supabase table
    for _, row in deliveries_df.iterrows():
        supabase.table("deliveries").insert(row.to_dict()).execute()
    print("✅ Deliveries loaded.")
else:
    print(f"❌ CSV not found: {DELIVERIES_CSV}, skipping deliveries")

# --- Load traffic routes ---
if os.path.exists(TRAFFIC_CSV):
    traffic_df = pd.read_csv(TRAFFIC_CSV)
    traffic_df = clean_df(traffic_df)
    for _, row in traffic_df.iterrows():
        supabase.table("traffic_routes").insert(row.to_dict()).execute()
    print("✅ Traffic routes loaded.")
else:
    print(f"❌ CSV not found: {TRAFFIC_CSV}, skipping traffic_routes")
