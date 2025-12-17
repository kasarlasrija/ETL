"""
Load step for AtmosTrack Air Quality ETL.

- Reads staged CSV: data/staged/air_quality_transformed.csv
- Batch inserts records into Supabase (table: air_quality_data)
- Converts NaN → None
- Converts timestamps → ISO strings
- Retries failed batches (2 retries)

Requires:
    pip install supabase
"""

import os
import math
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# --------------------------
# CONFIG
# --------------------------
STAGED_FILE = Path(os.getenv("STAGED_FILE", "data/staged/air_quality_transformed.csv"))
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BATCH_SIZE = 200
RETRY_LIMIT = 2

# --------------------------
# CONNECT
# --------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------
# LOAD CSV
# --------------------------
print(f"Reading staged CSV → {STAGED_FILE}")

df = pd.read_csv(STAGED_FILE)

# Replace NaN → None
df = df.where(pd.notnull(df), None)

# Convert datetime
def fix_time(val):
    try:
        return pd.to_datetime(val).isoformat()
    except:
        return None

df["time"] = df["time"].apply(fix_time)

records = df.to_dict(orient="records")
total_rows = len(records)
print(f"Total rows to insert: {total_rows}")

# --------------------------
# BATCH INSERT
# --------------------------
def insert_batch(batch):
    """Insert batch into Supabase with retry."""
    attempts = 0
    while attempts <= RETRY_LIMIT:
        try:
            result = supabase.table("air_quality_data").insert(batch).execute()
            return True
        except Exception as e:
            attempts += 1
            print(f"⚠ Batch insert failed (attempt {attempts}/{RETRY_LIMIT}): {e}")
            if attempts > RETRY_LIMIT:
                print("❌ Giving up on this batch.")
                return False

# --------------------------
# PROCESS BATCHES
# --------------------------
batches = math.ceil(total_rows / BATCH_SIZE)
success_count = 0
fail_count = 0

for i in range(batches):
    start = i * BATCH_SIZE
    end = start + BATCH_SIZE
    batch_data = records[start:end]

    print(f"📦 Inserting batch {i+1}/{batches} ({len(batch_data)} rows)")

    if insert_batch(batch_data):
        success_count += len(batch_data)
    else:
        fail_count += len(batch_data)

# --------------------------
# SUMMARY
# --------------------------
print("\n================ LOAD SUMMARY ================")
print(f"✔ Successfully inserted: {success_count} rows")
print(f"❌ Failed rows: {fail_count}")
print("==============================================")
