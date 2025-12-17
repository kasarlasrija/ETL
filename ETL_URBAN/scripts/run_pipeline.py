"""
run_pipeline.py

Combined runner for AtmosTrack Air Quality ETL.

Steps:
1. Extract raw data from Open-Meteo
2. Transform raw JSON to staged CSV
3. Load staged CSV to Supabase
4. Run analysis on loaded data

Usage:
    python run_pipeline.py
"""

import logging
import sys
from pathlib import Path

# --------------------------
# Logging Setup
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------
# Add current folder to sys.path
# --------------------------
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

# --------------------------
# Import ETL Modules
# --------------------------
try:
    from extract import fetch_all_cities
    from transform import run_transform
    from load import insert_batch, BATCH_SIZE, STAGED_FILE, supabase
    import pandas as pd
    import math
    import etl_analysis
except ModuleNotFoundError as e:
    logging.error(f"Failed to import module: {e}")
    sys.exit(1)

# --------------------------
# Load Step Helper
# --------------------------
def run_load():
    """Load staged CSV into Supabase table in batches."""
    if not Path(STAGED_FILE).exists():
        logging.error(f"Staged CSV not found: {STAGED_FILE}")
        return

    df = pd.read_csv(STAGED_FILE)
    df = df.where(pd.notnull(df), None)

    # Convert datetime to ISO string
    df["time"] = pd.to_datetime(df["time"], errors="coerce").apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    records = df.to_dict(orient="records")
    total_rows = len(records)
    logging.info(f"Total rows to insert: {total_rows}")

    success_count = 0
    fail_count = 0
    batches = math.ceil(total_rows / BATCH_SIZE)

    for i in range(batches):
        start = i * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_data = records[start:end]
        attempts = 0
        inserted = False
        while attempts <= 2 and not inserted:
            try:
                supabase.table("air_quality_data").insert(batch_data).execute()
                inserted = True
                success_count += len(batch_data)
            except Exception as e:
                attempts += 1
                logging.warning(f"Batch {i+1} insert attempt {attempts} failed: {e}")
        if not inserted:
            fail_count += len(batch_data)
            logging.error(f"Batch {i+1} failed after retries.")

    logging.info(f"✔ Successfully inserted: {success_count} rows")
    logging.info(f"❌ Failed rows: {fail_count}")

# --------------------------
# Main Pipeline Runner
# --------------------------
def main():
    logging.info("========== Starting ETL Pipeline ==========")

    # 1) Extract
    logging.info(">>> Step 1: Extracting raw data")
    try:
        saved_files = fetch_all_cities()
        if not saved_files:
            logging.warning("No data fetched in extract step.")
    except Exception as e:
        logging.error(f"Extract step failed: {e}")
        sys.exit(1)

    # 2) Transform
    logging.info(">>> Step 2: Transforming raw data")
    try:
        run_transform()
    except Exception as e:
        logging.error(f"Transform step failed: {e}")
        sys.exit(1)

    # 3) Load
    logging.info(">>> Step 3: Loading data to Supabase")
    try:
        run_load()
    except Exception as e:
        logging.error(f"Load step failed: {e}")
        sys.exit(1)

    # 4) Analysis
    logging.info(">>> Step 4: Running analysis")
    try:
        # Simply importing etl_analysis runs the analysis
        import etl_analysis
    except Exception as e:
        logging.error(f"Analysis step failed: {e}")
        sys.exit(1)

    logging.info("========== ETL Pipeline Complete ==========")


if __name__ == "__main__":
    main()
