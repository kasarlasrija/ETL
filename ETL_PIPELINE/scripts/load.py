import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# ----------------------------------------
# Batch Generator
# ----------------------------------------
def chunk_data(data, size=200):
    for i in range(0, len(data), size):
        yield data[i:i + size]

# ----------------------------------------
# Load Function
# ----------------------------------------
def load_data(transformed_path):
    load_dotenv()

    supabase = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

    df = pd.read_csv(transformed_path)
    df.columns = df.columns.str.lower()

    required_cols = [
        "tenure", "monthlycharges", "totalcharges", "churn",
        "internetservice", "contract", "paymentmethod",
        "tenure_group", "monthly_charge_segment",
        "has_internet_service", "is_multi_line_user",
        "contract_type_code"
    ]

    # ✅ Keep only valid columns
    df = df[required_cols]

    # ✅ Convert NaN → None (Postgres compatible)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict("records")

    print(f"\n⬆ Uploading {len(records)} rows to Supabase...\n")

    # ✅ OPTIONAL: Clear table before fresh load (prevents duplicates)
    supabase.table("telco_churn").delete().neq("contract_type_code", -1).execute()
    print("🧹 Old records cleared\n")

    success_count = 0

    for i, batch in enumerate(chunk_data(records), start=1):
        for attempt in range(3):
            try:
                response = supabase.table("telco_churn").insert(batch).execute()

                if response.data:
                    success_count += len(batch)
                    print(f"✅ Batch {i} uploaded ({len(batch)} records)")
                    break
                else:
                    raise Exception("Empty response from Supabase")

            except Exception as e:
                print(f"⚠ Batch {i} retry {attempt + 1} failed:", e)
                time.sleep(2)

    print(f"\n🎯 LOAD COMPLETE — {success_count} rows inserted successfully\n")

# ----------------------------------------
# MAIN PIPELINE
# ----------------------------------------
if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data

    raw = extract_data()
    staged = transform_data(raw)
    load_data(staged)
