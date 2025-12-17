import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def fetch_all_rows(table, supabase, batch_size=1000):
    all_data = []
    start = 0

    while True:
        response = (
            supabase
            .table(table)
            .select("*")
            .range(start, start + batch_size - 1)
            .execute()
        )

        data = response.data
        if not data:
            break

        all_data.extend(data)
        start += batch_size

    return pd.DataFrame(all_data)

def validate_data():
    print("\n🔍 STARTING DATA VALIDATION...\n")

    load_dotenv()

    supabase = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

    # ✅ Load Transformed CSV
    staged_path = os.path.join("data", "staged", "telco_transformed.csv")
    df_csv = pd.read_csv(staged_path)

    print(f"📄 Transformed CSV rows: {len(df_csv)}")

    # ✅ Load FULL Supabase Table (No 1000-row limit)
    df_db = fetch_all_rows("telco_churn", supabase)
    print(f"🗄 Supabase table rows: {len(df_db)}\n")

    print("✅ VALIDATION RESULTS")
    print("-" * 45)

    # ✅ No missing values check
    if df_db[["tenure", "monthlycharges", "totalcharges"]].isnull().sum().sum() == 0:
        print("✅ No missing values in tenure, MonthlyCharges, TotalCharges")
    else:
        print("❌ Missing values detected!")

    # ✅ Unique rows
    unique_rows = df_db.drop_duplicates().shape[0]
    print(f"✅ Unique rows in Supabase: {unique_rows}")

    # ✅ Row count match
    if len(df_csv) == len(df_db):
        print("✅ CSV row count matches Supabase table")
    else:
        print(f"❌ Row count mismatch! CSV={len(df_csv)}, Supabase={len(df_db)}")

    # ✅ Segment existence check (FIXED COLUMN NAME)
    if df_db["tenure_group"].isnull().sum() == 0:
        print("✅ tenure_group exists for all records")
    else:
        print("❌ tenure_group has missing values")

    if df_db["monthly_charge_segment"].isnull().sum() == 0:
        print("✅ monthly_charge_segment exists for all records")
    else:
        print("❌ monthly_charge_segment has missing values")

    # ✅ Contract code validation
    valid_codes = {0, 1, 2}
    invalid_codes = set(df_db["contract_type_code"].unique()) - valid_codes

    if len(invalid_codes) == 0:
        print("✅ Contract codes are valid {0,1,2}")
    else:
        print("❌ Invalid contract codes found:", invalid_codes)

    print("\n🎯 VALIDATION COMPLETE\n")

if __name__ == "__main__":
    validate_data()
