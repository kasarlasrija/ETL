import os
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client
from dotenv import load_dotenv

# -----------------------------
# CONNECT TO SUPABASE
# -----------------------------
def get_supabase_client():
    load_dotenv()
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

# -----------------------------
# MAIN ANALYSIS FUNCTION
# -----------------------------
def run_etl_analysis():
    print("📥 Fetching data from Supabase...")

    supabase = get_supABASE_client = get_supabase_client()

    response = supabase.table("telco_churn").select("*").execute()
    df = pd.DataFrame(response.data)

    print(f"✅ Retrieved {len(df)} records")

    # -----------------------------
    # 📊 METRICS
    # -----------------------------

    ## 1️⃣ Churn Percentage
    churn_rate = (df["churn"].value_counts(normalize=True).get("yes", 0)) * 100

    ## 2️⃣ Avg Monthly Charges per Contract
    avg_monthly_per_contract = df.groupby("contract")["monthlycharges"].mean()

    ## 3️⃣ Customer Segmentation Count
    segment_counts = df["tenure_group"].value_counts()

    ## 4️⃣ Internet Service Distribution
    internet_dist = df["internetservice"].value_counts()

    ## 5️⃣ Pivot Table: Churn vs Tenure Group
    churn_tenure_pivot = pd.pivot_table(
        df,
        index="tenure_group",
        columns="churn",
        values="tenure",
        aggfunc="count",
        fill_value=0
    )

    # -----------------------------
    # 📁 SAVE ANALYSIS CSV
    # -----------------------------
    processed_dir = os.path.join("data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    summary_df = pd.DataFrame({
        "metric": [
            "churn_percentage"
        ],
        "value": [
            churn_rate
        ]
    })

    summary_path = os.path.join(processed_dir, "analysis_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"📁 Analysis summary saved at: {summary_path}")

    # -----------------------------
    # 📊 VISUALIZATIONS
    # -----------------------------

    ## 1️⃣ Churn Rate by Monthly Charge Segment
    churn_segment = df.groupby("monthly_charge_segment")["churn"].value_counts(normalize=True).unstack().fillna(0)

    plt.figure()
    churn_segment["yes"].plot(kind="bar")
    plt.title("Churn Rate by Monthly Charge Segment")
    plt.xlabel("Charge Segment")
    plt.ylabel("Churn Rate")
    plt.tight_layout()
    plt.savefig("data/processed/churn_by_charge_segment.png")
    plt.close()

    ## 2️⃣ Histogram of TotalCharges
    plt.figure()
    plt.hist(df["totalcharges"], bins=30)
    plt.title("Distribution of Total Charges")
    plt.xlabel("Total Charges")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig("data/processed/totalcharges_hist.png")
    plt.close()

    ## 3️⃣ Bar Plot of Contract Types
    plt.figure()
    df["contract"].value_counts().plot(kind="bar")
    plt.title("Contract Type Distribution")
    plt.xlabel("Contract Type")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig("data/processed/contract_distribution.png")
    plt.close()

    print("✅ All visualizations saved in data/processed/")

    # -----------------------------
    # DISPLAY RESULTS
    # -----------------------------
    print("\n📊 FINAL ANALYSIS RESULTS")
    print("-" * 40)
    print(f"Churn Percentage: {round(churn_rate, 2)}%")
    print("\nAverage Monthly Charges per Contract:")
    print(avg_monthly_per_contract)
    print("\nCustomer Segments (Tenure Group):")
    print(segment_counts)
    print("\nInternet Service Distribution:")
    print(internet_dist)
    print("\nPivot Table: Churn vs Tenure Group")
    print(churn_tenure_pivot)

    print("\n🎯 ETL ANALYSIS COMPLETE")

# -----------------------------
# RUN FILE
# -----------------------------
if __name__ == "__main__":
    run_etl_analysis()
