import os
import pandas as pd

def transform_data(raw_path):
    print("🔄 Starting transformation...")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_path)

    # ✅ Normalize column names for safety
    df.columns = df.columns.str.strip()

    # -----------------------------
    # 1️⃣ HANDLE MISSING VALUES
    # -----------------------------
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    df["TotalCharges"] = df["TotalCharges"].fillna(df["TotalCharges"].median())
    df = df.ffill()

    # -----------------------------
    # 2️⃣ STANDARDIZE CATEGORICAL DATA
    # -----------------------------
    cat_cols = df.select_dtypes(include=["object"]).columns
    df[cat_cols] = df[cat_cols].apply(lambda col: col.str.lower())

    # -----------------------------
    # 3️⃣ FEATURE ENGINEERING
    # -----------------------------

    # ✅ Tenure Group
    df["tenure_group"] = pd.cut(
        df["tenure"],
        bins=[0, 12, 24, 48, 60, 120],
        labels=["0–1 year", "1–2 years", "2–4 years", "4–5 years", "5+ years"],
        right=False
    )

    # ✅ Monthly Charge Segment
    df["monthly_charge_segment"] = pd.cut(
        df["MonthlyCharges"],
        bins=[0, 35, 70, 120],
        labels=["low", "medium", "high"],
        right=False
    )

    # ✅ Binary Churn Flag
    df["churn_flag"] = df["Churn"].apply(lambda x: 1 if x == "yes" else 0)

    # ✅ Contract Type Encoding (FIXED)
    contract_map = {
        "month-to-month": 0,
        "one year": 1,
        "two year": 2
    }

    df["contract_type_code"] = df["Contract"].map(contract_map)
    df["contract_type_code"] = df["contract_type_code"].fillna(0).astype(int)

    # ✅ Internet Service Flag
    df["has_internet_service"] = df["InternetService"].apply(
        lambda x: 0 if x == "no" else 1
    )

    # ✅ Multi Line Flag
    df["is_multi_line_user"] = df["MultipleLines"].apply(
        lambda x: 0 if x == "no" else 1
    )

    # -----------------------------
    # 4️⃣ DROP UNNECESSARY COLUMNS
    # -----------------------------
    df.drop(columns=["customerID"], inplace=True, errors="ignore")

    # -----------------------------
    # 5️⃣ SAVE TRANSFORMED DATA
    # -----------------------------
    staged_path = os.path.join(staged_dir, "telco_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"✅ Transformation complete: {staged_path}")
    return staged_path


# -----------------------------
# RUN STANDALONE
# -----------------------------
if __name__ == "__main__":
    from extract import extract_data

    raw_path = extract_data()
    transform_data(raw_path)
