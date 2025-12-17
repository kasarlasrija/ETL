import os
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    downloads_path = os.path.join(
        os.path.expanduser("~"),
        "Downloads",
        "WA_Fn-UseC_-Telco-Customer-Churn.csv"
    )

    if not os.path.exists(downloads_path):
        raise FileNotFoundError(f"❌ File not found: {downloads_path}")

    df = pd.read_csv(downloads_path)

    raw_path = os.path.join(data_dir, "telco_raw.csv")
    df.to_csv(raw_path, index=False)

    print("✅ Extraction complete:", raw_path)
    return raw_path


if __name__ == "__main__":
    extract_data()
