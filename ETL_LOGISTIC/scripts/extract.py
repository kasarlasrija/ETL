"""
Mock Extract module for SwiftShip Express ETL pipeline.

Instead of calling the live APIs, it reads sample JSON files from data/raw_mock/
and returns them as if they were fetched from the API.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

RAW_DIR = Path(__file__).resolve().parent / "data" / "raw_mock"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Sample file paths
DELIVERIES_FILE = RAW_DIR / "deliveries_sample.json"
TRAFFIC_FILE = RAW_DIR / "traffic_sample.json"


def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _save_raw(payload: Any, api_name: str) -> str:
    ts = _now_ts()
    path = RAW_DIR / f"{api_name}_raw_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return str(path.resolve())


def fetch_deliveries_live() -> Dict[str, str]:
    """Mock fetching live deliveries"""
    try:
        # If sample exists, read it; otherwise, create sample data
        if DELIVERIES_FILE.exists():
            with open(DELIVERIES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            # Sample data
            data = [
                {
                    "shipment_id": "SHP001",
                    "source_city": "Delhi",
                    "destination_city": "Mumbai",
                    "dispatch_time": "2025-12-11T08:00:00",
                    "expected_delivery_time": "2025-12-11T16:00:00",
                    "actual_delivery_time": "2025-12-11T16:30:00",
                    "package_weight": 12.5,
                    "delivery_agent_id": "AG001"
                },
                {
                    "shipment_id": "SHP002",
                    "source_city": "Bengaluru",
                    "destination_city": "Hyderabad",
                    "dispatch_time": "2025-12-11T09:00:00",
                    "expected_delivery_time": "2025-12-11T14:00:00",
                    "actual_delivery_time": "2025-12-11T13:50:00",
                    "package_weight": 7.2,
                    "delivery_agent_id": "AG002"
                }
            ]
        saved_path = _save_raw(data, "deliveries")
        print(f"✅ [deliveries] Mock fetched and saved to {saved_path}")
        return {"payload": data, "raw_path": saved_path}
    except Exception as e:
        return {"error": str(e)}


def fetch_traffic_routes() -> Dict[str, str]:
    """Mock fetching traffic routes"""
    try:
        if TRAFFIC_FILE.exists():
            with open(TRAFFIC_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            # Sample traffic data
            data = [
                {
                    "city_name": "Delhi",
                    "congestion_score": 8,
                    "avg_speed": 25,
                    "weather_warning": "None"
                },
                {
                    "city_name": "Mumbai",
                    "congestion_score": 6,
                    "avg_speed": 35,
                    "weather_warning": "Heavy Rain"
                }
            ]
        saved_path = _save_raw(data, "traffic_routes")
        print(f"✅ [traffic_routes] Mock fetched and saved to {saved_path}")
        return {"payload": data, "raw_path": saved_path}
    except Exception as e:
        return {"error": str(e)}


def fetch_all_raw() -> Dict[str, Dict[str, str]]:
    results = {}
    results["deliveries"] = fetch_deliveries_live()
    results["traffic"] = fetch_traffic_routes()
    return results


if __name__ == "__main__":
    print("Starting mock extraction...")
    out = fetch_all_raw()
    print("Extraction complete. Summary:")
    for k, v in out.items():
        if "raw_path" in v:
            print(f" - {k}: saved -> {v['raw_path']}")
        else:
            print(f" - {k}: error -> {v.get('error')}")
