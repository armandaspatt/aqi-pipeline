"""Poll CPCB real-time AQI data (data.gov.in) for 10 locked cities and
publish each (station, pollutant) reading to Kafka.

Resource: 3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69 (Real time Air Quality Index)
Confirmed via Postman:
  - server-side city filtering works: filters[city]=<city>
  - last_update format: "DD-MM-YYYY HH:MM:SS" (not ISO)
  - missing readings arrive as the string "NA", not null
  - one record = one (station, pollutant) pair

Bronze principle: this script does NOT clean data (no NA handling) beyond
keeping the raw fields as-received. Real cleaning happens in dbt staging.

Mock mode (AQI_MOCK_MODE=1, default) generates fake readings for the same
10 cities so the rest of the pipeline can be built/tested without spending
real API calls.
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

TOPIC = os.environ.get("AQI_TOPIC", "cpcb-aqi-raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
API_KEY = os.environ.get("DATA_GOV_API_KEY", "")
MOCK = os.environ.get("AQI_MOCK_MODE", "1") == "1"

RESOURCE_URL = (
    "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
)
PER_CITY_LIMIT = 500

# data.gov.in appears to hang/drop requests carrying the default
# "python-requests/x.x" User-Agent (confirmed via testing: curl and a
# browser-like UA both return 200 instantly, default requests UA times out
# consistently). Sending a browser-like UA fixes it.
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}

# Locked 10-city list (tech hubs + high-AQI metros + home city)
CITIES = [
    "Bengaluru",
    "Hyderabad",
    "Pune",
    "Chennai",
    "Delhi",
    "Mumbai",
    "Gurugram",
    "Noida",
    "Kolkata",
    "Bhubaneswar",
]

# Used only in mock mode: rough lat/lon + station names per city
MOCK_CITY_META = {
    "Bengaluru":   (12.9716, 77.5946, ["Hombegowda Nagar", "BWSSB Kadabesanahalli", "Jayanagar 5th Block"]),
    "Hyderabad":   (17.3850, 78.4867, ["Sanathnagar", "ICRISAT Patancheru"]),
    "Pune":        (18.5204, 73.8567, ["Karve Road", "Bhosari"]),
    "Chennai":     (13.0827, 80.2707, ["Alandur", "Manali Village"]),
    "Delhi":       (28.7041, 77.1025, ["Anand Vihar", "RK Puram", "Punjabi Bagh"]),
    "Mumbai":      (19.0760, 72.8777, ["Bandra Kurla", "Sion"]),
    "Gurugram":    (28.4595, 77.0266, ["Sector 51", "Vikas Sadan"]),
    "Noida":       (28.5355, 77.3910, ["Sector 62", "Sector 125"]),
    "Kolkata":     (22.5726, 88.3639, ["Victoria", "Rabindra Bharati"]),
    "Bhubaneswar": (20.2961, 85.8245, ["IRC Village", "Capital Hospital"]),
}
POLLUTANTS = {"PM2.5": (30, 400), "PM10": (50, 500), "NO2": (10, 120),
              "SO2": (5, 60), "CO": (20, 150), "OZONE": (10, 130), "NH3": (1, 40)}


def fetch_city(city: str) -> list[dict]:
    """Fetch all records for one city via server-side filtering."""
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": PER_CITY_LIMIT,
        "filters[city]": city,
    }
    resp = requests.get(RESOURCE_URL, params=params, headers=REQUEST_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    records = data.get("records", [])
    total = data.get("total", len(records))
    if isinstance(total, str):
        total = int(total) if total.isdigit() else len(records)
    if total > PER_CITY_LIMIT:
        print(f"WARNING: {city} has {total} records, only fetched {PER_CITY_LIMIT}"
              f" (raise PER_CITY_LIMIT or add pagination)", file=sys.stderr)
    return records


def fetch_real() -> list[dict]:
    polled_at = datetime.now(timezone.utc).isoformat()
    out = []
    for city in CITIES:
        try:
            records = fetch_city(city)
        except requests.RequestException as e:
            print(f"ERROR fetching {city}: {e}", file=sys.stderr)
            continue
        for r in records:
            out.append({**r, "polled_at": polled_at, "source": "cpcb"})
        time.sleep(0.2)  # be polite to the API between city calls
    return out


def fetch_mock() -> list[dict]:
    now_str = datetime.now(timezone.utc).strftime("%d-%m-%Y %H:%M:%S")
    polled_at = datetime.now(timezone.utc).isoformat()
    out = []
    for city, (lat, lon, stations) in MOCK_CITY_META.items():
        for station in stations:
            if random.random() < 0.05:
                continue  # simulate a station occasionally not reporting
            for pollutant, (lo, hi) in POLLUTANTS.items():
                if random.random() < 0.05:
                    min_v = max_v = avg_v = "NA"
                else:
                    avg = random.randint(lo, hi)
                    min_v = str(max(0, avg - random.randint(5, 30)))
                    max_v = str(avg + random.randint(5, 30))
                    avg_v = str(avg)
                out.append({
                    "country": "India", "state": "NA", "city": city,
                    "station": f"{station}, {city} - MOCK",
                    "last_update": now_str,
                    "latitude": str(lat), "longitude": str(lon),
                    "pollutant_id": pollutant,
                    "min_value": min_v, "max_value": max_v, "avg_value": avg_v,
                    "polled_at": polled_at, "source": "mock",
                })
    return out


def main() -> int:
    if not MOCK and not API_KEY:
        print("ERROR: DATA_GOV_API_KEY not set and AQI_MOCK_MODE!=1", file=sys.stderr)
        return 1

    records = fetch_mock() if MOCK else fetch_real()
    if not records:
        print("WARNING: 0 records fetched")
        return 0

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for rec in records:
        key = f"{rec['city']}|{rec['station']}|{rec['pollutant_id']}"
        producer.produce(TOPIC, key=key.encode(), value=json.dumps(rec).encode())
    producer.flush(30)
    print(f"Produced {len(records)} records to '{TOPIC}' across {len(CITIES)} cities (mock={MOCK})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
