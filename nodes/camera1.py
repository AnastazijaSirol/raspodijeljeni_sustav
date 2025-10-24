import requests
import random
import time
import json
from datetime import datetime, timedelta
import boto3
import os

API_URL = "http://localhost:8000/readings"

CAMERA_ID = "CAMERA1"
LOCATION = "Kamera Rijeka"

TRAVEL_TIME_FROM_RIJEKA = 35
TRAVEL_TIME_FROM_PULA = 40
TRAVEL_VARIATION = 5  # +- 5 min

PROCESSED_FILE = "processed_vehicles.json"

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

table = dynamodb.Table("Readings")

def load_processed_records():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_processed_records(processed_records):
    with open(PROCESSED_FILE, "w") as f:
        json.dump(list(processed_records), f)

def make_unique_key(vehicle):
    return f"{vehicle.get('vehicle_id')}_{vehicle.get('camera_id')}_{vehicle.get('timestamp')}"

def scan_full_table():
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items

def get_entrances():
    all_items = scan_full_table()

    entrances = [
        item for item in all_items
        if str(item.get("is_entrance")).lower() == "true"
        and item.get("camera_id") in ["PULA-ENTRANCE", "RIJEKA-ENTRANCE"]
    ]

    if entrances:
        print(f"Pronađeno {len(entrances)} vozila na ulazima (Pula/Rijeka).")
    else:
        print("Nema pronađenih vozila na ulazima.")
    return entrances

def generate_speed():
    return random.randint(90, 130)

def generate_vehicle_passages(entrances, processed_records):
    passages = []

    for vehicle in entrances:
        key = make_unique_key(vehicle)
        if not key or key in processed_records:
            continue

        timestamp_str = vehicle.get("timestamp")
        if not timestamp_str:
            continue

        try:
            entry_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        origin = vehicle.get("camera_id", "")
        if origin == "RIJEKA-ENTRANCE":
            travel_time = TRAVEL_TIME_FROM_RIJEKA
        elif origin == "PULA-ENTRANCE":
            if random.random() > 0.6:
                processed_records.add(key)
                continue
            travel_time = TRAVEL_TIME_FROM_PULA
        else:
            continue

        travel_time += random.randint(-TRAVEL_VARIATION, TRAVEL_VARIATION)
        passage_time = entry_time + timedelta(minutes=travel_time)

        speed = generate_speed()
        speed_limit = 130

        passages.append({
            "camera_id": CAMERA_ID,
            "camera_location": LOCATION,
            "vehicle_id": vehicle["vehicle_id"],
            "timestamp": passage_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_camera": True,
            "speed": speed,
            "speed_limit": speed_limit,
        })

        processed_records.add(key)

    return passages

def send_data_to_server(passages):
    for data in passages:
        try:
            response = requests.post(API_URL, json=data)
            if response.status_code == 200:
                print(f"Kamera {data['camera_id']} snimila {data['vehicle_id']} u {data['timestamp']} ({data['speed']} km/h)")
            else:
                print(f"Greška: {response.status_code}, {response.text}")
        except requests.exceptions.ConnectionError:
            print("Ne mogu se spojiti na server. Provjeri da FastAPI radi.")
        
        time.sleep(random.uniform(1.5, 3.5))

def main():
    print("Pokrećem generiranje podataka za kameru PULA–RIJEKA...")

    processed_records = load_processed_records()

    while True:
        entrances = get_entrances()
        passages = generate_vehicle_passages(entrances, processed_records)
        if passages:
            send_data_to_server(passages)
            save_processed_records(processed_records)
        else:
            print("Nema novih vozila za obradu...")
        time.sleep(10)

if __name__ == "__main__":
    main()
