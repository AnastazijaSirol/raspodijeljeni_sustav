import requests
import random
import time
import json
from datetime import datetime, timedelta
import boto3
import os

API_URL = "http://localhost:8000/readings"

CAMERA_ID = "CAMERA2"
LOCATION = "Kamera Umag"

TRAVEL_TIME_FROM_PULA = 45
TRAVEL_TIME_FROM_RIJEKA = 90
TRAVEL_TIME_FROM_UMAG = 20
TRAVEL_VARIATION = 5  # +- 5 min

PROCESSED_FILE = "processed_vehicles_camera2.json"
CAMERA1_PROCESSED_FILE = "processed_vehicles_camera1.json"

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)
table = dynamodb.Table("Readings")

def load_processed_records(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return set(json.load(f))
    return set()

def save_processed_records(processed_records, file_path):
    with open(file_path, "w") as f:
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
        and item.get("camera_id") in ["PULA-ENTRANCE", "RIJEKA-ENTRANCE", "UMAG-ENTRANCE"]
    ]
    if entrances:
        print(f"Pronađeno {len(entrances)} vozila na ulazima (Pula/Rijeka/Umag).")
    else:
        print("Nema pronađenih vozila na ulazima.")
    return entrances

def generate_speed():
    return random.randint(90, 130)

def generate_vehicle_passages(entrances, processed_records, camera1_records):
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
        if origin == "UMAG-ENTRANCE":
            travel_time = TRAVEL_TIME_FROM_UMAG
        elif origin == "RIJEKA-ENTRANCE":
            if random.random() > 0.6:
                processed_records.add(key)
                continue
            travel_time = TRAVEL_TIME_FROM_RIJEKA
        elif origin == "PULA-ENTRANCE":
            camera1_keys = set(camera1_records)
            if key in camera1_keys:
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
    print("Pokrećem generiranje podataka za kameru CAMERA2 (Rijeka-Umag)...")
    processed_records = load_processed_records(PROCESSED_FILE)
    camera1_records = load_processed_records(CAMERA1_PROCESSED_FILE)

    while True:
        entrances = get_entrances()
        passages = generate_vehicle_passages(entrances, processed_records, camera1_records)
        if passages:
            send_data_to_server(passages)
            save_processed_records(processed_records, PROCESSED_FILE)
        else:
            print("Nema novih vozila za obradu...")
        time.sleep(10)

if __name__ == "__main__":
    main()
