import requests
import random
import time
import json
from datetime import datetime, timedelta
import boto3
import os

API_URL = "http://localhost:8000/readings"

EXIT_ID = "PULA-EXIT"
LOCATION = "Izlaz Pula"

TRAVEL_TIME_FROM_RIJEKA = 90
TRAVEL_TIME_FROM_UMAG = 60
TRAVEL_VARIATION = 10  # +- 10 min

PROCESSED_FILE = "processed_vehicles_pula_exit.json"
CAMERA1_PROCESSED_FILE = "processed_vehicles_camera1.json"
CAMERA2_PROCESSED_FILE = "processed_vehicles_camera2.json"

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
    print(f"🔍 Pronađeno {len(entrances)} ulaznih vozila.")
    return entrances


def generate_vehicle_exits(entrances, processed_records, camera1_vehicle_ids, camera2_vehicle_ids):
    exits = []

    for vehicle in entrances:
        vehicle_id = vehicle.get("vehicle_id")
        origin = vehicle.get("camera_id", "")
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

        must_exit = False
        travel_time = None

        if origin == "PULA-ENTRANCE":
            # vozila iz Pule NE MOGU izaći na ovom izlazu
            processed_records.add(key)
            continue

        elif origin == "UMAG-ENTRANCE":
            # izlaze samo ako NIJE prošlo pored CAMERA1
            if vehicle_id not in camera1_vehicle_ids:
                must_exit = True
                travel_time = TRAVEL_TIME_FROM_UMAG

        elif origin == "RIJEKA-ENTRANCE":
            # izlaze samo ako NIJE prošlo pored CAMERA2
            if vehicle_id not in camera2_vehicle_ids:
                must_exit = True
                travel_time = TRAVEL_TIME_FROM_RIJEKA

        if not must_exit:
            processed_records.add(key)
            continue

        travel_time += random.randint(-TRAVEL_VARIATION, TRAVEL_VARIATION)
        exit_time = entry_time + timedelta(minutes=travel_time)

        exits.append({
            "camera_id": EXIT_ID,
            "camera_location": LOCATION,
            "vehicle_id": vehicle_id,
            "timestamp": exit_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_exit": True,
        })

        processed_records.add(key)

    return exits


def send_data_to_server(exits):
    for data in exits:
        try:
            response = requests.post(API_URL, json=data)
            if response.status_code == 200:
                print(f"{data['camera_id']} snimila izlaz {data['vehicle_id']} u {data['timestamp']}")
            else:
                print(f"Greška: {response.status_code}, {response.text}")
        except requests.exceptions.ConnectionError:
            print("Nije moguće spojiti se na server. Provjeri FastAPI.")
        time.sleep(random.uniform(1.5, 3.5))


def main():
    print("Pokrećem simulaciju izlaza PULA...")
    processed_records = load_processed_records(PROCESSED_FILE)

    camera1_records = load_processed_records(CAMERA1_PROCESSED_FILE)
    camera2_records = load_processed_records(CAMERA2_PROCESSED_FILE)

    while True:
        entrances = get_entrances()
        exits = generate_vehicle_exits(entrances, processed_records, camera1_records, camera2_records)

        if exits:
            send_data_to_server(exits)
            save_processed_records(processed_records, PROCESSED_FILE)
        else:
            print("Nema novih vozila za izlaz.")

        time.sleep(10)


if __name__ == "__main__":
    main()
