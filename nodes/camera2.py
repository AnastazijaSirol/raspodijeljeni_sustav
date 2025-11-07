import requests
import random
import time
import json
from datetime import datetime, timedelta
import boto3
import os
from filelock import FileLock

API_URL = "http://localhost:8000/readings"

CAMERA_ID = "CAMERA2"
LOCATION = "Kamera Umag"

TRAVEL_TIME_FROM_PULA = 45
TRAVEL_TIME_FROM_RIJEKA = 90
TRAVEL_TIME_FROM_UMAG = 20
TRAVEL_VARIATION = 5  # +- 5 min

RIJEKA_CAMERA2_CHANCE = 0.4

PROCESSED_FILE = "processed_vehicles_camera2.json"
CAMERA1_PROCESSED_FILE = "processed_vehicles_camera1.json"

PULA_ROUTE_FILE = "pula_routes.json"
RIJEKA_ROUTE_FILE = "rijeka_routes.json"
UMAG_ROUTE_FILE = "umag_routes.json"

PULA_LOCK = PULA_ROUTE_FILE + ".lock"
RIJEKA_LOCK = RIJEKA_ROUTE_FILE + ".lock"
UMAG_LOCK = UMAG_ROUTE_FILE + ".lock"

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)
table = dynamodb.Table("Readings")


def load_json(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, "r") as f:
            return json.load(f)
    return {}


def save_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def load_processed_records():
    data = load_json(PROCESSED_FILE)
    return set(data) if isinstance(data, list) else set()


def save_processed_records(processed_records):
    save_json(list(processed_records), PROCESSED_FILE)


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
    print(f"Pronađeno {len(entrances)} ulaznih vozila (Pula/Rijeka/Umag).")
    return entrances


def generate_speed():
    return random.randint(90, 130)


def generate_vehicle_passages(entrances, processed_records, camera1_vehicle_ids):
    passages = []

    # 🔒 Sigurno učitavanje svih ruta
    with FileLock(PULA_LOCK):
        pula_routes = load_json(PULA_ROUTE_FILE)
    with FileLock(RIJEKA_LOCK):
        rijeka_routes = load_json(RIJEKA_ROUTE_FILE)
    with FileLock(UMAG_LOCK):
        umag_routes = load_json(UMAG_ROUTE_FILE)

    for vehicle in entrances:
        vehicle_id = vehicle.get("vehicle_id")
        origin = vehicle.get("camera_id")
        timestamp_str = vehicle.get("timestamp")

        if not vehicle_id or not timestamp_str or not origin:
            continue

        key = f"{vehicle_id}_{origin}_{timestamp_str}"
        if key in processed_records:
            continue

        try:
            entry_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        must_pass = False
        travel_time = None

        if origin == "UMAG-ENTRANCE":
            route = umag_routes.get(vehicle_id)
            if not route:
                umag_routes[vehicle_id] = "CAMERA2"
                must_pass = True
                travel_time = TRAVEL_TIME_FROM_UMAG
            elif route == "CAMERA2":
                must_pass = True
                travel_time = TRAVEL_TIME_FROM_UMAG

        elif origin == "RIJEKA-ENTRANCE":
            route = rijeka_routes.get(vehicle_id)
            if not route:
                if random.random() <= RIJEKA_CAMERA2_CHANCE:
                    rijeka_routes[vehicle_id] = "CAMERA2"
                    must_pass = True
                    travel_time = TRAVEL_TIME_FROM_RIJEKA
                else:
                    rijeka_routes[vehicle_id] = "CAMERA1"
            elif route == "CAMERA2":
                must_pass = True
                travel_time = TRAVEL_TIME_FROM_RIJEKA

        elif origin == "PULA-ENTRANCE":
            route = pula_routes.get(vehicle_id)
            if not route:
                pula_routes[vehicle_id] = "CAMERA2"
                must_pass = True
                travel_time = TRAVEL_TIME_FROM_PULA
            elif route == "CAMERA2":
                must_pass = True
                travel_time = TRAVEL_TIME_FROM_PULA

        if not must_pass:
            processed_records.add(key)
            continue

        travel_time += random.randint(-TRAVEL_VARIATION, TRAVEL_VARIATION)
        passage_time = entry_time + timedelta(minutes=travel_time)

        passages.append({
            "camera_id": CAMERA_ID,
            "camera_location": LOCATION,
            "vehicle_id": vehicle_id,
            "timestamp": passage_time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_camera": True,
            "speed": generate_speed(),
            "speed_limit": 130,
        })

        processed_records.add(key)

    # 🔒 Sigurno spremanje svih ruta
    with FileLock(PULA_LOCK):
        save_json(pula_routes, PULA_ROUTE_FILE)
    with FileLock(RIJEKA_LOCK):
        save_json(rijeka_routes, RIJEKA_ROUTE_FILE)
    with FileLock(UMAG_LOCK):
        save_json(umag_routes, UMAG_ROUTE_FILE)

    return passages


def send_data_to_server(passages):
    for data in passages:
        try:
            r = requests.post(API_URL, json=data)
            if r.status_code == 200:
                print(f"{CAMERA_ID} snimila {data['vehicle_id']} ({data['speed']} km/h) u {data['timestamp']}")
            else:
                print(f"Greška: {r.status_code}, {r.text}")
        except requests.exceptions.ConnectionError:
            print("Ne mogu se spojiti na server. Provjeri FastAPI.")
        time.sleep(random.uniform(1.5, 3.5))


def main():
    print("Pokrećem generiranje podataka za kameru CAMERA2...")
    processed_records = load_processed_records()

    camera1_records = load_json(CAMERA1_PROCESSED_FILE)
    camera1_vehicle_ids = {rec.split("_")[0] for rec in camera1_records} if isinstance(camera1_records, list) else set()

    while True:
        entrances = get_entrances()
        passages = generate_vehicle_passages(entrances, processed_records, camera1_vehicle_ids)

        if passages:
            send_data_to_server(passages)
            save_processed_records(processed_records)
        else:
            print("Nema novih vozila za obradu.")

        time.sleep(10)


if __name__ == "__main__":
    main()

# SVA vozila iz UMAGA prolaze pored ove kamere
# 40% vozila iz RIJEKE prolaze pored ove kamere
# vozila iz PULE koja NISU prošla camera1 prolaze pored ove kamere