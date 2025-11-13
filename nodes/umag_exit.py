import requests
import random
import time
import json
from datetime import datetime, timedelta
import os
from filelock import FileLock
import boto3

API_URL = "http://localhost:8000/readings"

EXIT_ID = "UMAG-EXIT"
LOCATION = "Izlaz Umag"

TRAVEL_TIME_FROM_PULA = 60
TRAVEL_TIME_FROM_RIJEKA = 70
TRAVEL_VARIATION = 10  # +- 10 min

PROCESSED_FILE = "processed_vehicles_umag_exit.json"
PULA_ROUTES_FILE = "pula_routes.json"
RIJEKA_ROUTES_FILE = "rijeka_routes.json"

PULA_LOCK = PULA_ROUTES_FILE + ".lock"
RIJEKA_LOCK = RIJEKA_ROUTES_FILE + ".lock"

def load_json(file_path):
    """Učitava JSON datoteku i vraća dict; ako je prazna/oštećena — vraća prazan dict."""
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Upozorenje: {file_path} je oštećen ili prazan. Resetiram...")
    return {}


def save_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def load_processed_records():
    if os.path.exists(PROCESSED_FILE) and os.path.getsize(PROCESSED_FILE) > 0:
        try:
            with open(PROCESSED_FILE, "r") as f:
                return set(json.load(f))
        except json.JSONDecodeError:
            print("Procesirani fajl oštećen — resetiram ga.")
            return set()
    return set()


def save_processed_records(processed_records):
    save_json(list(processed_records), PROCESSED_FILE)


def scan_full_table():
    items = []
    dynamodb = boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )
    table = dynamodb.Table("Readings")

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
    print(f"Pronađeno {len(entrances)} ulaznih vozila.")
    return entrances


def camera_has(vehicle_routes, vehicle_id, cam):
    if vehicle_id not in vehicle_routes:
        return False
    val = vehicle_routes[vehicle_id]
    if isinstance(val, str):
        return val == cam
    if isinstance(val, list):
        return cam in val
    return False

def generate_vehicle_exits(entrances, processed_records):
    exits = []

    with FileLock(PULA_LOCK):
        pula_routes = load_json(PULA_ROUTES_FILE)
    with FileLock(RIJEKA_LOCK):
        rijeka_routes = load_json(RIJEKA_ROUTES_FILE)

    print(f"Učitano {len(pula_routes)} iz Pula ruta i {len(rijeka_routes)} iz Rijeka ruta.")

    for vehicle in entrances:
        vehicle_id = vehicle.get("vehicle_id")
        origin = vehicle.get("camera_id", "")
        key = f"{vehicle_id}_{origin}_{vehicle.get('timestamp', '')}"

        if key in processed_records:
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

        # Vozila s ulaza UMAG ne mogu izaći ovdje
        if origin == "UMAG-ENTRANCE":
            processed_records.add(key)
            continue

        # Vozila iz RIJEKE izlaze ako su prošla i CAMERA1 i CAMERA2
        elif origin == "RIJEKA-ENTRANCE":
            if camera_has(rijeka_routes, vehicle_id, "CAMERA2"):
                must_exit = True
                travel_time = TRAVEL_TIME_FROM_RIJEKA

        # Vozila iz PULE izlaze ako su prošla CAMERA2
        elif origin == "PULA-ENTRANCE":
            if camera_has(pula_routes, vehicle_id, "CAMERA2"):
                must_exit = True
                travel_time = TRAVEL_TIME_FROM_PULA

        if must_exit:
            travel_time += random.randint(-TRAVEL_VARIATION, TRAVEL_VARIATION)
            exit_time = entry_time + timedelta(minutes=travel_time)

            exits.append({
                "camera_id": EXIT_ID,
                "camera_location": LOCATION,
                "vehicle_id": vehicle_id,
                "timestamp": exit_time.strftime("%Y-%m-%d %H:%M:%S"),
                "is_exit": True,
            })
            print(f"{vehicle_id} ({origin}) izlazi na {EXIT_ID}")
        else:
            print(f"{vehicle_id} ({origin}) ne ispunjava uvjete za izlaz.")

        processed_records.add(key)

    return exits

def send_data_to_server(exits):
    for data in exits:
        try:
            r = requests.post(API_URL, json=data)
            if r.status_code == 200:
                print(f"{EXIT_ID} zabilježio izlaz {data['vehicle_id']} u {data['timestamp']}")
            else:
                print(f"Greška {r.status_code}: {r.text}")
        except requests.exceptions.ConnectionError:
            print("Ne mogu se spojiti na server. Provjeri FastAPI.")
        time.sleep(random.uniform(1.5, 3.5))

def main():
    print("Pokrećem simulaciju izlaza UMAG...")
    processed_records = load_processed_records()

    while True:
        entrances = get_entrances()
        exits = generate_vehicle_exits(entrances, processed_records)

        if exits:
            send_data_to_server(exits)
            save_processed_records(processed_records)
        else:
            print("Nema novih vozila za izlaz.")

        time.sleep(10)


if __name__ == "__main__":
    main()

# Vozila s ulaza UMAG ne izlaze na ovaj izlaz
# Vozila s ulaza RIJEKA izlaze ako su prošla CAMERA1 i CAMERA2
# Vozila s ulaza PULA izlaze ako su prošla CAMERA2
