import requests
import random
import time
import json
from datetime import datetime, timedelta
import os
from filelock import FileLock
import boto3

API_URL = "http://localhost:8000/readings"

RESTAREA_ID = "RESTAREA1"
LOCATION = "Odmorište 1"

TRAVEL_TO_RESTAREA_FROM_ENTRANCE = (3, 7)
TRAVEL_TO_RESTAREA_BEFORE_EXIT = (3, 7)
STOP_DURATION = (15, 30)

PROCESSED_FILE = "processed_vehicles_restarea1.json"

PULA_ENTRANCE_ID = "PULA-ENTRANCE"
PULA_EXIT_ID = "PULA-EXIT"

ENTRANCE_PASS_CHANCE = 0.6
EXIT_PASS_CHANCE = 0.5


def load_json(file_path):
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

def get_entrances_and_exits():
    all_items = scan_full_table()

    entrances = [
        item for item in all_items
        if str(item.get("is_entrance")).lower() == "true"
        and item.get("camera_id") == PULA_ENTRANCE_ID
    ]

    exits = [
        item for item in all_items
        if str(item.get("is_exit")).lower() == "true"
        and item.get("camera_id") == PULA_EXIT_ID
    ]

    print(f"🔍 Pronađeno {len(entrances)} ulaza i {len(exits)} izlaza za PULA.")
    return entrances, exits

def generate_restarea_stops(entrances, exits, processed_records):
    stops = []

    # Vozila koja su ušla na ulaz PULA
    for vehicle in entrances:
        vehicle_id = vehicle.get("vehicle_id")
        key = f"{vehicle_id}_entrance_{vehicle.get('timestamp', '')}"
        if key in processed_records:
            continue

        if random.random() > ENTRANCE_PASS_CHANCE:
            processed_records.add(key)
            continue

        try:
            t_entry = datetime.strptime(vehicle["timestamp"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        delay = random.randint(*TRAVEL_TO_RESTAREA_FROM_ENTRANCE)
        t_rest_entry = t_entry + timedelta(minutes=delay)
        stop_time = random.randint(*STOP_DURATION)
        t_rest_exit = t_rest_entry + timedelta(minutes=stop_time)

        stops.append({
            "camera_id": RESTAREA_ID,
            "camera_location": LOCATION,
            "vehicle_id": vehicle_id,
            "is_restarea": True,
            "timestamp_entrance": t_rest_entry.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_exit": t_rest_exit.strftime("%Y-%m-%d %H:%M:%S"),
        })
        print(f"{vehicle_id} (ulaz PULA) zaustavlja se na odmorištu u {t_rest_entry.strftime('%H:%M:%S')}")
        processed_records.add(key)

    # Vozila koja izlaze na PULA
    for vehicle in exits:
        vehicle_id = vehicle.get("vehicle_id")
        key = f"{vehicle_id}_exit_{vehicle.get('timestamp', '')}"
        if key in processed_records:
            continue

        if random.random() > EXIT_PASS_CHANCE:
            processed_records.add(key)
            continue

        try:
            t_exit = datetime.strptime(vehicle["timestamp"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        t_rest_exit = t_exit - timedelta(minutes=random.randint(*TRAVEL_TO_RESTAREA_BEFORE_EXIT))
        stop_time = random.randint(*STOP_DURATION)
        t_rest_entry = t_rest_exit - timedelta(minutes=stop_time)

        stops.append({
            "camera_id": RESTAREA_ID,
            "camera_location": LOCATION,
            "vehicle_id": vehicle_id,
            "is_restarea": True,
            "timestamp_entrance": t_rest_entry.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_exit": t_rest_exit.strftime("%Y-%m-%d %H:%M:%S"),
        })
        print(f"{vehicle_id} (izlaz PULA) staje na odmorištu prije izlaska u {t_rest_entry.strftime('%H:%M:%S')}")
        processed_records.add(key)

    return stops

def send_data_to_server(stops):
    for data in stops:
        try:
            r = requests.post(API_URL, json=data)
            if r.status_code == 200:
                print(f"RESTAREA1 zabilježila vozilo {data['vehicle_id']} ({data['timestamp_entrance']} → {data['timestamp_exit']})")
            else:
                print(f"Greška {r.status_code}: {r.text}")
        except requests.exceptions.ConnectionError:
            print("Ne mogu se spojiti na server. Provjeri FastAPI.")
        time.sleep(random.uniform(1.0, 2.5))

def main():
    print("Pokrećem simulaciju odmorišta RESTAREA1...")
    processed_records = load_processed_records()

    while True:
        entrances, exits = get_entrances_and_exits()
        stops = generate_restarea_stops(entrances, exits, processed_records)

        if stops:
            send_data_to_server(stops)
            save_processed_records(processed_records)
        else:
            print("Nema novih vozila za odmorište.")

        time.sleep(10)

if __name__ == "__main__":
    main()

# Vozila koja su ušla na ulaz PULA mogu se zaustaviti ovdje (3–7 min nakon ulaska)
# Vozila koja izlaze na PULA mogu se zaustaviti ovdje (3–7 min prije izlaska)
# Ostala vozila se NE zaustavljaju
# Zaustavljanje traje 15-30 minuta