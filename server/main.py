from fastapi import FastAPI
from models import Reading
from database import dynamodb_client
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Attr

app = FastAPI()

TABLE_NAME = "Readings"

def can_enter(vehicle_id: str, table, hours: int = 12) -> bool:
    cutoff_time = datetime.now() - timedelta(hours=hours)
    cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

    response = table.scan(
        FilterExpression=Attr("vehicle_id").eq(vehicle_id)
        & Attr("is_entrance").eq(True)
        & Attr("timestamp").gte(cutoff_str)
    )

    return len(response.get("Items", [])) == 0

@app.get("/")
def root():
    return {"message": "Server radi!"}

@app.post("/readings")
def add_reading(reading: Reading):
    table = dynamodb_client.Table(TABLE_NAME)

    if reading.is_entrance:
        if not can_enter(reading.vehicle_id, table):
            return {
                "status": "blocked",
                "reason": "Vozilo je već ušlo u zadnjih 12 sati.",
            }

    table.put_item(Item=reading.model_dump())
    return {"status": "success", "data": reading}


@app.get("/readings")
def get_all_readings():
    table = dynamodb_client.Table(TABLE_NAME)

    response = table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return {"count": len(items), "data": items}


@app.get("/stats")
def get_statistics():
    table = dynamodb_client.Table(TABLE_NAME)

    response = table.scan()
    items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    entrances = {"PULA-ENTRANCE": set(), "RIJEKA-ENTRANCE": set(), "UMAG-ENTRANCE": set()}
    cameras = {}
    exits = {}

    for item in items:
        vehicle_id = item.get("vehicle_id")
        camera_id = item.get("camera_id")
        is_entrance = str(item.get("is_entrance")).lower() == "true"
        is_camera = str(item.get("is_camera")).lower() == "true"

        if not vehicle_id or not camera_id:
            continue

        # Vozila na ulazima
        if is_entrance and camera_id in entrances:
            entrances[camera_id].add(vehicle_id)

        # Vozila koja su prošla pored kamera
        if is_camera:
            if camera_id not in cameras:
                cameras[camera_id] = set()
            cameras[camera_id].add(vehicle_id)

        # Vozila koja su izašla
        if not is_entrance and not is_camera:
            if camera_id not in exits:
                exits[camera_id] = set()
            exits[camera_id].add(vehicle_id)

    # Koliko vozila je prošlo pored kamere sa svakog od ulaza
    detailed_stats = {}
    for entrance, vehicles in entrances.items():
        detailed_stats[entrance] = {
            "total_entrances": len(vehicles),
            "passed_cameras": {},
            "exited": {}
        }
        for cam_id, cam_vehicles in cameras.items():
            passed = len(vehicles.intersection(cam_vehicles))
            detailed_stats[entrance]["passed_cameras"][cam_id] = passed
        # Izlazi
        for exit_id, exit_vehicles in exits.items():
            exited_count = len(vehicles.intersection(exit_vehicles))
            detailed_stats[entrance]["exited"][exit_id] = exited_count

    return {"statistics": detailed_stats}
