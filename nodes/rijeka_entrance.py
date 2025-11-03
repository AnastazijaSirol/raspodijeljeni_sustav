import requests
import random
import string
import time
from datetime import datetime

API_URL = "http://localhost:8000/readings"

CAMERA_ID = "RIJEKA-ENTRANCE"
LOCATION = "Ulaz Rijeka"

def generate_random_registration():
    region = random.choice(["PU", "RI", "ZG", "ST", "ZD", "OS"])
    digits = ''.join(random.choices(string.digits, k=3))
    letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    return f"{region}{digits}{letters}"

def generate_vehicle_data():
    vehicle_id = generate_random_registration()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "camera_id": CAMERA_ID,
        "camera_location": LOCATION,
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "is_entrance": True,
    }

def send_data():
    while True:
        data = generate_vehicle_data()
        try:
            response = requests.post(API_URL, json=data)
            if response.status_code == 200:
                print(f"Poslano: {data['camera_id']} ({data['vehicle_id']}) u {data['timestamp']}")
            else:
                print(f"Greška: {response.status_code}, {response.text}")
        except requests.exceptions.ConnectionError:
            print("Ne mogu se spojiti na server. Provjeri da FastAPI radi.")

        delay = 30

        # simuliranje "slučajne greške" u vremenu, da ne bude pravilno svakih 30s
        if random.random() < 0.1:
            delay += random.choice([-5, 5])
            if delay < 1:
                delay = 1

        time.sleep(delay)

if __name__ == "__main__":
    print("Pokrećem generianje podataka ULAZA RIJEKA...")
    send_data()
