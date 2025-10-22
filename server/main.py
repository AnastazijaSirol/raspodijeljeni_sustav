from fastapi import FastAPI
from models import Reading
from database import dynamodb_client

app = FastAPI()

TABLE_NAME = "Readings"

@app.get("/")
def root():
    return {"message": "Server radi!"}

@app.post("/readings")
def add_reading(reading: Reading):
    table = dynamodb_client.Table(TABLE_NAME)
    table.put_item(Item=reading.model_dump())
    return {"status": "success", "data": reading}
