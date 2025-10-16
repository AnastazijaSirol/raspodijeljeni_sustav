import os
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMO_TABLE = os.getenv("DYNAMO_TABLE", "TrafficData")
LOCALSTACK_URL = os.getenv("LOCALSTACK_URL", "http://localhost:4566")
