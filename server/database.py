import boto3

dynamodb_client = boto3.resource(
    "dynamodb",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

print("DynamoDB client ready")
