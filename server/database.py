import boto3

dynamodb_client = boto3.resource(
    "dynamodb",
    endpoint_url="http://localstack:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

table_name = "Readings"

existing_tables = dynamodb_client.meta.client.list_tables()["TableNames"]

if table_name not in existing_tables:
    print(f"Creating DynamoDB table '{table_name}'...")
    table = dynamodb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "camera_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"}
        ],
        AttributeDefinitions=[
            {"AttributeName": "camera_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"}
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    print(f"Table '{table_name}' created successfully!")
else:
    print(f"Table '{table_name}' already exists")
