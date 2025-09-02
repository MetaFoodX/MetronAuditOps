import boto3
from typing import Any, Dict, Optional

from app.utils.config import get_config

dynamodb: Optional[Any] = None


def init_dynamodb() -> None:
    global dynamodb
    aws_config = get_config().get("aws")
    if not aws_config:
        raise ValueError("AWS configuration is missing in the config file")
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=aws_config["region"],
        aws_access_key_id=aws_config["access_key_id"],
        aws_secret_access_key=aws_config["secret_access_key"],
    )


def get_dynamodb() -> Any:
    if dynamodb is None:
        raise RuntimeError("DynamoDB not initialized. Call init_dynamodb() first.")
    return dynamodb


def get_table_item(table_name: str, key_dict: Dict[str, Any]) -> Dict[str, Any] | str:
    table = get_dynamodb().Table(table_name)
    try:
        response: Dict[str, Any] = table.get_item(Key=key_dict)
        item = response.get("Item")
        return item if isinstance(item, dict) else {}
    except Exception as e:
        return f"Error reading DynamoDB: {str(e)}"


def describe_table(table_name: str) -> Dict[str, Any] | str:
    try:
        description = get_dynamodb().meta.client.describe_table(TableName=table_name)
        print(description)
        return dict(description)
    except Exception as e:
        return f"Error describing table: {str(e)}"


def test_connection(table_name: str) -> str:
    try:
        table = get_dynamodb().Table(table_name)
        return f"Table '{table_name}' is {table.table_status}"
    except Exception as e:
        return f"Connection failed: {str(e)}"
