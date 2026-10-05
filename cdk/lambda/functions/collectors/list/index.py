from db import DynamoDBClient
from utils import lambda_handler

db = DynamoDBClient()

@lambda_handler
def handler():
    items = db.get_all_items()
    return {'collectors': items}