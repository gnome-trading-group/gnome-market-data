from db import DynamoDBClient
from utils import lambda_handler

@lambda_handler
def handler(body):
    db = DynamoDBClient()
    items = db.get_all_items()
    return {'collectors': items} 