from db import DynamoDBClient
from utils import lambda_handler
from constants import Status

@lambda_handler
def handler(listingId: int):
    db = DynamoDBClient()

    collector = db.get_item(listingId)
    if not collector:
        raise Exception(f'Collector with listing ID {listingId} not found')

    if collector.get('status') != Status.INACTIVE.value:
        raise Exception(
            f'Cannot purge collector {listingId} with status {collector.get("status")}. '
            f'Collector must be INACTIVE before purging.'
        )

    db.delete_item(listingId)

    return {
        'message': f'Collector {listingId} permanently deleted',
        'listingId': listingId
    }
