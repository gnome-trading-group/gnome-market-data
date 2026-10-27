import os

import boto3
from collection import shard_view
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_SHARDS_TABLE'])


@lambda_handler
def handler(includeDeleted: bool = False):
    """Every shard (deleted ones only if asked), newest first."""
    items, kwargs = [], {}
    while True:
        response = _table.scan(**kwargs)
        items += response.get('Items', [])
        if 'LastEvaluatedKey' not in response:
            break
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    shards = [shard_view(item) for item in items if includeDeleted or item.get('state') != 'DELETED']
    shards.sort(key=lambda shard: shard.get('created_at_ns', 0), reverse=True)
    return {'shards': shards}
