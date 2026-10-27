import os

import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_TARGETS_TABLE'])


@lambda_handler
def handler(shardId: str = None, state: str = None):
    """Every target, or a shard's (by its index); optionally only one state (DESIRED, RETIRING)."""
    if shardId:
        items, kwargs = [], {'IndexName': 'shard_id-index', 'KeyConditionExpression': Key('shard_id').eq(shardId)}
        while True:
            response = _table.query(**kwargs)
            items += response.get('Items', [])
            if 'LastEvaluatedKey' not in response:
                break
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    else:
        items, kwargs = [], {}
        while True:
            response = _table.scan(**kwargs)
            items += response.get('Items', [])
            if 'LastEvaluatedKey' not in response:
                break
            kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    if state:
        items = [item for item in items if item.get('state') == state]
    return {'targets': items}
