import json
import os

import boto3
from collection import shard_view
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_SHARDS_TABLE'])
_s3 = boto3.client('s3')
_bucket = os.environ['METADATA_BUCKET_NAME']


@lambda_handler
def handler(shardId: str):
    """A shard's item and each of its tasks' latest status (gnome-orchestrator ShardStatusWriter)."""
    item = _table.get_item(Key={'shard_id': shardId}).get('Item')
    if item is None:
        return {'statusCode': 404, 'headers': {'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'error': 'no such shard'})}
    shard = shard_view(item)
    statuses = []
    prefix = f'collection/shards/{shardId}/status/'
    for page in _s3.get_paginator('list_objects_v2').paginate(Bucket=_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            body = _s3.get_object(Bucket=_bucket, Key=obj['Key'])['Body'].read()
            statuses.append(json.loads(body))
    statuses.sort(key=lambda status: status.get('written_at_ns', 0), reverse=True)
    return {'shard': shard, 'statuses': statuses}
