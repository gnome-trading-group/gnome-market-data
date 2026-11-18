import json
import os

import boto3
from collection import shard_view
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_SHARDS_TABLE'])
_endpoints = boto3.resource('dynamodb', region_name=os.environ.get('SERVICE_ENDPOINTS_REGION')).Table(
    os.environ.get('SERVICE_ENDPOINTS_TABLE', 'service-endpoints'))
_s3 = boto3.client('s3')
_bucket = os.environ['METADATA_BUCKET_NAME']


@lambda_handler
def handler(shardId: str):
    """A shard's item, each of its tasks' latest status (gnome-orchestrator ShardStatusWriter), and the zone its running
    task's host is in (from the shard's own service-endpoints item; null while it writes none, or from a release that
    doesn't say)."""
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
    return {'shard': shard, 'statuses': statuses, 'placement': placement(shardId)}


def placement(shard_id: str) -> dict:
    """The zone the shard's task runs in, by name and ID, as it advertises it; nulls when it says nothing."""
    try:
        item = _endpoints.get_item(Key={'service_id': f'shard#{shard_id}'}).get('Item') or {}
    except Exception:
        item = {}
    return {'availability_zone': item.get('availability_zone'), 'availability_zone_id': item.get('availability_zone_id')}
