import json
import os
import time

import boto3
from botocore.exceptions import ClientError
from collection import actor, wake_reconciler
from leases import endpoint_view, lease_item, lease_view, readable_streams, release_update, shards_of, validate
from packed import unpack_ids
from utils import create_response, lambda_handler

_dynamo = boto3.resource('dynamodb')
_leases = _dynamo.Table(os.environ['COLLECTION_LEASES_TABLE'])
_targets_table = os.environ['COLLECTION_TARGETS_TABLE']
# Shards write their endpoints in the primary region whatever region they run in (gnome-shared-cdk SERVICE_ENDPOINTS).
_endpoints_dynamo = boto3.resource('dynamodb', region_name=os.environ['SERVICE_ENDPOINTS_REGION'])
_endpoints_table = os.environ['SERVICE_ENDPOINTS_TABLE']
_s3 = boto3.client('s3')
_bucket = os.environ['METADATA_BUCKET_NAME']
BATCH_GET_LIMIT = 100


@lambda_handler
def handler(event: dict, sessionId: str = None, streams: list = None, mode: str = None, region: str = None):
    """POST leases a session's streams (idempotent per session: replaces the list); DELETE releases them; GET says,
    per stream, which shard carries it, whether it is LIVE there, and where to subscribe."""
    method = event['httpMethod']
    if method == 'POST':
        return _lease(event, sessionId, streams, mode, region)
    if not sessionId:
        raise ValueError('sessionId is required')
    if method == 'DELETE':
        return _release(sessionId)
    if method == 'GET':
        return _view(sessionId)
    raise ValueError(f'unsupported method {method}')


def _not_found(session_id: str):
    return create_response(404, {'error': f'no lease for session {session_id}'})


def _lease(event, session_id, streams, mode, region):
    session_id, stream_ids, mode, region = validate(session_id, streams, mode, region)
    existing = _leases.get_item(Key={'lease_id': session_id}, ConsistentRead=True).get('Item')
    _leases.put_item(Item=lease_item(session_id, stream_ids, mode, region, existing, actor(event), int(time.time())))
    wake_reconciler('lease-created')
    return {'sessionId': session_id}


def _release(session_id):
    try:
        _leases.update_item(Key={'lease_id': session_id}, **release_update(int(time.time()), 'DELETED'))
    except ClientError as error:
        if error.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
        # Already released (idempotent), or never leased.
        if _leases.get_item(Key={'lease_id': session_id}).get('Item') is None:
            return _not_found(session_id)
        return {'sessionId': session_id, 'status': 'RELEASED'}
    wake_reconciler('lease-released')
    return {'sessionId': session_id, 'status': 'RELEASED'}


def _view(session_id):
    item = _leases.get_item(Key={'lease_id': session_id}, ConsistentRead=True).get('Item')
    if item is None:
        return _not_found(session_id)
    stream_ids = unpack_ids(item.get('streams'))
    targets = {int(t['stream_id']): t for t in _batch_get(_dynamo, _targets_table, 'stream_id', stream_ids)}
    shard_ids = sorted(shards_of(targets))
    now_ns = time.time_ns()
    readable = {shard_id: readable_streams(_latest_status(shard_id), now_ns) for shard_id in shard_ids}
    now_s = now_ns // 1_000_000_000
    endpoints = {}
    for found in _batch_get(_endpoints_dynamo, _endpoints_table, 'service_id', [f'shard#{s}' for s in shard_ids]):
        endpoints[found['service_id'].split('#', 1)[1]] = endpoint_view(found, now_s)
    return lease_view(item, targets, readable, endpoints)


def _batch_get(dynamo, table: str, key: str, values: list) -> list:
    items = []
    for start in range(0, len(values), BATCH_GET_LIMIT):
        request = {table: {'Keys': [{key: value} for value in values[start:start + BATCH_GET_LIMIT]]}}
        while request:
            response = dynamo.batch_get_item(RequestItems=request)
            items += response.get('Responses', {}).get(table, [])
            request = response.get('UnprocessedKeys') or None
    return items


def _latest_status(shard_id: str):
    """The newest of the shard's tasks' statuses (gnome-orchestrator ShardStatusWriter), or None."""
    newest = None
    prefix = f'collection/shards/{shard_id}/status/'
    for page in _s3.get_paginator('list_objects_v2').paginate(Bucket=_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if newest is None or obj['LastModified'] > newest['LastModified']:
                newest = obj
    if newest is None:
        return None
    return json.loads(_s3.get_object(Bucket=_bucket, Key=newest['Key'])['Body'].read())
