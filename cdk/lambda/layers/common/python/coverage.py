"""Reading a stream's collection coverage: its intervals, its gaps and the gap still open on its target."""
import os

import boto3
from boto3.dynamodb.conditions import Key

DAY_NS = 86_400 * 1_000_000_000

_dynamo = boto3.resource('dynamodb')


def table(env_name: str):
    return _dynamo.Table(os.environ[env_name])


def query_range(source, stream_id: int, from_ns: int, to_ns: int) -> list:
    """A stream's rows overlapping [from_ns, to_ns], from the coverage or gaps table."""
    # Intervals split at UTC midnight, so starting a day early catches the one already open at from_ns.
    items, kwargs = [], {
        'KeyConditionExpression': Key('stream_id').eq(stream_id) & Key('from_ns').between(from_ns - DAY_NS, to_ns),
    }
    while True:
        response = source.query(**kwargs)
        items += response.get('Items', [])
        if 'LastEvaluatedKey' not in response:
            break
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    return [item for item in items if int(item['to_ns']) >= from_ns or 'open_shard_id' in item]


def interval(item: dict, now_ns: int) -> dict:
    # An open interval's stored end is only extended every 10 minutes; it is covered up to now.
    is_open = 'open_shard_id' in item
    return {
        'fromNs': int(item['from_ns']),
        'toNs': now_ns if is_open else int(item['to_ns']),
        'open': is_open,
        'shardId': item.get('shard_id'),
        'taskId': item.get('task_id'),
        'closeReason': item.get('close_reason'),
    }


def gap(item: dict) -> dict:
    return {'fromNs': int(item['from_ns']), 'toNs': int(item['to_ns']), 'reason': item.get('reason')}


def open_gap(target: dict) -> dict | None:
    """The gap a target is in now: only written as a gap row once coverage resumes."""
    since = int(target.get('uncovered_since_ns') or 0)
    return {'fromNs': since, 'reason': target.get('uncovered_reason')} if since else None
