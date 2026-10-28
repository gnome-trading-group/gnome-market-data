import os
import time

import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler

_dynamo = boto3.resource('dynamodb')
_coverage = _dynamo.Table(os.environ['COLLECTION_COVERAGE_TABLE'])
_gaps = _dynamo.Table(os.environ['COLLECTION_GAPS_TABLE'])
_targets = _dynamo.Table(os.environ['COLLECTION_TARGETS_TABLE'])
DAY_NS = 86_400 * 1_000_000_000


def _query(table, stream_id: int, from_ns: int, to_ns: int) -> list:
    # Intervals split at UTC midnight, so starting a day early catches the one already open at from_ns.
    items, kwargs = [], {
        'KeyConditionExpression': Key('stream_id').eq(stream_id) & Key('from_ns').between(from_ns - DAY_NS, to_ns),
    }
    while True:
        response = table.query(**kwargs)
        items += response.get('Items', [])
        if 'LastEvaluatedKey' not in response:
            break
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    return [item for item in items if int(item['to_ns']) >= from_ns or 'open_shard_id' in item]


def _interval(item: dict, now_ns: int) -> dict:
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


@lambda_handler
def handler(streamId: int, fromNs: int, toNs: int):
    """A stream's coverage intervals and gaps over [fromNs, toNs], the gap still open now (only written as a gap once
    coverage resumes), and how much of the range was covered. Times are epoch nanoseconds."""
    stream_id, from_ns, to_ns = int(streamId), int(fromNs), int(toNs)
    now_ns = time.time_ns()
    intervals = [_interval(item, now_ns) for item in _query(_coverage, stream_id, from_ns, to_ns)]
    gaps = [{'fromNs': int(g['from_ns']), 'toNs': int(g['to_ns']), 'reason': g.get('reason')}
            for g in _query(_gaps, stream_id, from_ns, to_ns)]
    target = _targets.get_item(Key={'stream_id': stream_id}).get('Item') or {}
    since = int(target.get('uncovered_since_ns') or 0)
    open_gap = {'fromNs': since, 'reason': target.get('uncovered_reason')} if since else None
    covered = sum(max(0, min(i['toNs'], to_ns) - max(i['fromNs'], from_ns)) for i in intervals)
    return {
        'streamId': stream_id,
        'fromNs': from_ns,
        'toNs': to_ns,
        'intervals': intervals,
        'gaps': gaps,
        'openGap': open_gap,
        'targetState': target.get('state'),
        'coveredFraction': covered / max(1, min(to_ns, now_ns) - from_ns),
    }
