import os

import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler

_dynamo = boto3.resource('dynamodb')
_coverage = _dynamo.Table(os.environ['COLLECTION_COVERAGE_TABLE'])
_gaps = _dynamo.Table(os.environ['COLLECTION_GAPS_TABLE'])
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
    return [item for item in items if int(item['to_ns']) >= from_ns]


@lambda_handler
def handler(streamId: int, fromNs: int, toNs: int):
    """A stream's coverage intervals and gaps over [fromNs, toNs], and how much of it was covered."""
    intervals = _query(_coverage, streamId, fromNs, toNs)
    gaps = _query(_gaps, streamId, fromNs, toNs)
    covered = sum(max(0, min(int(i['to_ns']), toNs) - max(int(i['from_ns']), fromNs)) for i in intervals)
    return {
        'streamId': streamId,
        'intervals': intervals,
        'gaps': gaps,
        'coveredFraction': covered / max(1, toNs - fromNs),
    }
