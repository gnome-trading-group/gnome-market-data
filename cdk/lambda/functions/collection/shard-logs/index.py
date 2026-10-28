import boto3
from utils import lambda_handler

_clients = {}


def _logs(region: str):
    if region not in _clients:
        _clients[region] = boto3.client('logs', region_name=region)
    return _clients[region]


@lambda_handler
def handler(shardId: str, region: str, taskId: str, limit: int = 200):
    """The latest lines a shard's task logged, in its region's shard log group."""
    response = _logs(region).get_log_events(
        logGroupName=f'/collection/shards-{region}',
        logStreamName=f'shard/ShardContainer/{taskId}',
        limit=min(limit, 1000),
        startFromHead=False,
    )
    return {
        'shardId': shardId,
        'taskId': taskId,
        'events': [{'timestamp': e['timestamp'], 'message': e['message']} for e in response.get('events', [])],
    }
