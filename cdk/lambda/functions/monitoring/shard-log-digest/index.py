"""Once a day: the shards' unexpected errors of the last 24 h, by kind, posted as a SHARD_LOG_DIGEST alert.

Posts nothing on a day without any. Reads every collection region's shard log group with one Logs Insights query each.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

from digest import message, summarize

DAY_MS = 24 * 60 * 60 * 1000
# Enough for a bad day; past this the counts are a floor, and the digest still names the kinds.
MAX_LINES = 10000
QUERY = ('fields @timestamp, @message | filter @message like /UNKNOWN_ERROR|FATAL_ERROR_EXITING/ '
         f'| sort @timestamp desc | limit {MAX_LINES}')
REGIONS = os.environ['COLLECTION_REGIONS'].split(',')
BUS = os.environ.get('ALERT_BUS_NAME', 'gnome-alerts')
events = boto3.client('events')


def _lines(region, start_ms, end_ms):
    logs = boto3.client('logs', region_name=region)
    query = logs.start_query(
        logGroupName=f'/collection/shards-{region}',
        startTime=start_ms // 1000,
        endTime=end_ms // 1000,
        queryString=QUERY,
    )['queryId']
    while True:
        result = logs.get_query_results(queryId=query)
        if result['status'] in ('Complete', 'Failed', 'Cancelled', 'Timeout'):
            break
        time.sleep(1)
    if result['status'] != 'Complete':
        raise RuntimeError(f'{region}: the query ended {result["status"]}')
    for row in result['results']:
        fields = {field['field']: field['value'] for field in row}
        at = datetime.strptime(fields['@timestamp'], '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)
        yield region, int(at.timestamp() * 1000), fields['@message']


def handler(event, context):
    now_ms = int(time.time() * 1000)
    lines = []
    for region in REGIONS:
        lines.extend(_lines(region, now_ms - 2 * DAY_MS, now_ms))
    today, yesterday = summarize(lines, now_ms - DAY_MS)
    digest = message(today, yesterday)
    if digest is None:
        print('No unexpected shard errors in the last 24 h; nothing posted')
        return {'posted': False}
    title, body = digest
    date = datetime.fromtimestamp(now_ms / 1000, timezone.utc).strftime('%Y-%m-%d')
    detail = {
        'action': 'FIRING',
        'key': f'SHARD_LOG_DIGEST:{date}',
        'type': 'SHARD_LOG_DIGEST',
        'source': 'ingest',
        'title': title,
        'body': body,
        'labels': {'regions': ','.join(REGIONS)},
        'stateAt': datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
    }
    events.put_events(Entries=[{
        'EventBusName': BUS,
        'Source': 'gnome.alerts',
        'DetailType': 'Alert',
        'Detail': json.dumps(detail),
    }])
    return {'posted': True, 'kinds': len(today)}
