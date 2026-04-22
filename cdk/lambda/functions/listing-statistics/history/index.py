import datetime
import math
import os
import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler


@lambda_handler
def handler(listingId: int, lookbackDays: int = 30, hour: int = None):
    table_name = os.environ['LISTING_STATISTICS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=lookbackDays)

    if hour is not None:
        sk_start = f"{hour:02d}#{start_date.isoformat()}#"
        sk_end = f"{hour:02d}#{end_date.isoformat()}#~"
    else:
        sk_start = f"00#{start_date.isoformat()}#"
        sk_end = f"23#{end_date.isoformat()}#~"

    response = table.query(
        KeyConditionExpression=Key('listingId').eq(listingId) & Key('sk').between(sk_start, sk_end)
    )
    items = response.get('Items', [])

    by_date: dict = {}
    for item in items:
        sk = item.get('sk', '')
        parts = sk.split('#', 2)
        if len(parts) != 3:
            continue
        _, item_date, metric_name = parts
        if item_date not in by_date:
            by_date[item_date] = {}
        if metric_name not in by_date[item_date]:
            by_date[item_date][metric_name] = {'count': 0, 'sum': 0.0, 'sumOfSquares': 0.0}
        by_date[item_date][metric_name]['count'] += int(item.get('count', 0))
        by_date[item_date][metric_name]['sum'] += float(item.get('sum', 0))
        by_date[item_date][metric_name]['sumOfSquares'] += float(item.get('sumOfSquares', 0))

    history = []
    for date_str in sorted(by_date.keys()):
        metrics = {}
        for metric_name, agg in by_date[date_str].items():
            count = agg['count']
            mean = agg['sum'] / count if count > 0 else 0.0
            variance = (agg['sumOfSquares'] / count - mean ** 2) if count > 1 else 0.0
            stddev = math.sqrt(max(0.0, variance))
            metrics[metric_name] = {'mean': mean, 'stddev': stddev, 'count': count}
        history.append({'date': date_str, 'metrics': metrics})

    return {
        'listingId': listingId,
        'history': history,
        'lookbackDays': lookbackDays,
    }
