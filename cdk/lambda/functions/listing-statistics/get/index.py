import datetime
import math
import os
import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler


@lambda_handler
def handler(listingId: int, lookbackDays: int = 14, hour: int = None):
    table_name = os.environ['LISTING_STATISTICS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    end_date = datetime.date.today().isoformat()
    start_date = (datetime.date.today() - datetime.timedelta(days=lookbackDays)).isoformat()

    if hour is not None:
        sk_start = f"{hour:02d}#{start_date}#"
        sk_end = f"{hour:02d}#{end_date}#~"
    else:
        sk_start = f"00#{start_date}#"
        sk_end = f"23#{end_date}#~"

    response = table.query(
        KeyConditionExpression=Key('listingId').eq(listingId) & Key('sk').between(sk_start, sk_end)
    )
    items = response.get('Items', [])

    aggregated = {}
    for item in items:
        sk = item.get('sk', '')
        metric_name = sk.rsplit('#', 1)[-1] if '#' in sk else ''
        if not metric_name:
            continue
        if metric_name not in aggregated:
            aggregated[metric_name] = {'count': 0, 'sum': 0.0, 'sumOfSquares': 0.0}
        aggregated[metric_name]['count'] += int(item.get('count', 0))
        aggregated[metric_name]['sum'] += float(item.get('sum', 0))
        aggregated[metric_name]['sumOfSquares'] += float(item.get('sumOfSquares', 0))

    metrics = {}
    for metric_name, agg in aggregated.items():
        count = agg['count']
        mean = agg['sum'] / count if count > 0 else 0.0
        variance = (agg['sumOfSquares'] / count - mean ** 2) if count > 1 else 0.0
        stddev = math.sqrt(max(0.0, variance))
        metrics[metric_name] = {'mean': mean, 'stddev': stddev, 'count': count}

    return {
        'listingId': listingId,
        'metrics': metrics,
        'daysIncluded': len(items),
        'lookbackDays': lookbackDays,
        'hour': hour,
    }
