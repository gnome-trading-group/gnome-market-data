import math
import os
import boto3
from utils import lambda_handler


@lambda_handler
def handler(listingId: int):
    table_name = os.environ['LISTING_STATISTICS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.get_item(Key={'listingId': listingId})
    item = response.get('Item')

    if not item:
        return {'listingId': listingId, 'metrics': {}, 'lastUpdated': None}

    raw_stats = item.get('statistics', {})
    metrics = {}
    for metric_name, welford in raw_stats.items():
        count = int(welford.get('count', 0))
        mean = float(welford.get('mean', 0))
        m2 = float(welford.get('m2', 0))
        stddev = math.sqrt(m2 / count) if count > 1 else 0.0
        metrics[metric_name] = {
            'mean': mean,
            'stddev': stddev,
            'count': count,
        }

    last_updated = item.get('lastUpdated')

    return {
        'listingId': listingId,
        'metrics': metrics,
        'lastUpdated': int(last_updated) if last_updated is not None else None,
    }
