import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from utils import lambda_handler, CustomEncoder

@lambda_handler
def handler(status: str = None, limit: int = 100, lastEvaluatedKey: str = None):
    """
    List all gaps with pagination, sorted by timestamp (latest first).
    Uses the status-timestamp-index GSI to efficiently query gaps by status.

    Args:
        status: Filter by gap status (REVIEWED, UNREVIEWED). Defaults to 'UNREVIEWED'
        limit: Maximum number of items to return (default: 100)
        lastEvaluatedKey: Pagination token from previous request

    Returns:
        {
            'gaps': [...],
            'lastEvaluatedKey': '...' (if more results available)
        }
    """
    table_name = os.environ['GAPS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Default to UNREVIEWED status if not specified
    query_status = status or 'UNREVIEWED'

    last_key = None
    if lastEvaluatedKey:
        last_key = json.loads(lastEvaluatedKey)

    query_kwargs = {
        'IndexName': 'status-timestamp-index',
        'KeyConditionExpression': Key('status').eq(query_status),
        'Limit': min(limit, 100),
        'ScanIndexForward': False,  # Sort descending (latest first)
    }

    if last_key:
        query_kwargs['ExclusiveStartKey'] = last_key

    response = table.query(**query_kwargs)

    gaps = response.get('Items', [])

    result = {
        'gaps': gaps,
    }

    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'], cls=CustomEncoder)

    return result

