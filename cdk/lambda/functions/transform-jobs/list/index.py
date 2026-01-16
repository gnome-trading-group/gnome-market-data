import os
import boto3
from boto3.dynamodb.conditions import Key
from utils import lambda_handler
import json

@lambda_handler
def handler(status: str = None, limit: int = 100, lastEvaluatedKey: str = None):
    """
    List all transformation jobs with pagination, sorted by timestamp (latest first).
    Uses the status-timestamp-index GSI to efficiently query jobs.

    Args:
        status: Optional status filter (PENDING, COMPLETE, FAILED, etc.)
                If not provided, defaults to 'PENDING'
        limit: Maximum number of items to return (default: 100)
        lastEvaluatedKey: Pagination token from previous request

    Returns:
        {
            'jobs': [...],
            'lastEvaluatedKey': '...' (if more results available)
        }
    """
    table_name = os.environ['TRANSFORM_JOBS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Default to PENDING status if not specified
    query_status = status or 'PENDING'

    query_kwargs = {
        'IndexName': 'status-timestamp-index',
        'KeyConditionExpression': Key('status').eq(query_status),
        'Limit': min(limit, 100),
        'ScanIndexForward': False,  # Sort descending (latest first)
    }

    if lastEvaluatedKey:
        query_kwargs['ExclusiveStartKey'] = json.loads(lastEvaluatedKey)

    response = table.query(**query_kwargs)

    jobs = response.get('Items', [])

    result = {
        'jobs': jobs,
    }

    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'])

    return result

