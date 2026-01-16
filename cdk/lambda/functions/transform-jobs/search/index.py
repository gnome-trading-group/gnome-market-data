import os
import boto3
import json
from utils import lambda_handler
from boto3.dynamodb.conditions import Key

@lambda_handler
def handler(listingId: int, schemaType: str = None, limit: int = 100, lastEvaluatedKey: str = None):
    """
    Search for transformation jobs by listingId and optionally schemaType.
    Uses the listingId-schemaType-index GSI for efficient querying.

    Args:
        listingId: Filter by listing ID (required)
        schemaType: Filter by schema type (optional)
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

    last_key = None
    if lastEvaluatedKey:
        last_key = json.loads(lastEvaluatedKey)

    query_kwargs = {
        'IndexName': 'listingId-schemaType-index',
        'KeyConditionExpression': Key('listingId').eq(listingId),
        'Limit': min(limit, 100),
    }

    if schemaType:
        query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & Key('schemaType').eq(schemaType)

    if last_key:
        query_kwargs['ExclusiveStartKey'] = last_key

    response = table.query(**query_kwargs)
    jobs = response.get('Items', [])

    # Sort by timestamp descending (latest first)
    jobs.sort(key=lambda x: x.get('timestamp', 0), reverse=True)

    result = {
        'jobs': jobs,
    }

    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'], default=str)

    return result
