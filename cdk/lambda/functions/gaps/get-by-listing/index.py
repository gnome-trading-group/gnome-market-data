import os
import boto3
import json
from utils import lambda_handler

@lambda_handler
def handler(listingId: int, limit: int = 1000, lastEvaluatedKey: str = None):
    """
    Get all gaps for a specific listing, regardless of status.
    
    Args:
        listingId: The listing ID to query
        limit: Maximum number of items to return (default: 1000)
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
    
    query_kwargs = {
        'KeyConditionExpression': 'listingId = :listingId',
        'ExpressionAttributeValues': {':listingId': listingId},
        'Limit': min(limit, 1000),
        'ScanIndexForward': False,  # Sort descending (latest first)
    }
    
    if lastEvaluatedKey:
        query_kwargs['ExclusiveStartKey'] = json.loads(lastEvaluatedKey)
    
    response = table.query(**query_kwargs)
    
    result = {
        'gaps': response.get('Items', []),
    }
    
    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'])
    
    return result

