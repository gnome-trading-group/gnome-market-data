import os
import boto3
import json
from decimal import Decimal
from utils import lambda_handler, CustomEncoder

@lambda_handler
def handler(listingId: int, limit: int = 1000, lastEvaluatedKey: str = None):
    table_name = os.environ['QUALITY_ISSUES_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    query_kwargs = {
        'KeyConditionExpression': 'listingId = :listingId',
        'ExpressionAttributeValues': {':listingId': listingId},
        'Limit': min(limit, 1000),
        'ScanIndexForward': False,
    }

    if lastEvaluatedKey:
        query_kwargs['ExclusiveStartKey'] = json.loads(lastEvaluatedKey, parse_float=Decimal)

    response = table.query(**query_kwargs)

    result = {
        'issues': response.get('Items', []),
    }

    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'], cls=CustomEncoder)

    return result
