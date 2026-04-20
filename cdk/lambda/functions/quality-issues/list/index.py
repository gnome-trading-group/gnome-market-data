import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from utils import lambda_handler, CustomEncoder

@lambda_handler
def handler(status: str = None, ruleType: str = None, limit: int = 100, lastEvaluatedKey: str = None):
    table_name = os.environ['QUALITY_ISSUES_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    query_status = status or 'UNREVIEWED'

    last_key = None
    if lastEvaluatedKey:
        last_key = json.loads(lastEvaluatedKey)

    if ruleType:
        query_kwargs = {
            'IndexName': 'ruleType-timestamp-index',
            'KeyConditionExpression': Key('ruleType').eq(ruleType),
            'FilterExpression': Key('status').eq(query_status),
            'Limit': min(limit, 100),
            'ScanIndexForward': False,
        }
    else:
        query_kwargs = {
            'IndexName': 'status-timestamp-index',
            'KeyConditionExpression': Key('status').eq(query_status),
            'Limit': min(limit, 100),
            'ScanIndexForward': False,
        }

    if last_key:
        query_kwargs['ExclusiveStartKey'] = last_key

    response = table.query(**query_kwargs)

    result = {
        'issues': response.get('Items', []),
    }

    if 'LastEvaluatedKey' in response:
        result['lastEvaluatedKey'] = json.dumps(response['LastEvaluatedKey'], cls=CustomEncoder)

    return result
