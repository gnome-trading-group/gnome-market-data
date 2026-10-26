import os
import json
import boto3
from utils import lambda_handler

EMPTY_SUMMARY = {'totalDays': 0, 'totalMinutes': 0, 'totalSizeBytes': 0, 'recordTypes': []}


@lambda_handler
def handler(streamId: int):
    """
    Coverage of one stream: which days and minutes have stored records, by record type.

    Returns:
        {
            'streamId': int,
            'coverage': {
                'YYYY-MM-DD': {
                    'hasCoverage': bool,
                    'totalMinutes': int,
                    'recordTypes': {'mbp-10': {'hasCoverage': bool, 'minutes': int, 'sizeBytes': int}}
                }
            },
            'summary': {
                'totalDays': int,
                'totalMinutes': int,
                'earliestDate': str,
                'latestDate': str,
                'totalSizeBytes': int,
                'recordTypes': [str]
            }
        }
    """
    table = boto3.resource('dynamodb').Table(os.environ['COVERAGE_TABLE_NAME'])
    pk = f'STREAM#{streamId}'
    try:
        summary_response = table.get_item(Key={'pk': pk, 'sk': 'SUMMARY'})
        if 'Item' not in summary_response:
            return {
                'error': 'No coverage data found for this stream',
                'streamId': streamId,
                'coverage': {},
                'summary': EMPTY_SUMMARY,
            }
        summary = json.loads(summary_response['Item'].get('data', '{}'))

        coverage = {}
        query = {
            'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :prefix)',
            'ExpressionAttributeValues': {':pk': pk, ':prefix': 'DATE#'},
        }
        while True:
            response = table.query(**query)
            for item in response.get('Items', []):
                coverage[item['sk'].replace('DATE#', '')] = json.loads(item.get('data', '{}'))
            if 'LastEvaluatedKey' not in response:
                break
            query['ExclusiveStartKey'] = response['LastEvaluatedKey']

        return {
            'streamId': streamId,
            'coverage': coverage,
            'summary': {
                'totalDays': summary.get('totalDays', 0),
                'totalMinutes': summary.get('totalMinutes', 0),
                'earliestDate': summary.get('earliestDate'),
                'latestDate': summary.get('latestDate'),
                'totalSizeBytes': summary.get('totalSizeBytes', 0),
                'recordTypes': summary.get('recordTypes', []),
            },
        }
    except Exception as e:
        return {'error': str(e), 'streamId': streamId, 'coverage': {}, 'summary': EMPTY_SUMMARY}
