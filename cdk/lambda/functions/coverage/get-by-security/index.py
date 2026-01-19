import os
import json
import boto3
from utils import lambda_handler

@lambda_handler
def handler(securityId: int):
    """
    Get coverage for a specific security across all exchanges, showing data availability by date.
    Reads from DynamoDB coverage table.
    
    Args:
        securityId: The security ID to query
    
    Returns:
        {
            'securityId': int,
            'exchanges': {
                'exchangeId': {
                    'exchangeId': int,
                    'totalDays': int,
                    'totalMinutes': int,
                    'totalSizeBytes': int,
                    'earliestDate': str,
                    'latestDate': str,
                    'schemaTypes': [str]
                }
            },
            'coverage': {
                'YYYY-MM-DD': {
                    'hasCoverage': bool,
                    'totalMinutes': int,
                    'exchanges': {
                        'exchangeId': {
                            'minutes': int,
                            'schemaTypes': {
                                'schemaType': {
                                    'minutes': int,
                                    'sizeBytes': int
                                }
                            }
                        }
                    }
                }
            },
            'summary': {
                'totalExchanges': int,
                'totalDays': int,
                'totalMinutes': int,
                'earliestDate': str,
                'latestDate': str,
                'totalSizeBytes': int,
                'schemaTypes': [str]
            }
        }
    """
    table_name = os.environ['COVERAGE_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        pk = f'SEC#{securityId}'
        summary_response = table.get_item(
            Key={
                'pk': pk,
                'sk': 'SUMMARY'
            }
        )

        if 'Item' not in summary_response:
            return {
                'error': 'No coverage data found for this security',
                'securityId': securityId,
                'exchanges': {},
                'coverage': {},
                'summary': {
                    'totalExchanges': 0,
                    'totalDays': 0,
                    'totalMinutes': 0,
                    'totalSizeBytes': 0
                }
            }

        summary_data = json.loads(summary_response['Item'].get('data', '{}'))

        date_response = table.query(
            KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': pk,
                ':sk_prefix': 'DATE#'
            }
        )

        coverage_result = {}
        for item in date_response.get('Items', []):
            date_str = item['sk'].replace('DATE#', '')
            coverage_result[date_str] = json.loads(item.get('data', '{}'))

        return {
            'securityId': securityId,
            'exchanges': summary_data.get('exchanges', {}),
            'coverage': coverage_result,
            'summary': {
                'totalExchanges': summary_data.get('totalExchanges', 0),
                'totalDays': summary_data.get('totalDays', 0),
                'totalMinutes': summary_data.get('totalMinutes', 0),
                'earliestDate': summary_data.get('earliestDate'),
                'latestDate': summary_data.get('latestDate'),
                'totalSizeBytes': summary_data.get('totalSizeBytes', 0),
                'schemaTypes': summary_data.get('schemaTypes', [])
            }
        }

    except Exception as e:
        return {
            'error': str(e),
            'securityId': securityId,
            'exchanges': {},
            'coverage': {},
            'summary': {
                'totalExchanges': 0,
                'totalDays': 0,
                'totalMinutes': 0,
                'totalSizeBytes': 0
            }
        }

