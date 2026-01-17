import os
import boto3
from utils import lambda_handler

@lambda_handler
def handler(securityId: int, exchangeId: int):
    """
    Get coverage for a specific security+exchange combination, showing data availability by date.
    Reads from DynamoDB coverage table.

    Args:
        securityId: The security ID to query
        exchangeId: The exchange ID to query

    Returns:
        {
            'securityId': int,
            'exchangeId': int,
            'coverage': {
                'YYYY-MM-DD': {
                    'hasCoverage': bool,
                    'totalMinutes': int,
                    'schemaTypes': {
                        'schemaType': {
                            'hasCoverage': bool,
                            'minutes': int,
                            'sizeBytes': int
                        }
                    }
                }
            },
            'summary': {
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
        # Get summary for this security+exchange
        pk = f'SEC#{securityId}#EX#{exchangeId}'
        summary_response = table.get_item(
            Key={
                'pk': pk,
                'sk': 'SUMMARY'
            }
        )

        if 'Item' not in summary_response:
            return {
                'error': 'No coverage data found for this security+exchange combination',
                'securityId': securityId,
                'exchangeId': exchangeId,
                'coverage': {},
                'summary': {
                    'totalDays': 0,
                    'totalMinutes': 0,
                    'totalSizeBytes': 0
                }
            }

        summary_data = summary_response['Item'].get('data', {})

        # Query all date records for this security+exchange
        date_response = table.query(
            KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': pk,
                ':sk_prefix': 'DATE#'
            }
        )

        # Build coverage by date
        coverage_result = {}
        for item in date_response.get('Items', []):
            # Extract date from sk (format: DATE#YYYY-MM-DD)
            date_str = item['sk'].replace('DATE#', '')
            coverage_result[date_str] = item.get('data', {})

        return {
            'securityId': securityId,
            'exchangeId': exchangeId,
            'coverage': coverage_result,
            'summary': {
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
            'exchangeId': exchangeId,
            'coverage': {},
            'summary': {
                'totalDays': 0,
                'totalMinutes': 0,
                'totalSizeBytes': 0
            }
        }

