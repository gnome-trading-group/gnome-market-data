import os
import boto3
from utils import lambda_handler

@lambda_handler
def handler():
    """
    Get coverage summary of all market data from DynamoDB.
    
    Returns:
        {
            'totalFiles': int,
            'totalSizeBytes': int,
            'totalMinutes': int,
            'securityExchangeCount': int,
            'securities': {...},
            'schemaTypes': {...},
            'lastInventoryDate': str
        }
    """
    table_name = os.environ['COVERAGE_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    try:
        # Get global summary from DynamoDB
        response = table.get_item(
            Key={
                'pk': 'GLOBAL',
                'sk': 'SUMMARY'
            }
        )
        
        if 'Item' not in response:
            return {
                'error': 'No coverage data available',
                'totalFiles': 0,
                'totalSizeBytes': 0,
                'totalMinutes': 0,
                'securityExchangeCount': 0,
                'securities': {},
                'schemaTypes': {}
            }
        
        # Return the data field which contains all the coverage information
        data = response['Item'].get('data', {})
        
        # Add last updated timestamp
        if 'lastUpdated' in response['Item']:
            data['lastInventoryDate'] = response['Item']['lastUpdated']
        
        return data
        
    except Exception as e:
        return {
            'error': str(e),
            'totalFiles': 0,
            'totalSizeBytes': 0,
            'totalMinutes': 0,
            'securityExchangeCount': 0,
            'securities': {},
            'schemaTypes': {}
        }
