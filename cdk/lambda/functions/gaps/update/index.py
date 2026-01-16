import os
import boto3
import json
from utils import lambda_handler
from typing import List, Dict, Any

@lambda_handler
def handler(gaps: List[Dict[str, Any]] = None, event: dict = None):
    """
    Update the status of one or more gaps to REVIEWED. Updates the reason, expected, and note fields.
    
    Args:
        gaps: List of gap updates, each containing:
            - listingId: int (required)
            - timestamp: int (required)
            - reason: str (optional) - one of: EXCHANGE_CLOSED, INTERNAL_ERROR, OTHER, UNKNOWN
            - expected: bool (optional)
            - note: str (optional)
    
    Returns:
        {
            'updated': number of gaps updated,
            'errors': list of errors if any
        }
    """
    if gaps is None:
        body = json.loads(event.get('body', '{}'))
        gaps = body.get('gaps', [])
        
        if not gaps and 'listingId' in body and 'timestamp' in body:
            gaps = [body]
    
    if not gaps:
        raise ValueError('gaps parameter is required')
    
    table_name = os.environ['GAPS_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    updated_count = 0
    errors = []
    
    for gap in gaps:
        try:
            listing_id = gap.get('listingId')
            timestamp = gap.get('timestamp')
            
            if listing_id is None or timestamp is None:
                errors.append({
                    'gap': gap,
                    'error': 'listingId and timestamp are required'
                })
                continue
            
            update_parts = []
            expression_values = {}
            expression_names = {}

            update_parts.append('#status = :status')
            expression_values[':status'] = 'REVIEWED'
            expression_names['#status'] = 'status'

            if 'reason' in gap:
                update_parts.append('reason = :reason')
                expression_values[':reason'] = gap['reason']

            if 'expected' in gap:
                update_parts.append('expected = :expected')
                expression_values[':expected'] = gap['expected']

            if 'note' in gap:
                update_parts.append('note = :note')
                expression_values[':note'] = gap['note']

            update_kwargs = {
                'Key': {
                    'listingId': listing_id,
                    'timestamp': timestamp
                },
                'UpdateExpression': 'SET ' + ', '.join(update_parts),
                'ExpressionAttributeValues': expression_values
            }

            if expression_names:
                update_kwargs['ExpressionAttributeNames'] = expression_names

            table.update_item(**update_kwargs)
            
            updated_count += 1
            
        except Exception as e:
            errors.append({
                'gap': gap,
                'error': str(e)
            })
    
    return {
        'updated': updated_count,
        'errors': errors if errors else None
    }

