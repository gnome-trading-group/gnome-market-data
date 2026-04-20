import os
import boto3
import json
from utils import lambda_handler
from typing import List, Dict, Any

@lambda_handler
def handler(issues: List[Dict[str, Any]] = None, event: dict = None):
    if issues is None:
        body = json.loads(event.get('body', '{}'))
        issues = body.get('issues', [])

        if not issues and 'listingId' in body and 'issueId' in body:
            issues = [body]

    if not issues:
        raise ValueError('issues parameter is required')

    table_name = os.environ['QUALITY_ISSUES_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    updated_count = 0
    errors = []

    for issue in issues:
        try:
            listing_id = issue.get('listingId')
            issue_id = issue.get('issueId')

            if listing_id is None or issue_id is None:
                errors.append({
                    'issue': issue,
                    'error': 'listingId and issueId are required'
                })
                continue

            update_parts = []
            expression_values = {}
            expression_names = {}

            update_parts.append('#status = :status')
            expression_values[':status'] = 'REVIEWED'
            expression_names['#status'] = 'status'

            if 'note' in issue:
                update_parts.append('note = :note')
                expression_values[':note'] = issue['note']

            update_kwargs = {
                'Key': {
                    'listingId': listing_id,
                    'issueId': issue_id,
                },
                'UpdateExpression': 'SET ' + ', '.join(update_parts),
                'ExpressionAttributeValues': expression_values,
                'ExpressionAttributeNames': expression_names,
            }

            table.update_item(**update_kwargs)
            updated_count += 1

        except Exception as e:
            errors.append({
                'issue': issue,
                'error': str(e)
            })

    return {
        'updated': updated_count,
        'errors': errors if errors else None
    }
