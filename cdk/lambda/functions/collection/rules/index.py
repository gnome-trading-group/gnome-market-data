import os
import uuid

import boto3
from collection import actor, now_iso, wake_reconciler
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_RULES_TABLE'])
RULE_TYPES = {'static_streams'}
PRIORITIES = {'RESEARCH', 'PRODUCTION'}
STATUSES = {'ACTIVE', 'PAUSED'}
# The reconciler keeps its own state in this table under this id; it is never a rule.
RECONCILER_ROW = '_reconciler'


def _validate_stream_ids(stream_ids) -> list:
    if not isinstance(stream_ids, list) or not all(isinstance(s, int) and s > 0 for s in stream_ids):
        raise ValueError('stream_ids must be a list of positive stream ids')
    return sorted(set(stream_ids))


@lambda_handler
def handler(event: dict, ruleId: str = None, rule_type: str = 'static_streams', stream_ids: list = None,
            priority: str = 'RESEARCH', tier: str = 'standard', retire_grace_minutes: int = None,
            status: str = None, name: str = None):
    """GET lists rules; POST creates one; PATCH changes one; DELETE removes one. Every change wakes the reconciler."""
    method = event['httpMethod']
    if method == 'GET':
        items = _table.scan().get('Items', [])
        return {'rules': [item for item in items if item['rule_id'] != RECONCILER_ROW]}
    if method == 'POST':
        if rule_type not in RULE_TYPES:
            raise ValueError(f'rule_type must be one of {sorted(RULE_TYPES)}')
        if priority not in PRIORITIES:
            raise ValueError(f'priority must be one of {sorted(PRIORITIES)}')
        item = {
            'rule_id': str(uuid.uuid4()),
            'name': name or rule_type,
            'rule_type': rule_type,
            'parameters': {'stream_ids': _validate_stream_ids(stream_ids or [])},
            'priority': priority,
            'tier': tier,
            'status': 'ACTIVE',
            'created_by': actor(event),
            'created_at': now_iso(),
        }
        if retire_grace_minutes is not None:
            item['retire_grace_minutes'] = int(retire_grace_minutes)
        _table.put_item(Item=item)
        wake_reconciler('rule-created')
        return item
    if not ruleId or ruleId == RECONCILER_ROW:
        raise ValueError('ruleId is required')
    if method == 'DELETE':
        _table.delete_item(Key={'rule_id': ruleId})
        wake_reconciler('rule-deleted')
        return {'deleted': ruleId}
    if method == 'PATCH':
        updates, values = [], {}
        if stream_ids is not None:
            updates.append('parameters.stream_ids = :streams')
            values[':streams'] = _validate_stream_ids(stream_ids)
        if status is not None:
            if status not in STATUSES:
                raise ValueError(f'status must be one of {sorted(STATUSES)}')
            updates.append('#status = :status')
            values[':status'] = status
        if retire_grace_minutes is not None:
            updates.append('retire_grace_minutes = :grace')
            values[':grace'] = int(retire_grace_minutes)
        if name is not None:
            updates.append('#name = :name')
            values[':name'] = name
        if not updates:
            raise ValueError('nothing to change')
        updates.append('updated_by = :actor')
        values[':actor'] = actor(event)
        result = _table.update_item(
            Key={'rule_id': ruleId},
            UpdateExpression='SET ' + ', '.join(updates),
            ConditionExpression='attribute_exists(rule_id)',
            ExpressionAttributeNames={'#status': 'status', '#name': 'name'},
            ExpressionAttributeValues=values,
            ReturnValues='ALL_NEW',
        )
        wake_reconciler('rule-updated')
        return result['Attributes']
    raise ValueError(f'unsupported method {method}')
