import os
import uuid

import boto3
from botocore.exceptions import ClientError
from collection import actor, caller_kind, now_iso, wake_reconciler
from collection_rules import EVENT, item_fields, validate, view
from utils import lambda_handler

_table = boto3.resource('dynamodb').Table(os.environ['COLLECTION_RULES_TABLE'])
PRIORITIES = {'RESEARCH', 'PRODUCTION'}
STATUSES = {'ACTIVE', 'PAUSED'}
# The reconciler keeps its own state in this table under this id; it is never a rule.
RECONCILER_ROW = '_reconciler'


@lambda_handler
def handler(event: dict, ruleId: str = None, rule_type: str = None, parameters: dict = None,
            priority: str = 'RESEARCH', tier: str = 'standard', retire_grace_minutes: int = None,
            status: str = None, name: str = None, managed_by: str = None,
            add_event_ids: list = None, remove_event_ids: list = None):
    """GET lists rules; POST creates one; PATCH changes one; DELETE removes one. Every change wakes the reconciler.

    A rule with `managed_by` belongs to that system: people may rename, pause or delete it, but only services change
    what it selects."""
    method = event['httpMethod']
    if method == 'GET':
        items = _table.scan().get('Items', [])
        return {'rules': [view(item) for item in items if item['rule_id'] != RECONCILER_ROW]}
    if method == 'POST':
        return _create(event, rule_type, parameters, priority, tier, retire_grace_minutes, name, managed_by)
    if not ruleId or ruleId == RECONCILER_ROW:
        raise ValueError('ruleId is required')
    if method == 'DELETE':
        _table.delete_item(Key={'rule_id': ruleId})
        wake_reconciler('rule-deleted')
        return {'deleted': ruleId}
    if method == 'PATCH':
        selection = {'parameters': parameters, 'add_event_ids': add_event_ids, 'remove_event_ids': remove_event_ids}
        return _update(event, ruleId, selection, status, retire_grace_minutes, name)
    raise ValueError(f'unsupported method {method}')


def _create(event, rule_type, parameters, priority, tier, retire_grace_minutes, name, managed_by):
    if priority not in PRIORITIES:
        raise ValueError(f'priority must be one of {sorted(PRIORITIES)}')
    cleaned = validate(rule_type, parameters)
    item = {
        'rule_id': str(uuid.uuid4()),
        'name': name or rule_type,
        'rule_type': rule_type,
        **item_fields(rule_type, cleaned),
        'priority': priority,
        'tier': tier,
        'status': 'ACTIVE',
        'created_by': actor(event),
        'created_at': now_iso(),
    }
    if managed_by:
        item['managed_by'] = managed_by
    if retire_grace_minutes is not None:
        item['retire_grace_minutes'] = int(retire_grace_minutes)
    _table.put_item(Item=item)
    wake_reconciler('rule-created')
    return view(item)


def _update(event, rule_id, selection, status, retire_grace_minutes, name):
    current = _table.get_item(Key={'rule_id': rule_id}).get('Item')
    if current is None:
        raise ValueError(f'no rule {rule_id}')
    sets, adds, deletes, removes, values, names = [], [], [], [], {}, {}
    if any(v is not None for v in selection.values()):
        if current.get('managed_by') and caller_kind(event) == 'person':
            raise ValueError(f'rule {rule_id} is managed by {current["managed_by"]}; change what it selects there')
        _selection_updates(current['rule_type'], selection, sets, adds, deletes, removes, values, names)
    if status is not None:
        if status not in STATUSES:
            raise ValueError(f'status must be one of {sorted(STATUSES)}')
        sets.append('#status = :status')
        values[':status'] = status
        names['#status'] = 'status'
    if retire_grace_minutes is not None:
        sets.append('retire_grace_minutes = :grace')
        values[':grace'] = int(retire_grace_minutes)
    if name is not None:
        sets.append('#name = :name')
        values[':name'] = name
        names['#name'] = 'name'
    if not (sets or adds or deletes or removes):
        raise ValueError('nothing to change')
    sets += ['updated_by = :actor', 'updated_at = :at']
    values.update({':actor': actor(event), ':at': now_iso()})
    expression = ' '.join(f'{action} {", ".join(parts)}'
                          for action, parts in (('SET', sets), ('ADD', adds), ('DELETE', deletes), ('REMOVE', removes))
                          if parts)
    # DynamoDB refuses attribute names the expression doesn't use, so only the reserved words this update sets.
    update = dict(
        Key={'rule_id': rule_id},
        UpdateExpression=expression,
        ConditionExpression='attribute_exists(rule_id)',
        ExpressionAttributeValues=values,
        ReturnValues='ALL_NEW',
    )
    if names:
        update['ExpressionAttributeNames'] = names
    try:
        result = _table.update_item(**update)
    except ClientError as error:
        if error.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise ValueError(f'no rule {rule_id}') from error
        raise
    wake_reconciler('rule-updated')
    return view(result['Attributes'])


def _selection_updates(rule_type, selection, sets, adds, deletes, removes, values, names):
    """What to change for new parameters or, on an event rule, events added and removed. Adding and removing are
    DynamoDB ADD/DELETE on the rule's `event_ids` set, so concurrent callers never overwrite each other."""
    if selection['add_event_ids'] is not None or selection['remove_event_ids'] is not None:
        if rule_type != EVENT:
            raise ValueError('add_event_ids and remove_event_ids apply only to event rules')
        if selection['parameters'] is not None:
            raise ValueError('send parameters or add_event_ids / remove_event_ids, not both')
        if selection['add_event_ids']:
            adds.append('event_ids :add')
            values[':add'] = set(validate(EVENT, {'event_ids': selection['add_event_ids']})['event_ids'])
        if selection['remove_event_ids']:
            deletes.append('event_ids :remove')
            values[':remove'] = set(validate(EVENT, {'event_ids': selection['remove_event_ids']})['event_ids'])
        return
    fields = item_fields(rule_type, validate(rule_type, selection['parameters']))
    # `parameters` is a DynamoDB reserved word.
    sets.append('#parameters = :parameters')
    values[':parameters'] = fields['parameters']
    names['#parameters'] = 'parameters'
    if rule_type == EVENT:
        if 'event_ids' in fields:
            sets.append('event_ids = :events')
            values[':events'] = fields['event_ids']
        else:
            removes.append('event_ids')
