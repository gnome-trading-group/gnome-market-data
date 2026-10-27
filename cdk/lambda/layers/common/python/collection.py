"""Shared by the collection/* routes: the reconciler's wake-up queue and the shard item's packed stream lists."""
import os
import struct
import time
import uuid

import boto3

_sqs = boto3.client('sqs')


def wake_reconciler(reason: str) -> None:
    """Asks for a tick now. One message group, so ticks never overlap; a burst of wake-ups is one tick."""
    _sqs.send_message(
        QueueUrl=os.environ['RECONCILER_QUEUE_URL'],
        MessageBody=reason,
        MessageGroupId='reconcile',
        MessageDeduplicationId=f'{reason}:{uuid.uuid4()}',
    )


def unpack_ids(value) -> list:
    """A shard item's streams/draining: big-endian int32s in one binary attribute (gnome-orchestrator DynamoAssignment)."""
    if value is None:
        return []
    raw = bytes(value.value if hasattr(value, 'value') else value)
    return list(struct.unpack(f'>{len(raw) // 4}i', raw))


def unpack_cuts(value) -> dict:
    """A shard item's cuts: (stream id, minute since the epoch) int32 pairs; 2147483647 is WARMING (write nothing yet)."""
    ids = unpack_ids(value)
    return {str(ids[i]): ids[i + 1] for i in range(0, len(ids) - 1, 2)}


def shard_view(item: dict) -> dict:
    """A shard item with its packed binary attributes as JSON-friendly values."""
    shard = {k: v for k, v in item.items() if k not in ('streams', 'draining', 'cuts')}
    shard['streams'] = unpack_ids(item.get('streams'))
    shard['draining'] = unpack_ids(item.get('draining'))
    shard['cuts'] = unpack_cuts(item.get('cuts'))
    return shard


def actor(event: dict) -> str:
    """Who made the change: the authorizer's caller (a person's email from Cognito)."""
    claims = ((event.get('requestContext') or {}).get('authorizer') or {}).get('claims') or {}
    return claims.get('email') or claims.get('cognito:username') or 'unknown'


def now_iso() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
