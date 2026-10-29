"""Shared by the collection/* routes: the reconciler's wake-up queue and the shard item as JSON."""
import logging
import os
import time
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from packed import unpack_cuts, unpack_ids

_sqs = boto3.client('sqs')
_LOGGER = logging.getLogger(__name__)


def wake_reconciler(reason: str) -> bool:
    """Asks for a tick now. One message group, so ticks never overlap; a burst of wake-ups is one tick. Best effort:
    false (logged) if the queue refused it. The change is already stored and the scheduled tick picks it up within a
    minute, so a caller that changed something must not fail for this."""
    try:
        _sqs.send_message(
            QueueUrl=os.environ['RECONCILER_QUEUE_URL'],
            MessageBody=reason,
            MessageGroupId='reconcile',
            MessageDeduplicationId=f'{reason}:{uuid.uuid4()}',
        )
        return True
    except (BotoCoreError, ClientError):
        _LOGGER.exception('Could not wake the reconciler (%s); the scheduled tick will run it', reason)
        return False


def shard_view(item: dict) -> dict:
    """A shard item with its packed binary attributes as JSON-friendly values."""
    shard = {k: v for k, v in item.items() if k not in ('streams', 'draining', 'publish', 'cuts')}
    shard['streams'] = unpack_ids(item.get('streams'))
    shard['draining'] = unpack_ids(item.get('draining'))
    shard['publish'] = unpack_ids(item.get('publish'))
    shard['cuts'] = unpack_cuts(item.get('cuts'))
    return shard


def _authorizer(event: dict) -> dict:
    return (event.get('requestContext') or {}).get('authorizer') or {}


def actor(event: dict) -> str:
    """Who made the change, as the API's authorizer (gnome-shared-cdk GnomeApiAuthorizer) names the caller: a person's
    email, `service` for an API key, `bot:<clientId>`."""
    return _authorizer(event).get('actor') or 'unknown'


def caller_kind(event: dict) -> str:
    """`person`, `service` or `bot`, as the authorizer identified the caller."""
    return _authorizer(event).get('kind') or 'unknown'


def now_iso() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
