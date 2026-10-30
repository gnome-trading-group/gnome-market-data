"""Session leases on streams (`collection-leases`): the lease API's validation, the
item it stores, and the per-stream view a session polls. The reconciler (gnome-market-data-lambdas LeaseStep,
DynamoStore) reads the same items; change both together."""
from packed import pack_ids, unpack_ids

ACTIVE = 'ACTIVE'
RELEASED = 'RELEASED'
MODES = {'live': 'PRODUCTION', 'paper': 'RESEARCH'}
# Live sessions still open their own venue feeds and nothing on them reads a lease, so a live lease would only start
# on-demand production shards for nobody.
LEASABLE_MODES = {'paper'}
TIER = 'standard'
# Launch deadline plus margin; the reconciler extends it while the session is RUNNING.
LIFETIME_S = 2 * 3600
# A released lease stays readable (GET) this long before DynamoDB's TTL removes it.
RELEASED_TTL_S = 7 * 86400
# LIFETIME_S, RELEASED_TTL_S and STATUS_FRESH_NS have Java twins in the reconciler; lease-constants.json (the lambdas'
# test resources) holds the agreed values and tests on both sides read it.
MAX_STREAMS = 500
INT32_MAX = 2**31 - 1

UNPLACED = 'UNPLACED'
WARMING = 'WARMING'
LIVE = 'LIVE'
LIVE_STATES = {'LIVE', 'SUSPECT'}
# As the reconciler (CoverageStep.STATUS_FRESH_NANOS): an older status means the task stopped reporting.
STATUS_FRESH_NS = 45 * 1_000_000_000


def validate(session_id, streams, mode, region=None) -> tuple:
    """(sessionId, sorted unique stream ids, mode, region). `region` is the session's launch region, kept as said."""
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError('sessionId is required')
    if mode == 'live':
        raise ValueError('live sessions read their own venue feeds and lease no streams')
    if mode not in LEASABLE_MODES:
        raise ValueError(f'mode must be one of {sorted(LEASABLE_MODES)}')
    if not isinstance(streams, list):
        raise ValueError('streams must be a list of stream ids')
    ids = set()
    for stream in streams:
        if isinstance(stream, bool) or not isinstance(stream, int) or not 0 < stream <= INT32_MAX:
            raise ValueError(f'stream ids must be positive int32s, got {stream!r}')
        ids.add(stream)
    if len(ids) > MAX_STREAMS:
        raise ValueError(f'at most {MAX_STREAMS} streams per lease')
    if region is not None and not isinstance(region, str):
        raise ValueError('region must be a string')
    return session_id.strip(), sorted(ids), mode, region


def lease_item(session_id: str, streams: list, mode: str, region, existing, actor: str, now_s: int) -> dict:
    """The item a POST writes. Idempotent per session: it replaces the stream list and pushes `expires_at` out; an
    ACTIVE lease keeps its `created_at`, a released one becomes active again as if new."""
    still_active = existing is not None and existing.get('status') == ACTIVE
    item = {
        'lease_id': session_id,
        'streams': pack_ids(streams),
        'priority': MODES[mode],
        'tier': TIER,
        'mode': mode,
        'status': ACTIVE,
        'created_at': int(existing['created_at']) if still_active else now_s,
        'expires_at': now_s + LIFETIME_S,
        'updated_at': now_s,
        'updated_by': actor,
    }
    if region:
        item['region'] = region
    return item


def release_update(now_s: int, reason: str) -> dict:
    """update_item arguments that release an ACTIVE lease, as the reconciler's DynamoStore.releaseLease does."""
    return dict(
        UpdateExpression='SET #status = :released, released_at = :now, release_reason = :reason, #ttl = :ttl',
        ConditionExpression='#status = :active',
        ExpressionAttributeNames={'#status': 'status', '#ttl': 'ttl'},
        ExpressionAttributeValues={
            ':active': ACTIVE,
            ':released': RELEASED,
            ':now': now_s,
            ':reason': reason,
            ':ttl': now_s + RELEASED_TTL_S,
        },
    )


def readable_streams(status, now_ns: int) -> set:
    """A shard's latest task status (gnome-orchestrator ShardStatusWriter JSON) as the ids of the streams a session can
    read there now: collected (LIVE or SUSPECT) and offered over Aeron (`published`). Empty if stale. A shard on a
    version from before publishing writes no `published`, so it never counts: sessions wait until it is rolled."""
    if not status or now_ns - int(status.get('written_at_ns', 0)) > STATUS_FRESH_NS:
        return set()
    return {int(stream['id']) for stream in status.get('streams', [])
            if stream.get('state') in LIVE_STATES and stream.get('published') is True}


def endpoint_view(item, now_s: int):
    """A `service-endpoints` item (gnome-orchestrator ServiceEndpoints) as {privateIp, port}; None once expired, as
    DynamoDB removes expired items only eventually, or once the shard says it is stopping, so a replaced task's
    address is never handed out."""
    if not item or item.get('state') != 'ACTIVE' or int(item.get('expires_at', 0)) <= now_s:
        return None
    return {'privateIp': item['private_ip'], 'port': int(item['port'])}


def shards_of(targets: dict) -> set:
    """Every shard a session may need to read these targets from: their shards and, mid hand-off, the old ones."""
    shards = set()
    for target in targets.values():
        for key in ('shard_id', 'draining_from_shard_id'):
            if target.get(key):
                shards.add(target[key])
    return shards


def _shard_view(shard_id, stream_id, readable, endpoints):
    endpoint = endpoints.get(shard_id)
    return {
        'shardId': shard_id,
        'state': LIVE if endpoint is not None and stream_id in readable.get(shard_id, set()) else WARMING,
        'endpoint': endpoint,
    }


def stream_view(stream_id: int, target, readable: dict, endpoints: dict) -> dict:
    """Where a session reads one stream. Mid hand-off the old shard (still publishing) is current and the new one is
    `next`, which the session joins and switches to on its first record."""
    if not target or not target.get('shard_id') or target.get('state') == 'RETIRED':
        return {'streamId': stream_id, 'state': UNPLACED, 'shardId': None, 'endpoint': None, 'next': None}
    draining_from = target.get('draining_from_shard_id')
    current = draining_from or target['shard_id']
    view = {'streamId': stream_id, **_shard_view(current, stream_id, readable, endpoints), 'next': None}
    if draining_from:
        view['next'] = _shard_view(target['shard_id'], stream_id, readable, endpoints)
    return view


def lease_view(item: dict, targets: dict, readable: dict, endpoints: dict) -> dict:
    """GET's body. `targets` by stream id, `readable` shard id -> the stream ids it has LIVE and published
    (readable_streams), `endpoints` shard id -> endpoint."""
    streams = unpack_ids(item.get('streams'))
    return {
        'sessionId': item['lease_id'],
        'status': item.get('status'),
        'mode': item.get('mode'),
        'priority': item.get('priority'),
        'expiresAt': int(item['expires_at']) if item.get('expires_at') is not None else None,
        'releasedAt': int(item['released_at']) if item.get('released_at') is not None else None,
        'releaseReason': item.get('release_reason'),
        'streams': [stream_view(stream_id, targets.get(stream_id), readable, endpoints) for stream_id in streams],
    }
