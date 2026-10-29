import json
import os

import pytest

from leases import (LIFETIME_S, LIVE, RELEASED_TTL_S, STATUS_FRESH_NS, UNPLACED, WARMING, endpoint_view, lease_item,
                    lease_view, readable_streams, release_update, shards_of, validate)
from packed import pack_ids, unpack_ids

NOW_S = 1_800_000_000
NOW_NS = NOW_S * 1_000_000_000
# Shared with the reconciler's Java tests (LeaseConstantsTest).
CONSTANTS = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'gnome-market-data-lambdas', 'src', 'test',
                         'resources', 'lease-constants.json')


def test_lease_timings_agree_with_the_reconciler():
    with open(CONSTANTS) as f:
        constants = json.load(f)
    assert LIFETIME_S == constants['lifetime_seconds']
    assert RELEASED_TTL_S == constants['released_ttl_seconds']
    assert STATUS_FRESH_NS == constants['status_fresh_seconds'] * 1_000_000_000


# ----- POST /collection/leases -----

def test_a_lease_needs_a_session_a_mode_and_int32_stream_ids_kept_once():
    assert validate('s-1', [3, 1, 3], 'paper', 'eu-west-1') == ('s-1', [1, 3], 'paper', 'eu-west-1')
    assert validate('s-1', [], 'paper') == ('s-1', [], 'paper', None)
    with pytest.raises(ValueError, match='sessionId is required'):
        validate(' ', [1], 'paper')
    with pytest.raises(ValueError, match='mode must be one of'):
        validate('s-1', [1], 'backtest')
    with pytest.raises(ValueError, match='positive int32'):
        validate('s-1', [2**31], 'paper')
    with pytest.raises(ValueError, match='positive int32'):
        validate('s-1', ['7'], 'paper')


def test_a_live_session_leases_nothing_yet():
    with pytest.raises(ValueError, match='live sessions read their own venue feeds'):
        validate('s-1', [1], 'live')


def test_a_new_lease_takes_its_priority_from_the_mode_and_expires_in_two_hours():
    item = lease_item('s-1', [1, 3], 'live', 'us-east-1', None, 'service', NOW_S)

    assert item['priority'] == 'PRODUCTION'
    assert item['tier'] == 'standard'
    assert item['status'] == 'ACTIVE'
    assert item['created_at'] == NOW_S
    assert item['expires_at'] == NOW_S + 7200
    assert item['region'] == 'us-east-1'
    # Packed as the reconciler and shards read stream lists: big-endian int32s.
    assert item['streams'] == b'\x00\x00\x00\x01\x00\x00\x00\x03'
    assert lease_item('s-2', [1], 'paper', None, None, 'service', NOW_S)['priority'] == 'RESEARCH'
    assert 'region' not in lease_item('s-2', [1], 'paper', None, None, 'service', NOW_S)


def test_posting_again_replaces_the_streams_and_keeps_an_active_leases_creation():
    first = lease_item('s-1', [1], 'paper', None, None, 'service', NOW_S)
    again = lease_item('s-1', [2, 5], 'paper', None, first, 'service', NOW_S + 60)
    assert unpack_ids(again['streams']) == [2, 5]
    assert again['created_at'] == NOW_S
    assert again['expires_at'] == NOW_S + 60 + 7200

    released = {**first, 'status': 'RELEASED'}
    assert lease_item('s-1', [1], 'paper', None, released, 'service', NOW_S + 60)['created_at'] == NOW_S + 60


# ----- DELETE /collection/leases/{sessionId} -----

def test_releasing_only_touches_an_active_lease_and_sets_its_ttl():
    update = release_update(NOW_S, 'DELETED')
    assert update['ConditionExpression'] == '#status = :active'
    assert update['ExpressionAttributeValues'][':released'] == 'RELEASED'
    assert update['ExpressionAttributeValues'][':ttl'] == NOW_S + RELEASED_TTL_S


# ----- GET /collection/leases/{sessionId} -----

def status(written_at_ns, **streams):
    return {'written_at_ns': written_at_ns,
            'streams': [{'id': int(i), 'state': state, 'published': published}
                        for i, (state, published) in streams.items()]}


def test_a_stale_status_says_nothing_about_its_streams():
    assert readable_streams(status(NOW_NS - 10**9, **{'1': ('LIVE', True)}), NOW_NS) == {1}
    assert readable_streams(status(NOW_NS - 60 * 10**9, **{'1': ('LIVE', True)}), NOW_NS) == set()
    assert readable_streams(None, NOW_NS) == set()


def test_a_stream_is_readable_only_once_collected_and_published():
    fresh = status(NOW_NS, **{'1': ('LIVE', True), '2': ('SUSPECT', True), '3': ('LIVE', False),
                              '4': ('SUBSCRIBING', True)})
    assert readable_streams(fresh, NOW_NS) == {1, 2}


def test_a_shard_from_before_publishing_has_nothing_readable():
    old = {'written_at_ns': NOW_NS, 'streams': [{'id': 1, 'state': 'LIVE'}]}
    assert readable_streams(old, NOW_NS) == set()


def test_an_expired_or_stopping_endpoint_is_no_endpoint():
    item = {'service_id': 'shard#use1-1', 'private_ip': '10.0.1.5', 'port': 40000, 'state': 'ACTIVE',
            'expires_at': NOW_S + 30}
    assert endpoint_view(item, NOW_S) == {'privateIp': '10.0.1.5', 'port': 40000}
    assert endpoint_view({**item, 'expires_at': NOW_S}, NOW_S) is None
    assert endpoint_view({**item, 'state': 'STOPPING'}, NOW_S) is None
    assert endpoint_view({k: v for k, v in item.items() if k != 'state'}, NOW_S) is None
    assert endpoint_view(None, NOW_S) is None


def test_each_stream_is_unplaced_warming_or_live_on_its_shard():
    item = {'lease_id': 's-1', 'status': 'ACTIVE', 'mode': 'paper', 'priority': 'RESEARCH',
            'expires_at': NOW_S + 3600, 'streams': pack_ids([1, 2, 3, 4])}
    targets = {
        1: {'stream_id': 1, 'state': 'DESIRED', 'shard_id': 'use1-1'},
        2: {'stream_id': 2, 'state': 'DESIRED', 'shard_id': 'use1-1'},
        3: {'stream_id': 3, 'state': 'DESIRED'},
    }
    readable = {'use1-1': {1}}
    endpoints = {'use1-1': {'privateIp': '10.0.1.5', 'port': 40000}}

    view = lease_view(item, targets, readable, endpoints)

    assert view['sessionId'] == 's-1'
    assert view['status'] == 'ACTIVE'
    assert view['expiresAt'] == NOW_S + 3600
    assert view['streams'] == [
        {'streamId': 1, 'state': LIVE, 'shardId': 'use1-1', 'endpoint': endpoints['use1-1'], 'next': None},
        {'streamId': 2, 'state': WARMING, 'shardId': 'use1-1', 'endpoint': endpoints['use1-1'], 'next': None},
        {'streamId': 3, 'state': UNPLACED, 'shardId': None, 'endpoint': None, 'next': None},
        {'streamId': 4, 'state': UNPLACED, 'shardId': None, 'endpoint': None, 'next': None},
    ]


def test_mid_hand_off_the_old_shard_is_current_and_the_new_one_next():
    targets = {7: {'stream_id': 7, 'state': 'DESIRED', 'shard_id': 'use1-2', 'draining_from_shard_id': 'use1-1'}}
    assert shards_of(targets) == {'use1-1', 'use1-2'}
    item = {'lease_id': 's-1', 'status': 'ACTIVE', 'streams': pack_ids([7])}
    readable = {'use1-1': {7}, 'use1-2': {7}}
    endpoints = {'use1-1': {'privateIp': '10.0.1.5', 'port': 40000},
                 'use1-2': {'privateIp': '10.0.1.6', 'port': 40000}}

    [stream] = lease_view(item, targets, readable, endpoints)['streams']

    assert stream == {
        'streamId': 7, 'state': LIVE, 'shardId': 'use1-1', 'endpoint': endpoints['use1-1'],
        'next': {'shardId': 'use1-2', 'state': LIVE, 'endpoint': endpoints['use1-2']},
    }


def test_a_stream_on_a_shard_without_an_endpoint_is_still_warming():
    targets = {7: {'stream_id': 7, 'state': 'DESIRED', 'shard_id': 'use1-2', 'draining_from_shard_id': 'use1-1'}}
    item = {'lease_id': 's-1', 'status': 'ACTIVE', 'streams': pack_ids([7])}
    readable = {'use1-1': {7}, 'use1-2': {7}}

    [stream] = lease_view(item, targets, readable, {'use1-1': None, 'use1-2': None})['streams']

    assert stream['state'] == WARMING and stream['endpoint'] is None
    assert stream['next'] == {'shardId': 'use1-2', 'state': WARMING, 'endpoint': None}


def test_a_retiring_target_is_still_read_from_its_shard():
    targets = {7: {'stream_id': 7, 'state': 'RETIRING', 'shard_id': 'use1-1'}}
    item = {'lease_id': 's-1', 'status': 'RELEASED', 'streams': pack_ids([7])}
    endpoints = {'use1-1': {'privateIp': '10.0.1.5', 'port': 40000}}

    [stream] = lease_view(item, targets, {'use1-1': {7}}, endpoints)['streams']

    assert stream['state'] == LIVE and stream['shardId'] == 'use1-1'
