"""The lease routes end to end against in-memory tables; skipped where boto3 isn't installed."""
import importlib.util
import io
import json
import os
import time

import pytest

pytest.importorskip('boto3')
from botocore.exceptions import ClientError

# The common layer's boto3 clients are made at import, which needs a region.
os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')
import collection
from packed import pack_ids

HANDLER = os.path.join(os.path.dirname(__file__), '..', 'functions', 'collection', 'leases', 'index.py')


class FakeTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key, ConsistentRead=False):
        item = self.items.get(next(iter(Key.values())))
        return {'Item': dict(item)} if item else {}

    def put_item(self, Item):
        self.items[Item['lease_id']] = dict(Item)

    def update_item(self, Key, UpdateExpression, ConditionExpression, ExpressionAttributeNames,
                    ExpressionAttributeValues):
        item = self.items.get(Key['lease_id'])
        if item is None or item['status'] != ExpressionAttributeValues[':active']:
            raise ClientError({'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        item.update(status=ExpressionAttributeValues[':released'], ttl=ExpressionAttributeValues[':ttl'],
                    release_reason=ExpressionAttributeValues[':reason'])


class FakeDynamo:
    def __init__(self, tables):
        self.tables = tables

    def batch_get_item(self, RequestItems):
        responses = {}
        for table, request in RequestItems.items():
            rows = self.tables[table]
            responses[table] = [rows[next(iter(key.values()))] for key in request['Keys']
                                if next(iter(key.values())) in rows]
        return {'Responses': responses}


class FakeS3:
    def __init__(self, statuses):
        self.statuses = statuses

    def get_paginator(self, name):
        statuses = self.statuses

        class Paginator:
            def paginate(self, Bucket, Prefix):
                shard = Prefix.split('/')[2]
                yield {'Contents': [{'Key': f'{Prefix}{task}.json', 'LastModified': at}
                                    for task, (at, _) in statuses.get(shard, {}).items()]}
        return Paginator()

    def get_object(self, Bucket, Key):
        shard, task = Key.split('/')[2], Key.split('/')[-1][:-len('.json')]
        return {'Body': io.BytesIO(json.dumps(self.statuses[shard][task][1]).encode())}


@pytest.fixture
def route(monkeypatch):
    for name, value in {'AWS_DEFAULT_REGION': 'us-east-1', 'COLLECTION_LEASES_TABLE': 'collection-leases',
                        'COLLECTION_TARGETS_TABLE': 'collection-targets', 'SERVICE_ENDPOINTS_TABLE': 'service-endpoints',
                        'SERVICE_ENDPOINTS_REGION': 'us-east-1', 'METADATA_BUCKET_NAME': 'metadata',
                        'RECONCILER_QUEUE_URL': 'queue'}.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location('leases_route', HANDLER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.leases_table = FakeTable()
    module.targets = {}
    module.endpoints = {}
    module.statuses = {}
    module.wakes = []
    monkeypatch.setattr(module, '_leases', module.leases_table)
    monkeypatch.setattr(module, '_dynamo', FakeDynamo({'collection-targets': module.targets}))
    monkeypatch.setattr(module, '_endpoints_dynamo', FakeDynamo({'service-endpoints': module.endpoints}))
    monkeypatch.setattr(module, '_s3', FakeS3(module.statuses))
    monkeypatch.setattr(module, 'wake_reconciler', module.wakes.append)
    return module


def call(module, method, session_id=None, body=None):
    event = {'httpMethod': method, 'body': json.dumps(body) if body is not None else None,
             'pathParameters': {'sessionId': session_id} if session_id else None,
             'requestContext': {'authorizer': {'actor': 'service', 'kind': 'service'}}}
    response = module.handler(event, None)
    return response['statusCode'], json.loads(response['body'])


def test_post_leases_and_wakes_the_reconciler(route):
    code, body = call(route, 'POST', body={'sessionId': 's-1', 'streams': [2, 1], 'mode': 'paper',
                                           'region': 'ap-northeast-1'})
    assert (code, body) == (200, {'sessionId': 's-1'})
    item = route.leases_table.items['s-1']
    assert item['priority'] == 'RESEARCH' and item['region'] == 'ap-northeast-1'
    assert route.wakes == ['lease-created']

    code, body = call(route, 'POST', body={'sessionId': 's-1', 'streams': [1], 'mode': 'banana'})
    assert code == 400 and 'mode' in body['error']
    code, body = call(route, 'POST', body={'sessionId': 's-2', 'streams': [1], 'mode': 'live'})
    assert code == 400 and 'live' in body['error']


def test_a_dynamodb_failure_is_a_server_error_the_launcher_retries(route, monkeypatch):
    def throttled(Item):
        raise ClientError({'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'PutItem')
    monkeypatch.setattr(route.leases_table, 'put_item', throttled)

    code, body = call(route, 'POST', body={'sessionId': 's-1', 'streams': [1], 'mode': 'paper'})

    assert code == 500 and 'ProvisionedThroughputExceededException' in body['error']


def test_a_lease_stands_when_the_reconciler_cannot_be_woken(route, monkeypatch):
    class DownQueue:
        def send_message(self, **_):
            raise ClientError({'Error': {'Code': 'ServiceUnavailable'}}, 'SendMessage')
    monkeypatch.setattr(collection, '_sqs', DownQueue())
    monkeypatch.setattr(route, 'wake_reconciler', collection.wake_reconciler)

    assert call(route, 'POST', body={'sessionId': 's-1', 'streams': [1], 'mode': 'paper'}) == (200, {'sessionId': 's-1'})
    assert route.leases_table.items['s-1']['status'] == 'ACTIVE'
    assert call(route, 'DELETE', 's-1') == (200, {'sessionId': 's-1', 'status': 'RELEASED'})


def test_delete_releases_once_and_404s_an_unknown_session(route):
    call(route, 'POST', body={'sessionId': 's-1', 'streams': [1], 'mode': 'paper'})

    assert call(route, 'DELETE', 's-1') == (200, {'sessionId': 's-1', 'status': 'RELEASED'})
    assert route.leases_table.items['s-1']['release_reason'] == 'DELETED'
    assert call(route, 'DELETE', 's-1') == (200, {'sessionId': 's-1', 'status': 'RELEASED'})
    assert call(route, 'DELETE', 'nope')[0] == 404


def test_get_reads_targets_statuses_and_endpoints(route):
    route.leases_table.items['s-1'] = {'lease_id': 's-1', 'status': 'ACTIVE', 'mode': 'paper',
                                       'priority': 'RESEARCH', 'expires_at': 2_000_000_000,
                                       'streams': pack_ids([1, 2])}
    route.targets[1] = {'stream_id': 1, 'state': 'DESIRED', 'shard_id': 'use1-1'}
    now_ns = time.time_ns()
    route.statuses['use1-1'] = {
        'old': (1, {'written_at_ns': now_ns - 600 * 10**9, 'streams': []}),
        'new': (2, {'written_at_ns': now_ns, 'streams': [{'id': 1, 'state': 'LIVE', 'published': True}]}),
    }
    route.endpoints['shard#use1-1'] = {'service_id': 'shard#use1-1', 'private_ip': '10.0.1.5', 'port': 40000,
                                       'state': 'ACTIVE', 'expires_at': now_ns // 10**9 + 60}

    code, body = call(route, 'GET', 's-1')

    assert code == 200
    assert body['streams'] == [
        {'streamId': 1, 'state': 'LIVE', 'shardId': 'use1-1', 'endpoint': {'privateIp': '10.0.1.5', 'port': 40000},
         'next': None},
        {'streamId': 2, 'state': 'UNPLACED', 'shardId': None, 'endpoint': None, 'next': None},
    ]
    assert call(route, 'GET', 'nope')[0] == 404


@pytest.fixture
def one_stream(route):
    """Session s-1 leasing stream 1, on shard use1-1, which has an endpoint; each test sets the shard's status."""
    route.leases_table.items['s-1'] = {'lease_id': 's-1', 'status': 'ACTIVE', 'mode': 'paper',
                                       'priority': 'RESEARCH', 'expires_at': 2_000_000_000, 'streams': pack_ids([1])}
    route.targets[1] = {'stream_id': 1, 'state': 'DESIRED', 'shard_id': 'use1-1'}
    route.endpoints['shard#use1-1'] = {'service_id': 'shard#use1-1', 'private_ip': '10.0.1.5', 'port': 40000,
                                       'state': 'ACTIVE', 'expires_at': time.time_ns() // 10**9 + 60}
    return route


def stream_state(route):
    code, body = call(route, 'GET', 's-1')
    assert code == 200
    return body['streams'][0]['state']


@pytest.mark.parametrize('entry, written_ago_s, expected', [
    ({'id': 1, 'state': 'LIVE', 'published': True}, 0, 'LIVE'),
    ({'id': 1, 'state': 'LIVE', 'published': True}, 120, 'WARMING'),
    ({'id': 1, 'state': 'LIVE'}, 0, 'WARMING'),
    ({'id': 1, 'state': 'LIVE', 'published': False}, 0, 'WARMING'),
])
def test_get_says_live_only_for_a_fresh_status_that_publishes_the_stream(one_stream, entry, written_ago_s, expected):
    one_stream.statuses['use1-1'] = {'t': (1, {'written_at_ns': time.time_ns() - written_ago_s * 10**9,
                                               'streams': [entry]})}
    assert stream_state(one_stream) == expected


def test_get_hides_the_endpoint_of_a_stopping_shard(one_stream):
    one_stream.statuses['use1-1'] = {'t': (1, {'written_at_ns': time.time_ns(),
                                               'streams': [{'id': 1, 'state': 'LIVE', 'published': True}]})}
    one_stream.endpoints['shard#use1-1']['state'] = 'STOPPING'

    code, body = call(one_stream, 'GET', 's-1')

    assert body['streams'][0]['state'] == 'WARMING' and body['streams'][0]['endpoint'] is None


def test_get_reads_a_retiring_target_from_its_shard(one_stream):
    one_stream.targets[1]['state'] = 'RETIRING'
    one_stream.statuses['use1-1'] = {'t': (1, {'written_at_ns': time.time_ns(),
                                               'streams': [{'id': 1, 'state': 'LIVE', 'published': True}]})}
    assert stream_state(one_stream) == 'LIVE'
