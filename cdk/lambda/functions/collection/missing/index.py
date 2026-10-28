import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import registry
from coverage import gap, interval, open_gap, query_range
from missing import missing_stretches
from utils import lambda_handler

MAX_IDS = 100
MAX_RANGE_NS = 31 * 86_400 * 1_000_000_000
WORKERS = 16
TRACKED_SINCE_NS = int(os.environ['COVERAGE_TRACKED_SINCE_NS'])

# boto3 resources aren't thread-safe: each worker gets its own.
_local = threading.local()


def _tables():
    if not hasattr(_local, 'tables'):
        dynamo = boto3.session.Session().resource('dynamodb')
        _local.tables = tuple(dynamo.Table(os.environ[name]) for name in
                              ('COLLECTION_COVERAGE_TABLE', 'COLLECTION_GAPS_TABLE', 'COLLECTION_TARGETS_TABLE'))
    return _local.tables


@lambda_handler
def handler(fromNs: int, toNs: int, listingIds: str = None, streamIds: str = None):
    """Every stretch of [fromNs, toNs) with no collected data, per stream, for listings or streams: gaps with their
    reason, `NOT_COLLECTED` time nothing wanted the stream, and the time before coverage was tracked (unknown, so only
    measured). Up to now; times are epoch nanoseconds. Backtests mark these stretches as bad data."""
    if (listingIds is None) == (streamIds is None):
        raise ValueError('pass one of listingIds or streamIds')
    ids = sorted({int(i) for i in (listingIds or streamIds).split(',') if i.strip()})
    if not ids or len(ids) > MAX_IDS:
        raise ValueError(f'between 1 and {MAX_IDS} ids')
    from_ns, to_ns = int(fromNs), int(toNs)
    if to_ns <= from_ns or to_ns - from_ns > MAX_RANGE_NS:
        raise ValueError('toNs must be after fromNs, and the range at most 31 days')

    by_listing = listingIds is not None
    rows = registry.get('streams', {'listingIds' if by_listing else 'streamIds': ','.join(map(str, ids)),
                                    'limit': len(ids)})
    now_ns = time.time_ns()
    end_ns = min(to_ns, now_ns)
    with ThreadPoolExecutor(WORKERS) as pool:
        streams = list(pool.map(lambda row: _stream(row, from_ns, end_ns, now_ns), rows))
    found = {row['listing_id' if by_listing else 'stream_id'] for row in rows}
    return {
        'fromNs': from_ns,
        'toNs': end_ns,
        'trackedSinceNs': TRACKED_SINCE_NS,
        'streams': sorted(streams, key=lambda s: s['streamId']),
        'unknownIds': [i for i in ids if i not in found],
    }


def _stream(row: dict, from_ns: int, to_ns: int, now_ns: int) -> dict:
    coverage, gaps, targets = _tables()
    stream_id = row['stream_id']
    intervals = [interval(item, now_ns) for item in query_range(coverage, stream_id, from_ns, to_ns)]
    gap_rows = [gap(item) for item in query_range(gaps, stream_id, from_ns, to_ns)]
    target = targets.get_item(Key={'stream_id': stream_id}).get('Item') or {}
    stretches, untracked_ns = missing_stretches(intervals, gap_rows, open_gap(target), from_ns, to_ns,
                                                TRACKED_SINCE_NS)
    return {
        'streamId': stream_id,
        'listingId': row.get('listing_id'),
        'missing': stretches,
        'missingNs': sum(s['toNs'] - s['fromNs'] for s in stretches),
        'untrackedNs': untracked_ns,
    }
