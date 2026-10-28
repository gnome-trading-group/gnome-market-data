import time

from coverage import gap, interval, open_gap, query_range, table
from utils import lambda_handler

_coverage = table('COLLECTION_COVERAGE_TABLE')
_gaps = table('COLLECTION_GAPS_TABLE')
_targets = table('COLLECTION_TARGETS_TABLE')


@lambda_handler
def handler(streamId: int, fromNs: int, toNs: int):
    """A stream's coverage intervals and gaps over [fromNs, toNs], the gap still open now, and how much of the range
    was covered. Times are epoch nanoseconds."""
    stream_id, from_ns, to_ns = int(streamId), int(fromNs), int(toNs)
    now_ns = time.time_ns()
    intervals = [interval(item, now_ns) for item in query_range(_coverage, stream_id, from_ns, to_ns)]
    gaps = [gap(item) for item in query_range(_gaps, stream_id, from_ns, to_ns)]
    target = _targets.get_item(Key={'stream_id': stream_id}).get('Item') or {}
    covered = sum(max(0, min(i['toNs'], to_ns) - max(i['fromNs'], from_ns)) for i in intervals)
    return {
        'streamId': stream_id,
        'fromNs': from_ns,
        'toNs': to_ns,
        'intervals': intervals,
        'gaps': gaps,
        'openGap': open_gap(target),
        'targetState': target.get('state'),
        'coveredFraction': covered / max(1, min(to_ns, now_ns) - from_ns),
    }
