"""Where a stream has no data: the stretches of a range no coverage interval covers, each with why."""

# Nothing wanted the stream, so no shard collected it.
NOT_COLLECTED = 'NOT_COLLECTED'


def missing_stretches(intervals: list[dict], gaps: list[dict], open_gap: dict | None,
                      from_ns: int, to_ns: int, tracked_since_ns: int) -> tuple[list[dict], int]:
    """The uncovered stretches of [from_ns, to_ns) from tracked_since_ns on, and the time before it.

    A stretch takes the reason of the gap row or open gap it falls in; a stretch no gap explains is time nothing wanted
    the stream. Time before coverage was tracked is unknown rather than missing, so it is only measured. Intervals,
    gaps and the open gap use camelCase epoch-nanosecond bounds; the open gap runs to to_ns.
    """
    lo = max(from_ns, tracked_since_ns)
    untracked_ns = max(0, min(lo, to_ns) - from_ns)
    if lo >= to_ns:
        return [], untracked_ns

    holes, cursor = [], lo
    for interval in sorted(intervals, key=lambda i: i['fromNs']):
        start, end = max(interval['fromNs'], lo), min(interval['toNs'], to_ns)
        if end <= start:
            continue
        if start > cursor:
            holes.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < to_ns:
        holes.append((cursor, to_ns))

    reasons = sorted(
        [(g['fromNs'], g['toNs'], g.get('reason') or 'UNKNOWN') for g in gaps]
        + ([(open_gap['fromNs'], to_ns, open_gap.get('reason') or 'UNKNOWN')] if open_gap else []),
    )
    stretches: list[dict] = []
    for start, end in holes:
        at = start
        for r_from, r_to, reason in reasons:
            r_from, r_to = max(r_from, at), min(r_to, end)
            if r_to <= r_from:
                continue
            if r_from > at:
                _append(stretches, at, r_from, NOT_COLLECTED)
            _append(stretches, r_from, r_to, reason)
            at = r_to
        if at < end:
            _append(stretches, at, end, NOT_COLLECTED)
    return stretches, untracked_ns


def _append(stretches: list[dict], from_ns: int, to_ns: int, reason: str) -> None:
    last = stretches[-1] if stretches else None
    if last and last['toNs'] == from_ns and last['reason'] == reason:
        last['toNs'] = to_ns
    else:
        stretches.append({'fromNs': from_ns, 'toNs': to_ns, 'reason': reason})
