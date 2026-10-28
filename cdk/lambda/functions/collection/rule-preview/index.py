import time

import registry
from collection_rules import EVENT, STATIC_STREAMS, event_query, select, validate
from utils import lambda_handler

# Events per stream-by-event request, as the reconciler asks (gnome-registry SecurityMaster.EVENTS_PER_REQUEST).
EVENTS_PER_REQUEST = 100
SAMPLE = 10


@lambda_handler
def handler(rule_type: str = None, parameters: dict = None):
    """What a rule would collect if created now: its events and streams, and whether max_streams cuts it short. Asks
    the registry as the reconciler does; an event_filter rule here has no events yet to keep within its band."""
    cleaned = validate(rule_type, parameters)
    if rule_type == STATIC_STREAMS:
        return {'events': 0, 'streams': len(cleaned['stream_ids']), 'capped': False, 'sample': []}
    if rule_type == EVENT:
        ids = cleaned.get('event_ids', [])
        ranked = [e for e in registry.get('events', {'eventIds': ','.join(map(str, ids))}) if not e['resolved']] \
            if ids else []
        # An event rule has no cap: every open event it names.
        selection = {'max_streams': float('inf')}
    else:
        ranked = registry.get('events', event_query(cleaned, int(time.time() * 1000)))
        selection = cleaned
    by_event = _streams_of(ranked)
    chosen = select(selection, ranked, by_event)
    return {
        'events': len(chosen['events']),
        'streams': len(chosen['streams']),
        'capped': chosen['capped'],
        'sample': [{'eventId': e['event_id'], 'title': e['title'], 'volume24h': e.get('volume_24h'),
                    'streams': len(by_event.get(e['event_id'], []))} for e in chosen['events'][:SAMPLE]],
    }


def _streams_of(events: list[dict]) -> dict[int, list[int]]:
    ids = [e['event_id'] for e in events]
    by_event: dict[int, list[int]] = {}
    for i in range(0, len(ids), EVENTS_PER_REQUEST):
        chunk = ','.join(map(str, ids[i:i + EVENTS_PER_REQUEST]))
        for stream in registry.get('streams', {'eventIds': chunk, 'active': 'true'}):
            by_event.setdefault(stream['event_id'], []).append(stream['stream_id'])
    return by_event
