import time

import registry
from collection_rules import (EVENT, LISTING_FILTER, STATIC_STREAMS, event_query, listing_query, proposition_query,
                              select, select_listings, validate)
from utils import lambda_handler

# Events per stream-by-event request, as the reconciler asks (gnome-registry SecurityMaster.EVENTS_PER_REQUEST).
EVENTS_PER_REQUEST = 100
# Listings per stream-by-listing request, as the reconciler asks (gnome-registry SecurityMaster.STREAMS_PER_REQUEST).
LISTINGS_PER_REQUEST = 200
SAMPLE = 10


@lambda_handler
def handler(rule_type: str = None, parameters: dict = None):
    """What a rule would collect if created now: its events and streams, and whether max_streams cuts it short. Asks
    the registry as the reconciler does; an event_filter rule here has no events yet to keep within its band."""
    cleaned = validate(rule_type, parameters)
    if rule_type == STATIC_STREAMS:
        return {'events': 0, 'streams': len(cleaned['stream_ids']), 'capped': False, 'sample': []}
    if rule_type == LISTING_FILTER:
        return _preview_listings(cleaned)
    if rule_type == EVENT:
        ids = cleaned.get('event_ids', [])
        ranked = [e for e in registry.get('events', {'eventIds': ','.join(map(str, ids))}) if not e['resolved']] \
            if ids else []
        # An event rule has no cap: every open event it names.
        selection = {'max_streams': float('inf')}
    else:
        ranked = registry.get('events', event_query(cleaned, int(time.time() * 1000)))
        selection = cleaned
    by_event = _streams_of(ranked, proposition_query(selection))
    chosen = select(selection, ranked, by_event)
    return {
        'events': len(chosen['events']),
        'streams': len(chosen['streams']),
        'capped': chosen['capped'],
        'sample': [{'eventId': e['event_id'], 'title': e['title'], 'volume24h': e.get('volume_24h'),
                    'streams': len(by_event.get(e['event_id'], []))} for e in chosen['events'][:SAMPLE]],
    }


def _streams_of(events: list[dict], propositions: dict) -> dict[int, list[int]]:
    ids = [e['event_id'] for e in events]
    by_event: dict[int, list[int]] = {}
    for i in range(0, len(ids), EVENTS_PER_REQUEST):
        chunk = ','.join(map(str, ids[i:i + EVENTS_PER_REQUEST]))
        for stream in registry.get('streams', {'eventIds': chunk, 'active': 'true', **propositions}):
            by_event.setdefault(stream['event_id'], []).append(stream['stream_id'])
    return by_event


def _preview_listings(parameters: dict) -> dict:
    ranked = registry.get('listings', {**listing_query(parameters), 'denormalize': 'true'})
    stream_by_listing = _listing_streams([row['listing_id'] for row in ranked])
    chosen = select_listings(parameters, ranked, stream_by_listing)
    return {
        'events': 0,
        'listings': len(chosen['listings']),
        'streams': len(chosen['streams']),
        'capped': chosen['capped'],
        'sample': [{'listingId': row['listing_id'], 'title': f"{row['exchange_name']} {row['exchange_security_symbol']}",
                    'volume24h': row.get('volume_24h'), 'streams': 1} for row in chosen['listings'][:SAMPLE]],
    }


def _listing_streams(listing_ids: list[int]) -> dict[int, int]:
    by_listing: dict[int, int] = {}
    for i in range(0, len(listing_ids), LISTINGS_PER_REQUEST):
        chunk = ','.join(map(str, listing_ids[i:i + LISTINGS_PER_REQUEST]))
        for stream in registry.get('streams', {'listingIds': chunk, 'active': 'true', 'limit': LISTINGS_PER_REQUEST}):
            by_listing[stream['listing_id']] = stream['stream_id']
    return by_listing
