"""Collection rule types: their parameter schemas (served to the controller's rule form), validation, how a rule is
stored, and the selection a preview shows. The reconciler (gnome-market-data-lambdas RuleItems, RuleResolver) reads
the same items and selects the same way; change both together."""
import datetime
import math
from decimal import Decimal

STATIC_STREAMS = 'static_streams'
EVENT = 'event'
EVENT_FILTER = 'event_filter'

_IDS = {'type': 'array', 'items': {'type': 'integer'}}

# JSON Schema, in the subset the controller's SchemaFormFields renders.
RULE_TYPES = {
    STATIC_STREAMS: {
        'description': 'These streams.',
        'type': 'object',
        'properties': {
            'stream_ids': {**_IDS, 'description': 'Stream ids'},
        },
        'required': ['stream_ids'],
    },
    EVENT: {
        'description': 'Every stream of these events, both sides of every contract, including contracts added '
                       'later. Systems add and remove events through PATCH add_event_ids / remove_event_ids; an event '
                       'leaves the rule once it resolves.',
        'type': 'object',
        'properties': {
            'event_ids': {**_IDS, 'description': 'Event ids'},
        },
        'required': [],
    },
    EVENT_FILTER: {
        'description': 'Every stream of the open events matching every filter, up to max_streams. With a minimum '
                       'volume or top N, events join by 24h volume and stay down to half the minimum and 1.5x top N; '
                       'otherwise soonest expiry first.',
        'type': 'object',
        'properties': {
            'exchange_ids': {**_IDS, 'description': 'Any of these exchanges'},
            'categories': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Any of these categories'},
            'tags': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Any of these tags'},
            'expires_within_hours': {'type': 'integer', 'minimum': 1, 'description': 'Expiring within'},
            'min_volume_24h': {'type': 'number', 'minimum': 0,
                               'description': 'About USD over 24h (Polymarket US is a coarse bucket)'},
            'top_n': {'type': 'integer', 'minimum': 1, 'description': 'The N largest by 24h volume'},
            'max_streams': {'type': 'integer', 'minimum': 1, 'default': 200,
                            'description': 'Never more than this many streams'},
        },
        'required': ['max_streams'],
    },
}

# max_streams for a rule that doesn't set it.
DEFAULT_MAX_STREAMS = 200
RANK_BAND = 1.5
VOLUME_BAND = 0.5


def validate(rule_type: str, parameters) -> dict:
    """The parameters with defaults applied and lists de-duplicated; raises ValueError naming what's wrong."""
    if rule_type not in RULE_TYPES:
        raise ValueError(f'rule_type must be one of {sorted(RULE_TYPES)}')
    if parameters is None:
        parameters = {}
    if not isinstance(parameters, dict):
        raise ValueError('parameters must be an object')
    schema = RULE_TYPES[rule_type]
    unknown = set(parameters) - set(schema['properties'])
    if unknown:
        raise ValueError(f'unknown parameters for {rule_type}: {sorted(unknown)}')
    cleaned = {}
    for name, prop in schema['properties'].items():
        value = parameters.get(name, prop.get('default'))
        if value is None:
            if name in schema['required']:
                raise ValueError(f'{name} is required')
            continue
        cleaned[name] = _check(name, prop, value)
    if rule_type == STATIC_STREAMS and not cleaned['stream_ids']:
        raise ValueError('stream_ids must name at least one stream')
    return cleaned


def _check(name: str, prop: dict, value):
    kind = prop['type']
    if kind == 'array':
        if not isinstance(value, list):
            raise ValueError(f'{name} must be a list')
        item = prop['items']['type']
        if item == 'integer' and not all(_is_int(v) and v > 0 for v in value):
            raise ValueError(f'{name} must be positive ids')
        if item == 'string' and not all(isinstance(v, str) and v.strip() for v in value):
            raise ValueError(f'{name} must be non-empty strings')
        return sorted(set(value)) if item == 'integer' else sorted({v.strip() for v in value})
    if kind == 'integer':
        if not _is_int(value):
            raise ValueError(f'{name} must be a whole number')
    elif not (_is_int(value) or isinstance(value, float)):
        raise ValueError(f'{name} must be a number')
    if value < prop.get('minimum', value):
        raise ValueError(f'{name} must be at least {prop["minimum"]}')
    return value


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def item_fields(rule_type: str, parameters: dict) -> dict:
    """A rule's stored selection: `parameters`, except an event rule's ids, which are a top-level number set
    `event_ids` so systems can add and remove them atomically (DynamoDB ADD/DELETE work only on top-level sets).
    DynamoDB stores no empty set, so an event rule without events has no `event_ids`."""
    if rule_type == EVENT:
        ids = parameters.get('event_ids') or []
        return {'parameters': {}, **({'event_ids': set(ids)} if ids else {})}
    return {'parameters': {k: Decimal(str(v)) if isinstance(v, float) else v for k, v in parameters.items()}}


def view(item: dict) -> dict:
    """A stored rule as the API returns it: an event rule's ids back inside parameters."""
    rule = {k: v for k, v in item.items() if k != 'event_ids'}
    if item.get('rule_type') == EVENT:
        rule['parameters'] = {'event_ids': sorted(int(i) for i in item.get('event_ids', set()))}
    return rule


def event_query(parameters: dict, now_ms: int) -> dict:
    """The registry /events query an event_filter rule makes: open events, ranked, at most as many as it can keep."""
    max_streams = parameters.get('max_streams', DEFAULT_MAX_STREAMS)
    top_n = parameters.get('top_n')
    by_volume = top_n is not None or parameters.get('min_volume_24h') is not None
    query = {
        'resolved': 'false',
        'limit': min(math.ceil(RANK_BAND * top_n), max_streams) if top_n is not None else max_streams,
    }
    for key, param in (('exchange_ids', 'exchangeIds'), ('categories', 'categories'), ('tags', 'tags')):
        if parameters.get(key):
            query[param] = ','.join(str(v) for v in parameters[key])
    if parameters.get('expires_within_hours'):
        query['expiryTo'] = _iso(now_ms + parameters['expires_within_hours'] * 3_600_000)
    if parameters.get('min_volume_24h') is not None:
        query['minVolume24h'] = parameters['min_volume_24h'] * VOLUME_BAND
    query.update({'sortBy': 'volume_24h', 'sortOrder': 'desc'} if by_volume else {'sortBy': 'expiry'})
    return query


def select(parameters: dict, ranked: list[dict], streams_by_event: dict[int, list[int]]) -> dict:
    """What a new event_filter rule would collect from ranked events (registry rows, best first) and their active
    streams: events that join (top N, minimum volume) in order, stopping at the last whole event within max_streams.
    The reconciler also keeps events it already collects within the band; a new rule has none."""
    top_n = parameters.get('top_n')
    min_volume = parameters.get('min_volume_24h')
    max_streams = parameters.get('max_streams', DEFAULT_MAX_STREAMS)
    events, streams, capped = [], [], False
    for rank, event in enumerate(ranked):
        event_streams = streams_by_event.get(event['event_id'], [])
        if not event_streams:
            continue
        volume = event.get('volume_24h') or 0
        if (top_n is not None and rank >= top_n) or (min_volume is not None and volume < min_volume):
            continue
        if len(streams) + len(event_streams) > max_streams:
            capped = True
            break
        events.append(event)
        streams.extend(event_streams)
    return {'events': events, 'streams': streams, 'capped': capped}


def _iso(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
