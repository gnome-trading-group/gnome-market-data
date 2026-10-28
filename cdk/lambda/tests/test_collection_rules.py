from decimal import Decimal

import pytest

from collection_rules import EVENT, EVENT_FILTER, STATIC_STREAMS, event_query, item_fields, select, validate, view


def test_a_static_rule_needs_streams_and_keeps_each_once():
    assert validate(STATIC_STREAMS, {'stream_ids': [3, 1, 3]}) == {'stream_ids': [1, 3]}
    with pytest.raises(ValueError, match='stream_ids is required'):
        validate(STATIC_STREAMS, {})
    with pytest.raises(ValueError, match='at least one'):
        validate(STATIC_STREAMS, {'stream_ids': []})


def test_parameters_are_checked_against_the_types_schema():
    with pytest.raises(ValueError, match='rule_type must be one of'):
        validate('vendor_schedule', {})
    with pytest.raises(ValueError, match="unknown parameters for event: \\['stream_ids'\\]"):
        validate(EVENT, {'stream_ids': [1]})
    with pytest.raises(ValueError, match='positive ids'):
        validate(EVENT, {'event_ids': [1, True]})
    with pytest.raises(ValueError, match='top_n must be at least 1'):
        validate(EVENT_FILTER, {'top_n': 0})
    with pytest.raises(ValueError, match='min_volume_24h must be a number'):
        validate(EVENT_FILTER, {'min_volume_24h': 'lots'})


def test_an_event_filter_gets_the_default_cap_and_leaves_unset_filters_out():
    assert validate(EVENT_FILTER, {'exchange_ids': [2, 1], 'tags': [' nfl ', 'nfl'], 'top_n': 5}) == {
        'exchange_ids': [1, 2], 'tags': ['nfl'], 'top_n': 5, 'max_streams': 200}


def test_an_event_rules_ids_are_a_top_level_set_and_none_store_no_set():
    assert item_fields(EVENT, {'event_ids': [7, 8]}) == {'parameters': {}, 'event_ids': {7, 8}}
    assert item_fields(EVENT, {}) == {'parameters': {}}
    assert item_fields(EVENT_FILTER, {'min_volume_24h': 2500.5, 'max_streams': 10}) == {
        'parameters': {'min_volume_24h': Decimal('2500.5'), 'max_streams': 10}}


def test_an_event_rule_is_returned_with_its_ids_inside_its_parameters():
    item = {'rule_id': 'r1', 'rule_type': EVENT, 'parameters': {}, 'event_ids': {Decimal(8), Decimal(7)}}
    assert view(item) == {'rule_id': 'r1', 'rule_type': EVENT, 'parameters': {'event_ids': [7, 8]}}
    assert view({'rule_id': 'r2', 'rule_type': EVENT, 'parameters': {}})['parameters'] == {'event_ids': []}


def test_a_volume_filter_asks_for_the_band_ranked_by_volume():
    now = 1_793_000_000_000
    query = event_query({'exchange_ids': [1, 2], 'tags': ['nfl'], 'min_volume_24h': 1000, 'top_n': 4,
                         'expires_within_hours': 24, 'max_streams': 200}, now)
    assert query == {'resolved': 'false', 'limit': 6, 'exchangeIds': '1,2', 'tags': 'nfl',
                     'expiryTo': '2026-10-27T07:33:20Z', 'minVolume24h': 500.0,
                     'sortBy': 'volume_24h', 'sortOrder': 'desc'}


def test_without_a_volume_clause_events_rank_by_expiry_up_to_the_cap():
    assert event_query({'categories': ['Sports'], 'max_streams': 50}, 0) == {
        'resolved': 'false', 'limit': 50, 'categories': 'Sports', 'sortBy': 'expiry'}


def test_a_new_rule_takes_joining_events_and_stops_at_the_last_whole_event_that_fits():
    ranked = [{'event_id': e, 'volume_24h': v} for e, v in ((1, 900), (2, 800), (3, 700), (4, 600))]
    streams = {1: [11, 12], 2: [21, 22, 23], 3: [31], 4: [41]}
    chosen = select({'top_n': 3, 'max_streams': 5}, ranked, streams)
    assert [e['event_id'] for e in chosen['events']] == [1, 2]
    assert chosen['streams'] == [11, 12, 21, 22, 23]
    assert chosen['capped'] is True, "event 3 is in the top 3 but doesn't fit"

    assert select({'top_n': 3, 'max_streams': 6}, ranked, streams)['capped'] is False
    assert [e['event_id'] for e in select({'min_volume_24h': 750, 'max_streams': 99}, ranked, streams)['events']] == [1, 2]


def test_an_event_without_active_streams_is_skipped_but_keeps_its_rank():
    ranked = [{'event_id': 1, 'volume_24h': 9}, {'event_id': 2, 'volume_24h': 8}, {'event_id': 3, 'volume_24h': 7}]
    chosen = select({'top_n': 2, 'max_streams': 99}, ranked, {2: [21], 3: [31]})
    assert [e['event_id'] for e in chosen['events']] == [2]
