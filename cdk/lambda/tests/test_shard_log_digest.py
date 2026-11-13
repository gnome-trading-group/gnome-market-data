"""The shard log digest's summary, without AWS."""
import importlib.util
import os

SOURCE = os.path.join(os.path.dirname(__file__), '..', 'functions', 'monitoring', 'shard-log-digest', 'digest.py')
spec = importlib.util.spec_from_file_location('shard_log_digest', SOURCE)
digest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(digest)

DAY = 24 * 60 * 60 * 1000


def test_repeats_of_one_error_read_as_one_kind():
    a = '[1794595474334094368] UNKNOWN_ERROR Error occurred in POLYMARKET_US-5: IllegalStateException: A Polymarket US connection holds at most 100 markets'
    b = '[1794595999999999999] UNKNOWN_ERROR Error occurred in POLYMARKET_US-12: IllegalStateException: A Polymarket US connection holds at most 100 markets'
    assert digest.kind(a) == digest.kind(b)
    assert digest.kind(a).startswith('Error occurred in POLYMARKET_US-<n>: IllegalStateException')
    assert (digest.kind('[1] UNKNOWN_ERROR Polymarket US error: slug already subscribed: rotc-t-hexed-2026-11-30-gte85')
            == digest.kind('[2] UNKNOWN_ERROR Polymarket US error: slug already subscribed: astatc-valorant-kbg-lng'))


def test_today_and_the_day_before_are_counted_apart():
    lines = [
        ('us-east-1', 2 * DAY - 10, '[1] UNKNOWN_ERROR Shard status failed: IndexOutOfBoundsException'),
        ('us-east-1', 2 * DAY - 20, '[2] UNKNOWN_ERROR Shard status failed: IndexOutOfBoundsException'),
        ('us-east-1', DAY - 10, '[3] UNKNOWN_ERROR Shard status failed: IndexOutOfBoundsException'),
    ]
    today, yesterday = digest.summarize(lines, DAY)
    key = ('us-east-1', 'Shard status failed: IndexOutOfBoundsException')
    assert today[key] == 2
    assert yesterday[key] == 1


def test_a_quiet_day_posts_nothing():
    today, yesterday = digest.summarize([('eu-west-1', 5, '[1] UNKNOWN_ERROR old')], DAY)
    assert digest.message(today, yesterday) is None


def test_the_message_marks_kinds_new_since_the_day_before():
    lines = [
        ('us-east-1', DAY + 1, '[1] UNKNOWN_ERROR Shard status failed: IndexOutOfBoundsException'),
        ('us-east-1', DAY - 1, '[2] UNKNOWN_ERROR Shard status failed: IndexOutOfBoundsException'),
        ('ap-northeast-1', DAY + 2, '[3] UNKNOWN_ERROR HYPERLIQUID: rolled HYPERLIQUID-1 to HYPERLIQUID-2 with 50 markets in 900 us, 3 records replayed, 1 markets short of some'),
    ]
    title, body = digest.message(*digest.summarize(lines, DAY))
    assert title == 'Shards logged 2 unexpected errors in the last 24 h: 2 kinds, 1 new since the day before'
    rows = body.split('\n')
    assert any('new' in row and 'markets short of some' in row for row in rows)
    assert any('+0' in row and 'Shard status failed' in row for row in rows)


def test_a_long_day_is_cut_to_what_slack_shows():
    lines = [('us-east-1', DAY + i, f'[{i}] UNKNOWN_ERROR kind {"x" * i} ' + 'y' * 150) for i in range(40)]
    _, body = digest.message(*digest.summarize(lines, DAY))
    assert len(body) <= digest.MAX_BODY_CHARS + 4
    assert '... and 25 more kinds' in body or body.endswith('...')
