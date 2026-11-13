"""What the shard log digest says: a day of shards' unexpected errors, grouped by kind, against the day before.

Shards log what they handle by themselves (a venue closing a socket, a resync) at other levels, so every line here is
one someone should look at; a kind of error new since yesterday is the one most worth it.
"""
import re
from collections import Counter

# Slack shows a section's text up to 3000 characters.
MAX_BODY_CHARS = 2800
MAX_KINDS = 15
_PREFIX = re.compile(r'^\[\d+\] (UNKNOWN_ERROR|FATAL_ERROR_EXITING) ')
_IDS = [
    (re.compile(r'0x[0-9a-f]+(:\d+)?'), '<id>'),
    (re.compile(r'\b(POLYMARKET_US|POLYMARKET_INTL|HYPERLIQUID|KALSHI|LIGHTER)-\d+'), r'\1-<n>'),
    (re.compile(r'slug already subscribed: \S+'), 'slug already subscribed: <slug>'),
    (re.compile(r'\d+'), '<n>'),
]


def kind(message):
    """A line with its ids and numbers taken out, so repeats of one error read as one kind."""
    text = _PREFIX.sub('', message.strip())
    for pattern, replacement in _IDS:
        text = pattern.sub(replacement, text)
    return text[:160]


def summarize(lines, since_ms):
    """Counts per (region, kind) for lines at or after since_ms (today) and before it (the day before).

    lines: (region, timestamp_ms, message).
    """
    today, yesterday = Counter(), Counter()
    for region, at_ms, message in lines:
        (today if at_ms >= since_ms else yesterday)[(region, kind(message))] += 1
    return today, yesterday


def message(today, yesterday):
    """The digest's title and body, or None when there was nothing today."""
    if not today:
        return None
    kinds = today.most_common()
    new = [key for key, _ in kinds if key not in yesterday]
    title = (f'Shards logged {sum(today.values())} unexpected errors in the last 24 h: '
             f'{len(kinds)} kinds, {len(new)} new since the day before')
    rows = []
    for (region, text), count in kinds[:MAX_KINDS]:
        before = yesterday.get((region, text), 0)
        change = 'new' if before == 0 else f'{count - before:+d}'
        rows.append(f'{count:>5} ({change:>5})  {region:<14}  {text}')
    if len(kinds) > MAX_KINDS:
        rows.append(f'... and {len(kinds) - MAX_KINDS} more kinds')
    body = '\n'.join(rows)
    if len(body) > MAX_BODY_CHARS:
        body = body[:MAX_BODY_CHARS].rsplit('\n', 1)[0] + '\n...'
    return title, body
