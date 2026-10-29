"""Stream id lists as the control plane stores them: big-endian int32s packed into one binary attribute (shard items'
streams/draining/publish/cuts, read by gnome-orchestrator DynamoAssignment; lease items' streams)."""
import struct


def pack_ids(ids) -> bytes:
    return struct.pack(f'>{len(ids)}i', *ids)


def unpack_ids(value) -> list:
    if value is None:
        return []
    raw = bytes(value.value if hasattr(value, 'value') else value)
    return list(struct.unpack(f'>{len(raw) // 4}i', raw))


def unpack_cuts(value) -> dict:
    """A shard item's cuts: (stream id, minute since the epoch) int32 pairs; 2147483647 is WARMING (write nothing yet)."""
    ids = unpack_ids(value)
    return {str(ids[i]): ids[i + 1] for i in range(0, len(ids) - 1, 2)}
