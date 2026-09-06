import os
import time
import uuid

_VERSION_MASK = 0x0FFF
_VERSION_BITS = 0x7000  # version 7
_VARIANT_MASK = 0x3FFFFFFFFFFFFFFF
_VARIANT_BITS = 0x8000000000000000  # RFC 4122 variant (10xx)


def new_uuid7() -> uuid.UUID:
    """Generate a UUIDv7 (RFC 9562): a 48-bit big-endian Unix ms timestamp
    followed by random bits, so values are roughly time-sortable.

    Used as the File Service's `document_id` (docs/HLD.md §8, docs/LLD.md §8)
    instead of `gen_random_uuid()` — see docs/LLD.md §2.1 for why: the sort
    property lets `ORDER BY document_id` double as a chronological cursor for
    keyset pagination, on top of the B-tree index the primary key already has.

    Not strictly monotonic within the same millisecond (ties are broken by
    random bits), which is an accepted tradeoff — no dependency/extra state
    (e.g. a monotonic counter) is introduced just to guarantee sub-millisecond
    ordering that nothing in this service relies on.
    """
    unix_ts_ms = int(time.time() * 1000)
    rand_a = int.from_bytes(os.urandom(2), "big") & _VERSION_MASK
    rand_b = int.from_bytes(os.urandom(8), "big") & _VARIANT_MASK

    value = (unix_ts_ms & 0xFFFFFFFFFFFF) << 80
    value |= (_VERSION_BITS | rand_a) << 64
    value |= _VARIANT_BITS | rand_b

    return uuid.UUID(int=value)
