"""docs/HLD.md §12 — parsing for MinIO's bucket-notification event stream.

Shared between quarantine_service.py (Phase 6, consumes it live to trigger
AV scanning) and reconciliation_worker.py (Phase 9, consumes the backlog to
cross-check for orphans) — both need the same (bucket, key) extraction from
the same S3-style event shape, so it lives here rather than being duplicated
or imported privately from one service into the other.
"""

from urllib.parse import unquote


def parse_object_created_event(message: dict) -> list[tuple[str, str]]:
    """Extracts (bucket, key) pairs from a MinIO bucket-notification message
    — MinIO follows the S3 event notification shape (`Records[].s3.bucket.name`
    / `Records[].s3.object.key`), with the key URL-encoded the same way S3
    itself encodes it."""
    pairs = []
    for record in message.get("Records", []):
        s3 = record.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        key = s3.get("object", {}).get("key")
        if bucket and key:
            pairs.append((bucket, unquote(key)))
    return pairs
