import filetype


def sniff_content_type(data: bytes) -> str | None:
    """Sniffs the first bytes of an object to determine its real MIME type,
    independent of any client-declared Content-Type header.

    NOT wired into `/complete` (docs/LLD.md §3.5, §4.5) — doing so would
    require reading the object body there, which `/complete` deliberately
    avoids (it only calls StatObject, a metadata-only call). This function
    is invoked from `quarantine_service` during the AV-scan pass (Phase 6),
    which already streams the full object body for scanning, so sniffing
    magic bytes there is a free byproduct of a read that has to happen
    anyway.

    Returns None if the type can't be determined (e.g. plain text, which
    has no reliable magic-byte signature) — callers should not treat None
    as a failure on its own.
    """
    kind = filetype.guess(data)
    return kind.mime if kind is not None else None
