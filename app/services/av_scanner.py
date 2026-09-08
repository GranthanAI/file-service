"""docs/LLD.md Phase 6 task 4: a pluggable AV-scanner interface, so
quarantine_service.py never talks to ClamAV (or the absence of it) directly.

`get_scanner()` picks the implementation from `settings.CLAMAV_ENABLED`:
`ClamdScanner` against a real clamd daemon in production/staging, or
`NoOpScanner` (always reports clean) for local dev environments that don't
run ClamAV — the fallback docs/LLD.md explicitly calls for.
"""

import asyncio
import io
from typing import NamedTuple, Protocol

import clamd

from app.core.config import settings


class ScanVerdict(NamedTuple):
    clean: bool
    signature: str | None = None


class Scanner(Protocol):
    async def scan(self, data: bytes) -> ScanVerdict: ...


class NoOpScanner:
    """Always reports clean — the documented fallback for environments
    without ClamAV. Never selected when settings.CLAMAV_ENABLED is true."""

    async def scan(self, data: bytes) -> ScanVerdict:  # noqa: ARG002 — data intentionally unused
        return ScanVerdict(clean=True)


class ClamdScanner:
    """Talks to a running clamd daemon over its TCP INSTREAM protocol via the
    `clamd` package. The `clamd` client is a plain blocking socket client, so
    every call is offloaded to a worker thread (`asyncio.to_thread`) rather
    than blocking the event loop for the duration of the scan."""

    def __init__(self, host: str, port: int, *, timeout: float = 30.0) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout

    def _scan_sync(self, data: bytes) -> ScanVerdict:
        client = clamd.ClamdNetworkSocket(host=self._host, port=self._port, timeout=self._timeout)
        result = client.instream(io.BytesIO(data))
        # clamd's INSTREAM response shape: {"stream": (status, signature)},
        # status is "OK" (clean) or "FOUND" (infected); signature is the
        # matched signature name for "FOUND", None for "OK".
        status, signature = result["stream"]
        return ScanVerdict(clean=(status == "OK"), signature=signature)

    async def scan(self, data: bytes) -> ScanVerdict:
        return await asyncio.to_thread(self._scan_sync, data)


def get_scanner() -> Scanner:
    if settings.CLAMAV_ENABLED:
        return ClamdScanner(settings.CLAMAV_HOST, settings.CLAMAV_PORT, timeout=settings.CLAMAV_TIMEOUT_SECONDS)
    return NoOpScanner()
