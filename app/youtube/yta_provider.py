"""Adapter for the `youtube-transcript-api` library (the initial provider).

NOTE: this library is an unofficial client of YouTube's web endpoints (not the YouTube Data API). Its use from a server must be
cleared against YouTube's terms and may need an egress proxy (datacentre IPs are often blocked): see
docs/YOUTUBE_TRANSCRIPT_CONTEXT_ADR.md, decision D1. Every library exception is mapped to `provider.TranscriptError` here and
nowhere else.
"""

import json
import re
from urllib.parse import quote

import requests

from app.core.config import settings
from app.core.logging import get_logger
from app.youtube.provider import (
    FetchFailed,
    ProviderBlocked,
    RawSegment,
    TrackDiscoveryFailed,
    TranscriptError,
    TranscriptTrack,
    TransientDependency,
    VideoRestricted,
    VideoUnavailable,
    NoTranscript,
)
from app.youtube.urls import VIDEO_ID_RE, canonical_url

logger = get_logger(__name__)
_OEMBED = "https://www.youtube.com/oembed"


class _TimeoutSession(requests.Session):
    """Every request gets a (connect, read) timeout; the library would otherwise wait indefinitely."""

    def __init__(self, timeout: float) -> None:
        super().__init__()
        self._timeout = timeout

    def request(self, method, url, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._timeout)
        return super().request(method, url, **kwargs)


def _map(exc: Exception, phase: str) -> TranscriptError:
    """Maps a library/network exception to our taxonomy. Unknown exceptions are treated as retryable, never as success."""
    import youtube_transcript_api as y

    if isinstance(exc, TranscriptError):
        return exc
    if isinstance(exc, (y.TranscriptsDisabled, y.NoTranscriptFound)):
        return NoTranscript(type(exc).__name__)
    if isinstance(exc, (y.VideoUnavailable, y.VideoUnplayable, y.InvalidVideoId)):
        return VideoUnavailable(type(exc).__name__)
    if isinstance(exc, y.AgeRestricted):
        return VideoRestricted(type(exc).__name__)
    if isinstance(exc, (y.RequestBlocked, y.IpBlocked, y.PoTokenRequired)):
        return ProviderBlocked(type(exc).__name__)
    if isinstance(exc, (requests.RequestException, y.YouTubeRequestFailed)):
        return TransientDependency(type(exc).__name__)
    return TrackDiscoveryFailed(type(exc).__name__) if phase == "list" else FetchFailed(type(exc).__name__)


class YouTubeTranscriptApiProvider:
    name = "youtube-transcript-api"

    def __init__(self) -> None:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api.proxies import GenericProxyConfig

        session = _TimeoutSession(settings.YOUTUBE_REQUEST_TIMEOUT_SECONDS)
        proxy = GenericProxyConfig(http_url=settings.YOUTUBE_PROXY_URL, https_url=settings.YOUTUBE_PROXY_URL) if settings.YOUTUBE_PROXY_URL else None
        self._session = session
        self._api = YouTubeTranscriptApi(proxy_config=proxy, http_client=session)

    def list_tracks(self, video_id: str) -> list[TranscriptTrack]:
        try:
            return [TranscriptTrack(t.language_code, t.language, bool(t.is_generated)) for t in self._api.list(video_id)]
        except Exception as exc:  # noqa: BLE001 - mapped, never leaked
            raise _map(exc, "list") from None

    def fetch(self, video_id: str, track: TranscriptTrack) -> list[RawSegment]:
        try:
            fetched = self._api.fetch(video_id, languages=[track.language_code])
            return [RawSegment(s.start, s.duration, s.text) for s in fetched]
        except Exception as exc:  # noqa: BLE001
            raise _map(exc, "fetch") from None

    def video_title(self, video_id: str) -> str | None:
        """Best effort, official oEmbed endpoint on a fixed host with the canonical URL; failure just means no title."""
        if not VIDEO_ID_RE.fullmatch(video_id):
            return None
        try:
            r = self._session.get(f"{_OEMBED}?url={quote(canonical_url(video_id), safe='')}&format=json", allow_redirects=False)
            if r.status_code != 200:
                return None
            title = json.loads(r.text).get("title")
            return re.sub(r"\s+", " ", title).strip()[:200] if isinstance(title, str) and title.strip() else None
        except Exception:  # noqa: BLE001
            return None
