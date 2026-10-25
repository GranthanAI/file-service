"""Strict YouTube URL parsing.

The user's URL is never fetched. It is parsed with a real URL parser, must be HTTPS on an allow-listed host, and is reduced to
its canonical 11-character video id; everything downstream (provider calls, storage, display) is derived from that id only.
A string that merely contains "youtube.com" (``https://evil.com/?u=youtube.com``, ``https://youtube.com@evil.com``,
``https://youtube.com.evil.com``) is rejected because the *parsed hostname* must equal an allow-listed host exactly.
"""

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
MAX_URL_CHARS = 2048

WATCH_HOSTS = frozenset({"youtube.com", "www.youtube.com", "m.youtube.com"})
SHORT_LINK_HOSTS = frozenset({"youtu.be"})


class InvalidYouTubeUrl(ValueError):
    """The URL is not an accepted YouTube video link. `reason` is a stable, user-safe code."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class ParsedVideo:
    video_id: str
    canonical_url: str


def canonical_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _valid_id(candidate: str | None) -> str | None:
    return candidate if candidate and VIDEO_ID_RE.fullmatch(candidate) else None


def parse_youtube_url(raw: str, *, allow_shorts: bool = True) -> ParsedVideo:
    """Returns the canonical video for an accepted URL, or raises InvalidYouTubeUrl."""
    if not isinstance(raw, str):
        raise InvalidYouTubeUrl("malformed", "Enter a YouTube video link.")
    text = raw.strip()
    if not text or len(text) > MAX_URL_CHARS:
        raise InvalidYouTubeUrl("malformed", "Enter a YouTube video link.")
    if any(ord(c) < 0x20 or ord(c) == 0x7F or c in "\\" for c in text) or any(c.isspace() for c in text):
        raise InvalidYouTubeUrl("malformed", "That does not look like a valid link.")
    try:
        parts = urlsplit(text)
        port = parts.port                      # raises ValueError on a malformed port
    except ValueError:
        raise InvalidYouTubeUrl("malformed", "That does not look like a valid link.") from None

    if parts.scheme.lower() != "https":
        raise InvalidYouTubeUrl("unsupported_scheme", "Only https:// YouTube links are supported.")
    if parts.username is not None or parts.password is not None or port is not None:
        raise InvalidYouTubeUrl("unsupported_host", "Only plain youtube.com or youtu.be links are supported.")
    host = (parts.hostname or "").lower().rstrip(".")
    path_segments = [s for s in parts.path.split("/") if s]

    video_id: str | None = None
    if host in SHORT_LINK_HOSTS:
        video_id = _valid_id(path_segments[0]) if len(path_segments) == 1 else None
    elif host in WATCH_HOSTS:
        if parts.path.rstrip("/") == "/watch":
            video_id = _valid_id((parse_qs(parts.query).get("v") or [None])[0])
        elif len(path_segments) == 2 and path_segments[0] in ("embed", "live"):
            video_id = _valid_id(path_segments[1])
        elif len(path_segments) == 2 and path_segments[0] == "shorts":
            if not allow_shorts:
                raise InvalidYouTubeUrl("shorts_not_supported", "YouTube Shorts links are not supported.")
            video_id = _valid_id(path_segments[1])
        elif path_segments and path_segments[0] in ("playlist", "channel", "c", "user", "results", "feed"):
            raise InvalidYouTubeUrl("not_a_video", "Link a single video, not a playlist, channel or search page.")
    else:
        raise InvalidYouTubeUrl("unsupported_host", "Only youtube.com and youtu.be links are supported.")

    if video_id is None:
        raise InvalidYouTubeUrl("no_video_id", "Could not find a valid video id in that link.")
    return ParsedVideo(video_id=video_id, canonical_url=canonical_url(video_id))
