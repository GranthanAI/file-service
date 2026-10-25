"""Transcript provider interface and the error taxonomy the rest of the service understands.

The application never sees a library's exceptions or response objects: an adapter maps them to the types here. There is no
automatic fallback to a second extraction mechanism; a failure is reported as what it is.
"""

from dataclasses import dataclass
from typing import Protocol


class TranscriptError(Exception):
    """Base class. `code` is stable and stored; `retryable` says whether trying again later can help."""

    code = "FETCH_FAILED"
    retryable = True
    # Do not hammer the provider in-process for these (blocked / unavailable): back off at the job level instead.
    backoff_in_process = True

    def __init__(self, detail: str = "") -> None:
        super().__init__(detail or self.code)
        self.detail = detail


class NoTranscript(TranscriptError):
    code, retryable = "NO_TRANSCRIPT", False                 # captions disabled / none exist


class NoSupportedLanguage(TranscriptError):
    code, retryable = "NO_SUPPORTED_LANGUAGE", False         # tracks exist, none in an allowed language


class TrackDiscoveryFailed(TranscriptError):
    code, retryable = "TRACK_DISCOVERY_FAILED", True


class FetchFailed(TranscriptError):
    code, retryable = "FETCH_FAILED", True                   # a track exists but downloading it failed


class ProviderBlocked(TranscriptError):
    code, retryable, backoff_in_process = "PROVIDER_BLOCKED", True, False   # YouTube/the provider blocked this request


class VideoUnavailable(TranscriptError):
    code, retryable = "VIDEO_UNAVAILABLE", False             # private, removed, region-locked, invalid id


class VideoRestricted(TranscriptError):
    code, retryable = "VIDEO_RESTRICTED", False              # age-restricted or otherwise needs sign-in


class EmptyTranscript(TranscriptError):
    code, retryable = "TRANSCRIPT_EMPTY", False


class MalformedTranscript(TranscriptError):
    code, retryable = "TRANSCRIPT_MALFORMED", False


class TranscriptTooLarge(TranscriptError):
    code, retryable = "TRANSCRIPT_TOO_LARGE", False


class TransientDependency(TranscriptError):
    code, retryable = "TRANSIENT_DEPENDENCY", True           # timeouts, 5xx, network


@dataclass(frozen=True)
class TranscriptTrack:
    language_code: str
    language: str
    is_generated: bool


@dataclass(frozen=True)
class RawSegment:
    start: float
    duration: float
    text: str


class TranscriptProvider(Protocol):
    """Synchronous (callers run it in a worker thread under a timeout)."""

    name: str

    def list_tracks(self, video_id: str) -> list[TranscriptTrack]: ...

    def fetch(self, video_id: str, track: TranscriptTrack) -> list[RawSegment]: ...

    def video_title(self, video_id: str) -> str | None: ...


# Safe, user-facing text per code. Provider/internal detail is never returned to clients.
SAFE_MESSAGES: dict[str, str] = {
    "NO_TRANSCRIPT": "This video has no transcript (captions are off or unavailable).",
    "NO_SUPPORTED_LANGUAGE": "This video has captions, but none in a supported language.",
    "TRACK_DISCOVERY_FAILED": "Could not check which transcripts this video has. Try again shortly.",
    "FETCH_FAILED": "A transcript exists, but it could not be downloaded right now. Try again shortly.",
    "PROVIDER_BLOCKED": "YouTube temporarily blocked the request. Try again later.",
    "VIDEO_UNAVAILABLE": "This video is unavailable (private, removed, or not viewable from here).",
    "VIDEO_RESTRICTED": "This video is restricted (for example age-restricted), so its transcript cannot be read.",
    "TRANSCRIPT_EMPTY": "The transcript is empty.",
    "TRANSCRIPT_MALFORMED": "The transcript could not be understood.",
    "TRANSCRIPT_TOO_LARGE": "This transcript is too long to use.",
    "TRANSIENT_DEPENDENCY": "A service this depends on is temporarily unavailable. Try again shortly.",
    "INTERNAL_ERROR": "Something went wrong while preparing this video. Try again.",
}
