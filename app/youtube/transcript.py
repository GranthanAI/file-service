"""Track selection, validation and the canonical transcript representation (pure functions, no I/O).

The canonical JSON is the durable source of truth (stored in object storage). Chunks and embeddings are derived from it and can
always be rebuilt; they are never the only copy.
"""

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone

from app.youtube.provider import (
    EmptyTranscript,
    MalformedTranscript,
    NoSupportedLanguage,
    NoTranscript,
    RawSegment,
    TranscriptTooLarge,
    TranscriptTrack,
)

SCHEMA = "granthan.transcript.v1"
TRANSCRIPT_CONTENT_TYPE = "application/vnd.granthan.transcript+json"


def _base(code: str) -> str:
    return code.lower().replace("_", "-").split("-")[0]


def _matches(track_code: str, wanted: str) -> bool:
    t, w = track_code.lower().replace("_", "-"), wanted.lower().replace("_", "-")
    return t == w or _base(t) == _base(w) and "-" not in w


def select_track(tracks: list[TranscriptTrack], preferred: list[str], *, fallback_any: bool = True) -> TranscriptTrack:
    """Documented policy, in order:
    1. a manually created track in the first preferred language that has one;
    2. an automatically generated track in a preferred language (same order);
    3. only if `fallback_any`: any manually created track (alphabetical by language code), then any generated one.
    A video with no tracks is NoTranscript; tracks but nothing allowed is NoSupportedLanguage."""
    if not tracks:
        raise NoTranscript()
    for want_generated in (False, True):
        for lang in preferred:
            for t in sorted(tracks, key=lambda x: x.language_code):
                if t.is_generated == want_generated and _matches(t.language_code, lang):
                    return t
    if fallback_any:
        for want_generated in (False, True):
            for t in sorted(tracks, key=lambda x: x.language_code):
                if t.is_generated == want_generated:
                    return t
    raise NoSupportedLanguage()


@dataclass(frozen=True)
class Segment:
    start: float
    duration: float
    text: str


@dataclass(frozen=True)
class ValidatedTranscript:
    segments: list[Segment]
    duration_seconds: float
    char_count: int
    dropped_empty: int


def _clean_text(text: str) -> str:
    # Drop NUL/control characters (they break storage and prompts), keep the words exactly as spoken otherwise.
    return "".join(ch for ch in text if ch == "\n" or ch == "\t" or ord(ch) >= 0x20 and ord(ch) != 0x7F).strip()


def validate_segments(raw: list[RawSegment], *, max_segments: int, max_chars: int, max_duration: float) -> ValidatedTranscript:
    if not raw:
        raise EmptyTranscript()
    if len(raw) > max_segments:
        raise TranscriptTooLarge(f"{len(raw)} segments")
    out: list[Segment] = []
    chars = 0
    dropped = 0
    for r in raw:
        try:
            start, duration = float(r.start), float(r.duration)
        except (TypeError, ValueError):
            raise MalformedTranscript("non-numeric timing") from None
        if not isinstance(r.text, str):
            raise MalformedTranscript("non-text segment")
        if not (math.isfinite(start) and math.isfinite(duration)) or start < 0 or duration < 0:
            raise MalformedTranscript("invalid timing")
        text = _clean_text(r.text)
        if not text:
            dropped += 1
            continue
        chars += len(text)
        if chars > max_chars:
            raise TranscriptTooLarge("too many characters")
        out.append(Segment(round(start, 3), round(duration, 3), text))
    if not out:
        raise EmptyTranscript()
    end = max(s.start + s.duration for s in out)
    if end > max_duration:
        raise TranscriptTooLarge("video too long")
    return ValidatedTranscript(out, round(end, 3), chars, dropped)


def transcript_sha256(video_id: str, track: TranscriptTrack, segments: list[Segment]) -> str:
    """Identity of the transcript *content* (not of when it was fetched): equal content = no new version."""
    body = json.dumps(
        {"v": video_id, "l": track.language_code, "g": track.is_generated, "s": [[s.start, s.duration, s.text] for s in segments]},
        ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def build_canonical(
    *, video_id: str, canonical_url: str, title: str | None, track: TranscriptTrack, segments: list[Segment],
    provider: str, transcript_version: int, fetched_at: datetime | None = None,
) -> bytes:
    """UTF-8 JSON, original segment text and timing preserved."""
    doc = {
        "schema": SCHEMA,
        "video_id": video_id,
        "canonical_url": canonical_url,
        "title": title,
        "language": track.language_code,
        "language_name": track.language,
        "is_generated": track.is_generated,
        "provider": provider,
        "transcript_version": transcript_version,
        "fetched_at": (fetched_at or datetime.now(timezone.utc)).isoformat(),
        "segments": [{"start": s.start, "duration": s.duration, "text": s.text} for s in segments],
    }
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
