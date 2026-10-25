import asyncio
import json
import math

import pytest
import requests

from app.core.config import settings
from app.youtube import service as yt
from app.youtube import transcript as tr
from app.youtube.provider import (
    EmptyTranscript, FetchFailed, MalformedTranscript, NoSupportedLanguage, NoTranscript, ProviderBlocked, RawSegment,
    TrackDiscoveryFailed, TranscriptTooLarge, TranscriptTrack, TransientDependency, VideoRestricted, VideoUnavailable,
)

T = TranscriptTrack
EN, EN_AUTO, EN_US, HI, DE_AUTO = T("en", "English", False), T("en", "English (auto-generated)", True), T("en-US", "English (US)", False), T("hi", "Hindi", False), T("de", "German", True)


# ── track selection ──────────────────────────────────────────────────────────────────────────────────────────────────

def test_manual_beats_generated_in_the_same_language():
    assert tr.select_track([EN_AUTO, EN], ["en"]) == EN


def test_preferred_language_order_is_respected_and_manual_wins_within_each_language_first():
    assert tr.select_track([HI, EN], ["hi", "en"]) == HI
    assert tr.select_track([HI, EN], ["en", "hi"]) == EN


def test_a_manual_track_in_a_later_preferred_language_beats_generated_in_an_earlier_one():
    assert tr.select_track([EN_AUTO, HI], ["en", "hi"]) == HI


def test_regional_variants_match_the_base_language():
    assert tr.select_track([EN_US, DE_AUTO], ["en"]) == EN_US


def test_generated_in_a_preferred_language_is_used_when_no_manual_exists():
    assert tr.select_track([EN_AUTO, DE_AUTO], ["en"]) == EN_AUTO


def test_fallback_to_any_language_prefers_manual_then_generated_and_can_be_disabled():
    assert tr.select_track([DE_AUTO, HI], ["fr"]) == HI
    assert tr.select_track([DE_AUTO], ["fr"]) == DE_AUTO
    with pytest.raises(NoSupportedLanguage):
        tr.select_track([DE_AUTO, HI], ["fr"], fallback_any=False)


def test_no_tracks_means_no_transcript_not_unsupported_language():
    with pytest.raises(NoTranscript):
        tr.select_track([], ["en"])


# ── validation ───────────────────────────────────────────────────────────────────────────────────────────────────────

LIMITS = dict(max_segments=100, max_chars=10_000, max_duration=3600)


def seg(start, dur, text):
    return RawSegment(start, dur, text)


def test_valid_segments_keep_text_order_and_timing_and_compute_duration():
    v = tr.validate_segments([seg(0, 2, "Hello"), seg(2, 3.5, "wörld ✓ 日本")], **LIMITS)
    assert [(s.start, s.duration, s.text) for s in v.segments] == [(0, 2, "Hello"), (2, 3.5, "wörld ✓ 日本")]
    assert v.duration_seconds == 5.5 and v.char_count == len("Hello") + len("wörld ✓ 日本")


def test_empty_and_blank_only_transcripts_are_rejected_as_empty():
    with pytest.raises(EmptyTranscript):
        tr.validate_segments([], **LIMITS)
    with pytest.raises(EmptyTranscript):
        tr.validate_segments([seg(0, 1, "   "), seg(1, 1, "\x00\x01")], **LIMITS)


def test_blank_segments_are_dropped_and_control_characters_removed():
    v = tr.validate_segments([seg(0, 1, "a\x00b"), seg(1, 1, " "), seg(2, 1, "c")], **LIMITS)
    assert [s.text for s in v.segments] == ["ab", "c"] and v.dropped_empty == 1


@pytest.mark.parametrize("bad", [
    seg(float("nan"), 1, "x"), seg(0, float("inf"), "x"), seg(-1, 1, "x"), seg(0, -2, "x"), seg("abc", 1, "x"), seg(0, None, "x"),
    RawSegment(0, 1, 123),  # type: ignore[arg-type]
])
def test_malformed_timing_or_text_is_rejected(bad):
    with pytest.raises(MalformedTranscript):
        tr.validate_segments([bad], **LIMITS)


def test_size_limits_are_enforced():
    with pytest.raises(TranscriptTooLarge):
        tr.validate_segments([seg(i, 1, "x") for i in range(101)], **LIMITS)
    with pytest.raises(TranscriptTooLarge):
        tr.validate_segments([seg(0, 1, "x" * 6000), seg(1, 1, "y" * 6000)], **LIMITS)
    with pytest.raises(TranscriptTooLarge):
        tr.validate_segments([seg(0, 5000, "x")], **LIMITS)


def test_hash_identifies_content_not_fetch_time_and_changes_with_content_or_language():
    a = tr.Segment(0, 1, "hello")
    assert tr.transcript_sha256("v" * 11, EN, [a]) == tr.transcript_sha256("v" * 11, EN, [a])
    assert tr.transcript_sha256("v" * 11, EN, [a]) != tr.transcript_sha256("v" * 11, HI, [a])
    assert tr.transcript_sha256("v" * 11, EN, [a]) != tr.transcript_sha256("v" * 11, EN, [tr.Segment(0, 1, "hello!")])


def test_canonical_json_preserves_source_identity_language_flags_and_original_segments():
    segs = [tr.Segment(0, 1.5, "Hello 😀"), tr.Segment(1.5, 2, "tab\there")]
    doc = json.loads(tr.build_canonical(
        video_id="dQw4w9WgXcQ", canonical_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ", title="Título", track=EN_AUTO,
        segments=segs, provider="p", transcript_version=3))
    assert doc["schema"] == tr.SCHEMA and doc["video_id"] == "dQw4w9WgXcQ" and doc["title"] == "Título"
    assert (doc["language"], doc["is_generated"], doc["transcript_version"], doc["provider"]) == ("en", True, 3, "p")
    assert doc["segments"] == [{"start": 0, "duration": 1.5, "text": "Hello 😀"}, {"start": 1.5, "duration": 2, "text": "tab\there"}]


# ── extraction orchestration with a fake provider (no network) ───────────────────────────────────────────────────────

class FakeProvider:
    name = "fake"

    def __init__(self, tracks=None, segments=None, list_errors=(), fetch_errors=(), title="A title"):
        self.tracks, self.segments, self.title = tracks if tracks is not None else [EN], segments or [RawSegment(0, 1, "hi")], title
        self.list_errors, self.fetch_errors = list(list_errors), list(fetch_errors)
        self.list_calls = self.fetch_calls = 0

    def list_tracks(self, video_id):
        self.list_calls += 1
        if self.list_errors:
            raise self.list_errors.pop(0)
        return self.tracks

    def fetch(self, video_id, track):
        self.fetch_calls += 1
        if self.fetch_errors:
            raise self.fetch_errors.pop(0)
        return self.segments

    def video_title(self, video_id):
        return self.title


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(settings, "YOUTUBE_BACKOFF_BASE_SECONDS", 0.0)
    monkeypatch.setattr(settings, "YOUTUBE_FETCH_ATTEMPTS", 3)


def run(coro):
    return asyncio.run(coro)


def test_extract_happy_path_returns_track_segments_and_title():
    ex = run(yt.extract(FakeProvider(tracks=[EN_AUTO, HI]), "x" * 11))
    assert ex.track == EN_AUTO and ex.title == "A title" and ex.validated.segments[0].text == "hi"


def test_transient_failures_are_retried_with_a_bound_and_then_succeed():
    p = FakeProvider(fetch_errors=[TransientDependency(), FetchFailed()])
    assert run(yt.extract(p, "x" * 11)).validated.segments
    assert p.fetch_calls == 3


def test_retries_stop_at_the_attempt_limit_and_surface_the_error():
    p = FakeProvider(list_errors=[TransientDependency()] * 5)
    with pytest.raises(TransientDependency):
        run(yt.extract(p, "x" * 11))
    assert p.list_calls == 3


@pytest.mark.parametrize("exc", [NoTranscript(), VideoUnavailable(), VideoRestricted(), EmptyTranscript()])
def test_permanent_failures_are_not_retried(exc):
    p = FakeProvider(list_errors=[exc, exc, exc])
    with pytest.raises(type(exc)):
        run(yt.extract(p, "x" * 11))
    assert p.list_calls == 1


def test_a_blocked_request_is_not_hammered_in_process():
    p = FakeProvider(list_errors=[ProviderBlocked(), ProviderBlocked()])
    with pytest.raises(ProviderBlocked) as e:
        run(yt.extract(p, "x" * 11))
    assert p.list_calls == 1 and e.value.retryable


def test_unsupported_language_and_empty_tracks_are_distinct_outcomes(monkeypatch):
    monkeypatch.setattr(settings, "YOUTUBE_FALLBACK_ANY_LANGUAGE", False)
    with pytest.raises(NoSupportedLanguage):
        run(yt.extract(FakeProvider(tracks=[HI]), "x" * 11))
    with pytest.raises(NoTranscript):
        run(yt.extract(FakeProvider(tracks=[]), "x" * 11))


def test_an_empty_or_malformed_fetched_transcript_is_a_permanent_failure_not_a_success():
    with pytest.raises(EmptyTranscript):
        run(yt.extract(FakeProvider(segments=[RawSegment(0, 1, "  ")]), "x" * 11))
    with pytest.raises(MalformedTranscript):
        run(yt.extract(FakeProvider(segments=[RawSegment(math.nan, 1, "x")]), "x" * 11))


def test_a_title_failure_never_fails_the_extraction():
    class P(FakeProvider):
        def video_title(self, video_id):
            raise TransientDependency()

    assert run(yt.extract(P(), "x" * 11)).title is None


def test_every_code_has_a_safe_user_message_and_failures_distinguish_the_required_conditions():
    from app.youtube.provider import SAFE_MESSAGES

    needed = {"NO_TRANSCRIPT", "NO_SUPPORTED_LANGUAGE", "TRACK_DISCOVERY_FAILED", "FETCH_FAILED", "PROVIDER_BLOCKED",
              "VIDEO_UNAVAILABLE", "TRANSCRIPT_EMPTY", "TRANSCRIPT_MALFORMED", "TRANSIENT_DEPENDENCY"}
    assert needed <= set(SAFE_MESSAGES)
    assert len({SAFE_MESSAGES[c] for c in needed}) == len(needed)          # each condition reads differently to the user


def test_display_filename_is_safe():
    assert yt.display_filename("../../a/b\\c:d*?", "dQw4w9WgXcQ").count("/") == 0
    assert "\\" not in yt.display_filename("a\\b", "dQw4w9WgXcQ")
    assert yt.display_filename(None, "dQw4w9WgXcQ") == "YouTube dQw4w9WgXcQ transcript.json"
    assert len(yt.display_filename("x" * 500, "dQw4w9WgXcQ")) < 200


# ── library exception mapping ────────────────────────────────────────────────────────────────────────────────────────

def test_library_exceptions_map_to_the_taxonomy():
    import youtube_transcript_api as y
    from app.youtube.yta_provider import _map

    def make(cls):
        try:
            return cls("abc")
        except TypeError:
            return cls.__new__(cls)

    cases = {
        y.TranscriptsDisabled: "NO_TRANSCRIPT", y.NoTranscriptFound: "NO_TRANSCRIPT", y.VideoUnavailable: "VIDEO_UNAVAILABLE",
        y.VideoUnplayable: "VIDEO_UNAVAILABLE", y.InvalidVideoId: "VIDEO_UNAVAILABLE", y.AgeRestricted: "VIDEO_RESTRICTED",
        y.RequestBlocked: "PROVIDER_BLOCKED", y.IpBlocked: "PROVIDER_BLOCKED", y.PoTokenRequired: "PROVIDER_BLOCKED",
        y.YouTubeRequestFailed: "TRANSIENT_DEPENDENCY",
    }
    for cls, code in cases.items():
        assert _map(make(cls), "list").code == code, cls.__name__
    assert _map(requests.Timeout(), "fetch").code == "TRANSIENT_DEPENDENCY"
    assert _map(requests.ConnectionError(), "fetch").code == "TRANSIENT_DEPENDENCY"
    assert _map(RuntimeError("boom"), "list").code == "TRACK_DISCOVERY_FAILED"
    assert _map(RuntimeError("boom"), "fetch").code == "FETCH_FAILED"
    assert _map(RuntimeError("boom"), "fetch").retryable                    # unknown failures are retryable, never a success
