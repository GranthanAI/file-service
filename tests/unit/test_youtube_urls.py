import pytest

from app.youtube.urls import InvalidYouTubeUrl, parse_youtube_url

VID = "dQw4w9WgXcQ"
CANON = f"https://www.youtube.com/watch?v={VID}"


@pytest.mark.parametrize("url", [
    f"https://www.youtube.com/watch?v={VID}",
    f"https://youtube.com/watch?v={VID}",
    f"https://m.youtube.com/watch?v={VID}&t=42s",
    f"https://www.youtube.com/watch?feature=share&v={VID}",
    f"https://youtu.be/{VID}",
    f"https://youtu.be/{VID}?si=abc",
    f"https://www.youtube.com/shorts/{VID}",
    f"https://www.youtube.com/embed/{VID}",
    f"https://www.youtube.com/live/{VID}",
    f"  https://www.youtube.com/watch?v={VID}  ",
    f"HTTPS://WWW.YOUTUBE.COM/watch?v={VID}",
    f"https://www.youtube.com/watch?v={VID}#t=10",
])
def test_accepted_formats_reduce_to_the_canonical_video(url):
    p = parse_youtube_url(url)
    assert (p.video_id, p.canonical_url) == (VID, CANON)


@pytest.mark.parametrize("url,reason", [
    (f"http://www.youtube.com/watch?v={VID}", "unsupported_scheme"),
    (f"ftp://www.youtube.com/watch?v={VID}", "unsupported_scheme"),
    (f"javascript:alert('youtube.com/watch?v={VID}')", "unsupported_scheme"),
    (f"//www.youtube.com/watch?v={VID}", "unsupported_scheme"),
    (f"https://evil.com/watch?v={VID}&u=youtube.com", "unsupported_host"),
    (f"https://youtube.com@evil.com/watch?v={VID}", "unsupported_host"),
    (f"https://user:pw@www.youtube.com/watch?v={VID}", "unsupported_host"),
    (f"https://www.youtube.com.evil.com/watch?v={VID}", "unsupported_host"),
    (f"https://evilyoutube.com/watch?v={VID}", "unsupported_host"),
    (f"https://notyoutu.be/{VID}", "unsupported_host"),
    (f"https://www.youtube.com:8443/watch?v={VID}", "unsupported_host"),
    (f"https://127.0.0.1/watch?v={VID}", "unsupported_host"),
    (f"https://localhost/watch?v={VID}", "unsupported_host"),
    (f"https://[::1]/watch?v={VID}", "unsupported_host"),
    (f"https://www.youtube.com/watch?v={VID[:-1]}", "no_video_id"),
    (f"https://www.youtube.com/watch?v={VID}x", "no_video_id"),
    ("https://www.youtube.com/watch?v=../../etc/passwd", "no_video_id"),
    ("https://www.youtube.com/watch", "no_video_id"),
    ("https://www.youtube.com/", "no_video_id"),
    ("https://youtu.be/", "no_video_id"),
    (f"https://youtu.be/{VID}/extra", "no_video_id"),
    ("https://www.youtube.com/playlist?list=PL123", "not_a_video"),
    ("https://www.youtube.com/channel/UC123", "not_a_video"),
    ("https://www.youtube.com/results?search_query=x", "not_a_video"),
    (f"https://www.youtube.com\\@evil.com/watch?v={VID}", "malformed"),
    (f"https://www.youtube.com/watch?v={VID}\n", None),   # trailing whitespace is stripped -> valid
    (f"https://www.youtube.com/wat ch?v={VID}", "malformed"),
    (f"https://www.youtube.com/watch?v={VID}\x00", "malformed"),
    ("", "malformed"),
    ("   ", "malformed"),
    ("not a url", "malformed"),
    ("https://www.youtube.com:abc/watch?v=" + VID, "malformed"),
    ("https://www.youtube.com/watch?v=" + VID + "&" + "a" * 3000, "malformed"),
])
def test_everything_else_is_rejected_with_a_stable_reason(url, reason):
    if reason is None:
        assert parse_youtube_url(url).video_id == VID
        return
    with pytest.raises(InvalidYouTubeUrl) as e:
        parse_youtube_url(url)
    assert e.value.reason == reason
    assert e.value.message


def test_shorts_can_be_disabled_by_policy():
    with pytest.raises(InvalidYouTubeUrl) as e:
        parse_youtube_url(f"https://www.youtube.com/shorts/{VID}", allow_shorts=False)
    assert e.value.reason == "shorts_not_supported"
    assert parse_youtube_url(f"https://youtu.be/{VID}", allow_shorts=False).video_id == VID


def test_non_string_input_is_rejected_not_crashing():
    with pytest.raises(InvalidYouTubeUrl):
        parse_youtube_url(None)  # type: ignore[arg-type]


def test_the_video_id_is_the_only_thing_carried_forward():
    p = parse_youtube_url(f"https://www.youtube.com/watch?v={VID}&list=PL1&index=3&foo=<script>")
    assert p.canonical_url == CANON and "script" not in p.canonical_url
