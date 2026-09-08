from unittest.mock import MagicMock, patch

from app.services.av_scanner import ClamdScanner, NoOpScanner, get_scanner


class TestNoOpScanner:
    async def test_always_reports_clean(self) -> None:
        result = await NoOpScanner().scan(b"anything, including EICAR bytes")
        assert result.clean is True
        assert result.signature is None


class TestClamdScanner:
    async def test_ok_status_is_clean(self) -> None:
        fake_client = MagicMock()
        fake_client.instream.return_value = {"stream": ("OK", None)}

        with patch("clamd.ClamdNetworkSocket", return_value=fake_client):
            result = await ClamdScanner("localhost", 3310).scan(b"clean bytes")

        assert result.clean is True
        assert result.signature is None

    async def test_found_status_is_infected_with_signature(self) -> None:
        fake_client = MagicMock()
        fake_client.instream.return_value = {"stream": ("FOUND", "Eicar-Signature")}

        with patch("clamd.ClamdNetworkSocket", return_value=fake_client):
            result = await ClamdScanner("localhost", 3310).scan(b"eicar bytes")

        assert result.clean is False
        assert result.signature == "Eicar-Signature"


class TestGetScanner:
    def test_returns_noop_by_default(self) -> None:
        with patch("app.services.av_scanner.settings.CLAMAV_ENABLED", False):
            assert isinstance(get_scanner(), NoOpScanner)

    def test_returns_clamd_scanner_when_enabled(self) -> None:
        with patch("app.services.av_scanner.settings.CLAMAV_ENABLED", True):
            assert isinstance(get_scanner(), ClamdScanner)
