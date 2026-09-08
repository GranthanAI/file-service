"""docs/LLD.md §4.2/§4.5/§6.7 regression guard: `processing_status_consumer.py`
is the ONLY code path allowed to drive UPLOADED -> PROCESSING and
PROCESSING -> READY. A source-text check on the exact modules the LLD calls
out (upload_service.py, quarantine_service.py — Phase 6 exit criteria;
retry_service.py — Phase 7) rather than a behavioral test, because the point
is that these modules must never even ATTEMPT the edge, not just that their
current happy-path doesn't happen to hit it.
"""

import inspect

from app.services import quarantine_service, retry_service, upload_service


class TestNoDirectProcessingOrReadyTransitions:
    def test_upload_service_never_targets_processing_or_ready(self) -> None:
        source = inspect.getsource(upload_service)
        assert "FileStatus.PROCESSING" not in source
        assert "FileStatus.READY" not in source

    def test_quarantine_service_never_targets_processing_or_ready(self) -> None:
        """docs/LLD.md Phase 6 exit criteria, stated explicitly: quarantine_service
        never calls transition(SCANNING, PROCESSING) directly — the clean
        path lands on UPLOADED (see test_quarantine_service.py's
        test_lands_on_uploaded_never_processing for the behavioral proof)."""
        source = inspect.getsource(quarantine_service)
        assert "FileStatus.PROCESSING" not in source
        assert "FileStatus.READY" not in source

    def test_retry_service_never_targets_ready(self) -> None:
        """retry_service legitimately targets PROCESSING (FAILED ->
        PROCESSING, docs/LLD.md §4.4) — that's a different edge, not the one
        this regression guards — but it must never set READY itself
        (docs/LLD.md §3.6's closing note: retry never sets READY)."""
        source = inspect.getsource(retry_service)
        assert "FileStatus.READY" not in source
