"""Phase 10 exit-criteria test (docs/IMPLEMENTATION_PLAN.md): /metrics is
scrapeable and shows non-zero counters after driving the upload flow (the
same round trip Phase 3's own integration tests exercise) through the real
HTTP API.
"""

import httpx

CONTENT = b"%PDF-1.4 fake pdf content for the metrics exit-criteria test"


async def test_metrics_endpoint_shows_nonzero_counters_after_an_upload(app_client, auth_headers) -> None:
    initiate_resp = app_client.post(
        "/files/upload/initiate",
        json={"filename": "physics.pdf", "content_type": "application/pdf", "size_bytes": len(CONTENT)},
        headers=auth_headers,
    )
    assert initiate_resp.status_code == 201, initiate_resp.text
    initiated = initiate_resp.json()

    upload_resp = httpx.post(
        initiated["upload_url"], data=initiated["form_data"], files={"file": ("original.pdf", CONTENT)}
    )
    assert upload_resp.status_code in (200, 204), upload_resp.text

    complete_resp = app_client.post(f"/files/{initiated['document_id']}/complete", headers=auth_headers)
    assert complete_resp.status_code == 200, complete_resp.text

    metrics_resp = app_client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "text/plain" in metrics_resp.headers["content-type"]

    body = metrics_resp.text
    assert "upload_requests_total" in body
    assert "file_size_bytes" in body
    assert "processing_status_count" in body
    # prometheus_client auto-appends "_total" to a Counter's exposed name
    # when the name doesn't already end with it (docs/HLD.md §21 names this
    # metric "processing_status_count", without the suffix) — the actual
    # scraped series name is processing_status_count_total.
    assert 'processing_status_count_total{status="UPLOADED"}' in body

    # A non-zero counter: at least one upload_requests_total series with a
    # value >= 1 (this test alone drove at least one /upload/initiate call).
    upload_requests_lines = [
        line for line in body.splitlines() if line.startswith("upload_requests_total") and not line.startswith("#")
    ]
    assert upload_requests_lines
    assert any(float(line.rsplit(" ", 1)[-1]) >= 1 for line in upload_requests_lines)
