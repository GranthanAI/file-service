"""YouTube transcript sources against real PostgreSQL (real Alembic chain) and real MinIO. The transcript provider is a fake:
nothing here touches YouTube."""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import jwt
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.api.routers import youtube as youtube_router
from app.core.config import settings
from app.core.enums import FileStatus
from app.main import app
from app.models.file import File
from app.models.outbox import OutboxEvent
from app.models.youtube_source import YoutubeSource
from app.repositories.file_repository import FileRepository
from app.youtube import service as yt
from app.youtube.provider import (
    FetchFailed, NoTranscript, ProviderBlocked, RawSegment, TranscriptTrack, TransientDependency, VideoUnavailable,
)
from app.youtube.transcript import TRANSCRIPT_CONTENT_TYPE

VID = "dQw4w9WgXcQ"
URL = f"https://www.youtube.com/watch?v={VID}"
SHORT_URL = f"https://youtu.be/{VID}"
EN = TranscriptTrack("en", "English", False)


class FakeProvider:
    name = "fake"

    def __init__(self):
        self.tracks = [EN]
        self.segments = [RawSegment(0, 2, "Welcome to the show"), RawSegment(2, 3, "Today: gradient descent, 😀"), RawSegment(65, 4, "That is all")]
        self.errors: list[Exception] = []
        self.list_calls = 0
        self.title = "Gradient Descent Explained"
        self.delay = 0.0

    def list_tracks(self, video_id):
        self.list_calls += 1
        if self.delay:
            import time
            time.sleep(self.delay)
        if self.errors:
            raise self.errors.pop(0)
        return self.tracks

    def fetch(self, video_id, track):
        return self.segments

    def video_title(self, video_id):
        return self.title


@pytest.fixture
def provider():
    return FakeProvider()


@pytest.fixture
def client(app_client, provider, migrated_database_url, monkeypatch):
    """The shared app_client plus: a fake provider, and background extraction using the test database."""
    # NullPool: connections are never reused across the TestClient's event loop and the test's own loop.
    engine = create_async_engine(migrated_database_url, poolclass=NullPool)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(youtube_router, "get_session_factory", lambda: factory)
    monkeypatch.setattr(settings, "YOUTUBE_BACKOFF_BASE_SECONDS", 0.0)
    app.dependency_overrides[youtube_router.get_transcript_provider] = lambda: provider
    app_client.test_factory = factory
    yield app_client
    app.dependency_overrides.pop(youtube_router.get_transcript_provider, None)


def headers_for(user: UUID) -> dict[str, str]:
    token = jwt.encode({"sub": str(user), "exp": datetime.now(timezone.utc) + timedelta(minutes=15)}, settings.JWT_SECRET_KEY,
                       algorithm=settings.JWT_ALGORITHM)
    return {"Authorization": f"Bearer {token}"}


async def source_row(factory, doc) -> YoutubeSource:
    async with factory() as s:
        return (await s.execute(select(YoutubeSource).where(YoutubeSource.document_id == UUID(str(doc))))).scalar_one()


async def file_row(factory, doc) -> File:
    async with factory() as s:
        return await FileRepository.get(s, UUID(str(doc)))


async def outbox(factory, doc, event_type=None) -> list[OutboxEvent]:
    async with factory() as s:
        q = select(OutboxEvent).where(OutboxEvent.aggregate_id == UUID(str(doc)))
        if event_type:
            q = q.where(OutboxEvent.event_type == event_type)
        return list((await s.execute(q)).scalars().all())


async def set_file_status(factory, doc, *targets: FileStatus):
    async with factory() as s:
        f = await FileRepository.get(s, UUID(str(doc)))
        for t in targets:
            await FileRepository.update_status(s, f, t)
        await s.commit()


# ───────────────────────────────────────────────────────────────────────────────────────────── attach and ingest

class TestAttachAndIngest:
    async def test_attach_preserves_the_canonical_transcript_and_is_only_ready_after_indexing(self, client, provider, auth_headers, minio_client):
        r = client.post("/files/youtube", json={"url": URL}, headers=auth_headers)
        assert r.status_code == 202, r.text
        doc = r.json()["document_id"]
        assert r.json()["created"] is True and r.json()["video_id"] == VID

        got = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
        assert got["extraction_status"] == "FETCHED"
        assert got["title"] == "Gradient Descent Explained" and got["language"] == "en" and got["is_generated"] is False
        assert got["segment_count"] == 3 and got["duration_seconds"] == 69
        assert got["indexing_status"] == "UPLOADED"
        assert got["ready"] is False            # transcript preserved, but NOT usable until retrieval has indexed it

        f = await file_row(client.test_factory, doc)
        assert f.status == FileStatus.UPLOADED and f.content_type == TRANSCRIPT_CONTENT_TYPE
        assert f.filename == "Gradient Descent Explained (YouTube transcript).json"
        stored = json.loads(minio_client.get_object(f.storage_bucket, f.storage_key).read())
        assert stored["video_id"] == VID and stored["language"] == "en" and stored["is_generated"] is False
        assert stored["segments"] == [{"start": 0, "duration": 2, "text": "Welcome to the show"},
                                      {"start": 2, "duration": 3, "text": "Today: gradient descent, 😀"},
                                      {"start": 65, "duration": 4, "text": "That is all"}]
        events = await outbox(client.test_factory, doc, "file.uploaded")
        assert len(events) == 1
        payload = events[0].payload
        assert payload["content_type"] == TRANSCRIPT_CONTENT_TYPE and payload["storage"]["key"] == f.storage_key
        assert payload["owner_id"] == str(f.owner_id) and payload["content_version"] == 1

        # the existing pipeline (retrieval) reports completion: only now is it usable
        await set_file_status(client.test_factory, doc, FileStatus.PROCESSING, FileStatus.READY)
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).json()["ready"] is True

    async def test_failed_indexing_is_not_reported_as_ready(self, client, auth_headers):
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        await set_file_status(client.test_factory, doc, FileStatus.PROCESSING, FileStatus.FAILED)
        got = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
        assert got["ready"] is False and got["error_code"] and got["message"]

    async def test_repeated_submission_in_any_format_returns_the_same_document_and_fetches_once(self, client, provider, auth_headers):
        first = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        again = client.post("/files/youtube", json={"url": SHORT_URL}, headers=auth_headers).json()
        assert again["document_id"] == first["document_id"] and again["created"] is False
        assert provider.list_calls == 1
        assert len(await outbox(client.test_factory, first["document_id"], "file.uploaded")) == 1

    async def test_concurrent_attachment_creates_exactly_one_source(self, client, provider, migrated_database_url, current_user_id):
        engine = create_async_engine(migrated_database_url, poolclass=NullPool)
        factory = async_sessionmaker(bind=engine, expire_on_commit=False)

        async def one():
            async with factory() as s:
                try:
                    r = await yt.attach(s, owner_id=current_user_id, conversation_id=None, url=URL)
                    await s.commit()
                    return r.source.document_id
                except Exception:
                    await s.rollback()
                    raise

        ids = await asyncio.gather(*[one() for _ in range(8)])
        assert len(set(ids)) == 1
        async with factory() as s:
            n = (await s.execute(text("SELECT count(*) FROM youtube_sources WHERE owner_id = :o AND video_id = :v"),
                                 {"o": current_user_id, "v": VID})).scalar_one()
        assert n == 1
        await engine.dispose()

    async def test_the_same_video_is_a_separate_source_per_user_and_one_user_cannot_touch_anothers(self, client, auth_headers):
        mine = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        other = headers_for(uuid4())
        theirs = client.post("/files/youtube", json={"url": URL}, headers=other).json()["document_id"]
        assert mine != theirs
        assert client.get(f"/files/youtube/{mine}", headers=other).status_code == 404
        assert client.post(f"/files/youtube/{mine}/retry", headers=other).status_code == 404
        assert client.delete(f"/files/youtube/{mine}", headers=other).status_code == 404
        assert client.get(f"/files/youtube/{uuid4()}", headers=auth_headers).status_code == 404
        assert client.get(f"/files/youtube/{mine}", headers=auth_headers).status_code == 200

    async def test_requires_authentication(self, client):
        assert client.post("/files/youtube", json={"url": URL}).status_code in (401, 403)
        assert client.get(f"/files/youtube/{uuid4()}").status_code in (401, 403)

    async def test_owner_comes_from_the_token_and_unknown_body_fields_are_rejected(self, client, auth_headers, current_user_id):
        r = client.post("/files/youtube", json={"url": URL, "owner_id": str(uuid4())}, headers=auth_headers)
        assert r.status_code == 422
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        assert (await file_row(client.test_factory, doc)).owner_id == current_user_id


# ───────────────────────────────────────────────────────────────────────────────────────────── validation and quota

class TestValidationAndQuota:
    @pytest.mark.parametrize("url", [
        "http://www.youtube.com/watch?v=" + VID, "https://evil.com/watch?v=" + VID, "https://youtube.com@evil.com/watch?v=" + VID,
        "https://www.youtube.com/playlist?list=PL1", "javascript:alert(1)", "https://127.0.0.1/watch?v=" + VID,
    ])
    async def test_invalid_urls_are_422_and_never_reach_the_provider(self, client, provider, auth_headers, url):
        r = client.post("/files/youtube", json={"url": url}, headers=auth_headers)
        assert r.status_code == 422 and r.json()["error_code"] == "InvalidYouTubeUrl" and r.json()["reason"]
        assert provider.list_calls == 0

    async def test_quota_is_enforced_per_user(self, client, auth_headers, monkeypatch):
        monkeypatch.setattr(settings, "YOUTUBE_USER_HOURLY_QUOTA", 2)
        ids = ["aaaaaaaaaa1", "aaaaaaaaaa2", "aaaaaaaaaa3"]
        codes = [client.post("/files/youtube", json={"url": f"https://youtu.be/{i}"}, headers=auth_headers).status_code for i in ids]
        assert codes == [202, 202, 429]
        assert client.post("/files/youtube", json={"url": f"https://youtu.be/{ids[0]}"}, headers=auth_headers).status_code == 202  # existing: no new quota
        assert client.post("/files/youtube", json={"url": URL}, headers=headers_for(uuid4())).status_code == 202               # other user unaffected

    async def test_can_be_disabled(self, client, auth_headers, monkeypatch):
        monkeypatch.setattr(settings, "YOUTUBE_ENABLED", False)
        assert client.post("/files/youtube", json={"url": URL}, headers=auth_headers).status_code == 503


# ───────────────────────────────────────────────────────────────────────────────────────────── failures

class TestFailures:
    @pytest.mark.parametrize("error,code,message_part", [
        (NoTranscript(), "NO_TRANSCRIPT", "no transcript"),
        (VideoUnavailable(), "VIDEO_UNAVAILABLE", "unavailable"),
    ])
    async def test_permanent_failures_are_reported_truthfully_and_create_no_content(self, client, provider, auth_headers, minio_client, error, code, message_part):
        provider.errors = [error]
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        got = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
        assert got["extraction_status"] == "FAILED_PERMANENT" and got["error_code"] == code and message_part in got["message"].lower()
        assert got["ready"] is False and got["retryable"] is False
        f = await file_row(client.test_factory, doc)
        assert f.status == FileStatus.FAILED
        assert await outbox(client.test_factory, doc, "file.uploaded") == []          # never offered for indexing
        assert list(minio_client.list_objects(f.storage_bucket, prefix=f.storage_key)) == []

    async def test_no_transcript_is_distinguished_from_a_blocked_request_and_a_transient_outage(self, client, provider, auth_headers):
        out = {}
        for vid, err in (("bbbbbbbbbb1", NoTranscript()), ("bbbbbbbbbb2", ProviderBlocked()), ("bbbbbbbbbb3", TransientDependency()),
                         ("bbbbbbbbbb4", FetchFailed())):
            provider.errors = [err] * 5
            doc = client.post("/files/youtube", json={"url": f"https://youtu.be/{vid}"}, headers=auth_headers).json()["document_id"]
            g = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
            out[vid] = (g["extraction_status"], g["error_code"], g["retryable"], g["message"])
        assert out["bbbbbbbbbb1"][:3] == ("FAILED_PERMANENT", "NO_TRANSCRIPT", False)
        assert out["bbbbbbbbbb2"][:3] == ("FAILED_RETRYABLE", "PROVIDER_BLOCKED", True)
        assert out["bbbbbbbbbb3"][:3] == ("FAILED_RETRYABLE", "TRANSIENT_DEPENDENCY", True)
        assert out["bbbbbbbbbb4"][:3] == ("FAILED_RETRYABLE", "FETCH_FAILED", True)
        assert len({v[3] for v in out.values()}) == 4

    async def test_retryable_failure_keeps_the_source_pending_work_and_retry_respects_the_cooldown(self, client, provider, auth_headers):
        provider.errors = [TransientDependency()] * 10
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        g = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
        assert g["extraction_status"] == "FAILED_RETRYABLE" and g["retry_after_seconds"] and g["ready"] is False
        assert (await file_row(client.test_factory, doc)).status == FileStatus.UPLOADING     # nothing was stored
        calls = provider.list_calls
        client.post(f"/files/youtube/{doc}/retry", headers=auth_headers)                      # too early: no new attempt
        client.post("/files/youtube", json={"url": URL}, headers=auth_headers)                # resubmission neither
        assert provider.list_calls == calls
        async with client.test_factory() as s:                                                # cooldown elapses
            await s.execute(text("UPDATE youtube_sources SET next_retry_at = now() - interval '1 second' WHERE document_id = :d"), {"d": UUID(doc)})
            await s.commit()
        provider.errors = []
        r = client.post(f"/files/youtube/{doc}/retry", headers=auth_headers)
        assert r.status_code == 202
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).json()["extraction_status"] == "FETCHED"

    async def test_empty_and_malformed_transcripts_never_become_ready(self, client, provider, auth_headers):
        provider.segments = [RawSegment(0, 1, "   ")]
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        g = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()
        assert g["extraction_status"] == "FAILED_PERMANENT" and g["error_code"] == "TRANSCRIPT_EMPTY"
        assert await outbox(client.test_factory, doc, "file.uploaded") == []

    async def test_a_permanently_failed_source_can_be_replaced_after_its_cooldown(self, client, provider, auth_headers):
        provider.errors = [NoTranscript()]
        old = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        same = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        assert same["document_id"] == old and same["extraction_status"] == "FAILED_PERMANENT"      # cooldown: no hammering
        async with client.test_factory() as s:
            await s.execute(text("UPDATE youtube_sources SET next_retry_at = now() - interval '1 second' WHERE document_id = :d"), {"d": UUID(old)})
            await s.commit()
        new = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        assert new["document_id"] != old and new["created"] is True      # (POST answers before the background fetch runs)
        assert client.get(f"/files/youtube/{new['document_id']}", headers=auth_headers).json()["extraction_status"] == "FETCHED"
        assert client.get(f"/files/youtube/{old}", headers=auth_headers).status_code == 404

    async def test_a_worker_that_died_mid_fetch_is_taken_over_when_its_lease_expires(self, client, provider, auth_headers):
        r = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        doc = r["document_id"]
        async with client.test_factory() as s:        # simulate: stuck FETCHING with an expired lease and no result stored
            await s.execute(text("UPDATE files SET status='UPLOADING', etag=NULL WHERE document_id=:d"), {"d": UUID(doc)})
            await s.execute(text("UPDATE youtube_sources SET extraction_status='FETCHING', lease_until=now() - interval '1 minute', fetched_at=NULL WHERE document_id=:d"), {"d": UUID(doc)})
            await s.execute(text("DELETE FROM outbox_events WHERE aggregate_id=:d"), {"d": UUID(doc)})
            await s.execute(text("DELETE FROM file_versions WHERE document_id=:d"), {"d": UUID(doc)})
            await s.commit()
        got = client.get(f"/files/youtube/{doc}", headers=auth_headers).json()      # polling revives it
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).json()["extraction_status"] == "FETCHED" or got["extraction_status"] == "FETCHED"

    async def test_two_workers_never_both_fetch(self, client, provider, migrated_database_url, minio_client, current_user_id):
        engine = create_async_engine(migrated_database_url, poolclass=NullPool)
        factory = async_sessionmaker(bind=engine, expire_on_commit=False)
        async with factory() as s:
            r = await yt.attach(s, owner_id=current_user_id, conversation_id=None, url=URL)
            await s.commit()
            doc = r.source.document_id
        provider.delay = 0.3
        await asyncio.gather(*[yt.run_extraction(factory, minio_client, provider, doc) for _ in range(5)])
        assert provider.list_calls == 1
        assert len(await outbox(factory, doc, "file.uploaded")) == 1
        await engine.dispose()


# ───────────────────────────────────────────────────────────────────────────────────────────── removal and retention

class TestRemoval:
    async def test_removing_a_ready_source_deletes_object_and_announces_deletion_then_it_can_be_attached_again(self, client, auth_headers, minio_client):
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        await set_file_status(client.test_factory, doc, FileStatus.PROCESSING, FileStatus.READY)
        f = await file_row(client.test_factory, doc)
        assert client.delete(f"/files/youtube/{doc}", headers=auth_headers).status_code == 204
        assert (await file_row(client.test_factory, doc)).status == FileStatus.DELETED
        assert len(await outbox(client.test_factory, doc, "file.deleted")) == 1          # retrieval removes the chunks on this event
        assert list(minio_client.list_objects(f.storage_bucket, prefix=f.storage_key, include_version=False)) == []
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).status_code == 404
        assert client.delete(f"/files/youtube/{doc}", headers=auth_headers).status_code == 404
        again = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        assert again["document_id"] != doc and again["created"] is True

    async def test_removing_while_indexing_is_refused_like_documents(self, client, auth_headers):
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]      # UPLOADED: indexing pending
        assert client.delete(f"/files/youtube/{doc}", headers=auth_headers).status_code == 409
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).status_code == 200                   # still there, unchanged

    async def test_removing_a_failed_or_pending_source_works(self, client, provider, auth_headers):
        provider.errors = [TransientDependency()] * 10
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        assert client.delete(f"/files/youtube/{doc}", headers=auth_headers).status_code == 204
        assert client.get(f"/files/youtube/{doc}", headers=auth_headers).status_code == 404

    async def test_deleting_through_the_ordinary_files_api_frees_the_video_for_reattachment(self, client, auth_headers):
        doc = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()["document_id"]
        await set_file_status(client.test_factory, doc, FileStatus.PROCESSING, FileStatus.READY)
        assert client.delete(f"/files/{doc}", headers=auth_headers).status_code == 202            # the existing delete API's own contract
        again = client.post("/files/youtube", json={"url": URL}, headers=auth_headers).json()
        assert again["document_id"] != doc and again["created"] is True


# ───────────────────────────────────────────────────────────────────────────────────────────── existing behaviour untouched

class TestExistingFilesUnaffected:
    async def test_ordinary_file_routes_still_resolve_and_youtube_is_not_mistaken_for_a_document_id(self, client, auth_headers):
        assert client.get("/files/youtube", headers=auth_headers).status_code == 422             # no collection GET: falls to /files/{document_id} (non-UUID)
        assert client.get(f"/files/youtube/{uuid4()}", headers=auth_headers).status_code == 404  # but the youtube routes win for real ids
        assert client.get(f"/files/{uuid4()}/status", headers=auth_headers).status_code == 404
        assert client.get("/files", headers=auth_headers).status_code == 200

    async def test_youtube_content_type_cannot_be_uploaded_by_users(self, client, auth_headers):
        r = client.post("/files/upload/initiate", json={"filename": "x.json", "content_type": TRANSCRIPT_CONTENT_TYPE, "size_bytes": 10}, headers=auth_headers)
        assert r.status_code in (400, 404, 422)


# ───────────────────────────────────────────────────────────────────────────────────────────── migration

def test_migration_up_down_up_with_existing_files(database_url):
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    admin = database_url
    target = database_url.rsplit("/", 1)[0] + "/yt_migration"
    cfg = Config(os.path.join(root, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(root, "app", "db", "migrations"))
    cfg.set_main_option("sqlalchemy.url", target)

    async def sql(url, stmt, params=None, fetch=False, autocommit=False):
        eng = create_async_engine(url, isolation_level="AUTOCOMMIT" if autocommit else None) if autocommit else create_async_engine(url)
        try:
            async with (eng.connect() if autocommit else eng.begin()) as c:
                res = await c.execute(text(stmt), params or {})
                return res.all() if fetch else None
        finally:
            await eng.dispose()

    asyncio.run(sql(admin, "DROP DATABASE IF EXISTS yt_migration", autocommit=True))
    asyncio.run(sql(admin, "CREATE DATABASE yt_migration", autocommit=True))
    command.upgrade(cfg, "17fe3249e29f")
    doc = uuid4()
    asyncio.run(sql(target, "INSERT INTO files (document_id, owner_id, filename, content_type, size_bytes, storage_bucket, storage_key) "
                            "VALUES (:d, :o, 'a.pdf', 'application/pdf', 1, 'b', 'k')", {"d": doc, "o": uuid4()}))
    command.upgrade(cfg, "head")
    assert asyncio.run(sql(target, "SELECT count(*) FROM files", fetch=True))[0][0] == 1          # existing rows untouched
    asyncio.run(sql(target, "INSERT INTO youtube_sources (document_id, owner_id, video_id, canonical_url) VALUES (:d, :o, 'dQw4w9WgXcQ', 'u')",
                    {"d": doc, "o": uuid4()}))
    command.downgrade(cfg, "17fe3249e29f")
    tables = {r[0] for r in asyncio.run(sql(target, "SELECT tablename FROM pg_tables WHERE schemaname='public'", fetch=True))}
    assert "youtube_sources" not in tables and "files" in tables
    assert asyncio.run(sql(target, "SELECT count(*) FROM files", fetch=True))[0][0] == 1
    command.upgrade(cfg, "head")
    assert "youtube_sources" in {r[0] for r in asyncio.run(sql(target, "SELECT tablename FROM pg_tables WHERE schemaname='public'", fetch=True))}
