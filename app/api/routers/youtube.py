"""YouTube transcript sources: POST /files/youtube, GET/DELETE /files/youtube/{document_id}, POST .../retry.

Mounted under /files so the API gateway's existing /files/** proxy and JWT handling apply unchanged (registered BEFORE the
generic /files/{document_id} routes). The caller is the verified token subject; a document that is not the caller's is a 404.
The user's URL is validated and reduced to a video id; it is never fetched.
"""

from functools import lru_cache
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Request, Response, status
from fastapi.responses import JSONResponse
from minio import Minio
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user_id, get_db
from app.core.config import settings
from app.db.session import get_session_factory
from app.repositories.file_repository import FileRepository
from app.storage.minio_client import get_minio_client
from app.youtube import service as yt
from app.youtube.provider import TranscriptProvider
from app.youtube.urls import InvalidYouTubeUrl
from app.youtube.yta_provider import YouTubeTranscriptApiProvider

router = APIRouter(prefix="/files/youtube", tags=["youtube"])


class AttachYoutubeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str = Field(min_length=1, max_length=2048)
    conversation_id: UUID | None = None


class YoutubeSourceResponse(BaseModel):
    document_id: UUID
    video_id: str
    canonical_url: str
    title: str | None
    language: str | None
    is_generated: bool | None
    transcript_version: int
    segment_count: int | None
    duration_seconds: float | None
    extraction_status: str
    indexing_status: str
    ready: bool
    error_code: str | None
    message: str | None
    retryable: bool
    retry_after_seconds: int | None
    created: bool = False


@lru_cache
def _default_provider() -> TranscriptProvider:
    return YouTubeTranscriptApiProvider()


def get_transcript_provider() -> TranscriptProvider:
    """Overridable in tests (no network)."""
    return _default_provider()


def _error(request: Request, http_status: int, code: str, message: str, headers: dict | None = None, **extra) -> JSONResponse:
    body = {"error_code": code, "message": message, "request_id": getattr(request.state, "request_id", None), **extra}
    return JSONResponse(status_code=http_status, content=body, headers=headers)


async def _response(db: AsyncSession, result: yt.AttachResult) -> YoutubeSourceResponse:
    file = await FileRepository.get(db, result.source.document_id)
    return YoutubeSourceResponse(**yt.view(result.source, file), created=result.created)


def _schedule(tasks: BackgroundTasks, minio: Minio, provider: TranscriptProvider, document_id: UUID) -> None:
    tasks.add_task(yt.run_extraction, get_session_factory(), minio, provider, document_id)


@router.post("", response_model=YoutubeSourceResponse, status_code=status.HTTP_202_ACCEPTED)
async def attach_youtube(
    body: AttachYoutubeRequest,
    request: Request,
    tasks: BackgroundTasks,
    owner_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio: Minio = Depends(get_minio_client),
    provider: TranscriptProvider = Depends(get_transcript_provider),
):
    """Idempotent: the same video for the same user returns the same document. 202 = accepted; poll GET for
    extraction and indexing state. A source is usable only when `ready` is true."""
    if not settings.YOUTUBE_ENABLED:
        return _error(request, 503, "YoutubeDisabled", "YouTube links are not enabled on this server.")
    try:
        result = await yt.attach(db, owner_id=owner_id, conversation_id=body.conversation_id, url=body.url)
    except InvalidYouTubeUrl as e:
        return _error(request, 422, "InvalidYouTubeUrl", e.message, reason=e.reason)
    except yt.QuotaExceeded as e:
        return _error(request, 429, "YoutubeQuotaExceeded", "Too many videos added recently. Try again later.",
                      headers={"Retry-After": str(e.retry_after_seconds)})
    response = await _response(db, result)
    await db.commit()                           # the worker must be able to see the row
    if result.start_extraction:
        _schedule(tasks, minio, provider, result.source.document_id)
    return response


@router.get("/{document_id}", response_model=YoutubeSourceResponse)
async def get_youtube_source(
    document_id: UUID,
    request: Request,
    tasks: BackgroundTasks,
    owner_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio: Minio = Depends(get_minio_client),
    provider: TranscriptProvider = Depends(get_transcript_provider),
):
    source = await _own_live(db, owner_id, document_id)
    if source is None:
        return _error(request, 404, "FileRecordNotFoundError", "Source not found.")
    result = yt.AttachResult(source, created=False, start_extraction=False)
    response = await _response(db, result)
    # A worker that died mid-fetch leaves an expired lease; polling revives it (the claim itself is atomic).
    if source.extraction_status == "FETCHING" and yt.needs_extraction(source):
        _schedule(tasks, minio, provider, document_id)
    return response


@router.post("/{document_id}/retry", response_model=YoutubeSourceResponse, status_code=status.HTTP_202_ACCEPTED)
async def retry_youtube_source(
    document_id: UUID,
    request: Request,
    tasks: BackgroundTasks,
    owner_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio: Minio = Depends(get_minio_client),
    provider: TranscriptProvider = Depends(get_transcript_provider),
):
    try:
        result = await yt.request_retry(db, owner_id=owner_id, document_id=document_id)
    except InvalidYouTubeUrl as e:                # a stored canonical url can only be invalid if the row was tampered with
        return _error(request, 422, "InvalidYouTubeUrl", e.message, reason=e.reason)
    except yt.QuotaExceeded as e:
        return _error(request, 429, "YoutubeQuotaExceeded", "Too many videos added recently. Try again later.",
                      headers={"Retry-After": str(e.retry_after_seconds)})
    if result is None:
        return _error(request, 404, "FileRecordNotFoundError", "Source not found.")
    response = await _response(db, result)
    await db.commit()
    if result.start_extraction:
        _schedule(tasks, minio, provider, result.source.document_id)
    return response


@router.delete("/{document_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_youtube_source(
    document_id: UUID,
    request: Request,
    owner_id: UUID = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db),
    minio: Minio = Depends(get_minio_client),
):
    if not await yt.remove(db, minio, owner_id=owner_id, document_id=document_id):
        return _error(request, 404, "FileRecordNotFoundError", "Source not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _own_live(db: AsyncSession, owner_id: UUID, document_id: UUID):
    from sqlalchemy import select

    from app.models.youtube_source import YoutubeSource

    return (await db.execute(
        select(YoutubeSource).where(YoutubeSource.document_id == document_id, YoutubeSource.owner_id == owner_id,
                                    YoutubeSource.removed_at.is_(None))
    )).scalar_one_or_none()
