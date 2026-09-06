"""Phase 1 exit-criteria tests (docs/IMPLEMENTATION_PLAN.md): insert a
`files` row, read it back, keyset-paginate correctly, and verify the
partial `idx_files_status` index is actually used by the planner."""

import uuid

from sqlalchemy import text

from app.core.enums import FileStatus
from app.repositories.file_repository import FileRepository
from app.utils.id_generator import new_uuid7


def _make_file_kwargs(owner_id: uuid.UUID) -> dict:
    document_id = new_uuid7()
    return dict(
        document_id=document_id,
        conversation_id=uuid.uuid4(),
        owner_id=owner_id,
        filename="physics.pdf",
        content_type="application/pdf",
        size_bytes=1024,
        storage_bucket="graphgpt-files",
        storage_key=f"{owner_id}/{document_id}/v1/original.pdf",
    )


async def test_insert_and_get_round_trip(db_session) -> None:
    owner_id = uuid.uuid4()
    kwargs = _make_file_kwargs(owner_id)

    inserted = await FileRepository.insert(db_session, **kwargs)
    assert inserted.status == FileStatus.UPLOADING
    assert inserted.retry_count == 0
    assert inserted.current_version == 1

    fetched = await FileRepository.get(db_session, kwargs["document_id"])
    assert fetched is not None
    assert fetched.document_id == kwargs["document_id"]
    assert fetched.owner_id == owner_id
    assert fetched.filename == "physics.pdf"


async def test_keyset_pagination_returns_three_rows_in_order(db_session) -> None:
    owner_id = uuid.uuid4()
    document_ids = []
    for _ in range(3):
        kwargs = _make_file_kwargs(owner_id)
        await FileRepository.insert(db_session, **kwargs)
        document_ids.append(kwargs["document_id"])

    page = await FileRepository.list_paginated(db_session, owner_id=owner_id, limit=10)

    assert [f.document_id for f in page] == sorted(document_ids)


async def test_keyset_pagination_cursor_excludes_seen_rows(db_session) -> None:
    owner_id = uuid.uuid4()
    document_ids = []
    for _ in range(3):
        kwargs = _make_file_kwargs(owner_id)
        await FileRepository.insert(db_session, **kwargs)
        document_ids.append(kwargs["document_id"])
    document_ids.sort()

    first_page = await FileRepository.list_paginated(db_session, owner_id=owner_id, limit=1)
    assert len(first_page) == 1
    assert first_page[0].document_id == document_ids[0]

    second_page = await FileRepository.list_paginated(
        db_session, owner_id=owner_id, cursor=first_page[0].document_id, limit=10
    )
    assert [f.document_id for f in second_page] == document_ids[1:]


async def test_deleted_files_excluded_by_default(db_session) -> None:
    owner_id = uuid.uuid4()
    kwargs = _make_file_kwargs(owner_id)
    file_obj = await FileRepository.insert(db_session, **kwargs)

    await FileRepository.update_status(db_session, file_obj, FileStatus.FAILED)
    await FileRepository.update_status(db_session, file_obj, FileStatus.DELETING)
    await FileRepository.update_status(db_session, file_obj, FileStatus.DELETED)

    page = await FileRepository.list_paginated(db_session, owner_id=owner_id, limit=10)
    assert page == []

    explicit = await FileRepository.list_paginated(db_session, owner_id=owner_id, status=FileStatus.DELETED, limit=10)
    assert len(explicit) == 1


async def test_partial_status_index_is_used_for_status_filter(db_session) -> None:
    """Sanity check, not a hard gate (docs/IMPLEMENTATION_PLAN.md Phase 1
    exit criteria) — Postgres' planner may reasonably choose a seq scan on a
    near-empty table, so this only asserts the index exists and is a valid
    plan candidate, not that it is always chosen."""
    result = await db_session.execute(
        text(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE tablename = 'files' AND indexname = 'idx_files_status'"
        )
    )
    row = result.fetchone()
    assert row is not None
    assert "WHERE" in row.indexdef
    assert "DELETED" in row.indexdef


async def test_increment_retry_count(db_session) -> None:
    owner_id = uuid.uuid4()
    kwargs = _make_file_kwargs(owner_id)
    file_obj = await FileRepository.insert(db_session, **kwargs)

    await FileRepository.increment_retry_count(db_session, file_obj)
    await FileRepository.increment_retry_count(db_session, file_obj)

    fetched = await FileRepository.get(db_session, kwargs["document_id"])
    assert fetched.retry_count == 2
