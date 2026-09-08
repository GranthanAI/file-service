# Task Complete 4: Lifecycle, Permissions, Metadata APIs (Phase 4)

This document provides a detailed log of the read-side and authorization implementation completed for the **Granthan File Service** under **Phase 4 (Lifecycle, Permissions, Metadata APIs)** of [docs/IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md).

---

## 1. What Was Done

1. **Lifecycle state machine:** [services/lifecycle_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/lifecycle_service.py) — already existed from an earlier phase (used internally by `file_repository.update_status`); verified against [docs/LLD.md §4.2](../LLD.md#42-lifecycle-state-machine-appserviceslifecycle_servicepy) line-by-line, including the `ALLOWED_TRANSITIONS` table, the `SCANNING → UPLOADED` (not `→ PROCESSING`) correction, and the idempotent `current == target` no-op. No changes needed — `tests/unit/test_lifecycle_service.py` already exercises every allowed edge plus a set of illegal ones parametrized directly off `ALLOWED_TRANSITIONS`.
2. **Permission enforcement:** [services/permission_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/permission_service.py) — `assert_access(db, document_id, principal_id, required)`, implementing [docs/LLD.md §4.3](../LLD.md#43-permission-enforcement-appservicespermission_servicepy)'s `LEVEL_RANK` total ordering (`VIEWER < EDITOR < OWNER`). Owner short-circuits without touching `file_permissions`; everyone else needs an explicit grant at or above the required rank. A soft-deleted file (`status == DELETED`) is treated identically to a missing row — both raise `FileRecordNotFoundError` — so a permission check never leaks "this existed once" to a caller who shouldn't see it at all.
3. **Permission guard dependency:** [api/deps.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/deps.py) — `require_permission(level)`, a dependency factory matching the exact call-site shape shown in the LLD (`Depends(require_permission(PermissionLevel.OWNER))`). Resolves `document_id` from the path, `principal_id` via the existing `get_current_user_id`, and raises straight through `permission_service.assert_access` — a failed check short-circuits the route before its body ever runs.
4. **Metadata service:** [services/file_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/file_service.py) — `get_file` (delegates entirely to `assert_access`, which both authorizes and fetches in one query) and `list_files` (thin wrapper over `file_repository.list_paginated`, scoped to `owner_id=requester_id` — see [§2](#2-why-it-was-done) for why listing is owner-only and not permission-joined).
5. **Schemas:** [api/schemas/file.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/schemas/file.py) — `FileResponse`, `FileListItem` (a deliberately lighter projection than `FileResponse`, matching the LLD's documented list payload exactly), `FileListResponse`, `FileStatusResponse`. [api/schemas/permission.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/schemas/permission.py) — `GrantPermissionRequest`, `PermissionResponse`. `api/schemas/common.py` needed no changes — `ErrorResponse` already covers Phase 4's error shape and no new shared type was warranted.
6. **Routers:**
   * [api/routers/files.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/files.py) — `GET /files/{document_id}` and `GET /files` (query params `conversation_id?`, `status?`, `cursor?`, `limit` default 20 / max 100 via `Query(ge=1, le=100)`).
   * [api/routers/status.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/status.py) — `GET /files/{document_id}/status`.
   * [api/routers/permissions.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/permissions.py) — `PUT /files/{document_id}/permissions`, gated by `require_permission(PermissionLevel.OWNER)` as a route-level dependency.
7. **Wiring:** [main.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/main.py) — registered `files`, `status`, `permissions` routers alongside the existing `health`/`upload` ones.
8. **Tests:**
   * [tests/unit/test_permission_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/unit/test_permission_service.py) — `LEVEL_RANK` ordering, owner short-circuit (asserts the grant lookup is never even called), VIEWER-grant-satisfies-VIEWER, VIEWER-grant-denies-OWNER, no-grant-denies, missing-file-404, and soft-deleted-file-404-even-for-the-owner — all against mocked repositories, no DB needed.
   * [tests/integration/test_files_api.py](file:///c:/Users/hp/Desktop/Granthan/file-service/tests/integration/test_files_api.py) — the full permission matrix over real HTTP + a real ephemeral Postgres (owner GET succeeds; stranger gets 403 on GET/status/PUT-permissions; a VIEWER grant allows GET+status but still 403s on PUT-permissions; unknown and soft-deleted document ids both 404), plus the pagination exit criterion: 25 files, `limit=10`, walked page-by-page via `next_cursor` until it comes back `null`, asserting exactly 3 pages and the returned id set has no duplicates and no gaps against what was inserted.

---

## 2. Why It Was Done

* **`assert_access` returns the `File` row instead of `None`/`bool`:** every caller that passes a permission check immediately needs the row anyway (`file_service.get_file`, the `require_permission` dependency itself for routes that want it). Returning it avoids a second `SELECT` and means "return" doubles as the whole success path — there's no separate "now go fetch it" step to forget.
* **Soft-deleted files 404, even for the owner:** the LLD documents `GET /files/{document_id}` as `404 not found or soft-deleted` in the same breath, and nothing about the delete flow ([LLD §15](../LLD.md#15-delete-flow)/[§6.5](../LLD.md#65-delete)) suggests an owner should still be able to fetch a `DELETED` row through the normal read path once it's gone — a dedicated audit/undelete endpoint (not part of Phase 4's scope) would be the right place for that, not a quiet exception carved into `assert_access`.
* **`list_files` is owner-scoped only, not permission-joined against `file_permissions`:** the LLD's `file_repository.list_paginated` (built in Phase 1) already takes an `owner_id` filter and nothing else — there's no query in the LLD that joins `files` against `file_permissions` for a "everything I can see" listing, and [§2.4](../LLD.md#24-file_permissions-table)'s own scope decision is explicit that `file_permissions` only holds *shared* grants, kept deliberately small relative to the files table. Building a shared-files listing would be a real feature with its own pagination/ordering questions the LLD never specifies — adding it speculatively here would be scope creep past what Phase 4 actually asks for (`get_file`, `list_files` — LLD §4 task list, not a "list everything I have access to" endpoint).
* **`require_permission` is a route-level `dependencies=[...]` entry, not a function parameter, on the `PUT /permissions` route:** the handler itself never needs the resolved `File` — only the pass/fail matters — so declaring it as a bare dependency (matching FastAPI's own idiom for guard-only checks) keeps the handler signature free of an unused parameter, while `GET /files/{document_id}` and `.../status` instead call `file_service.get_file` directly since they *do* need the row's fields for the response body.
* **`GrantPermissionRequest.principal_type` defaults to `PrincipalType.USER` rather than being required:** [§2.4](../LLD.md#24-file_permissions-table) is explicit that only `USER` is implemented today and `TEAM` is a reserved, forward-compatible value — a client that only ever grants user access (the only thing that works right now) shouldn't have to pass a constant literal on every request.
* **No changes to `app/api/schemas/common.py`:** the Phase 4 task list mentions it, but nothing in §5.3/§5.4/§5.6/§5.9's response shapes needs a new shared type beyond the `ErrorResponse` already there from Phase 3 — pagination query params are plain FastAPI `Query(...)` parameters on the router function instead of a wrapping Pydantic model, since there was no second consumer of that shape to justify factoring it out.

---

## 3. Code Highlights & Explanations

### 3.1 The Owner Short-Circuit Avoids an Extra Query

Located in: [app/services/permission_service.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/services/permission_service.py)

```python
file = await FileRepository.get(db, document_id)
if file is None or file.status == FileStatus.DELETED:
    raise FileRecordNotFoundError(document_id)

if file.owner_id == principal_id:
    return file  # no file_permissions row needed

grant = await FilePermissionRepository.get(db, document_id, principal_id)
if grant is None or LEVEL_RANK[grant.access_level] < LEVEL_RANK[required]:
    raise PermissionDeniedError(document_id, principal_id)
```
* **Why it matters:** the overwhelmingly common case — a user accessing their own file — never touches `file_permissions` at all, keeping the hot path to a single `SELECT`. This is asserted directly in the unit tests (`test_owner_always_succeeds_without_a_grant_lookup` mocks `FilePermissionRepository.get` and asserts it's never called).

### 3.2 Keyset Pagination's `next_cursor` Signal

Located in: [app/api/routers/files.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/routers/files.py)

```python
next_cursor = files[-1].document_id if len(files) == limit else None
```
* **Why it matters:** a full page (exactly `limit` rows) means there *might* be more beyond it, so the client is handed a cursor to keep paging; a short page (fewer than `limit`) is unambiguously the last page, so `next_cursor` comes back `null` and the client knows to stop without an extra round trip that would return zero rows.

### 3.3 `require_permission` as a Reusable Guard Factory

Located in: [app/api/deps.py](file:///c:/Users/hp/Desktop/Granthan/file-service/app/api/deps.py)

```python
def require_permission(level: PermissionLevel) -> Callable[..., Coroutine[Any, Any, File]]:
    async def _dependency(
        document_id: UUID,
        principal_id: UUID = Depends(get_current_user_id),
        db: AsyncSession = Depends(get_db),
    ) -> File:
        return await permission_service.assert_access(db, document_id, principal_id, level)
    return _dependency
```
* **Why it matters:** routers declare their required access level declaratively (`Depends(require_permission(PermissionLevel.OWNER))`) instead of each hand-rolling the same three-line check — the closure captures `level` once at import time, so every route using it gets the identical enforcement logic with zero duplication.

---

## 4. Verification

* `uv run pytest`: **85 passed** (57 unit, 28 integration), 0 failures — `tests/unit/test_permission_service.py` contributes 7 of the unit tests, `tests/integration/test_files_api.py` contributes 8 of the integration tests.
* Ran against real, ephemeral **testcontainers** Postgres + MinIO (Docker confirmed available in this environment) — the permission matrix and pagination exit-criteria tests exercise the actual HTTP → FastAPI dependency graph → repository → Postgres path, not mocked-out services.
* No regressions: all Phase 0–3 tests (bootstrap, storage, upload flow, repository, lifecycle) still pass unchanged.
