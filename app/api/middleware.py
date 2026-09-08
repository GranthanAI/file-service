import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class RequestIDMiddleware(BaseHTTPMiddleware):
    """docs/LLD.md §1/§21 — assigns (or propagates, via `X-Request-ID`) a
    request_id, stores it on `request.state` for exceptions.py's handler to
    read (docs/core/exceptions.py's `register_exception_handlers` already
    looks for `request.state.request_id`, but nothing was ever setting it —
    this is that missing piece, not a new contract), echoes it back as a
    response header, and binds it into structlog's contextvars for the
    lifetime of the request.

    Binding into contextvars (rather than threading `request_id` through
    every function signature) is what makes docs/HLD.md §21's "every
    request/event traceable via request_id" true for free: `configure_logging`
    already has `structlog.contextvars.merge_contextvars` first in its
    processor chain, so any `logger.info(...)` call anywhere in the call
    stack during this request — a router, a service, a repository — picks up
    `request_id` automatically without being told about it.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id

        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            response = await call_next(request)
        finally:
            structlog.contextvars.clear_contextvars()

        response.headers["X-Request-ID"] = request_id
        return response
