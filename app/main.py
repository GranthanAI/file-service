from fastapi import FastAPI

from app.api.middleware import RequestIDMiddleware
from app.api.routers import download, files, health, internal, permissions, retry, status, upload, youtube
from app.core.config import settings
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.lifespan import lifespan


def create_app() -> FastAPI:
    configure_logging()

    app = FastAPI(
        title="Granthan File Service",
        description="Upload, storage, metadata, and lifecycle management for GraphGPT files.",
        version="0.1.0",
        debug=settings.DEBUG,
        lifespan=lifespan,
    )

    app.add_middleware(RequestIDMiddleware)

    app.include_router(health.router)
    app.include_router(upload.router)
    app.include_router(youtube.router)   # before files: /files/youtube must not be read as /files/{document_id}
    app.include_router(files.router)
    app.include_router(status.router)
    app.include_router(permissions.router)
    app.include_router(retry.router)
    app.include_router(download.router)
    app.include_router(internal.router)
    register_exception_handlers(app)

    return app


app = create_app()
