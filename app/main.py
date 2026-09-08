from fastapi import FastAPI

from app.api.routers import files, health, permissions, status, upload
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

    app.include_router(health.router)
    app.include_router(upload.router)
    app.include_router(files.router)
    app.include_router(status.router)
    app.include_router(permissions.router)
    register_exception_handlers(app)

    # Registered in later phases (per docs/IMPLEMENTATION_PLAN.md):
    #   Phase 7: retry, Phase 8: download, Phase 10: readiness route on health.router

    return app


app = create_app()
