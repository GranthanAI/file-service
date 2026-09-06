from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health/live")
async def liveness() -> dict[str, str]:
    """Process-alive check only — no dependency calls.

    Deliberately never checks DB/MinIO/Kafka: a slow dependency must not
    make this fail, since that would cause restart-loops that fix nothing.
    See docs/LLD.md §4.8 for the full liveness/readiness split
    (readiness — DB + MinIO, Kafka excluded — is added in Phase 10).
    """
    return {"status": "alive"}
