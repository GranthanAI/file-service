from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """docs/LLD.md §8: the consistent error body every registered exception
    handler in app/main.py returns."""

    error_code: str
    message: str
    request_id: str | None = None
