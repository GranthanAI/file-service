from pydantic import BaseModel


class DownloadResponse(BaseModel):
    """docs/LLD.md §5.5."""

    download_url: str
    expires_in: int
