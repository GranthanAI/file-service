from app.core.enums import FileStatus
from app.core.exceptions import InvalidStateTransitionError

# See docs/LLD.md §4.2 for the full rationale behind each edge — in short:
#   - UPLOADED is reached twice on the AV-scan-enabled path (right after
#     /complete, and again after a clean scan promotes the object).
#   - SCANNING only ever returns to UPLOADED, never jumps straight to
#     PROCESSING — publishing file.uploaded happens on that return.
#   - PROCESSING is only ever entered via file.processing.started, and only
#     ever leaves via file.processing.completed/failed (docs/LLD.md §3.6) —
#     upload/quarantine code never drives this edge directly.
ALLOWED_TRANSITIONS: dict[FileStatus, set[FileStatus]] = {
    FileStatus.UPLOADING: {FileStatus.UPLOADED, FileStatus.FAILED},
    FileStatus.UPLOADED: {FileStatus.SCANNING, FileStatus.PROCESSING, FileStatus.FAILED},
    FileStatus.SCANNING: {FileStatus.UPLOADED, FileStatus.FAILED},
    FileStatus.PROCESSING: {FileStatus.READY, FileStatus.FAILED},
    FileStatus.READY: {FileStatus.DELETING, FileStatus.PROCESSING},
    FileStatus.FAILED: {FileStatus.PROCESSING, FileStatus.DELETING},
    FileStatus.DELETING: {FileStatus.DELETED, FileStatus.FAILED},
}


def transition(current: FileStatus, target: FileStatus) -> None:
    """Validate a file_status edge. Raises InvalidStateTransitionError on an
    illegal edge; a no-op (not an error) when current == target, since that
    must be safe under Kafka's at-least-once redelivery (docs/LLD.md §3.7)."""
    if current == target:
        return
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(current, target)
