import pytest

from app.core.enums import FileStatus
from app.core.exceptions import InvalidStateTransitionError
from app.services.lifecycle_service import ALLOWED_TRANSITIONS, transition


@pytest.mark.parametrize(
    ("current", "target"),
    [(current, target) for current, targets in ALLOWED_TRANSITIONS.items() for target in targets],
)
def test_every_allowed_edge_succeeds(current: FileStatus, target: FileStatus) -> None:
    transition(current, target)  # should not raise


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (FileStatus.UPLOADING, FileStatus.PROCESSING),  # can't skip UPLOADED
        (FileStatus.SCANNING, FileStatus.PROCESSING),  # must return to UPLOADED first — the regression this guards
        (FileStatus.PROCESSING, FileStatus.UPLOADED),  # no going backwards
        (FileStatus.READY, FileStatus.UPLOADING),
        (FileStatus.DELETED, FileStatus.READY),
        (FileStatus.FAILED, FileStatus.READY),
    ],
)
def test_illegal_edges_raise(current: FileStatus, target: FileStatus) -> None:
    with pytest.raises(InvalidStateTransitionError):
        transition(current, target)


def test_same_state_transition_is_a_noop() -> None:
    transition(FileStatus.PROCESSING, FileStatus.PROCESSING)  # should not raise
