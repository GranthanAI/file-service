import time
import uuid

from app.utils.id_generator import new_uuid7


def test_new_uuid7_is_version_7() -> None:
    generated = new_uuid7()
    assert isinstance(generated, uuid.UUID)
    assert generated.version == 7


def test_new_uuid7_is_variant_rfc4122() -> None:
    generated = new_uuid7()
    assert generated.variant == uuid.RFC_4122


def test_new_uuid7_ordering_matches_insertion_order() -> None:
    """UUIDv7 is time-sortable (docs/LLD.md §2.1) — generating a handful of
    IDs with a small delay between each should come back in the same order
    when sorted lexicographically as UUID values."""
    generated = []
    for _ in range(5):
        generated.append(new_uuid7())
        time.sleep(0.002)  # cross a millisecond boundary reliably

    assert generated == sorted(generated)


def test_new_uuid7_values_are_unique() -> None:
    generated = {new_uuid7() for _ in range(1000)}
    assert len(generated) == 1000
