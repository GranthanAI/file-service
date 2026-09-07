import uuid

from app.storage.key_builder import build_storage_key


def test_build_storage_key_format() -> None:
    owner_id = uuid.uuid4()
    document_id = uuid.uuid4()

    key = build_storage_key(owner_id, document_id, version=1, filename="physics.pdf")

    assert key == f"{owner_id}/{document_id}/v1/original.pdf"


def test_build_storage_key_uses_extension_from_filename() -> None:
    owner_id = uuid.uuid4()
    document_id = uuid.uuid4()

    key = build_storage_key(owner_id, document_id, version=2, filename="notes.docx")

    assert key.endswith("/v2/original.docx")


def test_build_storage_key_defaults_to_bin_when_no_extension() -> None:
    owner_id = uuid.uuid4()
    document_id = uuid.uuid4()

    key = build_storage_key(owner_id, document_id, version=1, filename="README")

    assert key.endswith("/v1/original.bin")


def test_build_storage_key_is_deterministic() -> None:
    owner_id = uuid.uuid4()
    document_id = uuid.uuid4()

    key1 = build_storage_key(owner_id, document_id, version=1, filename="a.pdf")
    key2 = build_storage_key(owner_id, document_id, version=1, filename="a.pdf")

    assert key1 == key2
