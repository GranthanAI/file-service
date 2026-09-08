from app.utils.minio_events import parse_object_created_event


class TestParseObjectCreatedEvent:
    def test_extracts_bucket_and_key(self) -> None:
        message = {
            "Records": [
                {"s3": {"bucket": {"name": "graphgpt-files-quarantine"}, "object": {"key": "owner/doc/v1/original.pdf"}}}
            ]
        }
        assert parse_object_created_event(message) == [("graphgpt-files-quarantine", "owner/doc/v1/original.pdf")]

    def test_url_decodes_the_key(self) -> None:
        message = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "owner%2Fdoc%2Fv1%2Foriginal.pdf"}}}]}
        assert parse_object_created_event(message) == [("b", "owner/doc/v1/original.pdf")]

    def test_empty_records_yields_nothing(self) -> None:
        assert parse_object_created_event({"Records": []}) == []

    def test_missing_records_key_yields_nothing(self) -> None:
        assert parse_object_created_event({}) == []
