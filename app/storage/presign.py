from datetime import datetime, timedelta, timezone

from minio import Minio
from minio.datatypes import PostPolicy


def presign_put(client: Minio, bucket: str, key: str, expiry: timedelta) -> str:
    """See docs/LLD.md §3.4. Bare presigned PUT — used when
    settings.UPLOAD_STRICT_POLICY is False."""
    return client.presigned_put_object(bucket, key, expires=expiry)


def presign_post_policy(
    client: Minio,
    bucket: str,
    key: str,
    content_type: str,
    max_size: int,
    expiry: timedelta,
) -> tuple[str, dict[str, str]]:
    """See docs/LLD.md §3.4. Presigned POST policy — enforces exact key,
    exact content-type, and a content-length range AT THE STORAGE LAYER
    (not just app-layer validation), preferred whenever
    settings.UPLOAD_STRICT_POLICY is True.

    Returns (upload_url, form_data): the MinIO SDK's `presigned_post_policy`
    only returns the *signing* fields (algorithm, credential, date, policy
    document, signature) — it does NOT echo back the actual condition values
    as form fields. The caller's multipart POST must also include `key` and
    `Content-Type` as literal form fields matching the policy's
    equals-conditions, or MinIO rejects the upload with "Policy Condition
    failed" even though the signature itself is valid — so those are merged
    into the returned form_data here, giving callers one complete,
    ready-to-submit dict rather than requiring them to know this SDK quirk.

    The upload URL is built from the given `client`'s own configured
    endpoint (NOT from global settings — this function must work against
    whatever server `client` actually points at, e.g. an ephemeral test
    MinIO different from the app's configured one).
    """
    policy = PostPolicy(bucket, datetime.now(timezone.utc) + expiry)
    policy.add_equals_condition("key", key)
    policy.add_equals_condition("Content-Type", content_type)
    policy.add_content_length_range_condition(1, max_size)

    form_data = client.presigned_post_policy(policy)
    form_data["key"] = key
    form_data["Content-Type"] = content_type

    scheme = "https" if client._base_url.is_https else "http"
    upload_url = f"{scheme}://{client._base_url.host}/{bucket}"
    return upload_url, form_data


def presign_get(
    client: Minio,
    bucket: str,
    key: str,
    expiry: timedelta,
    response_filename: str | None = None,
) -> str:
    """See docs/LLD.md §3.4. Short expiry (minutes) limits the exposure
    window of a leaked URL; response_filename forces a friendly download
    name via response-content-disposition regardless of the internal key."""
    extra_headers = None
    if response_filename:
        extra_headers = {"response-content-disposition": f'attachment; filename="{response_filename}"'}
    return client.presigned_get_object(bucket, key, expires=expiry, response_headers=extra_headers)
