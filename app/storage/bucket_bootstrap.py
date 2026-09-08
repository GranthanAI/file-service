from minio import Minio
from minio.commonconfig import ENABLED, Filter
from minio.lifecycleconfig import Expiration, LifecycleConfig
from minio.lifecycleconfig import Rule as LifecycleRule
from minio.notificationconfig import NotificationConfig, QueueConfig
from minio.sseconfig import Rule as SSERule
from minio.sseconfig import SSEConfig
from minio.versioningconfig import VersioningConfig

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def bootstrap_buckets(client: Minio) -> None:
    """Idempotent desired-state bucket configuration, run once at startup
    (docs/LLD.md §3.2) — safe to re-run on every pod boot. Treated as
    infrastructure state (re-applied every boot), not a one-off migration.
    """
    for bucket in (settings.BUCKET_FILES, settings.BUCKET_QUARANTINE):
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("bucket_bootstrap.bucket_created", bucket=bucket)

    client.set_bucket_versioning(settings.BUCKET_FILES, VersioningConfig(ENABLED))

    client.set_bucket_encryption(settings.BUCKET_FILES, SSEConfig(SSERule.new_sse_s3_rule()))

    client.set_bucket_lifecycle(
        settings.BUCKET_QUARANTINE,
        LifecycleConfig(
            [
                LifecycleRule(
                    status=ENABLED,
                    rule_filter=Filter(prefix=""),
                    rule_id="expire-unpromoted-quarantine",
                    expiration=Expiration(days=settings.QUARANTINE_OBJECT_EXPIRY_DAYS),
                ),
            ]
        ),
    )

    # No public bucket access (docs/LLD.md §9 security table): MinIO buckets
    # are already private by default with no policy attached — this is the
    # SDK equivalent of `mc anonymous set none` referenced in
    # docker/minio/init-buckets.sh and docs/HLD.md §14, and is self-healing
    # on every boot against a Console misconfiguration that granted public
    # access (an explicit ALLOW policy for Principal "*"). There is
    # deliberately no ALLOW-anything policy applied — an explicit
    # "Effect: Deny, Action: s3:*" statement is not valid here: MinIO's
    # policy engine rejects wildcard actions like "s3:*" in bucket policies
    # (only specific s3:* action names are accepted), and it isn't needed
    # anyway since the absence of any policy already means no anonymous
    # access.
    for bucket in (settings.BUCKET_FILES, settings.BUCKET_QUARANTINE):
        client.delete_bucket_policy(bucket)

    if settings.MINIO_NOTIFICATIONS_ENABLED:
        # docs/LLD.md §4.5/§6.6: quarantine_service consumes ObjectCreated
        # for the QUARANTINE bucket (that's where uploads land when
        # AV_SCAN_ENABLED=true, not graphgpt-files) — notifications on
        # BUCKET_FILES would never fire for anything quarantine_service
        # needs to react to.
        client.set_bucket_notification(
            settings.BUCKET_QUARANTINE,
            NotificationConfig(
                queue_config_list=[
                    QueueConfig(
                        queue=settings.MINIO_KAFKA_NOTIFICATION_ARN,
                        events=["s3:ObjectCreated:*"],
                    ),
                ]
            ),
        )

    logger.info(
        "bucket_bootstrap.complete",
        bucket_files=settings.BUCKET_FILES,
        bucket_quarantine=settings.BUCKET_QUARANTINE,
    )
