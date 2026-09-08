from aiokafka import AIOKafkaProducer

from app.core.config import settings

# Process-scoped singleton, same shape as app/storage/minio_client.py and
# app/db/postgres.py — one producer per worker process, started once in
# lifespan.py and reused across every outbox_worker poll for that process's
# lifetime (aiokafka's own internal connection pool handles reuse).
_producer: AIOKafkaProducer | None = None


async def start_producer() -> AIOKafkaProducer:
    """Called once from lifespan.py startup. Idempotent — a second call
    while a producer is already running just returns the existing instance
    rather than starting a duplicate connection."""
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BROKERS)
        await _producer.start()
    return _producer


def get_producer() -> AIOKafkaProducer:
    """For any code path that needs the running producer outside of
    lifespan's own startup call (e.g. the outbox worker). Raises rather than
    lazily starting one, since starting a Kafka connection is an async
    operation that has no business happening implicitly inside a sync
    getter — unlike get_minio_client(), which is safe to lazily construct."""
    if _producer is None:
        raise RuntimeError("Kafka producer not started — call start_producer() from lifespan first")
    return _producer


async def stop_producer() -> None:
    """Called once from lifespan.py shutdown. Idempotent — safe to call even
    if start_producer() was never called."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
