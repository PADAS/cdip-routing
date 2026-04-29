import logging
from hashlib import md5

from gundi_core.events.integrations import (
    CustomActivityLog,
    IntegrationActionCustomLog,
    LogLevel,
)

from app.core import settings
from app.core.pubsub import send_event_to_integration_events_topic
from app.core.utils import get_redis_db


logger = logging.getLogger(__name__)

_cache_db = get_redis_db()


def _dedup_key(action_id: str, resource_id: str, exception: Exception) -> str:
    signature = md5(
        f"{type(exception).__name__}:{exception}".encode("utf-8")
    ).hexdigest()
    return f"activity_log_emitted.{action_id}.{resource_id}.{signature}"


async def log_portal_lookup_error(
    *, action_id: str, resource_id: str, exception: Exception
) -> None:
    """Publish an activity log for a failed portal lookup, deduped via Redis.

    Never raises. A Redis or PubSub problem must not affect the caller's path.
    """
    try:
        key = _dedup_key(action_id, str(resource_id), exception)
        if await _cache_db.set(key, 1, ex=settings.ACTIVITY_LOG_DEDUP_TTL, nx=True) is None:
            return  # Already logged within the dedup window.

        event = IntegrationActionCustomLog(
            payload=CustomActivityLog(
                integration_id=str(resource_id),
                action_id=action_id,
                title=f"Portal lookup failed: {action_id}",
                level=LogLevel.ERROR,
                data={
                    "error_type": type(exception).__name__,
                    "error_message": str(exception),
                },
            )
        )
        await send_event_to_integration_events_topic(event)
    except Exception as e:
        logger.warning(
            f"activity_logger: suppressed error while logging portal lookup failure "
            f"for {action_id}({resource_id}): {type(e).__name__}: {e}"
        )
