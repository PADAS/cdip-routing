import logging
from hashlib import md5
from typing import List

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


def _dedup_key_for_signature(action_id: str, resource_id: str, signature: str) -> str:
    digest = md5(signature.encode("utf-8")).hexdigest()
    return f"activity_log_emitted.{action_id}.{resource_id}.{digest}"


def _dedup_key(action_id: str, resource_id: str, exception: Exception) -> str:
    return _dedup_key_for_signature(
        action_id, resource_id, f"{type(exception).__name__}:{exception}"
    )


async def _publish_once(*, dedup_key: str, payload: CustomActivityLog) -> None:
    """Publish an activity log unless the same one was emitted within the dedup window."""
    if await _cache_db.set(dedup_key, 1, ex=settings.ACTIVITY_LOG_DEDUP_TTL, nx=True) is None:
        return  # Already logged within the dedup window.
    await send_event_to_integration_events_topic(IntegrationActionCustomLog(payload=payload))


async def log_portal_lookup_error(
    *, action_id: str, resource_id: str, exception: Exception
) -> None:
    """Publish an activity log for a failed portal lookup, deduped via Redis.

    Never raises. A Redis or PubSub problem must not affect the caller's path.
    """
    try:
        await _publish_once(
            dedup_key=_dedup_key(action_id, str(resource_id), exception),
            payload=CustomActivityLog(
                integration_id=str(resource_id),
                action_id=action_id,
                title=f"Portal lookup failed: {action_id}",
                level=LogLevel.ERROR,
                data={
                    "error_type": type(exception).__name__,
                    "error_message": str(exception),
                },
            ),
        )
    except Exception as e:
        logger.warning(
            f"activity_logger: suppressed error while logging portal lookup failure "
            f"for {action_id}({resource_id}): {type(e).__name__}: {e}"
        )


MISSING_DEFAULT_ROUTE_ACTION_ID = "route_observation"
MISSING_DEFAULT_ROUTE_TITLE = "Routing failed: connection has no default route"


async def log_missing_default_route(
    *, connection, observation_type: str, gundi_ids: List[str]
) -> None:
    """Publish an ERROR activity log for a provider whose connection has no
    default route, deduped per provider via Redis.

    Observations from such a provider cannot be routed and are discarded by
    the caller; this surfaces the configuration error in the portal.

    Never raises. A Redis or PubSub problem must not affect the caller's path.
    """
    provider_id = str(connection.provider.id)
    try:
        provider = connection.provider
        owner = provider.owner.name if provider.owner else None
        destinations = [str(d.id) for d in (connection.destinations or [])]
        routing_rules = [str(r.id) for r in (connection.routing_rules or [])]
        await _publish_once(
            dedup_key=_dedup_key_for_signature(
                MISSING_DEFAULT_ROUTE_ACTION_ID, provider_id, MISSING_DEFAULT_ROUTE_TITLE
            ),
            payload=CustomActivityLog(
                integration_id=provider_id,
                action_id=MISSING_DEFAULT_ROUTE_ACTION_ID,
                title=MISSING_DEFAULT_ROUTE_TITLE,
                level=LogLevel.ERROR,
                data={
                    "reason": "missing_default_route",
                    "provider": provider.name,
                    "owner": owner,
                    "destinations": destinations,
                    "routing_rules": routing_rules,
                    "observation_type": observation_type,
                    "discarded_count": len(gundi_ids),
                    "gundi_ids": [str(g) for g in gundi_ids],
                },
            ),
        )
    except Exception as e:
        logger.warning(
            f"activity_logger: suppressed error while logging missing default route "
            f"for provider {provider_id}: {type(e).__name__}: {e}"
        )
