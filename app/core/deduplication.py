import logging
from enum import Enum, IntEnum

import backoff
from redis import exceptions as redis_exceptions

from app.core import settings
from app.core.utils import get_redis_db


logger = logging.getLogger(__name__)


_cache_db = get_redis_db()


class EventProcessingStatus(IntEnum):
    PROCESSED = 1
    UNPROCESSED = 0


def get_event_status_key(event_id):
    return f"processed_events.{event_id}"


async def get_event_processing_status(event_id) -> EventProcessingStatus:
    @backoff.on_exception(backoff.expo, redis_exceptions.RedisError, max_time=10)
    async def read_from_redis(key):
        return await _cache_db.get(key)

    status = EventProcessingStatus.UNPROCESSED
    key = get_event_status_key(event_id)
    try:
        value = await read_from_redis(key)
        status = EventProcessingStatus(value)
    except redis_exceptions.RedisError as e:
        logger.warning(
            f"RedisError while reading key '{key}' from Redis:{type(e)} \n {e}",
        )
    except Exception as e:
        logger.warning(
            f"Unknown Error while reading key '{key}' from Redis:{type(e)} \n {e}",
        )
    finally:
        return status


async def set_event_processing_status(event_id, status: EventProcessingStatus):
    @backoff.on_exception(backoff.expo, redis_exceptions.RedisError, max_time=10)
    async def write_to_redis(key, ttl, value):
        return await _cache_db.setex(key, ttl, value)

    key = get_event_status_key(event_id)
    try:
        await write_to_redis(key, settings.EVENT_PROCESSING_STATUS_TTL, status.value)
    except redis_exceptions.RedisError as e:
        logger.warning(
            f"RedisError while writing status for key '{key}' from Redis:{type(e)} \n {e}",
        )
    except Exception as e:
        logger.warning(
            f"Unknown Error while writing status for key '{key}' from Redis:{type(e)} \n {e}",
        )


async def is_event_processed(event_id):
    status = await get_event_processing_status(event_id)
    return status == EventProcessingStatus.PROCESSED
