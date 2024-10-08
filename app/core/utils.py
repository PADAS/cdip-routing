import logging
from enum import Enum
import aioredis
import walrus
from hashlib import md5
from uuid import UUID
from app import settings


logger = logging.getLogger(__name__)


class Broker(str, Enum):
    def __str__(self):
        return str(self.value)

    KAFKA = "kafka"
    GCP_PUBSUB = "gcp_pubsub"


supported_brokers = {Broker.GCP_PUBSUB.value}


def get_redis_db():
    logger.debug(
        f"Connecting to REDIS DB :{settings.REDIS_DB} at {settings.REDIS_HOST}:{settings.REDIS_PORT}"
    )
    return aioredis.from_url(
        f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )


def create_cache_key(hashable_string):
    return md5(str(hashable_string).encode("utf-8")).hexdigest()


def is_uuid(*, id_str: str):

    if type(id_str) is UUID:
        return True

    if type(id_str) is not str:
        return False

    try:
        uuid_obj = UUID(id_str)
        return True
    except ValueError:
        return False


def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v is not None), None)


def is_valid_position(gundi_version: str, location):
    if not location:
        return False
    return (
        all([location.y, location.x])
        if gundi_version == "v1"
        else all([location.lat, location.lon])
    )


def get_provider_key(provider):
    return f"gundi_{provider.type.value}_{str(provider.id)}"
