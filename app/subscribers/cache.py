import redis
import uuid
import json
import app.settings


cache = redis.Redis(
    host=app.settings.REDIS_HOST, port=app.settings.REDIS_PORT, db=app.settings.REDIS_DB
)


def ensure_patrol(patrol_label):

    patrol_ids = cache.get(patrol_label)

    if patrol_ids:
        return json.loads(patrol_ids)

    patrol_ids = {
        "patrol_uuid": str(uuid.uuid4()),
        "patrol_leg_uuid": str(uuid.uuid4()),
    }

    cache.setex(patrol_label, 86400 * 365, json.dumps(patrol_ids))

    return patrol_ids
