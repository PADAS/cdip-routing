import logging
import sys

from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str("LOGGING_LEVEL", "INFO")

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": LOGGING_LEVEL,
        },
    },
}
logging.config.dictConfig(DEFAULT_LOGGING)

DEFAULT_REQUESTS_TIMEOUT = (10, 20)  # Connect, Read

CDIP_API_ENDPOINT = env.str("CDIP_API_ENDPOINT", None)
CDIP_ADMIN_ENDPOINT = env.str("CDIP_ADMIN_ENDPOINT", None)
CDIP_ADMIN_SSL_VERIFY = env.bool("CDIP_ADMIN_SSL_VERIFY", True)
PORTAL_API_ENDPOINT = f"{CDIP_ADMIN_ENDPOINT}/api/v1.0"
PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/outbound/configurations"
)
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = (
    f"{PORTAL_API_ENDPOINT}/integrations/inbound/configurations"
)

# Settings for caching admin portal request/responses
REDIS_HOST = env.str("REDIS_HOST", "localhost")
REDIS_PORT = env.int("REDIS_PORT", 6379)
REDIS_DB = env.int("REDIS_DB", 3)

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int("PORTAL_CONFIG_OBJECT_CACHE_TTL", 60)

TRACE_ENVIRONMENT = env.str("TRACE_ENVIRONMENT", "dev")
TRACING_ENABLED = env.bool("TRACING_ENABLED", True)

# GCP project ID is required to route messages to PubSub
GCP_PROJECT_ID = env.str("GCP_PROJECT_ID", "cdip-78ca")
GCP_ENVIRONMENT = env.str("GCP_ENVIRONMENT", "dev")
DEAD_LETTER_TOPIC = env.str("DEAD_LETTER_TOPIC", "transformer-dead-letter-dev")
MAX_EVENT_AGE_SECONDS = env.int("MAX_EVENT_AGE_SECONDS", 86400)  # 24hrs
