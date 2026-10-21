import sys
import logging
import logging.config
from enum import Enum
from app.core import settings

logging_level = settings.LOGGING_LEVEL

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.json.JsonFormatter",
            "format": "%(asctime)s %(levelname)s %(processName)s %(thread)d %(name)s %(message)s",
            # Cloud Logging only reads the level from `severity`; Error Reporting
            # only picks up tracebacks from `stack_trace`.
            "rename_fields": {"levelname": "severity", "exc_info": "stack_trace"},
        },
    },
    "handlers": {
        "console": {
            "level": logging_level,
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "json",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": logging_level,
        },
        # uvicorn installs its own plain-text handlers before importing the app
        "uvicorn": {
            "handlers": ["console"],
            "level": logging_level,
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["console"],
            "level": logging_level,
            "propagate": False,
        },
    },
}

is_initialized = False


def init():
    global is_initialized

    if is_initialized:
        return

    logging.config.dictConfig(DEFAULT_LOGGING)

    is_initialized = True


def mark_logged(exc: BaseException) -> BaseException:
    """Flag an exception as logged with full context at a routing boundary,
    so the request boundary nacks it without logging it again."""
    exc.gundi_logged = True
    return exc


def is_logged(exc: BaseException) -> bool:
    return getattr(exc, "gundi_logged", False)


class ExtraKeys(str, Enum):
    def __str__(self):
        return str(self.value)

    DeviceId = "device_id"
    InboundIntId = "inbound_integration_id"
    IntegrationType = "integration_type"
    OutboundIntId = "outbound_integration_id"
    AttentionNeeded = "attention_needed"
    StreamType = "stream_type"
    DestinationType = "destination_type"
    Provider = "provider"
    Error = "error"
    Url = "url"
    Observation = "observation"
    RetryTopic = "retry_topic"
    RetryAt = "retry_at"
    RetryAttempt = "retry_attempt"
    StatusCode = "status_code"
    DeadLetter = "dead_letter"
    GundiVersion = "gundi_version"
    GundiId = "gundi_id"
    RelatedTo = "related_to"
