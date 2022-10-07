import sys
import logging
import logging.config
from enum import Enum
from app import settings

logging_level = settings.LOGGING_LEVEL

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s %(levelname)s %(processName)s %(thread)d %(name)s %(message)s",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
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
        # Reduce flood of debug messages from following modules when in debug mode
        "mode.timers": {
            "handlers": ["console"],
            "level": "WARNING",
        },
        "aiokafka.consumer.fetcher": {
            "handlers": ["console"],
            "level": "INFO",
        },
        "aiokafka.conn": {
            "handlers": ["console"],
            "level": "INFO",
        },
        "aiokafka.consumer.group_coordinator": {
            "handlers": ["console"],
            "level": "INFO",
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


class ExtraKeys(str, Enum):
    def __str__(self):
        return str(self.value)

    ObservationId = "observation_id"
    DeviceId = "device_id"
    InboundIntId = "inbound_integration_id"
    OutboundIntId = "outbound_integration_id"
    AttentionNeeded = "attention_needed"
    StreamType = "stream_type"
    Provider = "provider"
    Error = "error"
    Url = "url"
    Observation = "observation"
    RetryTopic = "retry_topic"
    RetryAt = "retry_at"
    RetryAttempt = "retry_attempt"
    StatusCode = "status_code"
    DeadLetter = "dead_letter"


class Tracing(str, Enum):
    def __str__(self):
        return str(self.value)

    TimeStamp = "time_stamp"
    TracingMilestone = "tracing_milestone"
    Latency = "latency_seconds"
    # milestones
    MilestoneSensorsAPIReceived = "sensors_api_received"
    MilestoneUnprocessedObservationReceived = "unprocessed_observation_received"
    MilestoneTransformedObservationReceived = "transformed_observation_received"
    MilestoneTransformedObservationDispatched = "transformed_observation_dispatched"
