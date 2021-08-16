import sys
import logging
import logging.config
from app import settings

logging_level = settings.LOGGING_LEVEL

DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            'format': '%(asctime)s %(levelname)s %(processName)s %(thread)d %(name)s %(message)s',
            'class': 'pythonjsonlogger.jsonlogger.JsonFormatter',
        },
    },
    'handlers': {
        'console': {
            'level': logging_level,
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'json'
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': logging_level,
        },

    }
}

is_initialized = False


def init():
    global is_initialized

    if is_initialized:
        return

    logging.config.dictConfig(DEFAULT_LOGGING)

    is_initialized = True

