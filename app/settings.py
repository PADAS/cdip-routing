from cdip_connector.core import cdip_settings
from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str('LOGGING_LEVEL', 'INFO')

PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = f'{cdip_settings.PORTAL_API_ENDPOINT}/integrations/outbound/configurations'
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = f'{cdip_settings.PORTAL_API_ENDPOINT}/integrations/inbound/configurations'

# Settings for caching admin portal request/responses
REDIS_HOST = env.str('REDIS_HOST', 'localhost')
REDIS_PORT = env.int('REDIS_PORT', 6379)
REDIS_DB = env.int('REDIS_DB', 3)

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int('PORTAL_CONFIG_OBJECT_CACHE_TTL', 60)

RETRY_SHORT_ATTEMPTS = env.int('RETRY_SHORT_ATTEMPTS', 6)
RETRY_LONG_ATTEMPTS = env.int('RETRY_LONG_ATTEMPTS', 12)
RETRY_SHORT_DELAY_MINUTES = env.int('RETRY_SHORT_DELAY_MINUTES', 5)
RETRY_LONG_DELAY_MINUTES = env.int('RETRY_LONG_DELAY_MINUTES', 30)
