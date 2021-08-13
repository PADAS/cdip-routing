from environs import Env

env = Env()
env.read_env()

LOGGING_LEVEL = env.str('LOGGING_LEVEL', 'INFO')

KEYCLOAK_ISSUER = env.str('KEYCLOAK_ISSUER')
KEYCLOAK_CLIENT_ID = env.str('KEYCLOAK_CLIENT_ID')
KEYCLOAK_CLIENT_SECRET = env.str('KEYCLOAK_CLIENT_SECRET')
KEYCLOAK_AUDIENCE = env.str('KEYCLOAK_AUDIENCE')

OAUTH_TOKEN_URL = f'{KEYCLOAK_ISSUER}/protocol/openid-connect/token'

PORTAL_ENDPOINT = env.str('PORTAL_ENDPOINT')
PORTAL_API_ENDPOINT = f'{PORTAL_ENDPOINT}/api/v1.0'
PORTAL_OUTBOUND_INTEGRATIONS_ENDPOINT = f'{PORTAL_API_ENDPOINT}/integrations/outbound/configurations'
PORTAL_INBOUND_INTEGRATIONS_ENDPOINT = f'{PORTAL_API_ENDPOINT}/integrations/inbound/configurations'
PORTAL_SSL_VERIFY = env.bool('PORTAL_SSL_VERIFY', True)

# Settings for caching admin portal request/responses
REDIS_HOST = env.str('REDIS_HOST', 'localhost')
REDIS_PORT = env.int('REDIS_PORT', 6739)
REDIS_DB = env.int('REDIS_DB', 3)
# N-seconds window to keep hash of portal api response
REDIS_CHECK_SECONDS = env.int('REDIS_CHECK_SECONDS', 120)

# N-seconds to cache portal responses for configuration objects.
PORTAL_CONFIG_OBJECT_CACHE_TTL = env.int('PORTAL_CONFIG_OBJECT_CACHE_TTL', 60)

# Providing defaults so that this does not break application if not defined when kafka is used
GOOGLE_PUB_SUB_PROJECT_ID = env.str('GOOGLE_PUB_SUB_PROJECT_ID', 'string')
GOOGLE_APPLICATION_CREDENTIALS = env.str('GOOGLE_APPLICATION_CREDENTIALS', 'string')
STREAMING_SUBSCRIPTION_NAME = env.str('STREAMING_SUBSCRIPTION_NAME', 'streaming-subscription')
STREAMING_TRANSFORMED_SUBSCRIPTION_NAME = env.str('STREAMING_TRANSFORMED_SUBSCRIPTION_NAME', 'streaming-transformed-subscription')
STREAMING_TRANSFORMED_TOPIC_NAME = env.str('STREAMING_TRANSFORMED_TOPIC_NAME', 'streaming-transformed-topic')
TRANSFORM_SERVICE_ENDPOINT = env.str('TRANSFORM_SERVICE_ENDPOINT', 'http://127.0.0.1:8200')
TRANSFORM_SERVICE_POSITIONS_ENDPOINT = f'{TRANSFORM_SERVICE_ENDPOINT}/streaming/position'

KAFKA_BROKER = env.str('KAFKA_BROKER')

CONFLUENT_CLOUD_ENABLED = env.bool('CONFLUENT_CLOUD_ENABLED', False)
CONFLUENT_CLOUD_USERNAME = env.str('CONFLUENT_CLOUD_USERNAME')
CONFLUENT_CLOUD_PASSWORD = env.str('CONFLUENT_CLOUD_PASSWORD')
