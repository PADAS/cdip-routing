import logging
from typing import Dict

import requests
from hashlib import md5
import walrus
from cdip_connector.core import schemas

from app import settings
import random

logger = logging.getLogger(__name__)


def get_redis_db():
    return walrus.Database(host=settings.REDIS_HOST,
                           port=settings.REDIS_PORT,
                           db=settings.REDIS_DB)


def create_cache_key(hashable_string):
    return md5(str(hashable_string).encode('utf-8')).hexdigest()


def get_access_token(token_url: str,
                     client_id: str,
                     client_secret: str) -> schemas.OAuthToken:
    logger.debug(f'get_access_token from {token_url} using client_id: {client_id}')
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'audience': settings.KEYCLOAK_AUDIENCE,
        'grant_type': 'urn:ietf:params:oauth:grant-type:uma-ticket',
        'scope': 'openid',
    }

    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    logger.debug('get_access_token returning')
    return schemas.OAuthToken.parse_obj(response.json())


def get_auth_header() -> Dict[str, str]:
    token_object = get_access_token(settings.OAUTH_TOKEN_URL,
                                    settings.KEYCLOAK_CLIENT_ID,
                                    settings.KEYCLOAK_CLIENT_SECRET)
    return {
        "authorization": f"{token_object.token_type} {token_object.access_token}"
    }


def generate_random_execption():
    num = random.random()
    if num > .66:
        raise Exception()