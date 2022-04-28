import logging
import random
from hashlib import md5
from typing import Dict
from datetime import datetime, timezone, timedelta
from uuid import UUID

import requests
import walrus
from cdip_connector.core import schemas, cdip_settings

from app import settings

logger = logging.getLogger(__name__)


def get_redis_db():
    logger.debug(f"Connecting to REDIS DB :{settings.REDIS_DB} at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
    return walrus.Database(host=settings.REDIS_HOST,
                           port=settings.REDIS_PORT,
                           db=settings.REDIS_DB)


def create_cache_key(hashable_string):
    return md5(str(hashable_string).encode('utf-8')).hexdigest()


class PortalAuthException(Exception):
    pass


def get_access_token(token_url: str,
                     client_id: str,
                     client_secret: str) -> schemas.OAuthToken:
    logger.debug('get_access_token from %s using client_id: %s', token_url, client_id)
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'audience': cdip_settings.KEYCLOAK_AUDIENCE,
        'grant_type': 'urn:ietf:params:oauth:grant-type:uma-ticket',
        'scope': 'openid',
    }

    response = requests.post(token_url, data=payload)

    if not response.ok:
        raise PortalAuthException('Failed getting access token from Portal. (%s)', response.text)

    logger.debug('get_access_token returning')
    return schemas.OAuthToken.parse_obj(response.json())


auth_gen = None


def get_auth_header(refresh=False) -> Dict[str, str]:
    global auth_gen
    if not auth_gen or refresh:
        auth_gen = auth_generator()
    try:
        token = next(auth_gen)
    except StopIteration:
        auth_gen = auth_generator()
        token = next(auth_gen)
    return {
        "authorization": f"{token.token_type} {token.access_token}"
    }


def auth_generator():
    '''
    Simple generator to provide a header and keep it for a designated TTL.
    '''
    expire_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

    while True:
        present = datetime.now(tz=timezone.utc)
        try:
            if expire_at <= present:
                token = get_access_token(cdip_settings.OAUTH_TOKEN_URL,
                                         cdip_settings.KEYCLOAK_CLIENT_ID,
                                         cdip_settings.KEYCLOAK_CLIENT_SECRET)

                ttl_seconds = token.expires_in - 5
                expire_at = present + timedelta(seconds=ttl_seconds)
            if logger.isEnabledFor(logging.DEBUG):
                ttl = (expire_at - present).total_seconds()
                logger.debug(f'Using cached auth, expires in {ttl} seconds.')

        except: # Catch all exceptions to avoid a fast, endless loop.
            logger.exception('Failed to authenticate with Portal API.')
        else:
            yield token


def is_uuid(*, id_str: str):
    try:
        uuid_obj = UUID(id_str)
        return True
    except ValueError:
        return False


if __name__ == '__main__':

    while not input().startswith('q'):
        print(get_auth_header())
