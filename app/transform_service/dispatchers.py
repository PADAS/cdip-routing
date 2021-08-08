import logging
from abc import ABC, abstractmethod
from typing import Any

from cdip_connector.core import schemas
from dasclient.dasclient import DasClient
from urllib.parse import urlparse
import os

logger = logging.getLogger(__name__)


class Dispatcher(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, config: schemas.OutboundConfiguration):
        self.configuration = config

    @abstractmethod
    def send(self, messages: list):
        ...


class ERDispatcher(Dispatcher, ABC):

    def __init__(self, config: schemas.OutboundConfiguration, provider: str):
        super().__init__(config)
        self.das_client = self.make_das_client(config, provider)
        # self.load_batch_size = 1000

    @staticmethod
    def make_das_client(config: schemas.OutboundConfiguration, provider: str) -> DasClient:

        provider_key = provider
        url_parse = urlparse(config.endpoint)

        return DasClient(service_root=config.endpoint,
                         username=config.login,
                         password=config.password,
                         token=config.token,
                         token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
                         client_id="das_web_client",
                         provider_key=provider_key)

    @staticmethod
    def generate_batches(data, batch_size=1000):
        num_obs = len(data)
        for start_index in range(0, num_obs, batch_size):
            yield data[start_index: min(start_index + batch_size, num_obs)]


class ERPositionDispatcher(ERDispatcher):

    def __init__(self, config, provider):
        super(ERPositionDispatcher, self).__init__(config, provider)

    def send(self, position: dict):
        result = None
        try:
            result = self.das_client.post_sensor_observation(position)
        except Exception as ex:
            logger.exception(f'exception raised sending to dest {ex}')
            raise ex
        return result


class ERGeoEventDispatcher(ERDispatcher):

    def __init__(self, config, provider):
        super(ERGeoEventDispatcher, self).__init__(config, provider)

    def send(self, messages: list):
        results = []
        for m in messages:
            try:
                results.append(self.das_client.post_report(m))
            except Exception as ex:
                logger.exception(f'exception raised sending to dest {ex}')
                raise ex
        return results


class ERCameraTrapDispatcher(ERDispatcher):

    def __init__(self, config, provider):
        super(ERCameraTrapDispatcher, self).__init__(config, provider)

    def send(self, camera_trap_payload: dict):
        result = None
        try:
            result = self.das_client.post_camera_trap_report(camera_trap_payload)
        except Exception as ex:
            logger.exception(f'exception raised sending to dest {ex}')
            raise ex
        finally:
            os.remove(camera_trap_payload.get("file"))
        return result

