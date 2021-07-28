import logging
from abc import ABC, abstractmethod
from typing import Any

from cdip_connector.core import schemas
from dasclient.dasclient import DasClient

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

    def __init__(self, config: schemas.OutboundConfiguration):
        super().__init__(config)
        self.das_client = self.make_das_client(config)
        # self.load_batch_size = 1000

    @staticmethod
    def make_das_client(config: schemas.OutboundConfiguration) -> DasClient:
        inbound_type_slug = config.inbound_type_slug

        # get provider key from OIC.additional.
        # set to ib_type_slug if OIC.additional doesn't have an override.
        er_config = config.additional.get('earthranger', {})
        provider_key = er_config.get('provider_keys', {}).get(inbound_type_slug, inbound_type_slug)

        return DasClient(service_root=config.endpoint,
                         token=config.token,
                         provider_key=provider_key)

    @staticmethod
    def generate_batches(data, batch_size=1000):
        num_obs = len(data)
        for start_index in range(0, num_obs, batch_size):
            yield data[start_index: min(start_index + batch_size, num_obs)]


class ERPositionDispatcher(ERDispatcher):
    # stream_type = schemas.StreamPrefixEnum.position
    # destination_type = schemas.DestinationTypes.EarthRanger

    def __init__(self, config):
        super(ERPositionDispatcher, self).__init__(config)

    def send(self, position: dict):
        result = None
        try:
            result = self.das_client.post_sensor_observation(position)
        except Exception as ex:
            # todo: propagate exceptions back to caller
            logger.exception(f'exception raised sending to dest {ex}')
        return result


class ERGeoEventDispatcher(ERDispatcher):
    # stream_type = schemas.StreamPrefixEnum.position
    # destination_type = schemas.DestinationTypes.EarthRanger

    def __init__(self, config):
        super(ERGeoEventDispatcher, self).__init__(config)

    def send(self, messages: list):
        results = []
        for m in messages:
            try:
                results.append(self.das_client.post_report(m))
            except Exception as ex:
                # todo: propagate exceptions back to caller
                logger.exception(f'exception raised sending to dest {ex}')

        return results


class ERCameraTrapDispatcher(ERDispatcher):

    def __init__(self, config):
        super(ERCameraTrapDispatcher, self).__init__(config)

    def send(self, camera_trap_payload: dict):
        result = None
        try:
            # TODO: create new method in das client for posting to ER ?
            result = self.das_client.post_sensor_observation(camera_trap_payload)
        except Exception as ex:
            # todo: propagate exceptions back to caller
            logger.exception(f'exception raised sending to dest {ex}')
        return result

