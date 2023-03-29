import json
import logging
import mimetypes
import os
from abc import ABC, abstractmethod
from typing import Union
from urllib.parse import urlparse

import requests
from cdip_connector.core import schemas
from cdip_connector.core.cloudstorage import get_cloud_storage
from erclient import AsyncERClient
from smartconnect import SmartClient
from smartconnect.models import SMARTRequest, SMARTCompositeRequest

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = (3.1, 20)


class Dispatcher(ABC):
    stream_type: schemas.StreamPrefixEnum
    destination_type: schemas.DestinationTypes

    def __init__(self, config: schemas.OutboundConfiguration):
        self.configuration = config

    @abstractmethod
    async def send(self, messages: list):
        ...


class ERDispatcher(Dispatcher, ABC):
    DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

    def __init__(self, config: schemas.OutboundConfiguration, provider: str):
        super().__init__(config)
        self.er_client = self.make_er_client(config, provider)
        # self.load_batch_size = 1000

    @staticmethod
    def make_er_client(
        config: schemas.OutboundConfiguration, provider: str
    ) -> AsyncERClient:
        provider_key = provider
        url_parse = urlparse(config.endpoint)

        return AsyncERClient(
            service_root=config.endpoint,
            username=config.login,
            password=config.password,
            token=config.token,
            token_url=f"{url_parse.scheme}://{url_parse.hostname}/oauth2/token",
            client_id="das_web_client",
            provider_key=provider_key,
            connect_timeout=ERDispatcher.DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )

    @staticmethod
    def generate_batches(data, batch_size=1000):
        num_obs = len(data)
        for start_index in range(0, num_obs, batch_size):
            yield data[start_index : min(start_index + batch_size, num_obs)]


class ERPositionDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERPositionDispatcher, self).__init__(config, provider)

    async def send(self, position: dict):
        result = None
        try:
            result = await self.er_client.post_sensor_observation(position)
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        finally:
            await self.er_client.close()
        return result


class ERGeoEventDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERGeoEventDispatcher, self).__init__(config, provider)

    async def send(self, messages: Union[list, dict]):
        results = []
        if isinstance(messages, dict):
            messages = [messages]

        async with self.er_client as client:
            for m in messages:
                try:
                    results.append(await client.post_report(m))
                except Exception as ex:
                    logger.exception(f"exception raised sending to dest {ex}")
                    raise ex
        return results


class ERCameraTrapDispatcher(ERDispatcher):
    def __init__(self, config, provider):
        super(ERCameraTrapDispatcher, self).__init__(config, provider)
        self.cloud_storage = get_cloud_storage()

    async def send(self, camera_trap_payload: dict):
        result = None
        try:
            file_name = camera_trap_payload.get("file")
            file = self.cloud_storage.download(file_name)
            result = await self.er_client.post_camera_trap_report(
                camera_trap_payload, file
            )
        except Exception as ex:
            logger.exception(f"exception raised sending to dest {ex}")
            raise ex
        finally:
            self.cloud_storage.remove(file)
            await self.er_client.close()
        return result


class SmartConnectDispatcher:
    def __init__(self, config: schemas.OutboundConfiguration):
        self.config = config

    def clean_smart_composite_request(self, val:SMARTCompositeRequest):

        for item in val.waypoint_requests:
            if hasattr(item.properties.smartAttributes, 'observationUuid'):
                if item.properties.smartAttributes.observationUuid == 'None':
                    item.properties.smartAttributes.observationUuid = None


    def send(self, item: dict):
        item = SMARTCompositeRequest.parse_obj(item)

        self.clean_smart_composite_request(item)

        # orchestration order of operations
        smartclient = SmartClient(
            api=self.config.endpoint,
            username=self.config.login,
            password=self.config.password,
            version=self.config.additional.get("version"),
        )
        for patrol_request in item.patrol_requests:
            smartclient.post_smart_request(
                json=patrol_request.json(exclude_none=True), ca_uuid=item.ca_uuid
            )
        for waypoint_request in item.waypoint_requests:
            smartclient.post_smart_request(
                json=waypoint_request.json(exclude_none=True), ca_uuid=item.ca_uuid
            )
        for track_point_request in item.track_point_requests:
            smartclient.post_smart_request(
                json=track_point_request.json(exclude_none=True), ca_uuid=item.ca_uuid
            )
        return


class WPSWatchCameraTrapDispatcher:
    def __init__(self, config: schemas.OutboundConfiguration):
        self.config = config
        self.cloud_storage = get_cloud_storage()

    def send(self, camera_trap_payload: dict):
        try:
            file_name = camera_trap_payload.get("Attachment1")
            file = self.cloud_storage.download(file_name)
            file_data = self.get_file_data(file_name, file)
            result = self.wpswatch_post(camera_trap_payload, file_data)
        except Exception as ex:
            logger.exception(f"exception raised sending to WPS Watch {ex}")
            raise ex
        finally:
            self.cloud_storage.remove(file)
        return

    def wpswatch_post(self, camera_trap_payload, file_data=None):
        sanitized_endpoint = self.sanitize_endpoint(
            f"{self.config.endpoint}/api/Upload"
        )
        headers = {
            "Wps-Api-Key": self.config.token,
        }
        files = {"Attachment1": file_data}

        body = camera_trap_payload
        try:
            response = requests.post(
                sanitized_endpoint,
                data=body,
                headers=headers,
                files=files,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.exceptions.Timeout as ex:
            logger.exception("Timeout occurred posting to WPS Watch", extra=body)
            raise ex
        response.raise_for_status()
        return response

    def get_file_data(self, file_name, file):
        name, file_ext = os.path.splitext(file_name)
        mimetype = mimetypes.types_map[file_ext]
        return file_name, file, mimetype

    @staticmethod
    def sanitize_endpoint(endpoint):
        scheme = urlparse(endpoint).scheme
        host = urlparse(endpoint).hostname
        path = urlparse(endpoint).path.replace(
            "//", "/"
        )  # in case tailing forward slash configured in portal
        sanitized_endpoint = f"{scheme}://{host}{path}"
        return sanitized_endpoint
