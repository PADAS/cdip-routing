import asyncio
import json
import logging
import aiohttp
from datetime import datetime
import backoff
import certifi
from app.core import tracing
from opentelemetry.trace import SpanKind
from app.core.local_logging import DEFAULT_LOGGING, ExtraKeys
from app.core.utils import (
    ReferenceDataError,
    Broker,
    supported_brokers,
)
from app.subscribers.services import (
    extract_fields_from_message,
    convert_observation_to_cdip_schema,
    get_key_for_transformed_observation,
    wait_until_retry_at,
    create_retry_message,
    update_attributes_for_unprocessed_retry,
    build_kafka_message,
    build_gcp_pubsub_message,
)
from app.transform_service.services import (
    get_source_id,
    get_data_provider_id,
    apply_source_configurations,
    transform_observation_to_destination_schema,
    get_all_outbound_configs_for_id,
    build_transformed_message_attributes,
    get_connection,
    get_route,
    get_integration,
)
import app.settings as routing_settings
from gcloud.aio import pubsub


logger = logging.getLogger(__name__)


@backoff.on_exception(
    backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=20
)
async def send_message_to_gcp_pubsub_dispatcher(
    message, attributes, destination, broker_config
):
    with tracing.tracer.start_as_current_span(  # Trace observations with Open Telemetry
        "routing_service.send_message_to_gcp_pubsub_dispatcher",
        kind=SpanKind.PRODUCER,
    ) as current_span:
        destination_id_str = str(destination.id)
        current_span.set_attribute("destination_id", destination_id_str)
        # Propagate OTel context in message attributes
        tracing_context = json.dumps(
            tracing.pubsub_instrumentation.build_context_headers(),
            default=str,
        )
        attributes["tracing_context"] = tracing_context
        timeout_settings = aiohttp.ClientTimeout(total=60.0)
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=timeout_settings
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic name from config or use a default naming convention
            topic_name = broker_config.get(
                "topic",
                f"destination-{destination_id_str}-{routing_settings.GCP_ENVIRONMENT}",
            ).strip()
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(routing_settings.GCP_PROJECT_ID, topic_name)
            messages = [pubsub.PubsubMessage(message, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:
                response = await client.publish(
                    topic, messages, timeout=int(timeout_settings.total)
                )
            except Exception as e:
                error_msg = (
                    f"Error sending observation to PubSub topic {topic_name}: {e}."
                )
                logger.exception(error_msg)
                current_span.set_attribute("error", error_msg)
                raise e
            else:
                logger.info(f"Observation sent successfully.")
                logger.debug(f"GCP PubSub response: {response}")
        current_span.add_event(
            name="routing_service.transformed_observation_sent_to_dispatcher"
        )


def get_provider_key(provider):
    return f"gundi_{provider.type.value}_{str(provider.id)}"


# ToDo: Refactor this to process PubSub messages
async def process_observation(message):
    """
    Handle one message that has not yet been processed.
    This function transforms a message into an appropriate Model and
    it decorates it with Device-specific information fetched from
    Gundi's portal.
    """
    # ToDo: Consider splitting this in two functions for gundi v1 and gundi v2
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
        "routing_service.process_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.add_event(name="routing_service.observations_received_at_consumer")
        current_span.set_attribute("message", str(message))
        current_span.set_attribute("environment", routing_settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "cdip-routing")
        try:
            logger.debug(f"message received: {message}")
            raw_observation, attributes = extract_fields_from_message(message)
            logger.debug(f"observation: {raw_observation}")
            logger.debug(f"attributes: {attributes}")
            # Get the schema version to process it accordingly
            gundi_version = attributes.get("gundi_version", "v1")
            observation = convert_observation_to_cdip_schema(
                raw_observation, gundi_version=gundi_version
            )
            observation_logging_extra = {
                ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                ExtraKeys.InboundIntId: get_data_provider_id(
                    observation, gundi_version
                ),
                ExtraKeys.StreamType: observation.observation_type,
                ExtraKeys.GundiVersion: gundi_version,
                ExtraKeys.GundiId: observation.gundi_id
                if gundi_version == "v2"
                else observation.id,
                ExtraKeys.RelatedTo: observation.related_to
                if gundi_version == "v2"
                else "",
            }
            logger.info(
                "received unprocessed observation",
                extra=observation_logging_extra,
            )
        except Exception as e:
            logger.exception(
                f"Exception occurred prior to processing observation",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.Observation: message},
            )
            raise e

        try:
            if observation:
                # Process the observation differently according to the Gundi Version
                await apply_source_configurations(
                    observation=observation, gundi_version=gundi_version
                )
                if gundi_version == "v2":
                    # ToDo: Implement a destination resolution algorithm considering all the routes and filters
                    connection = await get_connection(
                        connection_id=observation.data_provider_id
                    )
                    provider = connection.provider
                    destinations = connection.destinations
                    default_route = await get_route(
                        route_id=connection.default_route.id
                    )
                    route_configuration = default_route.configuration
                    provider_key = get_provider_key(
                        provider
                    )  # i.e. gundi_cellstop_abc1234..
                else:  # Default to v1
                    provider = None
                    route_configuration = None
                    provider_key = None
                    destinations = await get_all_outbound_configs_for_id(
                        observation.integration_id, observation.device_id
                    )
                current_span.set_attribute("destinations_qty", len(destinations))
                current_span.set_attribute(
                    "destinations", str([str(d.id) for d in destinations])
                )
                if len(destinations) < 1:
                    current_span.add_event(
                        name="routing_service.observation_has_no_destinations"
                    )
                    logger.warning(
                        "Updating observation with Device info, but it has no Destinations. This is a configuration error.",
                        extra={
                            ExtraKeys.DeviceId: get_source_id(
                                observation, gundi_version
                            ),
                            ExtraKeys.InboundIntId: get_data_provider_id(
                                observation, gundi_version
                            ),
                            ExtraKeys.StreamType: observation.observation_type,
                            ExtraKeys.GundiVersion: gundi_version,
                            ExtraKeys.GundiId: observation.gundi_id
                            if gundi_version == "v2"
                            else observation.id,
                            ExtraKeys.AttentionNeeded: True,
                        },
                    )

                for destination in destinations:
                    # Get additional configuration for the destination
                    if gundi_version == "v2":
                        destination_integration = await get_integration(
                            integration_id=destination.id
                        )
                        broker_config = destination_integration.additional
                    else:
                        broker_config = destination.additional
                    # Transform the observation for the destination
                    transformed_observation = (
                        await transform_observation_to_destination_schema(
                            observation=observation,
                            destination=destination_integration
                            if gundi_version == "v2"
                            else destination,
                            provider=provider,
                            route_configuration=route_configuration,
                            gundi_version=gundi_version,
                        )
                    )
                    if not transformed_observation:
                        continue
                    attributes = build_transformed_message_attributes(
                        observation=observation,
                        destination=destination,
                        gundi_version=gundi_version,
                        provider_key=provider_key,
                    )
                    logger.debug(
                        f"Transformed observation: {transformed_observation}, attributes: {attributes}"
                    )

                    broker_type = (
                        broker_config.get("broker", Broker.KAFKA.value).strip().lower()
                    )
                    current_span.set_attribute("broker", broker_type)
                    if broker_type not in supported_brokers:
                        raise ReferenceDataError(
                            f"Invalid broker type `{broker_type}` at outbound config. Supported brokers are: {supported_brokers}"
                        )
                    if broker_type == Broker.KAFKA.value:  # Route to a kafka topic
                        raise ReferenceDataError(
                            f"Kafka is no longer supported. Please use `{Broker.GCP_PUBSUB}` instead."
                        )
                    elif broker_type == Broker.GCP_PUBSUB.value:
                        pubsub_message = build_gcp_pubsub_message(
                            payload=transformed_observation
                        )
                        await send_message_to_gcp_pubsub_dispatcher(
                            message=pubsub_message,
                            attributes=attributes,
                            destination=destination,
                            broker_config=broker_config,
                        )
                        logger.info(
                            "Observation transformed and sent to pubsub topic successfully.",
                            extra={
                                **observation_logging_extra,
                                **attributes,
                                "destination_id": str(destination.id),
                            },
                        )
            else:
                logger.error(
                    "Logic error, expecting 'observation' to be not None.",
                    extra={
                        ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                        ExtraKeys.InboundIntId: get_data_provider_id(
                            observation, gundi_version
                        ),
                        ExtraKeys.StreamType: observation.observation_type,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
        except ReferenceDataError as e:
            logger.exception(
                f"External error occurred obtaining reference data for observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                    ExtraKeys.InboundIntId: get_data_provider_id(
                        observation, gundi_version
                    ),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            # ToDo: refactor to use PubSub retries
            # await process_failed_unprocessed_observation(message)
            raise e

        except Exception as e:
            error_msg = (
                f"Unexpected internal exception occurred processing observation: {e}"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                    ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                    ExtraKeys.InboundIntId: get_data_provider_id(
                        observation, gundi_version
                    ),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            # ToDo: refactor to use PubSub
            # await observations_unprocessed_deadletter.send(
            #     value=message, headers=tracing_headers
            # )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


# ToDo: refactor to use PubSub
async def process_observations(streaming_data):
    async for key, message in streaming_data.items():
        try:
            await process_observation(key, message)
        except Exception as e:
            error_msg = f"Unexpected error prior to processing observation"
            logger.exception(
                error_msg,
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            # When all else fails post to dead letter
            with tracing.tracer.start_as_current_span(
                "routing_service.process_observations", kind=SpanKind.PRODUCER
            ) as current_span:
                current_span.set_attribute("error", error_msg)
                # ToDo: refactor to use PubSub
                # await observations_unprocessed_deadletter.send(
                #     value=message, headers=tracing_headers
                # )
                current_span.set_attribute("is_sent_to_dead_letter_queue", True)
                current_span.add_event(
                    name="routing_service.observation_sent_to_dead_letter_queue"
                )


if __name__ == "__main__":
    # ToDo: refactor to use FastAPI
    logger.info("Application getting started")
    app.main()
