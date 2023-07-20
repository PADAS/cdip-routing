import asyncio
import json
import logging
import aiohttp
from datetime import datetime
import backoff
import certifi
import faust
from app.core import tracing
from aiokafka.helpers import create_ssl_context
from cdip_connector.core.routing import TopicEnum
from cdip_connector.core import cdip_settings
from opentelemetry.trace import SpanKind
from app.core.local_logging import DEFAULT_LOGGING, ExtraKeys
from app.core.utils import (
    ReferenceDataError,
    DispatcherException,
    Broker,
    supported_brokers,
)
from app.subscribers.services import (
    extract_fields_from_message,
    convert_observation_to_cdip_schema,
    get_key_for_transformed_observation,
    dispatch_transformed_observation,
    wait_until_retry_at,
    update_attributes_for_transformed_retry,
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

cloud_enabled = cdip_settings.CONFLUENT_CLOUD_ENABLED
if cloud_enabled:
    logger.debug(f"Entering Confluent Cloud Enabled Flow")
    cert_path = certifi.where()
    logger.debug(f"cert path: {cert_path}")
    ssl_context = create_ssl_context(cafile=cert_path)

    """ Currently there are limitations on the basic Confluent Cloud account. Automatic topic creation is restricted
        which requires the disabling of the leader topic. This may have repercussions regarding the durability of this
        process. Any topics that are specified in code and utilized in the flow must be created ahead of time in the
        cloud.
    """
    logger.debug(
        f"username: {cdip_settings.CONFLUENT_CLOUD_USERNAME}, pw: {cdip_settings.CONFLUENT_CLOUD_PASSWORD}"
    )

    app = faust.App(
        routing_settings.FAUST_APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        broker_credentials=faust.SASLCredentials(
            username=cdip_settings.CONFLUENT_CLOUD_USERNAME,
            password=cdip_settings.CONFLUENT_CLOUD_PASSWORD,
            ssl_context=ssl_context,
            mechanism="PLAIN",
        ),
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
        topic_disable_leader=True,
    )
else:
    app = faust.App(
        routing_settings.FAUST_APP_ID,
        broker=f"{cdip_settings.KAFKA_BROKER}",
        value_serializer="raw",
        logging_config=DEFAULT_LOGGING,
    )

observations_unprocessed_topic = app.topic(TopicEnum.observations_unprocessed.value)
observations_unprocessed_retry_short_topic = app.topic(
    TopicEnum.observations_unprocessed_retry_short.value
)
observations_unprocessed_retry_long_topic = app.topic(
    TopicEnum.observations_unprocessed_retry_long.value
)
observations_unprocessed_deadletter = app.topic(
    TopicEnum.observations_unprocessed_deadletter.value
)
observations_transformed_topic = app.topic(TopicEnum.observations_transformed.value)
observations_transformed_retry_short_topic = app.topic(
    TopicEnum.observations_transformed_retry_short.value
)
observations_transformed_retry_long_topic = app.topic(
    TopicEnum.observations_transformed_retry_long.value
)
observations_transformed_deadletter = app.topic(
    TopicEnum.observations_transformed_deadletter.value
)

topics_dict = {
    TopicEnum.observations_unprocessed.value: observations_unprocessed_topic,
    TopicEnum.observations_unprocessed_retry_short: observations_unprocessed_retry_short_topic,
    TopicEnum.observations_unprocessed_retry_long: observations_unprocessed_retry_long_topic,
    TopicEnum.observations_unprocessed_deadletter: observations_unprocessed_deadletter,
    TopicEnum.observations_transformed: observations_transformed_topic,
    TopicEnum.observations_transformed_retry_short: observations_transformed_retry_short_topic,
    TopicEnum.observations_transformed_retry_long: observations_transformed_retry_long_topic,
    TopicEnum.observations_transformed_deadletter: observations_transformed_deadletter,
}


async def send_message_to_kafka_dispatcher(key, message, destination):
    key = get_key_for_transformed_observation(key, destination.id)
    with tracing.tracer.start_as_current_span(  # Trace observations with Open Telemetry
        "routing_service.send_message_to_kafka_dispatcher",
        kind=SpanKind.PRODUCER,
    ) as current_span:
        current_span.set_attribute("destination_id", str(destination.id))
        tracing_headers = tracing.faust_instrumentation.build_context_headers()
        await observations_transformed_topic.send(
            key=key,
            value=message,
            headers=tracing_headers,  # Tracing context
        )
        current_span.add_event(
            name="routing_service.transformed_observation_sent_to_dispatcher"
        )


@backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=20)
async def send_message_to_gcp_pubsub_dispatcher(message, attributes, destination, broker_config):
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
                response = await client.publish(topic, messages, timeout=int(timeout_settings.total))
            except Exception as e:
                error_msg = f"Error sending observation to PubSub topic {topic_name}: {e}."
                logger.exception(error_msg)
                current_span.set_attribute("error", error_msg)
                raise e
            else:
                logger.info(f"Observation sent successfully.")
                logger.debug(f"GCP PubSub response: {response}")
        current_span.add_event(
            name="routing_service.transformed_observation_sent_to_dispatcher"
        )


@tracing.faust_instrumentation.load_context
async def process_observation(key, message):
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
            observation = convert_observation_to_cdip_schema(raw_observation, gundi_version=gundi_version)
            observation_logging_extra = {
                ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                ExtraKeys.InboundIntId: get_data_provider_id(observation, gundi_version),
                ExtraKeys.StreamType: observation.observation_type,
                ExtraKeys.GundiVersion: gundi_version,
                ExtraKeys.GundiId: observation.gundi_id if gundi_version == "v2" else observation.id,
                ExtraKeys.RelatedTo: observation.related_to if gundi_version == "v2" else ""
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
                await apply_source_configurations(observation=observation, gundi_version=gundi_version)
                if gundi_version == "v2":
                    # ToDo: Implement a destination resolution algorithm considering all the routes and filters
                    connection = await get_connection(connection_id=observation.data_provider_id)
                    destinations = connection.destinations
                    default_route = await get_route(route_id=connection.default_route.id)
                    route_configuration = default_route.configuration
                    # ToDo: Get provider key from the route configuration
                    provider_key = str(observation.data_provider_id)
                else:  # Default to v1
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
                            ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                            ExtraKeys.InboundIntId: get_data_provider_id(observation, gundi_version),
                            ExtraKeys.StreamType: observation.observation_type,
                            ExtraKeys.GundiVersion: gundi_version,
                            ExtraKeys.GundiId: observation.gundi_id if gundi_version == "v2" else observation.id,
                            ExtraKeys.AttentionNeeded: True,
                        },
                    )

                for destination in destinations:
                    # Transform the observation for the destination
                    transformed_observation = transform_observation_to_destination_schema(
                        observation=observation,
                        destination=destination,
                        route_configuration=route_configuration,
                        gundi_version=gundi_version
                    )
                    if not transformed_observation:
                        continue
                    attributes = build_transformed_message_attributes(
                        observation=observation,
                        destination=destination,
                        gundi_version=gundi_version,
                        provider_key=provider_key
                    )
                    logger.debug(
                        f"Transformed observation: {transformed_observation}, attributes: {attributes}"
                    )

                    # Check which broker to use to route the message
                    if gundi_version == "v2":
                        destination_integration = await get_integration(integration_id=destination.id)
                        broker_config = destination_integration.additional
                    else:
                        broker_config = destination.additional
                    broker_type = broker_config.get("broker", Broker.KAFKA.value).strip().lower()
                    current_span.set_attribute("broker", broker_type)
                    if broker_type not in supported_brokers:
                        raise ReferenceDataError(
                            f"Invalid broker type `{broker_type}` at outbound config. Supported brokers are: {supported_brokers}"
                        )
                    if broker_type == Broker.KAFKA.value:  # Route to a kafka topic
                        if gundi_version == "v2":
                            raise ReferenceDataError(
                                f"Kafka is not supported in Gundi v2. Please use `{Broker.GCP_PUBSUB}` instead."
                            )
                        kafka_message = build_kafka_message(
                            payload=transformed_observation, attributes=attributes
                        )
                        await send_message_to_kafka_dispatcher(
                            key=key, message=kafka_message, destination=destination
                        )
                        logger.info(
                            "Observation transformed and sent to kafka topic successfully.",
                            extra={
                                **observation_logging_extra,
                                **attributes,
                                "destination_id": str(destination.id)
                            },
                        )
                    elif broker_type == Broker.GCP_PUBSUB.value:
                        pubsub_message = build_gcp_pubsub_message(
                            payload=transformed_observation
                        )
                        await send_message_to_gcp_pubsub_dispatcher(
                            message=pubsub_message,
                            attributes=attributes,
                            destination=destination,
                            broker_config=broker_config
                        )
                        logger.info(
                            "Observation transformed and sent to pubsub topic successfully.",
                            extra={
                                **observation_logging_extra,
                                **attributes,
                                "destination_id": str(destination.id)
                            },
                        )
            else:
                logger.error(
                    "Logic error, expecting 'observation' to be not None.",
                    extra={
                        ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                        ExtraKeys.InboundIntId: get_data_provider_id(observation, gundi_version),
                        ExtraKeys.StreamType: observation.observation_type,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
        except ReferenceDataError:
            logger.exception(
                f"External error occurred obtaining reference data for observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, gundi_version),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            await process_failed_unprocessed_observation(key, message)

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
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, gundi_version),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_unprocessed_deadletter.send(
                value=message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


@tracing.faust_instrumentation.load_context
async def process_transformed_observation(key, transformed_message):
    with tracing.tracer.start_as_current_span(
        "routing_service.process_transformed_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.add_event(
            name="routing_service.transformed_observation_received_at_dispatcher"
        )
        current_span.set_attribute("transformed_message", str(transformed_message))
        current_span.set_attribute("environment", routing_settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "cdip-routing")
        try:
            transformed_observation, attributes = extract_fields_from_message(
                transformed_message
            )

            observation_type = attributes.get("observation_type")
            device_id = attributes.get("device_id")
            integration_id = attributes.get("integration_id")
            outbound_config_id = attributes.get("outbound_config_id")
            retry_attempt: int = attributes.get("retry_attempt") or 0
            logger.debug(f"transformed_observation: {transformed_observation}")
            logger.info(
                "received transformed observation",
                extra={
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                    ExtraKeys.RetryAttempt: retry_attempt,
                },
            )

        except Exception as e:
            logger.exception(
                f"Exception occurred prior to dispatching transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Observation: transformed_message,
                },
            )
            raise e
        try:
            logger.info(
                "Dispatching for transformed observation.",
                extra={
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )
            with tracing.tracer.start_as_current_span(
                "routing_service.dispatch_transformed_observation", kind=SpanKind.CLIENT
            ) as current_span:
                await dispatch_transformed_observation(
                    observation_type,
                    outbound_config_id,
                    integration_id,
                    transformed_observation,
                )
                current_span.set_attribute("is_dispatched_successfully", True)
                current_span.set_attribute("destination_id", str(outbound_config_id))
                current_span.add_event(
                    name="routing_service.observation_dispatched_successfully"
                )
        except (DispatcherException, ReferenceDataError):
            logger.exception(
                f"External error occurred processing transformed observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )
            await process_failed_transformed_observation(key, transformed_message)

        except Exception:
            error_msg = (
                "Unexpected internal error occurred processing transformed observation"
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeadLetter: True,
                    ExtraKeys.DeviceId: device_id,
                    ExtraKeys.InboundIntId: integration_id,
                    ExtraKeys.OutboundIntId: outbound_config_id,
                    ExtraKeys.StreamType: observation_type,
                },
            )
            # Unexpected internal errors will be redirected straight to deadletter
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_transformed_deadletter.send(
                value=transformed_message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


@tracing.faust_instrumentation.load_context
async def process_failed_transformed_observation(key, transformed_message):
    with tracing.tracer.start_as_current_span(
        "routing_service.process_failed_transformed_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            transformed_observation, attributes = extract_fields_from_message(
                transformed_message
            )
            attributes = update_attributes_for_transformed_retry(attributes)
            observation_type = attributes.get("observation_type")
            device_id = attributes.get("device_id")
            integration_id = attributes.get("integration_id")
            outbound_config_id = attributes.get("outbound_config_id")
            retry_topic_str = attributes.get("retry_topic")
            retry_attempt = attributes.get("retry_attempt")
            retry_transformed_message = create_retry_message(
                transformed_observation, attributes
            )
            retry_topic: faust.Topic = topics_dict.get(retry_topic_str)
            extra_dict = {
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.OutboundIntId: outbound_config_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryTopic: retry_topic_str,
                ExtraKeys.RetryAttempt: retry_attempt,
                ExtraKeys.Observation: transformed_observation,
            }
            current_span.set_attribute("retries", retry_attempt)
            current_span.set_attribute("retry_topic", retry_topic_str)
            if retry_topic_str != TopicEnum.observations_transformed_deadletter.value:
                logger.info(
                    "Putting failed transformed observation back on queue",
                    extra=extra_dict,
                )
                current_span.add_event(
                    name="routing_service.send_observation_to_retry_topic"
                )
            else:
                logger.exception(
                    "Retry attempts exceeded for transformed observation, sending to dead letter",
                    extra={
                        **extra_dict,
                        ExtraKeys.AttentionNeeded: True,
                        ExtraKeys.DeadLetter: True,
                    },
                )
                current_span.set_attribute("is_sent_to_dead_letter_queue", True)
                current_span.add_event(
                    name="routing_service.observation_sent_to_dead_letter_queue"
                )

            current_span.set_attribute("destination_id", str(outbound_config_id))
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await retry_topic.send(
                value=retry_transformed_message, headers=tracing_headers
            )
        except Exception as e:
            error_msg = "Unexpected Error occurred while preparing failed transformed observation for reprocessing"
            logger.exception(
                error_msg,
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            # When all else fails post to dead letter
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_transformed_deadletter.send(
                value=transformed_message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


@tracing.faust_instrumentation.load_context
async def process_failed_unprocessed_observation(key, message):
    with tracing.tracer.start_as_current_span(
        "routing_service.process_failed_unprocessed_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            raw_observation, attributes = extract_fields_from_message(message)
            attributes = update_attributes_for_unprocessed_retry(attributes)
            observation_type = attributes.get("observation_type")
            device_id = attributes.get("device_id")
            integration_id = attributes.get("integration_id")
            retry_topic_str = attributes.get("retry_topic")
            retry_attempt = attributes.get("retry_attempt")
            retry_unprocessed_message = create_retry_message(
                raw_observation, attributes
            )
            retry_topic: faust.Topic = topics_dict.get(retry_topic_str)
            extra_dict = {
                ExtraKeys.DeviceId: device_id,
                ExtraKeys.InboundIntId: integration_id,
                ExtraKeys.StreamType: observation_type,
                ExtraKeys.RetryTopic: retry_topic_str,
                ExtraKeys.RetryAttempt: retry_attempt,
                ExtraKeys.Observation: raw_observation,
            }
            current_span.set_attribute("retries", retry_attempt)
            current_span.set_attribute("retry_topic", retry_topic_str)
            if retry_topic_str != TopicEnum.observations_unprocessed_deadletter.value:
                logger.info(
                    "Putting failed unprocessed observation back on queue",
                    extra=extra_dict,
                )
                current_span.add_event(
                    name="routing_service.send_observation_to_retry_topic"
                )
            else:
                logger.exception(
                    "Retry attempts exceeded for unprocessed observation, sending to dead letter",
                    extra={
                        **extra_dict,
                        ExtraKeys.AttentionNeeded: True,
                        ExtraKeys.DeadLetter: True,
                    },
                )
                current_span.set_attribute("is_sent_to_dead_letter_queue", True)
                current_span.add_event(
                    name="routing_service.observation_sent_to_dead_letter_queue"
                )

            outbound_config_id = attributes.get("outbound_config_id")
            current_span.set_attribute("destination_id", str(outbound_config_id))
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await retry_topic.send(
                value=retry_unprocessed_message, headers=tracing_headers
            )
        except Exception as e:
            # When all else fails post to dead letter
            error_msg = "Unexpected Error occurred while preparing failed unprocessed observation for reprocessing"
            logger.exception(
                error_msg,
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_unprocessed_deadletter.send(
                value=message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


@tracing.faust_instrumentation.load_context
async def process_transformed_retry_observation(key, transformed_message):
    with tracing.tracer.start_as_current_span(
        "routing_service.process_transformed_retry_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            transformed_observation, attributes = extract_fields_from_message(
                transformed_message
            )
            retry_at = datetime.fromisoformat(attributes.get("retry_at"))
            await wait_until_retry_at(retry_at)
            await process_transformed_observation(key, transformed_message)
        except Exception as e:
            error_msg = "Unexpected Error occurred while attempting to process failed transformed observation"
            logger.exception(
                error_msg,
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            # When all else fails post to dead letter
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_transformed_deadletter.send(
                value=transformed_message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


async def process_retry_observation(key, message):
    # When all else fails post to dead letter
    with tracing.tracer.start_as_current_span(
        "routing_service.process_retry_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            raw_observation, attributes = extract_fields_from_message(message)
            retry_at = datetime.fromisoformat(attributes.get("retry_at"))
            await wait_until_retry_at(retry_at)
            await process_observation(key, message)
        except Exception as e:
            error_msg = "Unexpected Error occurred while attempting to process failed unprocessed observation"
            logger.exception(
                error_msg,
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
            )
            current_span.set_attribute("error", error_msg)
            tracing_headers = tracing.faust_instrumentation.build_context_headers()
            await observations_unprocessed_deadletter.send(
                value=message, headers=tracing_headers
            )
            current_span.set_attribute("is_sent_to_dead_letter_queue", True)
            current_span.add_event(
                name="routing_service.observation_sent_to_dead_letter_queue"
            )


if routing_settings.ENABLE_UNPROCESSED_TOPIC:

    @tracing.faust_instrumentation.load_context
    @app.agent(
        observations_unprocessed_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED,
    )
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
                    tracing_headers = (
                        tracing.faust_instrumentation.build_context_headers()
                    )
                    await observations_unprocessed_deadletter.send(
                        value=message, headers=tracing_headers
                    )
                    current_span.set_attribute("is_sent_to_dead_letter_queue", True)
                    current_span.add_event(
                        name="routing_service.observation_sent_to_dead_letter_queue"
                    )


if routing_settings.ENABLE_UNPROCESSED_RETRY_SHORT:

    @app.agent(
        observations_unprocessed_retry_short_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED_RETRY_SHORT,
    )
    async def process_retry_short_observations(streaming_data):
        async for key, message in streaming_data.items():
            await process_retry_observation(key, message)


if routing_settings.ENABLE_UNPROCESSED_RETRY_LONG:

    @app.agent(
        observations_unprocessed_retry_long_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_UNPROCESSED_RETRY_LONG,
    )
    async def process_retry_long_observations(streaming_data):
        async for key, message in streaming_data.items():
            await process_retry_observation(key, message)


if routing_settings.ENABLE_TRANSFORMED_TOPIC:

    @tracing.faust_instrumentation.load_context
    @app.agent(
        observations_transformed_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED,
    )
    async def process_transformed_observations(streaming_transformed_data):
        async for key, transformed_message in streaming_transformed_data.items():
            try:
                await process_transformed_observation(key, transformed_message)
            except Exception as e:
                error_msg = (
                    "Unexpected error prior to processing transformed observation"
                )
                logger.exception(
                    error_msg,
                    extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.DeadLetter: True},
                )
                # When all else fails post to dead letter
                with tracing.tracer.start_as_current_span(
                    "routing_service.error_prior_to_processing_transformed_observation",
                    kind=SpanKind.PRODUCER,
                ) as current_span:
                    current_span.set_attribute("error", error_msg)
                    tracing_headers = (
                        tracing.faust_instrumentation.build_context_headers()
                    )
                    await observations_transformed_deadletter.send(
                        value=transformed_message, headers=tracing_headers
                    )
                    current_span.set_attribute("is_sent_to_dead_letter_queue", True)
                    current_span.add_event(
                        name="routing_service.observation_sent_to_dead_letter_queue"
                    )


if routing_settings.ENABLE_TRANSFORMED_RETRY_SHORT:

    @app.agent(
        observations_transformed_retry_short_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED_RETRY_SHORT,
    )
    async def process_transformed_retry_short_observations(streaming_transformed_data):
        async for key, transformed_message in streaming_transformed_data.items():
            await process_transformed_retry_observation(key, transformed_message)


if routing_settings.ENABLE_TRANSFORMED_RETRY_LONG:

    @app.agent(
        observations_transformed_retry_long_topic,
        concurrency=routing_settings.ROUTING_CONCURRENCY_TRANSFORMED_RETRY_LONG,
    )
    async def process_transformed_retry_long_observations(streaming_transformed_data):
        async for key, transformed_message in streaming_transformed_data.items():
            await process_transformed_retry_observation(key, transformed_message)


@app.timer(interval=120.0)
async def log_metrics(app):
    m = app.monitor
    metrics_dict = {
        "messages_received_by_topic": m.messages_received_by_topic,
        "messages_sent_by_topic": m.messages_sent_by_topic,
        "messages_active": m.messages_active,
        "assignment_latency": m.assignment_latency,
        "send_errors": m.send_errors,
        "rebalances": m.rebalances,
        "rebalance_return_avg": m.rebalance_return_avg,
    }
    logger.info(f"Metrics heartbeat for Consumer", extra=metrics_dict)


# @app.on_rebalance_start()
# async def on_rebalance_start():
#     pass


if __name__ == "__main__":
    logger.info("Application getting started")
    app.main()
