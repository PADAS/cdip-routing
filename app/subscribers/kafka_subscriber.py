import json
import logging
import aiohttp
from datetime import datetime
import certifi
import faust
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
    get_all_outbound_configs_for_id,
    update_observation_with_device_configuration,
    transform_observation,
)
import app.settings as routing_settings
from app.core import tracing
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


async def send_message_to_gcp_pubsub_dispatcher(message, attributes, destination):
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
        async with aiohttp.ClientSession(
            raise_for_status=True, timeout=20.0
        ) as session:
            client = pubsub.PublisherClient(session=session)
            # Get the topic name from the outbound config or use a default following a naming convention
            topic_name = destination.additional.get(
                "topic",
                f"destination-{destination_id_str}-{routing_settings.GCP_ENVIRONMENT}",
            ).strip()
            current_span.set_attribute("topic", topic_name)
            topic = client.topic_path(routing_settings.GCP_PROJECT_ID, topic_name)
            messages = [pubsub.PubsubMessage(message, **attributes)]
            logger.info(f"Sending observation to PubSub topic {topic_name}..")
            try:
                response = await client.publish(topic, messages)
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

            observation = convert_observation_to_cdip_schema(raw_observation)
            logger.info(
                "received unprocessed observation",
                extra={
                    ExtraKeys.DeviceId: observation.device_id,
                    ExtraKeys.InboundIntId: observation.integration_id,
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
        except Exception as e:
            logger.exception(
                f"Exception occurred prior to processing observation",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.Observation: message},
            )
            raise e

        try:
            if observation:
                observation = await update_observation_with_device_configuration(
                    observation
                )
                int_id = observation.integration_id
                destinations = await get_all_outbound_configs_for_id(
                    int_id, observation.device_id
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
                            ExtraKeys.DeviceId: observation.device_id,
                            ExtraKeys.InboundIntId: observation.integration_id,
                            ExtraKeys.StreamType: observation.observation_type,
                            ExtraKeys.AttentionNeeded: True,
                        },
                    )

                for destination in destinations:
                    # Transform the observation for the destination
                    transformed_observation = transform_observation(
                        observation=observation,
                        stream_type=observation.observation_type,
                        config=destination,
                    )
                    if not transformed_observation:
                        continue
                    attributes = {
                        "observation_type": str(observation.observation_type),
                        "device_id": str(observation.device_id),
                        "outbound_config_id": str(destination.id),
                        "integration_id": str(observation.integration_id),
                    }
                    logger.debug(
                        f"Transformed observation: {transformed_observation}, attributes: {attributes}"
                    )

                    # Check which broker to use to route the message
                    broker_type = (
                        destination.additional.get("broker", Broker.KAFKA.value)
                        .strip()
                        .lower()
                    )
                    current_span.set_attribute("broker", broker_type)
                    if broker_type not in supported_brokers:
                        raise ReferenceDataError(
                            f"Invalid broker type `{broker_type}` at outbound config. Supported brokers are: {supported_brokers}"
                        )
                    if broker_type == Broker.KAFKA.value:  # Route to a kafka topic
                        kafka_message = build_kafka_message(
                            payload=transformed_observation, attributes=attributes
                        )
                        await send_message_to_kafka_dispatcher(
                            key=key, message=kafka_message, destination=destination
                        )
                    elif broker_type == Broker.GCP_PUBSUB.value:
                        pubsub_message = build_gcp_pubsub_message(
                            payload=transformed_observation
                        )
                        await send_message_to_gcp_pubsub_dispatcher(
                            message=pubsub_message,
                            attributes=attributes,
                            destination=destination,
                        )
            else:
                logger.error(
                    "Logic error, expecting 'observation' to be not None.",
                    extra={
                        ExtraKeys.DeviceId: observation.device_id,
                        ExtraKeys.InboundIntId: observation.integration_id,
                        ExtraKeys.StreamType: observation.observation_type,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )
        except ReferenceDataError:
            logger.exception(
                f"External error occurred obtaining reference data for observation",
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: observation.device_id,
                    ExtraKeys.InboundIntId: observation.integration_id,
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
                    ExtraKeys.DeviceId: observation.device_id,
                    ExtraKeys.InboundIntId: observation.integration_id,
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
