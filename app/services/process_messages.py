import logging
from datetime import datetime, timezone
from app.core import tracing
from opentelemetry.trace import SpanKind
from gundi_core import schemas
from app.core.local_logging import ExtraKeys
from app.core.utils import Broker
from app.core.errors import ReferenceDataError
from app.services.event_handlers import event_handlers, event_schemas
from app.services.transformers import (
    extract_fields_from_message,
    build_gcp_pubsub_message,
    get_source_id,
    get_data_provider_id,
    transform_observation_to_destination_schema,
    build_transformed_message_attributes,
)
from app.core.gundi import (
    get_all_outbound_configs_for_id, update_observation_with_device_configuration,
)
from app.core import settings
from app.core.pubsub import send_message_to_gcp_pubsub_dispatcher, send_observation_to_dead_letter_topic


logger = logging.getLogger(__name__)


async def process_observation_event(raw_message, attributes):
    with tracing.tracer.start_as_current_span(
            "routing_service.process_observations_event", kind=SpanKind.CONSUMER
    ) as current_span:
        logger.debug(f"Message received: \npayload: {raw_message} \nattributes: {attributes}")
        current_span.add_event(name="routing_service.observations_received_at_consumer")
        event_type = raw_message.get("event_type")
        # ToDo: Check event schema version
        # ToDo: Discard duplicate events
        current_span.set_attribute("system_event_type", event_type)
        current_span.set_attribute("observation_type", str(raw_message.get("observation_type")))
        current_span.set_attribute("message", str(raw_message))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "cdip-routing")
        try:
            handler = event_handlers[event_type]
        except KeyError:
            logger.warning(f"Event Type {event_type} not  supported. Message discarded.")
            return
        try:
            schema = event_schemas[event_type]
        except KeyError:
            logger.warning(f"Event Schema for {event_type} not found. Message discarded.")
        parsed_event = schema.parse_obj(raw_message)
        return await handler(event=parsed_event)


async def process_observation(raw_observation, attributes):
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
        current_span.set_attribute("message", str(raw_observation))
        current_span.set_attribute("environment", settings.TRACE_ENVIRONMENT)
        current_span.set_attribute("service", "cdip-routing")
        try:
            logger.debug(f"message received")
            logger.debug(f"observation: {raw_observation}")
            logger.debug(f"attributes: {attributes}")
            # Get the schema version to process it accordingly
            gundi_version = attributes.get("gundi_version", "v1")
            schema = schemas.models_by_stream_type[raw_observation.get("observation_type")]
            observation = schema.parse_obj(raw_observation)
            observation_logging_extra = {
                ExtraKeys.DeviceId: observation.device_id,
                ExtraKeys.InboundIntId: observation.integration_id,
                ExtraKeys.StreamType: observation.observation_type,
                ExtraKeys.GundiVersion: "v1",
                ExtraKeys.GundiId: "",
                ExtraKeys.RelatedTo: "",
            }
            logger.info(
                f"Received unprocessed observation: {observation_logging_extra}",
                extra=observation_logging_extra,  # FixMe: Extra is ignored by GCP
            )
        except Exception as e:
            logger.exception(
                f"Exception occurred prior to processing observation: \n{observation_logging_extra} \n error: {e}",
                extra={   # FixMe: Extra is ignored by GCP
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.Observation: raw_observation,
                },
            )
            raise e

        try:
            if observation:
                await update_observation_with_device_configuration(observation)
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
                            ExtraKeys.DeviceId: observation.device_id,
                            ExtraKeys.InboundIntId: observation.integration_id,
                            ExtraKeys.StreamType: observation.observation_type,
                            ExtraKeys.GundiVersion: "v1",
                            ExtraKeys.GundiId: "",
                            ExtraKeys.AttentionNeeded: True,
                        },
                    )

                for destination in destinations:
                    # Get additional configuration for the destination
                    broker_config = destination.additional
                    # Transform the observation for the destination
                    transformed_observation = (
                        await transform_observation_to_destination_schema(
                            observation=observation,
                            destination=destination,
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
                        broker_config.get("broker", Broker.GCP_PUBSUB.value).strip().lower()
                    )
                    current_span.set_attribute("broker", broker_type)
                    if broker_type != Broker.GCP_PUBSUB.value:
                        raise ReferenceDataError(
                            f"Broker '{broker_type}' is no longer supported. Please use `{Broker.GCP_PUBSUB}` instead."
                        )
                    # Route to a GCP PubSub topic
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
            error_msg = (
                f"External error occurred obtaining reference data for observation: {e}",
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, gundi_version),
                    ExtraKeys.InboundIntId: get_data_provider_id(
                        observation, gundi_version
                    ),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            current_span.set_attribute("error", error_msg)
            raise e  # Raise the exception so the message is retried later by GCP
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
            # Unexpected internal errors
            current_span.set_attribute("error", error_msg)
            raise e  # Raise the exception so the message is retried later by GCP


def is_too_old(timestamp):
    if not timestamp:
        return False
    try:  # The timestamp does not always include the microseconds part
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        event_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    event_time = event_time.replace(tzinfo=timezone.utc)
    event_age_seconds = (datetime.now(timezone.utc) - event_time).seconds
    return event_age_seconds > settings.MAX_EVENT_AGE_SECONDS


async def process_request(request):
    # Extract the observation and attributes from the CloudEvent
    json_data = await request.json()
    payload, attributes = extract_fields_from_message(json_data["message"])
    # Load tracing context
    tracing.pubsub_instrumentation.load_context_from_attributes(attributes)
    with tracing.tracer.start_as_current_span(
        "routing_service.process_request", kind=SpanKind.CLIENT
    ) as current_span:
        if is_too_old(timestamp=request.headers.get("ce-time")):
            logger.warning(
                f"Message discarded. The message is too old or the retry time limit has been reached."
            )
            await send_observation_to_dead_letter_topic(payload, attributes)
            return {
                "status": "discarded",
                "reason": "Message is too old or the retry time limit has been reach",
            }
        if (version := attributes.get("gundi_version", "v1")) == "v1":
            await process_observation(payload, attributes)
        elif version == "v2":
            await process_observation_event(payload, attributes)
        else:
            logger.warning(
                f"Message discarded. Version '{version}' is not supported by this dispatcher."
            )
            await send_observation_to_dead_letter_topic(payload, attributes)
            return {
                "status": "discarded",
                "reason": f"Gundi '{version}' messages are not supported",
            }

        return {"status": "processed"}
