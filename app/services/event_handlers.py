import logging
from gundi_core.events import (
    ObservationReceived,
    ObservationsBatchReceived,
    EventReceived,
    EventUpdateReceived,
    AttachmentReceived,
    TextMessageReceived,
    GundiDelivery,
    ProviderInfo,
    ERObservationsBatch,
    ObservationsBatchTransformedER,
    TransformedERObservationItem,
)
from gundi_core.schemas.v2 import StreamPrefixEnum, ERObservation
from gundi_core.events.transformers import (
    EventTransformedER,
    EventUpdateTransformedER,
    AttachmentTransformedER,
    ObservationTransformedER,
    EventTransformedSMART,
    EventUpdateTransformedSMART,
    EventTransformedWPSWatch,
    AttachmentTransformedWPSWatch,
    EventTransformedTrapTagger,
    AttachmentTransformedTrapTagger,
    MessageTransformedER,
    MessageTransformedInReach,
)
from opentelemetry.trace import SpanKind
from app.core import settings, tracing
from app.core.errors import ReferenceDataError
from app.core.gundi import get_connection, get_route, get_integration
from app.core.local_logging import ExtraKeys
from app.core.utils import Broker
from app.core.utils import get_provider_key
from app.core.pubsub import send_message_to_gcp_pubsub_dispatcher
from app.services.activity_logger import log_missing_default_route
from app.services.transformers import (
    build_transformed_message_attributes,
    build_gcp_pubsub_message,
    get_source_id,
    get_data_provider_id,
    transform_observation_v2,
)


logger = logging.getLogger(__name__)


def _uses_generic_model(destination_integration) -> bool:
    """Whether a destination uses the generic-model path (publish a
    GundiDelivery for its action runner to transform) instead of a legacy
    in-process Transformer.

    Decided by the destination's integration *type* (e.g. ``cmore``) via
    ``settings.GENERIC_MODEL_DESTINATION_TYPES`` so it requires no per-integration
    config. The legacy per-integration ``additional.generic_model`` flag is still
    honored as an override for one-off opt-in.
    """
    type_value = getattr(getattr(destination_integration, "type", None), "value", None)
    if type_value and type_value in settings.GENERIC_MODEL_DESTINATION_TYPES:
        return True
    return bool((destination_integration.additional or {}).get("generic_model"))


transformer_events_by_data_type = {
    "EREvent": EventTransformedER,
    "EREventUpdate": EventUpdateTransformedER,
    "ERAttachment": AttachmentTransformedER,
    "ERObservation": ObservationTransformedER,
    "SMARTCompositeRequest": EventTransformedSMART,
    "SMARTUpdateRequest": EventUpdateTransformedSMART,
    "WPSWatchImageMetadata": EventTransformedWPSWatch,
    "WPSWatchImage": AttachmentTransformedWPSWatch,
    "TrapTaggerImageMetadata": EventTransformedTrapTagger,
    "TrapTaggerImage": AttachmentTransformedTrapTagger,
    "ERMessage": MessageTransformedER,
    "InReachIPCMessage": MessageTransformedInReach,
}


def build_transformer_event(transformed_observation):
    Event = transformer_events_by_data_type[type(transformed_observation).__name__]
    return Event(payload=transformed_observation)


def _build_provider_info(provider) -> ProviderInfo:
    provider_type = ""
    if getattr(provider, "type", None) is not None:
        provider_type = getattr(provider.type, "value", "") or ""

    owner_id = ""
    owner_name = ""
    if getattr(provider, "owner", None) is not None:
        owner_id = str(getattr(provider.owner, "id", "") or "")
        owner_name = getattr(provider.owner, "name", "") or ""

    return ProviderInfo(
        provider_id=str(provider.id),
        provider_type=provider_type,
        provider_name=getattr(provider, "name", "") or "",
        owner_id=owner_id,
        owner_name=owner_name,
    )


def _build_gundi_delivery(*, observation, provider, route_configuration) -> GundiDelivery:
    return GundiDelivery(
        payload=observation,
        route_configuration=route_configuration,
        provider=_build_provider_info(provider),
    )


async def _publish_gundi_delivery(
    *,
    observation,
    destination,
    destination_integration,
    provider,
    provider_key,
    route_configuration,
    broker_config,
    destination_str,
    provider_str,
    current_span,
):
    """Generic-model publish path: wrap the Gundi payload in a GundiDelivery
    envelope and publish to the destination's PubSub topic. The action runner
    is responsible for transformation."""

    # Validate broker — same restriction as the legacy path.
    broker_value = (broker_config or {}).get("broker", Broker.GCP_PUBSUB.value).strip().lower()
    if broker_value != Broker.GCP_PUBSUB.value:
        current_span.set_attribute("broker", broker_value)
        raise ReferenceDataError(
            f"Broker '{broker_value}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
        )

    try:
        delivery = _build_gundi_delivery(
            observation=observation,
            provider=provider,
            route_configuration=route_configuration,
        )
    except Exception as e:
        error_msg = (
            f"Error building GundiDelivery for observation {observation.gundi_id} "
            f"from {provider_str} for destination {destination_str}: "
            f"{type(e).__name__}: {e}. Discarded."
        )
        logger.exception(error_msg)
        current_span.set_attribute("error", error_msg)
        current_span.set_attribute("is_discarded", True)
        current_span.add_event(
            name="routing_service.observation_discarded_on_generic_envelope_error"
        )
        return

    attributes = build_transformed_message_attributes(
        observation=observation,
        destination=destination,
        gundi_version="v2",
        provider_key=provider_key,
    )

    pubsub_message = build_gcp_pubsub_message(
        payload=delivery.dict(exclude_none=True)
    )
    ordering_key = (
        str(observation.gundi_id)
        if observation.observation_type == StreamPrefixEnum.event_update.value
        else ""
    )
    await send_message_to_gcp_pubsub_dispatcher(
        message=pubsub_message,
        attributes=attributes,
        destination=destination,
        broker_config=broker_config,
        ordering_key=ordering_key,
    )
    logger.info(
        f"Observation {observation.gundi_id} published as GundiDelivery to {destination_str}.",
        extra=attributes,
    )


async def _discard_on_missing_default_route(
    *, connection, observations, observation_type, current_span
):
    """Log, trace and emit an activity log for a connection with no default route.

    This is a portal configuration error (the provider has no routing rule), so
    the observations cannot be routed. Callers discard them instead of raising,
    because raising makes PubSub retry a message that can never succeed.
    """
    provider = connection.provider
    provider_id = str(provider.id)
    owner_name = provider.owner.name if provider.owner else None
    gundi_ids = [str(o.gundi_id) for o in observations]
    destinations = [str(d.id) for d in (connection.destinations or [])]
    routing_rules = [str(r.id) for r in (connection.routing_rules or [])]
    error_msg = (
        f"Connection '{owner_name} - {provider.name}'({provider_id}) has no default route. "
        f"This is a configuration error. "
        f"{len(gundi_ids)} {observation_type} observation(s) discarded: {gundi_ids}. "
        f"destinations={destinations} routing_rules={routing_rules}"
    )
    logger.error(
        error_msg,
        extra={
            ExtraKeys.AttentionNeeded: True,
            ExtraKeys.InboundIntId: provider_id,
            ExtraKeys.Provider: provider.name,
            ExtraKeys.StreamType: observation_type,
            ExtraKeys.GundiVersion: "v2",
            ExtraKeys.GundiId: gundi_ids[0] if len(gundi_ids) == 1 else gundi_ids,
            "destinations": destinations,
            "routing_rules": routing_rules,
        },
    )
    current_span.set_attribute("error", error_msg)
    current_span.set_attribute("is_discarded", True)
    current_span.add_event(
        name="routing_service.observation_discarded_on_missing_default_route"
    )
    await log_missing_default_route(
        connection=connection,
        observation_type=observation_type,
        gundi_ids=gundi_ids,
    )


async def transform_and_route_observation(observation):
    with tracing.tracer.start_as_current_span(
        "routing_service.transform_and_route_observation", kind=SpanKind.CONSUMER
    ) as current_span:
        try:
            # ToDo: Implement a destination resolution algorithm considering all the routes and filters
            connection = await get_connection(
                connection_id=observation.data_provider_id
            )
            if not connection:
                error = f"Connection '{observation.data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            if not connection.default_route:
                await _discard_on_missing_default_route(
                    connection=connection,
                    observations=[observation],
                    observation_type=observation.observation_type,
                    current_span=current_span,
                )
                return
            provider = connection.provider
            destinations = connection.destinations
            default_route = await get_route(
                route_id=connection.default_route.id,
                data_provider_id=observation.data_provider_id,
            )
            if not default_route:
                error = f"Default route '{connection.default_route.id}', for provider '{observation.data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            route_configuration = default_route.configuration
            provider_key = get_provider_key(provider)  # i.e. gundi_cellstop_abc1234..
            current_span.set_attribute("destinations_qty", len(destinations))
            current_span.set_attribute(
                "destinations", str([str(d.id) for d in destinations])
            )
            if len(destinations) < 1:
                current_span.add_event(
                    name="routing_service.observation_has_no_destinations"
                )
                logger.warning(
                    f"Connection {observation.data_provider_id} has no Destinations. This is a configuration error.",
                    extra={
                        ExtraKeys.DeviceId: observation.source_id,
                        ExtraKeys.InboundIntId: observation.data_provider_id,
                        ExtraKeys.StreamType: observation.observation_type,
                        ExtraKeys.GundiVersion: "v2",
                        ExtraKeys.GundiId: observation.gundi_id,
                        ExtraKeys.AttentionNeeded: True,
                    },
                )

            provider_str = f"'{connection.provider.owner.name} - {connection.provider.name}'({connection.provider.id})"
            for destination in destinations:
                # Get additional configuration for the destination
                destination_integration = await get_integration(
                    integration_id=destination.id
                )
                broker_config = destination_integration.additional
                destination_str = (
                    f"'{destination.owner.name} - {destination.name}'({destination.id})"
                )

                # Generic-model path: publish a GundiDelivery envelope and let
                # the action runner perform destination-specific transformation.
                if _uses_generic_model(destination_integration):
                    await _publish_gundi_delivery(
                        observation=observation,
                        destination=destination,
                        destination_integration=destination_integration,
                        provider=provider,
                        provider_key=provider_key,
                        route_configuration=route_configuration,
                        broker_config=broker_config,
                        destination_str=destination_str,
                        provider_str=provider_str,
                        current_span=current_span,
                    )
                    continue

                # Transform the observation for the destination
                try:
                    transformed_observation = await transform_observation_v2(
                        observation=observation,
                        destination=destination_integration,
                        provider=provider,
                        route_configuration=route_configuration,
                    )
                except Exception as e:
                    error_msg = f"Error transforming observation {observation.gundi_id} from {provider_str} for destination {destination_str}: {type(e).__name__}: {e}. Discarded."
                    logger.exception(error_msg)
                    current_span.set_attribute("error", error_msg)
                    current_span.set_attribute("is_discarded", True)
                    current_span.add_event(
                        name="routing_service.observation_discarded_on_transformer_error"
                    )
                    continue  # Skip this destination and try the next one

                if not transformed_observation:
                    logger.warning(
                        f"Observation {observation.gundi_id} from {provider_str} could not be transformed for destination {destination_str}. Discarded."
                    )
                    current_span.set_attribute("is_discarded", True)
                    current_span.add_event(
                        name="routing_service.observation_discarded_by_transformer"
                    )
                    continue

                logger.debug(
                    f"Observation {observation.gundi_id} from {provider_str} transformed for destination {destination_str}."
                )
                # Add metadata used to dispatch the observation
                attributes = build_transformed_message_attributes(
                    observation=observation,
                    destination=destination,
                    gundi_version="v2",
                    provider_key=getattr(
                        transformed_observation, "provider_key", provider_key
                    ),  # Field mappings overrides take precedence
                )
                logger.debug(
                    f"Transformed observation: {repr(transformed_observation)}, attributes: {attributes}"
                )

                if (
                    broker_type := broker_config.get("broker", Broker.GCP_PUBSUB.value)
                    .strip()
                    .lower()
                    != Broker.GCP_PUBSUB.value
                ):
                    current_span.set_attribute("broker", broker_type)
                    raise ReferenceDataError(
                        f"Broker '{broker_type}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
                    )

                # Build message for dispatcher
                if isinstance(transformed_observation, dict):
                    # Pass the data as a raw dict for backward compatibility with older dispatchers (e.g. Movebank)
                    pubsub_message_payload = transformed_observation
                else:
                    # Build system event using pydantic models
                    pubsub_message_payload = build_transformer_event(
                        transformed_observation
                    ).dict(exclude_none=True)

                # Publish to a GCP PubSub topic
                pubsub_message = build_gcp_pubsub_message(
                    payload=pubsub_message_payload
                )
                # Set ordering key only for updates
                ordering_key = (
                    str(observation.gundi_id)
                    if observation.observation_type
                    == StreamPrefixEnum.event_update.value
                    else ""
                )
                await send_message_to_gcp_pubsub_dispatcher(
                    message=pubsub_message,
                    attributes=attributes,
                    destination=destination,
                    broker_config=broker_config,
                    ordering_key=ordering_key,
                )
                logger.info(
                    f"Observation {observation.gundi_id} transformed and sent to pubsub topic successfully.",
                    extra=attributes,
                )
        except ReferenceDataError as e:
            error_msg = (
                f"External error occurred obtaining reference data for observation: {e}",
            )
            logger.exception(
                error_msg,
                extra={
                    ExtraKeys.AttentionNeeded: True,
                    ExtraKeys.DeviceId: get_source_id(observation, "v2"),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, "v2"),
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
                    ExtraKeys.DeviceId: get_source_id(observation, "v2"),
                    ExtraKeys.InboundIntId: get_data_provider_id(observation, "v2"),
                    ExtraKeys.StreamType: observation.observation_type,
                },
            )
            # Unexpected internal errors
            current_span.set_attribute("error", error_msg)
            raise e  # Raise the exception so the message is retried later by GCP


async def _publish_transformed_batch_group(
    *,
    batch,
    items,
    effective_provider_key,
    destination,
    broker_config,
):
    er_batch = ERObservationsBatch(
        batch_id=batch.batch_id,
        data_provider_id=batch.data_provider_id,
        destination_id=str(destination.id),
        provider_key=effective_provider_key,
        items=items,
    )
    envelope = ObservationsBatchTransformedER(payload=er_batch)
    attributes = {
        "gundi_version": "v2",
        "batch": "true",
        "batch_count": str(len(items)),
        "provider_key": effective_provider_key,
        "stream_type": batch.observation_type,
        "destination_id": str(destination.id),
        "data_provider_id": str(batch.data_provider_id),
    }
    pubsub_message = build_gcp_pubsub_message(payload=envelope.dict(exclude_none=True))
    await send_message_to_gcp_pubsub_dispatcher(
        message=pubsub_message,
        attributes=attributes,
        destination=destination,
        broker_config=broker_config,
        ordering_key="",
    )
    logger.info(
        f"Batch {batch.batch_id}: {len(items)} observations transformed and sent to destination {destination.id}.",
        extra=attributes,
    )


async def transform_and_route_observations_batch(batch):
    with tracing.tracer.start_as_current_span(
        "routing_service.transform_and_route_observations_batch", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("batch_id", str(batch.batch_id))
        current_span.set_attribute("batch_count", len(batch.observations))
        if not batch.observations:
            return
        try:
            data_provider_id = str(batch.data_provider_id)
            # ONE connection/route lookup for the whole batch — every item
            # shares the provider by the envelope invariant.
            connection = await get_connection(connection_id=data_provider_id)
            if not connection:
                error = f"Connection '{data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            if not connection.default_route:
                await _discard_on_missing_default_route(
                    connection=connection,
                    observations=batch.observations,
                    observation_type=batch.observation_type,
                    current_span=current_span,
                )
                return
            provider = connection.provider
            default_route = await get_route(
                route_id=connection.default_route.id,
                data_provider_id=data_provider_id,
            )
            if not default_route:
                error = f"Default route '{connection.default_route.id}', for provider '{data_provider_id}' not found."
                current_span.set_attribute("error", error)
                raise ReferenceDataError(error)
            route_configuration = default_route.configuration
            provider_key = get_provider_key(provider)
            destinations = connection.destinations
            current_span.set_attribute("destinations_qty", len(destinations))

            provider_str = f"'{connection.provider.owner.name} - {connection.provider.name}'({connection.provider.id})"
            for destination in destinations:
                destination_integration = await get_integration(
                    integration_id=destination.id
                )
                broker_config = destination_integration.additional
                destination_str = (
                    f"'{destination.owner.name} - {destination.name}'({destination.id})"
                )

                # Validate the broker once per destination, before any
                # transform/publish work — same restriction the single-item
                # path (transform_and_route_observation) and the generic-model
                # publish path (_publish_gundi_delivery) already enforce per
                # item. Hoisted here so a batch can't slip an unsupported
                # broker past this check the way per-item publishing would
                # have caught it.
                # str() + `or` guard: `additional.broker` can be present but
                # null in portal data; .strip() on None would fail the whole
                # batch before the unsupported-broker check even runs.
                broker_value = (
                    str((broker_config or {}).get("broker") or Broker.GCP_PUBSUB.value).strip().lower()
                )
                if broker_value != Broker.GCP_PUBSUB.value:
                    current_span.set_attribute("broker", broker_value)
                    raise ReferenceDataError(
                        f"Broker '{broker_value}' is no longer supported. Please use `{Broker.GCP_PUBSUB.value}` instead."
                    )

                # Generic-model destinations keep the per-item GundiDelivery
                # path (splitting the batch is allowed; merging never is).
                if _uses_generic_model(destination_integration):
                    for observation in batch.observations:
                        await _publish_gundi_delivery(
                            observation=observation,
                            destination=destination,
                            destination_integration=destination_integration,
                            provider=provider,
                            provider_key=provider_key,
                            route_configuration=route_configuration,
                            broker_config=broker_config,
                            destination_str=destination_str,
                            provider_str=provider_str,
                            current_span=current_span,
                        )
                    continue

                # Transform per item; group per effective provider_key because
                # field mappings may override it per item and one ER bulk post
                # allows exactly one provider_key in its URL path.
                groups = {}
                for observation in batch.observations:
                    try:
                        transformed = await transform_observation_v2(
                            observation=observation,
                            destination=destination_integration,
                            provider=provider,
                            route_configuration=route_configuration,
                        )
                    except Exception as e:
                        # Shrink the batch, never abort it
                        error_msg = (
                            f"Error transforming observation {observation.gundi_id} in batch {batch.batch_id} "
                            f"from {provider_str} for destination {destination_str}: {type(e).__name__}: {e}. Discarded."
                        )
                        logger.exception(error_msg)
                        current_span.add_event(
                            name="routing_service.batch_item_discarded_on_transformer_error"
                        )
                        continue
                    if not transformed:
                        current_span.add_event(
                            name="routing_service.batch_item_discarded_by_transformer"
                        )
                        continue
                    if not isinstance(transformed, ERObservation):
                        # Non-ER destination in the same connection (e.g. a raw-dict
                        # transformer): no batch envelope exists for it yet, so this
                        # item publishes individually exactly as the single path does.
                        pubsub_message_payload = (
                            transformed
                            if isinstance(transformed, dict)
                            else build_transformer_event(transformed).dict(exclude_none=True)
                        )
                        attributes = build_transformed_message_attributes(
                            observation=observation,
                            destination=destination,
                            gundi_version="v2",
                            provider_key=getattr(transformed, "provider_key", provider_key),
                        )
                        await send_message_to_gcp_pubsub_dispatcher(
                            message=build_gcp_pubsub_message(payload=pubsub_message_payload),
                            attributes=attributes,
                            destination=destination,
                            broker_config=broker_config,
                            ordering_key="",
                        )
                        continue
                    effective_key = getattr(transformed, "provider_key", None) or provider_key
                    groups.setdefault(effective_key, []).append(
                        TransformedERObservationItem(
                            gundi_id=observation.gundi_id,
                            observation=transformed,
                        )
                    )

                for effective_provider_key, items in groups.items():
                    await _publish_transformed_batch_group(
                        batch=batch,
                        items=items,
                        effective_provider_key=effective_provider_key,
                        destination=destination,
                        broker_config=broker_config,
                    )
        except ReferenceDataError as e:
            logger.exception(
                f"External error occurred obtaining reference data for batch {batch.batch_id}: {e}",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.InboundIntId: str(batch.data_provider_id)},
            )
            current_span.set_attribute("error", str(e))
            raise e  # Raise so the whole envelope is retried later by GCP
        except Exception as e:
            logger.exception(
                f"Unexpected internal exception occurred processing batch {batch.batch_id}: {e}",
                extra={ExtraKeys.AttentionNeeded: True, ExtraKeys.InboundIntId: str(batch.data_provider_id)},
            )
            current_span.set_attribute("error", str(e))
            raise e


async def handle_observations_batch_received(event: ObservationsBatchReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_observations_batch_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("batch_count", len(event.payload.observations))
        await transform_and_route_observations_batch(batch=event.payload)


async def handle_observation_received(event: ObservationReceived):
    # Trace observations with Open Telemetry
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_observation_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


async def handle_event_received(event: EventReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_event_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


async def handle_event_update(event: EventUpdateReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_event_update", kind=SpanKind.CONSUMER
    ) as current_span:
        event_update = event.payload
        current_span.set_attribute("payload", repr(event.payload))
        current_span.set_attribute("changes", str(event_update.changes))
        await transform_and_route_observation(observation=event_update)


async def handle_attachment_received(event: AttachmentReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_attachment_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


async def handle_text_message_received(event: TextMessageReceived):
    with tracing.tracer.start_as_current_span(
        "routing_service.handle_text_message_received", kind=SpanKind.CONSUMER
    ) as current_span:
        current_span.set_attribute("payload", repr(event.payload))
        await transform_and_route_observation(observation=event.payload)


event_handlers = {
    "ObservationReceived": handle_observation_received,
    "ObservationsBatchReceived": handle_observations_batch_received,
    "EventReceived": handle_event_received,
    "EventUpdateReceived": handle_event_update,
    "AttachmentReceived": handle_attachment_received,
    "TextMessageReceived": handle_text_message_received,
}

event_schemas = {
    "ObservationReceived": ObservationReceived,
    "ObservationsBatchReceived": ObservationsBatchReceived,
    "EventReceived": EventReceived,
    "EventUpdateReceived": EventUpdateReceived,
    "AttachmentReceived": AttachmentReceived,
    "TextMessageReceived": TextMessageReceived,
}
