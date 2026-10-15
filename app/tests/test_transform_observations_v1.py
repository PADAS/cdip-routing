import pytest
import pytz
from datetime import datetime
from gundi_core import schemas
from smartconnect.models import ConfigurableDataModel
from app.conftest import async_return
from app.core.gundi import URN_GUNDI_PREFIX, URN_GUNDI_INTSRC_FORMAT
from app.services.transformers import (
    SmartEventTransformer,
    _timezone_finder,
    map_choice_value_to_option_key,
    transform_observation,
)


@pytest.mark.asyncio
async def test_movebank_transformer(
    outbound_configuration_default,
    raw_observation_position,
    raw_observation_position_attributes,
):

    schema = schemas.models_by_stream_type[
        raw_observation_position.get("observation_type")
    ]
    observation = schema.parse_obj(raw_observation_position)
    # Set Movebank values
    outbound_configuration_default.type_slug = "movebank"
    transformed_observation = await transform_observation(
        stream_type=observation.observation_type,
        config=outbound_configuration_default,
        observation=observation,
    )
    # Check dictionary schema
    expected_keys = [
        "recorded_at",
        "tag_id",
        "lon",
        "lat",
        "sensor_type",
        "tag_manufacturer_name",
        "gundi_urn",
    ]
    for key in transformed_observation.keys():
        assert key in expected_keys
    # Check transformed_observation values
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    assert (
        bool(datetime.strptime(transformed_observation["recorded_at"], date_format))
        is True
    )
    assert transformed_observation["recorded_at"] == observation.recorded_at.astimezone(
        tz=pytz.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert (
        transformed_observation["tag_id"]
        == f"{outbound_configuration_default.inbound_type_slug}.{observation.device_id}.{observation.integration_id}"
    )
    assert (
        transformed_observation["gundi_urn"]
        == f"{URN_GUNDI_PREFIX}v1.{URN_GUNDI_INTSRC_FORMAT}.{observation.integration_id}.{observation.device_id}"
    )


@pytest.mark.asyncio
async def test_smart_transformer_with_er_event(
    mocker,
    mock_gundi_client,
    mock_smart_async_client_class,
    smart_outbound_configuration_gcp_pubsub,
    raw_observation_er_event,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )

    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )
    schema = schemas.models_by_stream_type[
        raw_observation_er_event.get("observation_type")
    ]
    event = schema.parse_obj(raw_observation_er_event)
    assert event

    transformed_observation = await transform_observation(
        stream_type=event.observation_type,
        config=smart_outbound_configuration_gcp_pubsub,
        observation=event,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, dict)
    assert "waypoint_requests" in transformed_observation
    assert len(transformed_observation["waypoint_requests"]) == 1
    waypoint = transformed_observation["waypoint_requests"][0]
    assert waypoint.get("type") == "Feature"
    assert waypoint.get("geometry") == {
        "coordinates": [event.location.longitude, event.location.latitude]
    }
    waypoint_properties = waypoint.get("properties")
    assert waypoint_properties.get("smartDataType") == "integrateincident"
    assert waypoint_properties.get("smartFeatureType") == "waypoint/new"
    smart_attributes = waypoint_properties.get("smartAttributes")
    assert smart_attributes.get("incidentId") == f"ER-{event.serial_number}"
    assert smart_attributes.get("incidentUuid") == str(event.id)
    observations_groups = smart_attributes.get("observationGroups", [])
    assert len(observations_groups) == 1
    observations = observations_groups[0].get("observations", [])
    assert len(observations) == 1
    observation = observations[0]
    assert observation.get("category") == "animals.sign"
    assert observation.get("observationUuid") == str(event.id)


@pytest.mark.asyncio
async def test_smart_transformer_with_er_patrol(
    mocker,
    mock_gundi_client,
    mock_smart_async_client_class,
    smart_outbound_configuration_gcp_pubsub,
    raw_observation_er_patrol,
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient",
        mock_smart_async_client_class,
    )

    # Mock external dependencies
    mock_gundi_client.get_outbound_integration_list.return_value = async_return(
        [smart_outbound_configuration_gcp_pubsub]
    )

    schema = schemas.models_by_stream_type[
        raw_observation_er_patrol.get("observation_type")
    ]
    patrol = schema.parse_obj(raw_observation_er_patrol)
    assert patrol

    transformed_observation = await transform_observation(
        stream_type=patrol.observation_type,
        config=smart_outbound_configuration_gcp_pubsub,
        observation=patrol,
    )
    assert transformed_observation
    assert isinstance(transformed_observation, dict)
    assert "patrol_requests" in transformed_observation
    assert len(transformed_observation["patrol_requests"]) == 1
    patrol_request = transformed_observation["patrol_requests"][0]
    assert patrol_request.get("type") == "Feature"
    patrol_properties = patrol_request.get("properties")
    assert patrol_properties.get("smartDataType") == "patrol"
    assert patrol_properties.get("smartFeatureType") == "patrol/new"
    smart_attributes = patrol_properties.get("smartAttributes")
    assert smart_attributes.get("patrolId") == f"ER-{patrol.serial_number}"
    assert smart_attributes.get("patrolUuid") == str(patrol.id)



def test_map_choice_value_exact_key_unchanged():
    assert map_choice_value_to_option_key(
        attribute_key="wildlifestate", value="healthy", option_keys=["healthy", "injured"]
    ) == "healthy"


def test_map_choice_value_resolves_sanitized_tree_path():
    assert map_choice_value_to_option_key(
        attribute_key="causeofdeath",
        value="natural_disease",
        option_keys=["natural", "natural.disease"],
    ) == "natural.disease"


def test_map_choice_value_passes_non_strings_through():
    assert map_choice_value_to_option_key(
        attribute_key="x", value=None, option_keys=["a"]
    ) is None
    assert map_choice_value_to_option_key(
        attribute_key="x", value=3, option_keys=["a"]
    ) == 3


def test_map_choice_value_logs_and_passes_through_when_unresolvable(caplog):
    with caplog.at_level("ERROR"):
        out = map_choice_value_to_option_key(
            attribute_key="wildlifestate", value="nope", option_keys=["healthy"]
        )
    assert out == "nope"
    assert "wildlifestate" in caplog.text and "nope" in caplog.text


@pytest.mark.asyncio
async def test_smart_transformer_resolves_v2_choice_values(
    mocker, mock_smart_async_client_class, smart_outbound_configuration_gcp_pubsub
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformer(
        config=smart_outbound_configuration_gcp_pubsub, ca_uuid=None
    )
    # TREE leaf stored by ER as its sanitized Choice value.
    assert await transformer._resolve_attribute("causeofdeath", "natural_disease") == (
        "causeofdeath",
        "natural.disease",
    )
    # v1 sites: exact key stays untouched.
    assert await transformer._resolve_attribute("wildlifestate", "healthy") == (
        "wildlifestate",
        "healthy",
    )
    # Unknown attribute still falls through to the transformation rules path.
    assert await transformer._resolve_attribute("not_an_attribute", "x") == (None, None)


@pytest.mark.asyncio
async def test_smart_transformer_resolves_across_all_attribute_configs(
    mocker, mock_smart_async_client_class, smart_outbound_configuration_gcp_pubsub
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    # Attribute key absent from the CA data model (whose lower-case wildlifestate
    # keys would otherwise match exactly), so only the CM's configs can resolve it.
    xml = (
        '<ConfigurableModel xmlns="http://www.smartconservationsoftware.org/xml/1.0/configurablemodel">'
        '<name value="Test CM" language_code="en"/><nodes/>'
        '<attributeConfig id="cfg-1" attributeKey="cmonlystate" isDefault="true">'
        '<listItem keyRef="Healthy" isActive="true"/></attributeConfig>'
        '<attributeConfig id="cfg-2" attributeKey="cmonlystate" isDefault="false">'
        '<listItem keyRef="Injured" isActive="true"/></attributeConfig>'
        "</ConfigurableModel>"
    )
    cm = ConfigurableDataModel()
    cm.load(xml)
    transformer = SmartEventTransformer(
        config=smart_outbound_configuration_gcp_pubsub, ca_uuid=None
    )
    mocker.patch.object(
        transformer, "get_configurable_models", return_value=[cm]
    )
    assert await transformer._resolve_attribute("cmonlystate", "injured") == (
        "cmonlystate",
        "Injured",
    )


@pytest.mark.parametrize("value", ["", "Some Label", "mixedCase_value"])
def test_map_choice_value_non_sanitized_shapes_pass_through_quietly(value, caplog):
    with caplog.at_level("DEBUG"):
        out = map_choice_value_to_option_key(
            attribute_key="x", value=value, option_keys=["Lion", "healthy"]
        )
    assert out == value
    assert not [r for r in caplog.records if r.levelname == "ERROR"]


def test_map_choice_value_exact_mixed_case_key_unchanged():
    assert (
        map_choice_value_to_option_key(
            attribute_key="x", value="Lion", option_keys=["Lion", "LION"]
        )
        == "Lion"
    )


def test_map_choice_value_ambiguous_logs_error_with_integration_id(caplog):
    with caplog.at_level("ERROR"):
        out = map_choice_value_to_option_key(
            attribute_key="species",
            value="lion",
            option_keys=["Lion", "LION"],
            integration_id="integration-123",
        )
    assert out == "lion"
    errors = [r for r in caplog.records if r.levelname == "ERROR"]
    assert errors and "integration-123" in errors[0].getMessage()


@pytest.mark.asyncio
async def test_smart_transformer_reuses_timezone_finder(
    mocker, mock_smart_async_client_class, smart_outbound_configuration_gcp_pubsub
):
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    transformer = SmartEventTransformer(
        config=smart_outbound_configuration_gcp_pubsub, ca_uuid=None
    )
    _timezone_finder.cache_clear()
    for _ in range(2):
        tz = transformer.guess_location_timezone(longitude=36.82, latitude=-1.29)
        assert tz.zone == "Africa/Nairobi"
    assert _timezone_finder.cache_info().misses <= 1


@pytest.mark.asyncio
async def test_smart_transformer_with_er_event_publishes_smart_option_key(
    mocker,
    mock_smart_async_client_class,
    smart_outbound_configuration_gcp_pubsub,
    raw_observation_er_event,
):
    # ER v2 stores causeofdeath "natural.disease" as the Choice value
    # "natural_disease"; SMART must receive the original option key.
    mocker.patch(
        "app.services.transformers.AsyncSmartClient", mock_smart_async_client_class
    )
    raw = dict(raw_observation_er_event)
    raw["event_details"] = {**raw["event_details"], "causeofdeath": "natural_disease"}
    event = schemas.models_by_stream_type[raw["observation_type"]].parse_obj(raw)

    transformed = await transform_observation(
        stream_type=event.observation_type,
        config=smart_outbound_configuration_gcp_pubsub,
        observation=event,
    )

    waypoint = transformed["waypoint_requests"][0]
    observation = waypoint["properties"]["smartAttributes"]["observationGroups"][0]["observations"][0]
    assert observation["attributes"]["causeofdeath"] == "natural.disease"
