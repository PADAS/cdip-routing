import pytest
from fastapi.testclient import TestClient
from app.main import app

api_client = TestClient(app)


# ToDo: Finish refactoring this test
@pytest.mark.asyncio
async def test_process_geoevent_v1_successfully(
    mocker,
    mock_cache,
    mock_gundi_client,
    mock_pubsub_client,
    pubsub_cloud_event_headers,
    geoevent_v1_cloud_event_payload,
):
    # Mock external dependencies
    mocker.patch("app.core.gundi._cache_db", mock_cache)
    mocker.patch("app.core.gundi._portal", mock_gundi_client)
    mocker.patch("app.services.process_messages.pubsub", mock_pubsub_client)
    response = api_client.post(
        "/",
        headers=pubsub_cloud_event_headers,
        json=geoevent_v1_cloud_event_payload,
    )
    assert response.status_code == 200
    # Check that the report was sent to a pubsub topic
    assert mock_pubsub_client.PublisherClient.called
    assert mock_pubsub_client.PublisherClient.return_value.publish.called
