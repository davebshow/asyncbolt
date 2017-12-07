import pytest

from asyncbolt import messaging, exception


@pytest.mark.asyncio
async def test_run(echo_client_session_server_pair):
    client, _ = echo_client_session_server_pair
    async for message in client.run('Hello world'):
        assert message.fields == ['Hello world']


@pytest.mark.asyncio
async def test_pipeline(echo_client_session_server_pair):
    client, _ = echo_client_session_server_pair
    client.pipeline('Hello world')
    client.pipeline('Hello world2')
    responses = [message.fields[0] async for message in client.run()]
    assert responses == ['Hello world', 'Hello world2']


@pytest.mark.asyncio
async def test_pipeline_run(echo_client_session_server_pair):
    client, _ = echo_client_session_server_pair
    client.pipeline('Hello world')
    client.pipeline('Hello world2')
    responses = [message.fields[0] async for message in client.run('Hello world3')]
    assert responses == ['Hello world', 'Hello world2', 'Hello world3']


@pytest.mark.asyncio
async def test_failure_ack_failure(echo_client_session_server_pair):
    client, _ = echo_client_session_server_pair
    client._on_failure = messaging.Message.ACK_FAILURE
    with pytest.raises(exception.ServerFailedError):
        responses = [message.fields[0] async for message in client.run('fail')]


@pytest.mark.asyncio
async def test_failure_reset(echo_client_session_server_pair):
    client, _ = echo_client_session_server_pair
    with pytest.raises(exception.ServerFailedError):
        responses = [message.fields[0] async for message in client.run('fail')]


