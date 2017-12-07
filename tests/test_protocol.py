import pytest

from asyncbolt import messaging, exception


@pytest.mark.asyncio
async def test_handshake_init(client_server_pair):
    client, _ = client_server_pair
    await client.handshake_waiter
    assert client.handshake_waiter.done() is True
    client_name, auth_token = client.get_init_params()
    client.init(client_name, auth_token)
    client.flush()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS


@pytest.mark.asyncio
async def test_echo(echo_client_server_pair):
    client, _ = echo_client_server_pair
    await client.handshake_waiter
    client_name, auth_token = client.get_init_params()
    client.init(client_name, auth_token)
    client.flush()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS
    client.run('hello world', {})
    client.pull_all()
    client.flush()
    # await client.waiter
    success1 = await client.read()
    assert success1.signature == messaging.Message.SUCCESS
    echo = await client.read()
    assert echo.signature == messaging.Message.RECORD
    assert echo.fields[0] == 'hello world'
    success2 = await client.read()
    assert success2.signature == messaging.Message.SUCCESS


@pytest.mark.asyncio
async def test_failure_ack_failure(echo_client_server_pair):
    client, _ = echo_client_server_pair
    await client.handshake_waiter
    client_name, auth_token = client.get_init_params()
    client.init(client_name, auth_token)
    client.flush()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS
    client.run('fail', {})
    client.pull_all()
    client.flush()
    with pytest.raises(exception.ServerFailedError):
        await client.read()
    client.ack_failure()
    client.flush()
    with pytest.raises(exception.ServerIgnoredError):
        await client.read()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS
    #Everything should be fine now

    client.run('hello world', {})
    client.pull_all()
    client.flush()
    success1 = await client.read()
    assert success1.signature == messaging.Message.SUCCESS
    echo = await client.read()
    assert echo.signature == messaging.Message.RECORD
    assert echo.fields[0] == 'hello world'
    success2 = await client.read()
    assert success2.signature == messaging.Message.SUCCESS


@pytest.mark.asyncio
async def test_failure_reset(echo_client_server_pair):
    client, _ = echo_client_server_pair
    await client.handshake_waiter
    client_name, auth_token = client.get_init_params()
    client.init(client_name, auth_token)
    client.flush()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS
    client.run('fail', {})
    client.pull_all()
    client.flush()
    with pytest.raises(exception.ServerFailedError):
        await client.read()
    client.reset()
    client.flush()
    with pytest.raises(exception.ServerIgnoredError):
        await client.read()
    success = await client.read()
    assert success.signature == messaging.Message.SUCCESS
    #Everything should be fine now
    client.run('hello world', {})
    client.pull_all()
    client.flush()
    success1 = await client.read()
    assert success1.signature == messaging.Message.SUCCESS
    echo = await client.read()
    assert echo.signature == messaging.Message.RECORD
    assert echo.fields[0] == 'hello world'
    success2 = await client.read()
    assert success2.signature == messaging.Message.SUCCESS



