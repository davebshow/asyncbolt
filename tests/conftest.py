from urllib.parse import urlparse

import pytest

from asyncbolt.client import connect
from asyncbolt.protocol import BoltClientProtocol, BoltServerProtocol
from asyncbolt.server import ServerSession, create_server


class EchoServerSession(ServerSession):

    async def run(self, statement, parameters):
        if statement == 'fail':
            raise RuntimeError('Server received bad statement')
        return statement


def pytest_addoption(parser):
    parser.addoption('--host', default='127.0.0.1')
    parser.addoption('--port', default='8888')


@pytest.fixture
def host(request):
    return request.config.getoption('host')


@pytest.fixture
def port(request):
    return request.config.getoption('port')


@pytest.fixture(scope='function')
def client(event_loop, host, port):
    coro = event_loop.create_connection(lambda: BoltClientProtocol(event_loop), host, port)
    _, protocol = event_loop.run_until_complete(coro)
    yield protocol
    protocol.close()


@pytest.fixture(scope='function')
def server(event_loop, host, port):
    coro = create_server(EchoServerSession, loop=event_loop, host=host, port=port, ssl=None)
    server = event_loop.run_until_complete(coro)
    yield server
    server.close()
    event_loop.run_until_complete(server.wait_closed())


@pytest.fixture(scope='function')
def client_server_pair(event_loop, host, port):
    # Get server
    coro = create_server(EchoServerSession, loop=event_loop, host=host, port=port, ssl=None)
    server = event_loop.run_until_complete(coro)
    # Get client
    coro = event_loop.create_connection(lambda: BoltClientProtocol(event_loop), host, port)
    _, protocol = event_loop.run_until_complete(coro)
    yield protocol, server
    server.close()
    event_loop.run_until_complete(server.wait_closed())
    protocol.close()


@pytest.fixture(scope='function')
def echo_client_server_pair(event_loop, host, port):
    # Get server
    coro = create_server(EchoServerSession, loop=event_loop, host=host, port=port, ssl=None)
    server = event_loop.run_until_complete(coro)
    # Get clientc
    coro = event_loop.create_connection(lambda: BoltClientProtocol(event_loop), host, port)
    _, protocol = event_loop.run_until_complete(coro)
    yield protocol, server
    server.close()
    event_loop.run_until_complete(server.wait_closed())
    protocol.close()


@pytest.fixture(scope='function')
def echo_client_session_server_pair(event_loop, host, port):
    coro = create_server(EchoServerSession, loop=event_loop, host=host, port=port, ssl=None)
    server = event_loop.run_until_complete(coro)
    # Get client
    client_session = event_loop.run_until_complete(connect(loop=event_loop, host=host, port=port))
    yield client_session, server
    server.close()
    event_loop.run_until_complete(server.wait_closed())
    client_session.close()