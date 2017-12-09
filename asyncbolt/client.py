"""Bolt client session."""
import asyncio
import collections
import logging

from asyncbolt.protocol import BoltClientProtocol
from asyncbolt.exception import ProtocolError, BoltClientError, ServerFailedError, ServerIgnoredError
from asyncbolt.messaging import Message, pack_message

logger = logging.getLogger(__name__)
log_debug = logger.debug
log_info = logger.info
log_warning = logger.warning
log_error = logger.error


client_response = collections.namedtuple('ClientResponse', ['fields', 'metadata', 'eof'])


async def connect(*,
                  protocol_class=None,
                  loop=None,
                  host=None,
                  port=None,
                  ssl=None,
                  on_failure=None,
                  max_inflight=1024,
                  **kwargs):
    """
    Connect to the Bolt server and initialize session
    :param str uri:
    :param asyncio.BaseEventLoop loop:
    :param asyncbolt.ClientProtocol protocol_class:
    :param asyncbolt.messaging.Message on_failure: Either asyncbolt.messaging.Message.ACK_FALIURE
        or asyncbolt.messaging.Message.RESET. Default is RESET
    :param int max_inflight: Max number run messages in pipeline
    :returns: asyncbolt.client.ClientSession
    """
    # Defaults
    if loop is None:
        loop = asyncio.get_event_loop()
    if host is None:
        host = '127.0.0.1'
    if port is None:
        port = 8888
    if on_failure is None:
        on_failure = Message.RESET
    if protocol_class is None:
        protocol_class = BoltClientProtocol

    # Create connection to server
    _, protocol = await loop.create_connection(
        lambda: protocol_class(loop, **kwargs), host, port, ssl=ssl)
    await protocol.handshake_waiter

    # Initialize session. If successful, return a new client session
    # Get init params
    try:
        client_name, auth_token = protocol.get_init_params()
    except Exception as e:
        raise ProtocolError('Invalid `get_init_params` implementation`') from e

    # Send init message to server
    protocol.init(client_name, auth_token)
    protocol.flush()

    # Confirm successful session init
    try:
        success = await protocol.read()
        assert success.signature == Message.SUCCESS
    except AssertionError:
        raise ProtocolError("Server responded with '{}'".format(success))
    except Exception as e:
        protocol.close()
        raise ProtocolError('Unable to initialize server') from e
    else:
        # Success! Return a new client session!
        log_debug("Client session initialized with server metadata:\n\n{}\n".format(success.metadata))
        return ClientSession(protocol, loop, on_failure, client_name, auth_token, max_inflight)


class ClientSession:
    """Implement a Bolt client session. Maintains client session state."""

    def __init__(self, protocol, loop, on_failure, client_name, auth_token, max_inflight):
        self._protocol = protocol
        self._loop = loop
        self._on_failure = on_failure
        self._client_name = client_name
        self._auth_token = auth_token
        self._inflight = 0
        self._max_inflight = max_inflight

    @property
    def loop(self):
        return self._loop()

    @property
    def client_name(self):
        return self._client_name

    @property
    def auth_token(self):
        return self._auth_token

    async def run(self, statement=None, parameters=None, *, get_eof=False):
        """Submit a statement and parameters to the Bolt server and pull all results"""
        if statement is not None:
            if parameters is None:
                parameters = {}
            self.pipeline(statement, parameters)
        self._protocol.flush()
        while self._inflight > 0:
            try:
                # Wait for the result of run task and verify success
                success_msg = await self._protocol.read()
                assert success_msg[0] == Message.SUCCESS
            except (ServerFailedError, AssertionError) as e:
                # Failed run. Send ack and read ignored messages until server succeeds with reset
                self._inflight -= 1
                pack_message(self._on_failure, buf=self._protocol.write_buffer)
                self._protocol.flush()
                while True:
                    try:
                        msg = await self._protocol.read()
                    except ServerIgnoredError:
                        self._inflight -= 1
                        pass
                    else:
                        assert msg[0] == Message.SUCCESS
                        raise ServerFailedError("Server failed. Reset with '{}'".format(self._on_failure)) from e
            else:
                # Successful execution of run task, iterate results
                success_meta = success_msg[1]
                self._inflight -= 1
                while True:
                    msg = await self._protocol.read()
                    if msg[0] == Message.SUCCESS:
                        self._inflight -= 1
                        if get_eof:
                            # EOF message with final meta
                            yield client_response(None, msg[1], True)
                        break
                    yield client_response(msg.fields, success_meta, False)

    def pipeline(self, statement, parameters=None):
        """
        Pipeline requests to the Bolt server. Written to underlying protocol write buffer, but not processed until
        ClientSession.pull_all or ClientSession.run is called.
        called
        """
        if self._inflight > self._max_inflight:
            raise BoltClientError('Exceeded max number of pipelined messages')
        self._protocol.run(statement, parameters)
        log_debug('Pipelining statement and params:\n{}\n{}\n'.format(statement, parameters))
        self._protocol.pull_all()
        log_debug('Pipelining PULL_ALL\n')
        self._inflight += 2

    async def reset(self):
        if self._inflight > self._max_inflight:
            raise BoltClientError('Exceeded max number of pipelined messages')
        self._protocol.reset()
        self._protocol.flush()
        success = await self._protocol.read()
        assert success[0] == Message.SUCCESS
        return client_response(['Successfully reset sever session'], success[1], True)

    def close(self):
        self._protocol.close()
