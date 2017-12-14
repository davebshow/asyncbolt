"""Bolt Protocol for Asyncio"""
import asyncio
import collections
import logging

from enum import IntEnum

from asyncbolt import buffer, messaging, parser
from asyncbolt.exception import HandshakeError, ProtocolError, ServerFailedError, ServerIgnoredError

logger = logging.getLogger(__name__)
log_debug = logger.debug
log_info = logger.info
log_warning = logger.warning
log_error = logger.error


class ServerProtocolState(IntEnum):
    PROTOCOL_UNINITIALIZED = 0
    PROTOCOL_READY = 1
    PROTOCOL_RUNNING = 2
    PROTOCOL_FAILED = 3
    PROTOCOL_CLOSING = 4
    PROTOCOL_CLOSED = 5


class BoltProtocol:

    PROTOCOL_VERSION = 0x01

    def __init__(self, loop):
        self.loop = loop
        self.read_buffer = buffer.ChunkedReadBuffer()
        self.write_buffer = buffer.ChunkedWriteBuffer(8192)
        self.transport = None
        self.write = None
        self.parser = parser.BoltParser(self)

    # Public API for inheriting classes
    def data_received(self, data):
        self.parser.feed_data(data)
        log_debug('Data received:\n{}\n'.format(data))
        self._handle_incoming()

    def flush(self):
        for message in self.write_buffer.flush():
            log_debug("Writing message to transport\n'{}'\n".format(message))
            self.write(message)

    # Parser callbacks
    def on_chunk(self, chunk):
        self.read_buffer.feed_data(chunk)

    def on_message_complete(self):
        self.read_buffer.feed_eof()

    # Required for inheriting classes
    def write(self):
        raise NotImplementedError


class BoltServerProtocol(BoltProtocol):
    """Implement the Bolt protocol for server"""

    def __init__(self, loop, *, server=None):
        super().__init__(loop)
        self.server = server
        self.handshake_done = False
        self.init_done = False
        self.state = ServerProtocolState.PROTOCOL_UNINITIALIZED

    # Hooks for custom behavior in inheriting classes
    def get_server_metadata(self):
        """Inheriting server protocol should implement this method"""

    def on_ack_failure(self):
        """Called when server receives ACK_FAILURE message"""

    def on_discard_all(self):
        """Called when server receives DISCARD_ALL message"""

    def on_init(self, auth_token):
        """Inheriting server protocol should implement this method"""

    def on_pull_all(self):
        """Required! Send completed tasks to client!"""
        raise NotImplementedError

    def on_reset(self):
        """Called when server receives ACK_FAILURE message"""

    def on_run(self, statement, parameters):
        """Required! Run task received from client"""
        raise NotImplementedError

    # Public API for inheriting classes
    def failure(self, metadata):
        messaging.serialize_message(messaging.Message.FAILURE, buf=self.write_buffer, params=(metadata,))

    def ignored(self, metadata):
        messaging.serialize_message(messaging.Message.IGNORED, buf=self.write_buffer, params=(metadata,))

    def receive_handshake(self, data):
        data_view = memoryview(data)
        magic = data_view[:4]
        if not magic == messaging.MAGIC:
            raise ProtocolError('Incorrect magic byte sequence')
        log_debug('Handshake received')
        self._check_protocol(data_view[4:])
        self.handshake_done = True

    def record(self, fields):
        messaging.serialize_message(messaging.Message.RECORD, buf=self.write_buffer, params=(fields,))

    def success(self, metadata):
        messaging.serialize_message(messaging.Message.SUCCESS, buf=self.write_buffer, params=(metadata,))

    # Internal methods
    def _check_protocol(self, data):
        """Verify requested protocol against supported versions"""
        # TODO handle future versions
        try:
            v1, v2, v3, v4 = messaging.unpack_4v(data)
            log_debug('Client requesting protocol: {}'.format(v1))
            # Only support V1 Bolt
            if not v1 == self.PROTOCOL_VERSION:
                raise ProtocolError('Invalid protocol version')
            log_debug('Using protocol version: {}'.format(v1))
            # Write protocol version back to client
            self.write(messaging.V1)
        except Exception as e:
            raise HandshakeError from e

    def handle_incoming(self):
        while self.read_buffer.ready:
            data = messaging.deserialize_message(self.read_buffer)
            if self.state == ServerProtocolState.PROTOCOL_READY:
                if data.signature == messaging.Message.RUN:
                    self.state = ServerProtocolState.PROTOCOL_RUNNING
                    self.on_run(data.statement, data.parameters)
                elif data.signature == messaging.RESET:
                    self._reset()
                else:
                    self.state = ServerProtocolState.PROTOCOL_FAILED
                    self.failure({})
                    self.flush()
            elif self.state == ServerProtocolState.PROTOCOL_RUNNING:
                if data.signature == messaging.Message.PULL_ALL:
                    # Client ready to consume stream
                    self.on_pull_all()
                    self.state = ServerProtocolState.PROTOCOL_READY
                elif data.signature == messaging.Message.DISCARD_ALL:
                    self.on_discard_all()
                    self.write_buffer = buffer.ChunkedWriteBuffer(8192)
                    self.state = ServerProtocolState.PROTOCOL_READY
                else:
                    self.state = ServerProtocolState.PROTOCOL_FAILED
                    self.failure({})
                    self.flush()
            elif self.state == ServerProtocolState.PROTOCOL_FAILED:
                if data.signature == messaging.Message.ACK_FAILURE:
                    self.on_ack_failure()
                    self.state = ServerProtocolState.PROTOCOL_READY
                    self.success({})
                    self.flush()
                elif data.signature == messaging.Message.RESET:
                    self._reset()
                else:
                    self.ignored({})
                    self.flush()
            elif self.state == ServerProtocolState.PROTOCOL_UNINITIALIZED:
                if data.signature == messaging.Message.INIT:
                    self.on_init(data.auth_token)
                    self.state = ServerProtocolState.PROTOCOL_READY
                    log_debug("Server session initialized with auth token '{}'".format(data.auth_token))
                    metadata = self.get_server_metadata()
                    self.success(metadata)
                    self.flush()
                else:
                    self.state = ServerProtocolState.PROTOCOL_FAILED
                    self.failure({})
                    self.flush()

    def _reset(self):
        self.on_reset()
        # Reset the server session state, clear buffers
        self.write_buffer = buffer.ChunkedWriteBuffer(8192)
        while self.read_buffer.queue:
            self.read_buffer.queue_popleft()
            self.ignored({})
        self.state = ServerProtocolState.PROTOCOL_READY
        self.success({})
        self.flush()


class BoltClientProtocol(BoltProtocol):
    """Implement the Bolt protocol for client"""

    def __init__(self, loop, **kwargs):
        super().__init__(loop)

    # Bolt handshake/initialization methods
    def do_handshake(self):
        # TODO: Add support for future versions
        version_info = messaging.V1 + messaging.NULL_V + messaging.NULL_V + messaging.NULL_V
        log_debug('Sending handshake with version info: {}'.format(version_info))
        self.write(messaging.MAGIC + version_info)

    def verify_protocol(self, data):
        v, = messaging.unpack_v(data)
        assert v == self.PROTOCOL_VERSION
        log_info('Using Bolt protocol version {}\n'.format(data))

    def get_init_params(self):
        """
        Inheriting client protocol must should implement this method. Must return a tuple containing
        required BOLT INIT message params clientName and authToken,
        :returns tuple: tuple of (clientName: str, authToken: dict)
        """
        return 'AsyncBolt/1.0', {"scheme": "none"}

    # Bolt communication logic methods
    def handle_incoming(self):
        raise NotImplementedError

    # Bolt message packing methods
    def init(self, client_name, auth_token):
        messaging.serialize_message(messaging.Message.INIT, buf=self.write_buffer, params=(client_name, auth_token))

    def run(self, statement, parameters):
        messaging.serialize_message(messaging.Message.RUN, buf=self.write_buffer, params=(statement, parameters))

    def discard_all(self):
        messaging.serialize_message(messaging.Message.DISCARD_ALL, buf=self.write_buffer)

    def pull_all(self):
        messaging.serialize_message(messaging.Message.PULL_ALL, buf=self.write_buffer)

    def ack_failure(self):
        messaging.serialize_message(messaging.Message.ACK_FAILURE, buf=self.write_buffer)

    def reset(self):
        messaging.serialize_message(messaging.Message.RESET, buf=self.write_buffer)
