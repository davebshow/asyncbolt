class ProtocolError(Exception):
    """Base protocol error"""


class HandshakeError(ProtocolError):
    """Error during client server handshake"""


class BufferError(ProtocolError):
    """Error on buffer operation"""


class ServerError(ProtocolError):
    """Map server messages to errors"""


class ServerFailedError(ServerError):
    """Raised by client on server failure"""


class ServerIgnoredError(ServerError):
    """Raised by client on server ignored"""


class BoltClientError(ProtocolError):
    """Raised by client on pilot error"""