"""Buffers for queueing incoming and outgoing messages"""
import collections
import io

from asyncbolt import messaging
from asyncbolt.exception import BufferError


class ChunkedWriteBuffer:

    def __init__(self, max_chunk_size):
        self._max_chunk_size = max_chunk_size
        self.queue = collections.deque()
        self.queue_popleft = self.queue.popleft
        self._queue_append = self.queue.append
        self._buf_class = io.BytesIO
        self._incoming = self._buf_class()
        self._current_buf = self._buf_class()
        self._current_buf_size = 0
        self._current_buf_remaining = self._max_chunk_size

    def write(self, data):
        self._incoming.write(data)

    def write_eof(self):
        data = self._incoming.getvalue()
        # Reset incoming
        self._incoming = self._buf_class()

        # Ensure there is a buffer ready
        if self._current_buf_remaining <= 0:
            self.append_and_reset_buffer()

        # Buffer, chunk, and append
        data_size = len(data)
        data_size_plus2 = data_size + 2
        data_size_plus4 = data_size + 4
        if data_size_plus4 <= self._current_buf_remaining:
            # This means we can fit len header, message, and eof all in current buf. Yay.
            self._current_buf.write(messaging.pack_uint_16(data_size) + data + messaging.END_MARKER)
            self._current_buf_remaining -= data_size_plus4
            self._current_buf_size += data_size_plus4
        elif data_size_plus2 > self._current_buf_remaining:
            # Avoid zero length messages
            chunk_size = min(self._current_buf_remaining - 2, data_size)  # data_size could be remaining - 3
            data_view = memoryview(data)
            self._current_buf.write(messaging.pack_uint_16(chunk_size))
            self._current_buf.write(data_view[:chunk_size])
            more_data = data_view[chunk_size:]
            self.append_and_reset_buffer()
            self.write(more_data)
            self.write_eof()
        else:
            # BOOO
            # Buffer almost full, short message that would result in zero length message.
            self.append_and_reset_buffer()
            self.write(data)
            self.write_eof()

    def append_and_reset_buffer(self):
        if self._current_buf:
            self._queue_append(self._current_buf.getvalue())
            self._current_buf = self._buf_class()
            self._current_buf_remaining = self._max_chunk_size
            self._current_buf_size = 0

    def flush(self):
        self.append_and_reset_buffer()
        while self.queue:
            yield self.queue_popleft()

    def getvalue(self):
        self.append_and_reset_buffer()
        return self.queue_popleft()


class ChunkedReadBuffer:
    """
    This class is used to assemble and queue incoming data from the
    BoltProtocol classes
    """

    def __init__(self):
        self.queue = collections.deque()
        self.queue_popleft = self.queue.popleft
        self._queue_append = self.queue.append
        self._num_queued_bufs = 0
        self._current_buf = None
        self._current_buf_size = 0
        self._current_buf_ready = False
        self._current_pos = 0
        self._buf_class = io.BytesIO
        self._incoming_buf = None

    @property
    def ready(self):
        """Read only property. Buffer is ready to be read from"""
        return self._current_buf_ready

    def feed_data(self, data):
        """Feed data into the buffer."""
        if self._incoming_buf is None:
            self._incoming_buf = self._buf_class(data)
        else:
            self._incoming_buf.write(data)

    def feed_eof(self):
        buf = self._incoming_buf.getvalue()
        self._incoming_buf.close()
        self._incoming_buf = self._buf_class()
        if not self._current_buf:
            self._current_buf = memoryview(buf)
            self._current_buf_size = len(buf)
        else:
            self._queue_append(buf)
            self._num_queued_bufs += 1
        self._current_buf_ready = True

    def read(self, num):
        self._ensure_buffer()
        if self._current_pos + num > self._current_buf_size:
            return BufferError('Not enough bytes to read in current buffer')
        result = self._current_buf[self._current_pos:self._current_pos + num]
        self._current_pos += num
        if self._current_pos == self._current_buf_size and self._num_queued_bufs == 0:  # check
            self._current_buf_ready = False
        return result

    def _ensure_buffer(self):
        if self._current_pos == self._current_buf_size:
            try:
                self._current_buf = memoryview(self.queue_popleft())
            except IndexError:
                raise BufferError("Trying to read from empty buffer")
            else:
                self._current_pos = 0
                self._num_queued_bufs -= 1
                self._current_buf_size = len(self._current_buf)
                self._current_buf_ready = True
