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

    def write_eof(self, eof):
        data = self._incoming.getvalue()
        # Reset incoming
        self._incoming = self._buf_class()

        # Ensure there is a buffer ready
        if self._current_buf_remaining <= 0:
            self.append_and_reset_buffer()

        # Buffer, chunk, and append
        data_size = len(data)
        data_size_plus4 = data_size + 4
        if data_size_plus4 <= self._current_buf_remaining:
            # This means we can fit len header, message, and eof all in current buf. Yay.
            self._current_buf.write(messaging.pack_uint_16(data_size))
            self._current_buf.write(data)
            self._current_buf.write(eof)
            self._current_buf_remaining -= data_size_plus4
            self._current_buf_size += data_size_plus4
        else:
            # Booo
            chunk_size = min(self._current_buf_remaining - 2, data_size)  # data_size could be remaining - 3
            data_view = memoryview(data)
            self._current_buf.write(messaging.pack_uint_16(chunk_size))
            self._current_buf.write(data_view[:chunk_size])
            more_data = data_view[chunk_size:]
            self.append_and_reset_buffer()
            self.write(more_data)

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
        self._buf_class = io.BytesIO
        self._current_buf = None
        self._current_buf_size = 0
        self._current_buf_ready = False
        self._current_pos = 0
        self._incoming_buf = None

    @property
    def ready(self):
        """Read only property. Buffer is ready to be read from"""
        return self._current_buf_ready

    def feed_data(self, data):
        """
        Feed data into the buffer. This method is aware of Bolt protocol
        message transfer encoding, and handles the assembly of chunked
        messages when necessary.
        """
        current_pos = 0
        data_size = len(data)
        if data_size == 0:
            return
        data_view = memoryview(data)
        while current_pos < data_size:
            current_pos_plus2 = current_pos + 2
            payload_len, = messaging.unpack_uint_16(data_view[current_pos:current_pos_plus2])
            right_bound = current_pos_plus2 + payload_len
            payload = data_view[current_pos_plus2:right_bound]
            right_bound_plus2 = right_bound + 2
            eof = data_view[right_bound:right_bound_plus2] == messaging.END_MARKER
            # Handle possible chunked result
            # After parsing, buffers are simply chunks of messages encoded as bytes
            if eof:
                current_pos = right_bound_plus2
                if self._incoming_buf is not None:
                    self._incoming_buf.write(payload)
                    buf = memoryview(self._incoming_buf.getvalue())
                    self._incoming_buf = None
                else:
                    buf = payload
                buf_len = len(buf)
                # Complete buffer
                if not self._current_buf:
                    self._current_buf = buf
                    self._current_buf_size = buf_len
                    # self._current_buf_ready = True
                else:
                    self._queue_append(buf)
                    self._num_queued_bufs += 1
                self._current_buf_ready = True
            else:
                current_pos = right_bound
                # Incomplete message
                if self._incoming_buf is None:
                    self._incoming_buf = self._buf_class()
                self._incoming_buf.write(payload)

    def read(self, num):
        self._ensure_buffer()
        if self._current_pos + num > self._current_buf_size:
            return BufferError('Not enough bytes to read in current buffer')
        result = self._current_buf[self._current_pos:self._current_pos + num]
        self._current_pos += num
        if self.at_eof and self._num_queued_bufs == 0:  # check
            self._current_buf_ready = False
        return result

    @property
    def at_eof(self):
        if self._current_pos == self._current_buf_size:
            return True
        return False

    def _ensure_buffer(self):
        if self.at_eof:
            try:
                # Should I release memoryview obj stored in self._current_buf here?
                self._current_buf = memoryview(self.queue_popleft())
            except IndexError:
                raise BufferError("Trying to read from empty buffer")
            else:
                self._current_pos = 0
                self._num_queued_bufs -= 1
                self._current_buf_size = len(self._current_buf)
                self._current_buf_ready = True
