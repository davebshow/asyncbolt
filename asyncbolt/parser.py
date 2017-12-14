"""Parser classes for the Bolt protocol. Somewhat based on httptools"""
from asyncbolt import messaging


class BoltParser:

    def __init__(self, protocol):
        self._proto_message_on_chunk = getattr(protocol, 'on_chunk', None)
        self._proto_message_on_complete = getattr(protocol, 'on_message_complete', None)

    def feed_data(self, data):
        """
        Feed data into the parser. Data is chunked based on Bolt message transfer encoding
        """
        data_size = len(data)
        if data_size == 0:
            return
        current_pos = 0
        data_view = memoryview(data)
        while current_pos < data_size:
            current_pos_plus2 = current_pos + 2
            payload_len, = messaging.unpack_uint_16(data_view[current_pos:current_pos_plus2])
            right_bound = current_pos_plus2 + payload_len
            chunk = data_view[current_pos_plus2:right_bound]
            if self._proto_message_on_chunk:
                self._proto_message_on_chunk(chunk)
            right_bound_plus2 = right_bound + 2
            if data_view[right_bound:right_bound_plus2] == messaging.END_MARKER:
                current_pos = right_bound_plus2
                if self._proto_message_on_complete:
                    self._proto_message_on_complete()
