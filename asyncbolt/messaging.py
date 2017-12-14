"""Bolt messaging format"""
import collections
import struct

from collections import OrderedDict
from enum import IntEnum

from asyncbolt import buffer
from asyncbolt.exception import ProtocolError


# Client/Server message
zero_arg_message = collections.namedtuple('ZeroArgMsg', ['signature'])

# Client messages
init = collections.namedtuple('Init', ['signature', 'client_name', 'auth_token'])
run = collections.namedtuple('Run', ['signature', 'statement', 'parameters'])

# Server messages
detail = collections.namedtuple('Detail', ['signature', 'fields'])
summary = collections.namedtuple('Summary', ['signature', 'metadata'])

# Graph Type structure (Neo4j compat)
node = collections.namedtuple('Node', ['signature', 'nodeIdentity', 'labels', 'properties'])
relationship = collections.namedtuple(
    'Relationship', ['signature', 'relIdentity', 'startNodeIdentity', 'endNodeIdentity', 'type', 'properties'])
path = collections.namedtuple('Path', ['signature', 'nodes', 'relationships', 'sequence'])
unbound_relationship = collections.namedtuple(
    'UnboundRelationship', ['signature', 'relIdentity', 'type', 'properties'])

# Handshake
# constants
MAGIC = struct.pack('>BBBB', 0x60, 0x60, 0xB0, 0x17)
V1 = struct.pack('>I', 0x01)
NULL_V = struct.pack('>I', 0x00)

# deserializers
unpack_magic = struct.Struct('>BBBB').unpack
unpack_v = struct.Struct('>I').unpack
unpack_4v = struct.Struct('>IIII').unpack

# Message Transfer Encoding
# constants
END_MARKER = struct.pack('>BB', 0x00, 0x00)

# pack message heacder
pack_uint_16 = struct.Struct('>H').pack

# Pack "tiny" values with header
pack_uint_8 = struct.Struct('>B').pack
# for integers
pack_sint_8 = struct.Struct('>b').pack

# Data type header/length de/serialization
pack_header_uint_8 = struct.Struct('>BB').pack
pack_header_uint_16 = struct.Struct('>BH').pack
pack_header_uint_32 = struct.Struct('>BL').pack

# Unpack marker/sizes/message headers
unpack_uint_8 = struct.Struct('>B').unpack
unpack_uint_16 = struct.Struct('>H').unpack
unpack_uint_32 = struct.Struct('>L').unpack

# Pack integers
pack_header_sint_8 = struct.Struct('>Bb').pack
pack_header_sint_16 = struct.Struct('>Bh').pack
pack_header_sint_32 = struct.Struct('>Bl').pack
pack_header_sint_64 = struct.Struct('>Bq').pack

# Unpack ints
unpack_sint_8 = struct.Struct('>b').unpack
unpack_sint_16 = struct.Struct('>h').unpack
unpack_sint_32 = struct.Struct('>l').unpack
unpack_sint_64 = struct.Struct('>q').unpack

# Pack floats
pack_double = struct.Struct('>d').pack

# Unpack floats
unpack_double = struct.Struct('>d').unpack


# Marker enumerations
class Boolean(IntEnum):
    TRUE = 0xC3
    FALSE = 0xC2


# These codes give us the size of the int that tells us the max data size
class Integer(IntEnum):
    # Tiny integer is identified by either hex < 128 (0x7F)
    # or if higher order nibble is all 1s (0xF0 - 0xFF)
    # deal with this separately
    # Big endian signed values
    TINY_MIN = 0x00
    TINY_MAX = 0x7F
    F_TINY_MIN = 0xF0
    F_TINY_MAX = 0xFF
    INT_8 = 0xC8
    INT_16 = 0xC9
    INT_32 = 0xCA
    INT_64 = 0xCB


class String(IntEnum):
    # Tiny string 0x80...0x8F (Higher order nibble is 1000 (128 or 0x80))
    # size in low order nibble
    TINY_MIN = 0x80
    TINY_MAX = 0x8F
    STRING_8 = 0xD0   # 8 bit unsigned, followed by 8 bit sized
    STRING_16 = 0xD1  # 16 bit big-endian unsigned integer, followed by 8 bit sized
    STRING_32 = 0xD2  # 32 bit big-endian unsigned integer, followed by 8 bit sized


class List(IntEnum):
    # Tiny list 0x90...0x9F size in low order nibble
    TINY_MIN = 0x90
    TINY_MAX = 0x9F
    LIST_8 = 0xD4  # 8 bit unsigned int
    LIST_16 = 0xD5  # 16 bit big endian unsigned integer
    LIST_32 = 0xD6  # 32 bit big endian unsigned integer


class Map(IntEnum):
    # Tiny map 0xA0...0xAF size in low order nibble
    TINY_MIN = 0xA0
    TINY_MAX = 0xAF
    MAP_8 = 0xD8  # 8 bit unsigned int
    MAP_16 = 0xD9  # 16 bit big endian unsigned integer
    MAP_32 = 0xDA  # 32 bit big endian unsigned integer


class Structure(IntEnum):
    TINY_MIN = 0xB0
    TINY_MAX = 0xBF
    STRUCT_8 = 0xDC  # 8 bit unsigned int
    STRUCT_16 = 0xDD  # 16 bit big endian unsigned integer


class Marker(IntEnum):
    NULL = 0xC0
    BOOLEAN_TRUE = Boolean.TRUE
    BOOLEAN_FALSE = Boolean.FALSE
    INT_8 = Integer.INT_8
    INT_16 = Integer.INT_16
    INT_32 = Integer.INT_32
    INT_64 = Integer.INT_64
    FLOAT_64 = 0xC1
    STRING_8 = String.STRING_8
    STRING_16 = String.STRING_16
    STRING_32 = String.STRING_32
    LIST_8 = List.LIST_8
    LIST_16 = List.LIST_16
    LIST_32 = List.LIST_32
    MAP_8 = Map.MAP_8
    MAP_16 = Map.MAP_16
    MAP_32 = Map.MAP_32
    STRUCT_8 = Structure.STRUCT_8
    STRUCT_16 = Structure.STRUCT_16


# Structure enumerations
class Message(IntEnum):
    INIT = 0x01
    RUN = 0x10
    DISCARD_ALL = 0x2F
    PULL_ALL = 0x3F
    ACK_FAILURE = 0x0E
    RESET = 0x0F
    RECORD = 0x71
    SUCCESS = 0x70
    FAILURE = 0x7F
    IGNORED = 0x7E


class GraphStructure(IntEnum):
    NODE = 0x4E
    RELATIONSHIP = 0x52
    PATH = 0x50
    UNBOUND_RELATIONSHIP = 0x72


# Serialize messages as structs (bytes objects)
def serialize_message(signature, *, buf=None, params=None, max_chunk_size=8192):
    """Take a marker and params and return a chunked Bolt message"""
    if not params:
        params = ()
    if not buf:
        buf = buffer.ChunkedWriteBuffer(max_chunk_size)
    buf = pack_structure(signature, buf, params)
    buf.write_eof()
    return buf


def pack_structure(marker, buf, params):
    """The basic bolt message is itself a structure, so al`l message packing starts here"""
    size = len(params)
    if size < 16:
        struct_header = pack_uint_8(Structure.TINY_MIN | size)
    elif size < 256:
        struct_header = pack_header_uint_8(Structure.STRUCT_8.value, size)
    elif size < 65536:
        struct_header = pack_header_uint_16(Structure.STRUCT_16.value, size)
    else:
        raise BufferError('Structure message exceeds max size 65535')
    buf.write(struct_header)
    buf.write(pack_uint_8(marker))
    for param in params:
        pack(param, buf)
    return buf


def pack_bool(val, buf):
    if val is True:
        buf.write(pack_uint_8(Boolean.TRUE))
    elif val is False:
        buf.write(pack_uint_8(Boolean.FALSE))
    else:
        raise BufferError('Invalid boolean values')
    return buf


def pack_str(val, buf):
    size = len(val)
    if size < 16:
        str_header = pack_uint_8(String.TINY_MIN | size)
    elif size < 256:
        str_header = pack_header_uint_8(String.STRING_8.value, size)
    elif size < 65536:
        str_header = pack_header_uint_16(String.STRING_16.value, size)
    elif size < 4294967296:
        str_header = pack_header_uint_32(String.STRING_32.value, size)
    else:
        raise BufferError('String message exceeds max size 4294967296')
    buf.write(str_header)
    val = val.encode('utf-8')
    buf.write(val)
    return buf


def pack_float(val, buf):
    buf.write(pack_uint_8(Marker.FLOAT_64))
    buf.write(pack_double(val))
    return buf


def pack_int(val, buf):
    # Recommended value ranges from protocol docs
    # Fix this
    if 0 <= val < 128:
        val = pack_uint_8(val)
    elif -16 <= val < 0:
        val = pack_uint_8(Integer.F_TINY_MIN | abs(val))
    elif -128 <= val < -16:
        val = pack_header_sint_8(Integer.INT_8, val)
    elif -32768 <= val < 32768:
        val = pack_header_sint_16(Integer.INT_16, val)
    elif -2147483648 <= val < 2147483648:
        val = pack_header_sint_32(Integer.INT_32, val)
    elif -9223372036854775808 <= val < 9223372036854775808:
        val = pack_header_sint_64(Integer.INT_64, val)
    else:
        raise BufferError('Integer out of maximum range -9223372036854775808 <= val < 9223372036854775808')
    buf.write(val)
    return buf


def pack_map(val, buf):
    size = len(val)
    if size < 16:
        map_header = pack_uint_8(Map.TINY_MIN | size)
    elif size < 256:
        map_header = pack_header_uint_8(Map.MAP_8, size)
    elif size < 65536:
        map_header = pack_header_uint_16(Map.MAP_16, size)
    elif size < 4294967296:
        map_header = pack_header_uint_32(Map.MAP_32, size)
    else:
        raise BufferError('Map message exceeds max size 4294967296')
    buf.write(map_header)
    for k, v in val.items():
        buf = pack(k, buf)
        buf = pack(v, buf)
    return buf


def pack_list(val, buf):
    size = len(val)
    if size < 16:
        list_header = pack_uint_8(List.TINY_MIN | size)
    elif size < 256:
        list_header = pack_header_uint_8(List.LIST_8, size)
    elif size < 65536:
        list_header = pack_header_uint_16(List.LIST_16, size)
    elif size < 4294967296:
        list_header = pack_header_uint_32(List.LIST_32, size)
    else:
        raise BufferError('Map message exceeds max size 4294967296')
    buf.write(list_header)
    for v in val:
        buf = pack(v, buf)
    return buf


def pack(val, buf):
    if val is None:
        buf.write(pack_uint_8(Marker.NULL))
        return buf
    try:
        encode = SERIALIZERS[type(val)]
    except KeyError:
        raise ProtocolError("Unknown data type '{}' for value: {}".format(type(val), val))
    return encode(val, buf)


def unpack(buf):
    marker, = unpack_uint_8(buf.read(1))
    if marker <= Structure.TINY_MAX or marker >= Integer.F_TINY_MIN:
        if marker <= Integer.TINY_MAX:
            return marker
        else:
            decoder_key = marker >> 4
    else:
        decoder_key = marker >> 2
    try:
        decode = DESERIALIZERS[decoder_key]
    except KeyError:
        raise ProtocolError("Unknown marker '{}'".format(marker))
    else:
        return decode(marker, buf)


def deserialize_message(buf):
    marker, = unpack_uint_8(buf.read(1))
    try:
        return unpack_structure(marker, buf)
    except ProtocolError as e:
        raise ProtocolError(
            'Messages should always be wrapped in a Bolt Structure') from e


def unpack_structure(marker, buf):
    high_nib = marker & 0xF0
    if high_nib == Structure.TINY_MIN:
        size = marker & 0x0F
    elif marker == Structure.STRUCT_8:
        size, = unpack_uint_8(buf.read(1))
    elif marker == Structure.STRUCT_16:
        size, = unpack_uint_16(buf.read(2))
    else:
        raise ProtocolError("Not a Structure marker: '{}'".format(marker))
    signature, = unpack_uint_8(buf.read(1))
    if size == 0:
        return zero_arg_message(signature)
    try:
        structure, num_args = STRUCTURE_SIGNATURE_MAP[signature]
    except KeyError:
        raise ProtocolError(
            "Unrecognized signature '{}' with size '{}'".format(
                signature, size))
    else:
        return structure(signature, *[unpack(buf) for _ in range(num_args)])


def unpack_null_float_or_bool(marker, buf):
    if marker == Marker.FLOAT_64:
        result, = unpack_double(buf.read(8))
        return result
    if marker == Marker.NULL:
        return
    if marker == Marker.BOOLEAN_TRUE:
        return True
    if marker == Marker.BOOLEAN_FALSE:
        return False


def unpack_int(marker, buf):
    if marker == Integer.INT_8:
        result, = unpack_sint_8(buf.read(1))
    elif marker == Integer.INT_16:
        result, = unpack_sint_16(buf.read(2))
    elif marker == Integer.INT_32:
        result, = unpack_sint_32(buf.read(4))
    elif marker == Integer.INT_64:
        result, = unpack_sint_64(buf.read(8))
    else:
        raise ProtocolError("Not a Integer marker: '{}'".format(marker))
    return result


def unpack_str(marker, buf):
    high_nib = marker & 0xF0
    if high_nib == String.TINY_MIN:
        size = marker & 0x0F
    elif marker == String.STRING_8:
        size, = unpack_uint_8(buf.read(1))
    elif marker == String.STRING_16:
        size, = unpack_uint_16(buf.read(2))
    elif marker == String.STRING_32:
        size, = unpack_uint_32(buf.read(4))
    else:
        raise ProtocolError("Not a String marker: '{}'".format(marker))
    return buf.read(size).tobytes().decode('utf-8')


def unpack_neg_tiny_int(marker, _):
    return -(marker & 0x0f)


def unpack_list(marker, buf):
    high_nib = marker & 0xF0
    if high_nib == List.TINY_MIN:
        size = marker & 0x0F
    elif marker == List.LIST_8:
        size, = unpack_uint_8(buf.read(1))
    elif marker == List.LIST_16:
        size, = unpack_uint_16(buf.read(2))
    elif marker == List.LIST_32:
        size, = unpack_uint_32(buf.read(4))
    else:
        raise ProtocolError("Not a List marker: '{}'".format(marker))
    return [unpack(buf) for _ in range(size)]


def unpack_map(marker, buf):
    high_nib = marker & 0xF0
    if high_nib == Map.TINY_MIN:
        size = marker & 0x0F
    elif marker == Map.MAP_8:
        size, = unpack_uint_8(buf.read(1))
    elif marker == Map.MAP_16:
        size, = unpack_uint_16(buf.read(2))
    elif marker == Map.MAP_32:
        size, = unpack_uint_32(buf.read(4))
    else:
        raise ProtocolError("Not a Map marker: '{}'".format(marker))
    output = {}
    for _ in range(size):
        k = unpack(buf)
        v = unpack(buf)
        output[k] = v
    return output


DESERIALIZERS = {
    0b1000: unpack_str,
    0b1001: unpack_list,
    0b1010: unpack_map,
    0b1011: unpack_structure,
    0b1111: unpack_neg_tiny_int,
    0b110000: unpack_null_float_or_bool,
    0b110010: unpack_int,
    0b110100: unpack_str,
    0b110101: unpack_list,
    0b110110: unpack_map,
    0b110111: unpack_structure
}


SERIALIZERS = {
    bool: pack_bool,
    str: pack_str,
    float: pack_float,
    int: pack_int,
    dict: pack_map,
    list: pack_list,
    OrderedDict: pack_map  # this is for testing
}


STRUCTURE_SIGNATURE_MAP = {
    Message.INIT: (init, 2),
    Message.RECORD: (detail, 1),
    Message.FAILURE: (summary, 1),
    Message.SUCCESS: (summary, 1),
    Message.IGNORED: (summary, 1),
    Message.RUN: (run, 2),
    GraphStructure.NODE: (node, 3),
    GraphStructure.RELATIONSHIP: (relationship, 5),
    GraphStructure.PATH: (path, 3),
    GraphStructure.UNBOUND_RELATIONSHIP: (unbound_relationship, 3)
}
