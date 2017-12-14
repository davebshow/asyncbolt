import collections
import pytest

from asyncbolt.buffer import ChunkedReadBuffer
from asyncbolt.messaging import serialize_message, deserialize_message, Message


def get_hexbytes(packed):
    return ''.join([hex(b) for b in packed])


# These tests are based on Error handling with Reset
# https://boltprotocol.org/v1/#examples-reset
def test_pack_init_message():
    output = serialize_message(
        Message.INIT,
        params=("MyClient/1.0", collections.OrderedDict([("scheme", "basic"), ("principal", "neo4j"), ("credentials", "secret")])))
    expected = """0 40 B2 1  8C 4D 79 43  6C 69 65 6E  74 2F 31 2E
                  30 A3 86 73  63 68 65 6D  65 85 62 61  73 69 63 89
                  70 72 69 6E  63 69 70 61  6C 85 6E 65  6F 34 6A 8B
                  63 72 65 64  65 6E 74 69  61 6C 73 86  73 65 63 72
                  65 74 0 0""".replace(' ', '').replace('\n', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_success_message():
    output = serialize_message(
        Message.SUCCESS,
        params=({"server": "Neo4j/3.1.0"},))
    expected = """0 16 B1 70  A1 86 73 65  72 76 65 72  8B 4E 65 6F
                  34 6A 2F 33  2E 31 2E 30  0 0""".replace(' ', '').replace('\n', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_run_message():
    output = serialize_message(
        Message.RUN,
        params=("This will cause a syntax error", {}))
    expected = """0 23 b2 10  d0 1e 54 68  69 73 20 77  69 6c 6c 20
                  63 61 75 73  65 20 61 20  73 79 6e 74  61 78 20 65
                  72 72 6f 72  a0 0 0""".replace(' ', '').replace('\n', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_good_run_message():
    output = serialize_message(
        Message.RUN,
        params=("RETURN 1 AS num", {}))
    expected = """0 13 b2 10  8f 52 45 54  55 52 4e 20  31 20 41 53
                  20 6e 75 6d  a0 0 0""".replace(' ', '').replace('\n', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


@pytest.mark.skip(reason="Suspicious failure. Under investigation")
def test_pack_failure_message():
    output = serialize_message(
        Message.FAILURE,
        params=(collections.OrderedDict(
            [("code", "Neo.ClientError.Statement.SyntaxError"),
             ("message","""Invalid input 'T': expected <init> (line 1, column 1 (offset: 0))
                          "This will cause a syntax error"^""")])))
    expected = """0 9E B1 7F  A2 84 63 6F  64 65 D0 25  4E 65 6F 2E
                  43 6C 69 65  6E 74 45 72  72 6F 72 2E  53 74 61 74
                  65 6D 65 6E  74 2E 53 79  6E 74 61 78  45 72 72 6F
                  72 87 6D 65  73 73 61 67  65 D0 65 49  6E 76 61 6C
                  69 64 20 69  6E 70 75 74  20 27 54 27  3A 20 65 78
                  70 65 63 74  65 64 20 3C  69 6E 69 74  3E 20 28 6C
                  69 6E 65 20  31 2C 20 63  6F 6C 75 6D  6E 20 31 20
                  28 6F 66 66  73 65 74 3A  20 30 29 29  A 22 54 68
                  69 73 20 77  69 6C 6C 20  63 61 75 73  65 20 61 20
                  73 79 6E 74  61 78 20 65  72 72 6F 72  22 A 20 5E
                  0 0""".replace(' ', '').replace('\n', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_pull_all():
    output = serialize_message(Message.PULL_ALL)
    expected = """0 2 b0 3f 0 0""".replace(' ', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_ignored():
    output = serialize_message(Message.IGNORED)
    expected = """0 2 b0 7e 0 0""".replace(' ', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_reset():
    output = serialize_message(Message.RESET)
    expected = """0 2 b0 f 0 0""".replace(' ', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_ack_failure():
    output = serialize_message(Message.ACK_FAILURE)
    expected = """0 2 B0 E  0 0""".replace(' ', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


def test_pack_record():
    output = serialize_message(Message.RECORD, params=([1],))
    expected = """0 4 b1 71  91 1 0 0""".replace(' ', '').lower()
    output = get_hexbytes(output.getvalue())
    assert output.replace('0x', '').lower() == expected


# Test chunker, discard all
def test_unpack_record_int(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([1, 2, 3],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [1, 2, 3]


def test_unpack_record_int_sizes(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([1, 1234, -1234, 40000, -40000],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [1, 1234, -1234, 40000, -40000]


def test_unpack_record_negint(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([-1, -2, -3],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [-1, -2, -3]


def test_unpack_record_float(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([1.1, 2.2, 3.3],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [1.1, 2.2, 3.3]


def test_unpack_record_string(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=(['a', 'b', 'c'],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == ['a', 'b', 'c']


def test_unpack_record_dict(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([{'a': 'b'}],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [{'a': 'b'}]


def test_unpack_record_mixed_types(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.RECORD, params=([1, 'hello', {'hello': 'world'}],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [1, 'hello', {'hello': 'world'}]


def test_unpack_record_mixed_types_containers(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(
        Message.RECORD, params=([1, 'hello', {'hello': ['world', 'hello', None, True, False]}, [1, 2, 3]],)).getvalue()
    dummy.feed_data(output)
    record = deserialize_message(buf)
    assert record.fields == [1, 'hello', {'hello': ['world', 'hello', None, True, False]}, [1, 2, 3]]


def test_unpack_init_message(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(
        Message.INIT,
        params=("MyClient/1.0",
                collections.OrderedDict([("scheme", "basic"), ("principal", "neo4j"), ("credentials", "secret")]))).getvalue()
    dummy.feed_data(output)
    init = deserialize_message(buf)
    assert init.client_name == "MyClient/1.0"
    assert init.auth_token["scheme"] == "basic"
    assert init.auth_token["principal"] == "neo4j"
    assert init.auth_token["credentials"] == "secret"


def test_unpack_success_message(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(
        Message.SUCCESS,
        params=({"server": "Neo4j/3.1.0"},)).getvalue()
    dummy.feed_data(output)
    success = deserialize_message(buf)
    assert success.metadata == {"server": "Neo4j/3.1.0"}


def test_unpack_good_run_message(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(
        Message.RUN,
        params=("RETURN 1 AS num", {})).getvalue()
    dummy.feed_data(output)
    run = deserialize_message(buf)
    assert run.statement == "RETURN 1 AS num"
    assert run.parameters == {}


def test_unpack_good_run_message_with_args(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(
        Message.RUN,
        params=("RETURN 1 AS num", {'a': 1})).getvalue()
    dummy.feed_data(output)
    run = deserialize_message(buf)
    assert run.statement == "RETURN 1 AS num"
    assert run.parameters == {'a': 1}


def test_unpack_pull_all(dummy_read_buffer_pair):
    dummy, buf = dummy_read_buffer_pair
    output = serialize_message(Message.PULL_ALL).getvalue()
    dummy.feed_data(output)
    pull_all = deserialize_message(buf)
    assert pull_all.signature == Message.PULL_ALL
