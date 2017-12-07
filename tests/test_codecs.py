import collections
import io

import pytest

from asyncbolt.messaging import pack, pack_structure, Message


def get_hexbytes(packed):
    return ''.join([hex(b) for b in packed])


# Output comparison string taken from protocol documentation whenever possible
def test_pack_null():
    buffer = io.BytesIO()
    packed = pack(None, buffer).getvalue()
    marker = hex(packed[0])
    assert marker.lower() == '0xc0'


def test_pack_true():
    buffer = io.BytesIO()
    packed = pack(True, buffer).getvalue()
    marker = hex(packed[0])
    assert marker.lower() == '0xc3'


def test_pack_false():
    buffer = io.BytesIO()
    packed = pack(False, buffer).getvalue()
    marker = hex(packed[0])
    assert marker.lower() == '0xc2'


def test_pack_tiny_int():
    buffer = io.BytesIO()
    packed = pack(42, buffer).getvalue()
    val = hex(packed[0])
    assert val.lower() == '0x2A'.lower()


def test_pack_int8():
    buffer = io.BytesIO()
    packed = pack(-128, buffer).getvalue()
    output = get_hexbytes(packed)
    # TODO test value
    assert output.lower() == '0xC80x80'.lower()


def test_pack_int16():
    buffer = io.BytesIO()
    packed = pack(1234, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xC90x40xD2'.lower()


def test_pack_int32():
    buffer = io.BytesIO()
    packed = pack(40000, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xca0x00x00x9c0x40'.lower()


def test_pack_min_int():
    buffer = io.BytesIO()
    packed = pack(-9223372036854775808, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xCB0x800x00x00x00x00x00x00x0'.lower()


def test_pack_max_int():
    buffer = io.BytesIO()
    packed = pack(9223372036854775807, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xCB0x7F0xFF0xFF0xFF0xFF0xFF0xFF0xFF'.lower()


def test_pack_float():
    buffer = io.BytesIO()
    packed = pack(1.1, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xC10x3F0xF10x990x990x990x990x990x9A'.lower()


def test_pack_neg_float():
    buffer = io.BytesIO()
    packed = pack(-1.1, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xC10xBF0xF10x990x990x990x990x990x9A'.lower()


def test_pack_tiny_string():
    buffer = io.BytesIO()
    packed = pack('MyClient/1.0', buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0x8C0x4D0x790x430x6C0x690x650x6E0x740x2F0x310x2E0x30'.lower()


def test_pack_regular_string():
    buffer = io.BytesIO()
    packed = pack('abcdefghijklmnopqrstuvwxyz', buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xD00x1A0x610x620x630x640x650x660x670x680x690x6A0x6B0x6C0x6D0x6E0x6F0x700x710x720x730x740x750x760x770x780x790x7A'.lower()


@pytest.mark.skip(reason="Suspicious failure. Under investigation")
def test_pack_special_string():
    buffer = io.BytesIO()
    packed = pack("En å flöt över ängen", buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xD00x180x450x6E0x200xC30xA50x200x660x6C0xC30xB60x740x200xC30xB60x760x650x720x200xC30xA40x6E0x670x650x6E'.lower()


def test_pack_empty_list():
    buffer = io.BytesIO()
    packed = pack([], buffer).getvalue()
    val = hex(packed[0])
    assert val.lower() == '0x90'.lower()


def test_pack_tiny_list():
    buffer = io.BytesIO()
    packed = pack([1,2,3], buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0x930x10x20x3'.lower()


def test_pack_regular_list():
    buffer = io.BytesIO()
    packed = pack([1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0], buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xD40x140x10x20x30x40x50x60x70x80x90x00x10x20x30x40x50x60x70x80x90x0'.lower()


def test_pack_empty_map():
    buffer = io.BytesIO()
    packed = pack({}, buffer).getvalue()
    val = hex(packed[0])
    assert val.lower() == '0xA0'.lower()


def test_pack_tiny_map():
    buffer = io.BytesIO()
    packed = pack({"a":1}, buffer).getvalue()
    output = get_hexbytes(packed)
    assert output.lower() == '0xA10x810x610x1'.lower()


def test_pack_regular_map():
    buffer = io.BytesIO()
    packed = pack(collections.OrderedDict(
        [("a", 1),("b", 1), ("c", 3), ("d", 4), ("e", 5), ("f", 6), ("g", 7), ("h", 8), ("i", 9),
         ("j", 0), ("k", 1), ("l", 2), ("m", 3), ("n", 4), ("o", 5), ("p", 6)]), buffer).getvalue()
    output = get_hexbytes(packed)
    print(output)
    assert output.lower() == "0xD80x100x810x610x10x810x620x10x810x630x30x810x640x40x810x650x50x810x660x60x810x670x70x810x680x80x810x690x90x810x6A0x00x810x6B0x10x810x6C0x20x810x6D0x30x810x6E0x40x810x6F0x50x810x700x6".lower()


def test_pack_init():
    buffer = io.BytesIO()
    packed = pack_structure(
        Message.INIT, buffer,
        ("MyClient/1.0", collections.OrderedDict([("scheme", "basic"), ("principal", "neo4j"), ("credentials", "secret")]))).getvalue()
    output = get_hexbytes(packed)
    # Docs stat B1, need to double check, maybe open issue
    expected = """B2  1 8C 4D  79 43 6C 69  65 6E 74 2F  31 2E 30 A3
                  86 73 63 68  65 6D 65 85  62 61 73 69  63 89 70 72
                  69 6E 63 69  70 61 6C 85  6E 65 6F 34  6A 8B 63 72
                  65 64 65 6E  74 69 61 6C  73 86 73 65  63 72 65 74""".replace(' ', '').replace('\n', '').lower()
    assert output.replace('0x', '').lower() == expected


def test_pack_run():
    buffer = io.BytesIO()
    packed = pack_structure(Message.RUN, buffer, ("RETURN 1 AS num", {})).getvalue()
    output = get_hexbytes(packed)
    expected = "B2 10 8F 52  45 54 55 52  4E 20 31 20  41 53 20 6E  75 6D A0".replace(' ', '').lower()
    assert output.replace('0x', '').lower() == expected
#
#
# def test_pack_init():
#     buffer = io.BytesIO()
#     packed = pack_structure(marker, buffer, params)
#     output = get_hexbytes(packed)
#     assert output.lower() == ''
#
#
# def test_pack_init():
#     buffer = io.BytesIO()
#     packed = pack_structure(marker, buffer, params)
#     output = get_hexbytes(packed)
#     assert output.lower() == ''
