from __future__ import annotations

import pytest

from coredis._utils import b
from coredis.constants import RESPDataType
from coredis.exceptions import (
    ConnectionError,
    InvalidResponse,
    ResponseError,
    UnknownCommandError,
)
from coredis.parser import NOT_ENOUGH_DATA, Parser


@pytest.fixture
def parser():
    parser = Parser()
    return parser


@pytest.mark.parametrize(
    "decode",
    [
        True,
        False,
    ],
)
class TestPyParser:
    def encoded_value(self, decode: bool, value: bytes):
        if decode:
            return value.decode("latin-1")
        return value

    def test_incomplete_data(self, parser, decode):
        parser.feed(b"$10")
        assert (
            parser.parse(
                decode=decode,
                encoding="latin-1",
            )
            == NOT_ENOUGH_DATA
        )
        parser.feed(b"\r\nhello")
        assert (
            parser.parse(
                decode=decode,
                encoding="latin-1",
            )
            == NOT_ENOUGH_DATA
        )
        parser.feed(b"world\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BULK_STRING, self.encoded_value(decode, b"helloworld"))

    def test_none(self, parser, decode):
        parser.feed(b"_\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.NONE, None)

    def test_simple_string(self, parser, decode):
        parser.feed(b"+PONG\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.SIMPLE_STRING, self.encoded_value(decode, b"PONG"))

    def test_nil_bulk_string(self, parser, decode):
        parser.feed(b"$-1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BULK_STRING, None)

    def test_bulk_string(self, parser, decode):
        parser.feed(b"$5\r\nhello\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BULK_STRING, self.encoded_value(decode, b"hello"))

    def test_bulk_string_forced_raw(self, parser, decode):
        parser.feed(b"$5\r\nhello\r\n")
        assert parser.parse(decode=False, encoding="latin-1") == (
            RESPDataType.BULK_STRING,
            b"hello",
        )

    def test_bulk_string_undecodable(self, parser, decode):
        parser.feed(b"$6\r\n" + "世界".encode() + b"\r\n")
        assert parser.parse(decode=True, encoding="big5") == (
            RESPDataType.BULK_STRING,
            b"\xe4\xb8\x96\xe7\x95\x8c",
        )

    def test_nil_verbatim_text(self, parser, decode):
        parser.feed(b"=-1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.VERBATIM, None)

    def test_verbatim_text(self, parser, decode):
        parser.feed(b"=9\r\ntxt:hello\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.VERBATIM, self.encoded_value(decode, b"hello"))

    def test_unknown_verbatim_text_type(self, parser, decode):
        parser.feed(b"=9\r\nrst:hello\r\n")
        with pytest.raises(InvalidResponse, match="Unexpected verbatim string of type b'rst'"):
            parser.parse(
                decode=decode,
                encoding="latin-1",
            )

    def test_bool(self, parser, decode):
        parser.feed(b"#f\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BOOLEAN, False)
        parser.feed(b"#t\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BOOLEAN, True)

    def test_int(self, parser, decode):
        parser.feed(b":1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.INT, 1)
        parser.feed(b":-2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.INT, -2)

    def test_big_number(self, parser, decode):
        parser.feed(b"(" + b(pow(2, 128)) + b"\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.BIGNUMBER, pow(2, 128))

    def test_double(self, parser, decode):
        parser.feed(b",3.142\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.DOUBLE, 3.142)

    def test_nil_array(self, parser, decode):
        parser.feed(b"*-1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.ARRAY, None)

    def test_empty_array(self, parser, decode):
        parser.feed(b"*0\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.ARRAY, [])

    def test_int_array(self, parser, decode):
        parser.feed(b"*2\r\n:1\r\n:2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.ARRAY, [1, 2])

    def test_string_array(self, parser, decode):
        parser.feed(b"*2\r\n$2\r\nco\r\n$5\r\nredis\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (
            RESPDataType.ARRAY,
            [
                self.encoded_value(decode, b"co"),
                self.encoded_value(decode, b"redis"),
            ],
        )

    def test_mixed_array(self, parser, decode):
        parser.feed(b"*3\r\n:-1\r\n$2\r\nco\r\n$5\r\nredis\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (
            RESPDataType.ARRAY,
            [
                -1,
                self.encoded_value(decode, b"co"),
                self.encoded_value(decode, b"redis"),
            ],
        )

    def test_nested_array(self, parser, decode):
        parser.feed(b"*2\r\n*2\r\n$2\r\nco\r\n$5\r\nredis\r\n:1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (
            RESPDataType.ARRAY,
            [
                [
                    self.encoded_value(decode, b"co"),
                    self.encoded_value(decode, b"redis"),
                ],
                1,
            ],
        )

    def test_simple_push_array(self, parser, decode):
        parser.feed(b">2\r\n$7\r\nmessage\r\n$5\r\nredis\r\n")
        assert parser.parse(decode=decode, encoding="latin-1") == (
            RESPDataType.PUSH,
            [
                self.encoded_value(decode, b"message"),
                self.encoded_value(decode, b"redis"),
            ],
        )

    def test_interleaved_simple_push_array(self, parser, decode):
        parser.feed(b":3\r\n>2\r\n$7\r\nmessage\r\n$5\r\nredis\r\n:4\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.INT, 3)
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (
            RESPDataType.PUSH,
            [
                self.encoded_value(decode, b"message"),
                self.encoded_value(decode, b"redis"),
            ],
        )
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.INT, 4)

    def test_nil_map(self, parser, decode):
        parser.feed(b"%-1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.MAP, None)

    def test_empty_map(self, parser, decode):
        parser.feed(b"%0\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.MAP, {})

    def test_simple_map(self, parser, decode):
        parser.feed(b"%2\r\n:1\r\n:2\r\n:3\r\n:4\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.MAP, {1: 2, 3: 4})

    def test_nil_set(self, parser, decode):
        parser.feed(b"~-1\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.SET, None)

    def test_empty_set(self, parser, decode):
        parser.feed(b"~0\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.SET, set())

    def test_simple_set(self, parser, decode):
        parser.feed(b"~2\r\n:1\r\n:2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.SET, {1, 2})

    def test_multi_container(self, parser, decode):
        # dict containing list and set
        parser.feed(b"%2\r\n$2\r\nco\r\n*1\r\n:1\r\n$2\r\nre\r\n~3\r\n:1\r\n:2\r\n:3\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (
            RESPDataType.MAP,
            {
                self.encoded_value(decode, b"co"): [1],
                self.encoded_value(decode, b"re"): {1, 2, 3},
            },
        )

    # edge cases with RESP3 where RESP3 structures can't be mapped 1:1
    # to python types
    def test_set_with_dict(self, parser, decode):
        parser.feed(b"~1\r\n%1\r\n:1\r\n:2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.SET, {((1, 2),)})

    def test_dict_with_set_key(self, parser, decode):
        # dict with a set as a key
        parser.feed(b"%1\r\n~1\r\n:1\r\n:2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.MAP, {frozenset([1]): 2})

    def test_dict_with_list_key(self, parser, decode):
        # dict with a list as a key
        parser.feed(b"%1\r\n*1\r\n:1\r\n:2\r\n")
        assert parser.parse(
            decode=decode,
            encoding="latin-1",
        ) == (RESPDataType.MAP, {(1,): 2})

    @pytest.mark.parametrize(
        "err_string, expected_exception",
        [
            ("ERR max number of clients reached", ConnectionError),
            ("ERR unknown command", UnknownCommandError),
            ("Random bad thing", ResponseError),
        ],
    )
    def test_parse_error(self, parser, decode, err_string, expected_exception):
        parser.feed(b"-" + b(err_string) + b"\r\n")
        _, err = parser.parse(
            decode=decode,
            encoding="latin-1",
        )
        assert isinstance(err, expected_exception)

    def test_invalid_marker(self, parser, decode):
        parser.feed(b"a\r\nxxxx")
        with pytest.raises(InvalidResponse, match="Protocol Error: Unknown RESP data type: 'a'"):
            parser.parse(
                decode=decode,
                encoding="latin-1",
            )
