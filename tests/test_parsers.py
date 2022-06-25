from __future__ import annotations

from asyncio import StreamReader

import pytest

from coredis import BaseConnection
from coredis.exceptions import InvalidResponse
from coredis.parser import Parser


class DummyConnection(BaseConnection):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._reader = StreamReader()

    def feed(self, data):
        self.buffer.append()

    async def _connect(self) -> None:
        pass


@pytest.fixture
def connection(request):
    return DummyConnection(decode_responses=request.getfixturevalue("decode"))


@pytest.fixture
def parser(connection):
    parser = Parser(1024)
    parser.on_connect(connection)
    return parser


@pytest.mark.asyncio
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

    async def test_none(self, connection, parser, decode):
        connection._reader.feed_data(b"_\r\n")
        assert await parser.read_response() is None

    async def test_simple_string(self, connection, parser, decode):
        connection._reader.feed_data(b"+PONG\r\n")
        assert await parser.read_response() == self.encoded_value(decode, b"PONG")

    async def test_nil_bulk_string(self, connection, parser, decode):
        connection._reader.feed_data(b"$-1\r\n")
        assert await parser.read_response() is None

    async def test_bulk_string(self, connection, parser, decode):
        connection._reader.feed_data(b"$5\r\nhello\r\n")
        assert await parser.read_response() == self.encoded_value(decode, b"hello")

    async def test_bulk_string_forced_raw(self, connection, parser, decode):
        connection._reader.feed_data(b"$5\r\nhello\r\n")
        assert await parser.read_response(decode=False) == b"hello"

    async def test_nil_verbatim_text(self, connection, parser, decode):
        connection._reader.feed_data(b"=-1\r\n")
        assert await parser.read_response() is None

    async def test_verbatim_text(self, connection, parser, decode):
        connection._reader.feed_data(b"=9\r\ntxt:hello\r\n")
        assert await parser.read_response() == self.encoded_value(decode, b"hello")

    async def test_unknown_verbatim_text_type(self, connection, parser, decode):
        connection._reader.feed_data(b"=9\r\nrst:hello\r\n")
        with pytest.raises(
            InvalidResponse, match="Unexpected verbatim string of type b'rst'"
        ):
            await parser.read_response()

    async def test_bool(self, connection, parser, decode):
        connection._reader.feed_data(b"#f\r\n")
        assert await parser.read_response() is False
        connection._reader.feed_data(b"#t\r\n")
        assert await parser.read_response() is True

    async def test_int(self, connection, parser, decode):
        connection._reader.feed_data(b":1\r\n")
        assert await parser.read_response() == 1
        connection._reader.feed_data(b":2\r\n")
        assert await parser.read_response() == 2

    async def test_double(self, connection, parser, decode):
        connection._reader.feed_data(b",3.142\r\n")
        assert await parser.read_response() == 3.142

    async def test_bignumber(self, connection, parser, decode):
        connection._reader.feed_data(b"(3.142\r\n")
        with pytest.raises(InvalidResponse):
            await parser.read_response()

    async def test_nil_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*-1\r\n")
        assert await parser.read_response() is None

    async def test_empty_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*0\r\n")
        assert await parser.read_response() == []

    async def test_int_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*2\r\n:1\r\n:2\r\n")
        assert await parser.read_response() == [1, 2]

    async def test_string_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*2\r\n$2\r\nco\r\n$5\r\nredis\r\n")
        assert await parser.read_response() == [
            self.encoded_value(decode, b"co"),
            self.encoded_value(decode, b"redis"),
        ]

    async def test_mixed_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*3\r\n:-1\r\n$2\r\nco\r\n$5\r\nredis\r\n")
        assert await parser.read_response() == [
            -1,
            self.encoded_value(decode, b"co"),
            self.encoded_value(decode, b"redis"),
        ]

    async def test_nested_array(self, connection, parser, decode):
        connection._reader.feed_data(b"*2\r\n*2\r\n$2\r\nco\r\n$5\r\nredis\r\n:1\r\n")
        assert await parser.read_response() == [
            [
                self.encoded_value(decode, b"co"),
                self.encoded_value(decode, b"redis"),
            ],
            1,
        ]

    async def test_simple_push_array(self, connection, parser, decode):
        connection._reader.feed_data(b">2\r\n$2\r\nco\r\n$5\r\nredis\r\n")
        assert await parser.read_response(push_message_types=[b"co"]) == [
            self.encoded_value(decode, b"co"),
            self.encoded_value(decode, b"redis"),
        ]

    async def test_interleaved_simple_push_array(self, connection, parser, decode):
        connection._reader.feed_data(b":3\r\n>2\r\n:1\r\n:2\r\n:4\r\n")
        assert await parser.read_response() == 3
        assert await parser.read_response() == 4
        assert connection.push_messages.get_nowait() == [1, 2]

    async def test_nil_map(self, connection, parser, decode):
        connection._reader.feed_data(b"%-1\r\n")
        assert await parser.read_response() is None

    async def test_empty_map(self, connection, parser, decode):
        connection._reader.feed_data(b"%0\r\n")
        assert await parser.read_response() == {}

    async def test_simple_map(self, connection, parser, decode):
        connection._reader.feed_data(b"%2\r\n:1\r\n:2\r\n:3\r\n:4\r\n")
        assert await parser.read_response() == {1: 2, 3: 4}

    async def test_nil_set(self, connection, parser, decode):
        connection._reader.feed_data(b"~-1\r\n")
        assert await parser.read_response() is None

    async def test_empty_set(self, connection, parser, decode):
        connection._reader.feed_data(b"~0\r\n")
        assert await parser.read_response() == set()

    async def test_simple_set(self, connection, parser, decode):
        connection._reader.feed_data(b"~2\r\n:1\r\n:2\r\n")
        assert await parser.read_response() == {1, 2}

    async def test_multi_container(self, connection, parser, decode):
        # dict containing list and set
        connection._reader.feed_data(
            b"%2\r\n$2\r\nco\r\n*1\r\n:1\r\n$2\r\nre\r\n~3\r\n:1\r\n:2\r\n:3\r\n"
        )
        assert await parser.read_response() == {
            self.encoded_value(decode, b"co"): [1],
            self.encoded_value(decode, b"re"): {1, 2, 3},
        }

    async def test_set_with_dict(self, connection, parser, decode):
        # set containing a dict
        # This specifically represents a minimal example of the response from
        # ``COMMANDS INFO with RESP 3``
        connection._reader.feed_data(b"~1\r\n%1\r\n:1\r\n:2\r\n")
        with pytest.raises(TypeError, match="unhashable type"):
            await parser.read_response()
