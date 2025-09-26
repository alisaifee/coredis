from __future__ import annotations

import asyncio
import os
import re
import ssl
from collections import deque

import pytest

import coredis
from coredis._utils import query_param_to_bool
from coredis.exceptions import (
    ConnectionError,
    RedisError,
)


class DummyConnection:
    description = "DummyConnection<>"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self.awaiting_response = False
        self.is_connected = False
        self.needs_handshake = True
        self._last_error = None
        self._requests = deque()
        self.average_response_time = 0.0
        self.lag = 0.0
        self.requests_pending = 0
        self.requests_processed = 0
        self.estimated_time_to_idle = 0
        self.latency = 0

    async def connect(self):
        self.is_connected = True

    def disconnect(self):
        self.is_connected = False
        self._last_error = None

    async def perform_handshake(self) -> None:
        self.needs_handshake = False


@pytest.fixture(autouse=True)
def setup(redis_basic):
    pass


class TestConnectionPool:
    def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=DummyConnection,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = coredis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )

        return pool

    async def test_connection_creation(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = await pool.acquire()
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    async def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        assert c1 != c2

    async def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        await pool.acquire()
        await pool.acquire()
        with pytest.raises(ConnectionError):
            await pool.acquire()

    async def test_pool_disconnect(self):
        pool = self.get_pool(max_connections=3)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        c3 = await pool.acquire()
        pool.release(c3)
        pool.disconnect()
        assert not c1.is_connected
        assert not c2.is_connected
        assert not c3.is_connected

    async def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = await pool.acquire()
        await c1.connect()
        pool.release(c1)
        c2 = await pool.acquire()
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {"host": "localhost", "port": 6379, "db": 1}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=coredis.Connection
        )
        expected = "ConnectionPool<Connection<host=localhost,port=6379,db=1>>"
        assert repr(pool) == expected

    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=coredis.UnixDomainSocketConnection,
        )
        expected = "ConnectionPool<UnixDomainSocketConnection<path=/abc,db=1>>"
        assert repr(pool) == expected

    @pytest.mark.xfail
    async def test_connection_idle_check(self):
        rs = coredis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            max_idle_time=0.2,
            idle_check_interval=0.1,
        )
        await rs.info()
        assert len(rs.connection_pool._available_connections) == 1
        assert len(rs.connection_pool._in_use_connections) == 0
        conn = rs.connection_pool._available_connections[0]
        last_active_at = conn.last_active_at
        await asyncio.sleep(0.3)
        assert len(rs.connection_pool._available_connections) == 0
        assert len(rs.connection_pool._in_use_connections) == 0
        assert last_active_at == conn.last_active_at
        assert conn._transport is None


class TestBlockingConnectionPool:
    def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=DummyConnection,
        timeout=None,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = coredis.BlockingConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs,
        )

        return pool

    async def test_connection_creation(self):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        connection = await pool.acquire()
        assert isinstance(connection, DummyConnection)
        assert connection.kwargs == connection_kwargs

    async def test_multiple_connections(self):
        pool = self.get_pool()
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        assert c1 != c2

    async def test_max_connections_timeout(self):
        pool = self.get_pool(max_connections=2, timeout=0.1)
        await pool.acquire()
        await pool.acquire()
        with pytest.raises(ConnectionError):
            await pool.acquire()

    async def test_max_connections_no_timeout(self):
        pool = self.get_pool(max_connections=2)
        await pool.acquire()
        released_conn = await pool.acquire()

        def releaser():
            pool.release(released_conn)

        loop = asyncio.get_running_loop()
        loop.call_later(0.2, releaser)
        new_conn = await pool.acquire()
        assert new_conn == released_conn

    async def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        c1 = await pool.acquire()
        pool.release(c1)
        c2 = await pool.acquire()
        assert c1 == c2

    async def test_pool_disconnect(self):
        pool = self.get_pool()
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        c3 = await pool.acquire()
        pool.release(c3)
        pool.disconnect()
        assert not c1.is_connected
        assert not c2.is_connected
        assert not c3.is_connected

    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {"host": "localhost", "port": 6379, "db": 1}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=coredis.Connection
        )
        expected = "BlockingConnectionPool<Connection<host=localhost,port=6379,db=1>>"
        assert repr(pool) == expected

    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=coredis.UnixDomainSocketConnection,
        )
        expected = "BlockingConnectionPool<UnixDomainSocketConnection<path=/abc,db=1>>"
        assert repr(pool) == expected

    @pytest.mark.xfail
    async def test_connection_idle_check(self):
        rs = coredis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            connection_pool=coredis.BlockingConnectionPool(
                max_idle_time=0.2, idle_check_interval=0.1, host="127.0.01", port=6379
            ),
        )
        await rs.info()
        assert len(rs.connection_pool._in_use_connections) == 0
        conn = await rs.connection_pool.acquire()
        last_active_at = conn.last_active_at
        rs.connection_pool.release(conn)
        await asyncio.sleep(0.3)
        assert len(rs.connection_pool._in_use_connections) == 0
        assert last_active_at == conn.last_active_at
        assert conn._transport is None
        new_conn = await rs.connection_pool.acquire()
        assert conn == new_conn


class TestConnectionPoolURLParsing:
    def test_defaults(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_hostname(self):
        pool = coredis.ConnectionPool.from_url("redis://myhost")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "myhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_quoted_hostname(self):
        pool = coredis.ConnectionPool.from_url(
            "redis://my %2F host %2B%3D+", decode_components=True
        )
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "my / host +=+",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_port(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost:6380")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6380,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_password(self):
        pool = coredis.ConnectionPool.from_url("redis://:mypassword@localhost")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": "",
            "password": "mypassword",
        }

    def test_quoted_password(self):
        pool = coredis.ConnectionPool.from_url(
            "redis://:%2Fmypass%2F%2B word%3D%24+@localhost", decode_components=True
        )
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "/mypass/+ word=$+",
        }

    def test_db_as_argument(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost", db="1")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "username": None,
            "password": None,
        }

    def test_db_in_path(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost/2", db="1")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 2,
            "username": None,
            "password": None,
        }

    def test_db_in_querystring(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost/2?db=3", db="1")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 3,
            "username": None,
            "password": None,
        }

    def test_extra_typed_querystring_options(self):
        pool = coredis.ConnectionPool.from_url(
            "redis://localhost/2?stream_timeout=20&connect_timeout=10"
        )

        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 2,
            "stream_timeout": 20.0,
            "connect_timeout": 10.0,
            "username": None,
            "password": None,
        }

    def test_boolean_parsing(self):
        for expected, value in (
            (None, None),
            (None, ""),
            (False, 0),
            (False, "0"),
            (False, "f"),
            (False, "F"),
            (False, "False"),
            (False, "n"),
            (False, "N"),
            (False, "No"),
            (True, 1),
            (True, "1"),
            (True, "y"),
            (True, "Y"),
            (True, "Yes"),
        ):
            assert expected is query_param_to_bool(value)

    def test_invalid_extra_typed_querystring_options(self):
        import warnings

        with warnings.catch_warnings(record=True) as warning_log:
            coredis.ConnectionPool.from_url(
                "redis://localhost/2?stream_timeout=_&connect_timeout=abc"
            )
        # Compare the message values
        assert [str(m.message) for m in sorted(warning_log, key=lambda log: str(log.message))] == [
            "Invalid value for `connect_timeout` in connection URL.",
            "Invalid value for `stream_timeout` in connection URL.",
        ]

    def test_max_connections_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost?max_connections=32")
        assert pool.max_connections == 32

    def test_max_idle_times_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost?max_idle_time=5")
        assert pool.max_idle_time == 5

    def test_idle_check_interval_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost?idle_check_interval=1")
        assert pool.idle_check_interval == 1

    def test_extra_querystring_options(self):
        pool = coredis.ConnectionPool.from_url("redis://localhost?a=1&b=2")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
            "a": "1",
            "b": "2",
        }

    def test_client_creates_connection_pool(self):
        r = coredis.Redis.from_url("redis://myhost")
        assert r.connection_pool.connection_class == coredis.Connection
        assert r.connection_pool.connection_kwargs == {
            "host": "myhost",
            "port": 6379,
            "db": 0,
            "decode_responses": False,
            "protocol_version": 3,
            "username": None,
            "password": None,
            "noreply": False,
            "noevict": False,
            "notouch": False,
        }


class TestConnectionPoolUnixSocketURLParsing:
    def test_defaults(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket")
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_password(self):
        pool = coredis.ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": "",
            "password": "mypassword",
        }

    def test_quoted_password(self):
        pool = coredis.ConnectionPool.from_url(
            "unix://:%2Fmypass%2F%2B word%3D%24+@/socket", decode_components=True
        )
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": None,
            "password": "/mypass/+ word=$+",
        }

    def test_quoted_path(self):
        pool = coredis.ConnectionPool.from_url(
            "unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket",
            decode_components=True,
        )
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/my/path/to/../+_+=$ocket",
            "db": 0,
            "username": None,
            "password": "mypassword",
        }

    def test_db_as_argument(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 1,
            "username": None,
            "password": None,
        }

    def test_db_in_querystring(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 2,
            "username": None,
            "password": None,
        }

    def test_max_connections_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("unix:///localhost?max_connections=32")
        assert pool.max_connections == 32

    def test_max_idle_times_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("unix:///localhost?max_idle_time=5")
        assert pool.max_idle_time == 5

    def test_idle_check_interval_querystring_option(self):
        pool = coredis.ConnectionPool.from_url("unix:///localhost?idle_check_interval=1")
        assert pool.idle_check_interval == 1

    def test_extra_querystring_options(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket?a=1&b=2")
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": None,
            "password": None,
            "a": "1",
            "b": "2",
        }


class TestSSLConnectionURLParsing:
    def test_defaults(self):
        pool = coredis.ConnectionPool.from_url("rediss://localhost")
        assert pool.connection_class == coredis.Connection
        assert pool.connection_kwargs.pop("ssl_context") is not None
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    @pytest.mark.parametrize(
        "query_param, expected",
        [
            (
                "none",
                ssl.CERT_NONE,
            ),
            (
                "optional",
                ssl.CERT_OPTIONAL,
            ),
            ("required", ssl.CERT_REQUIRED),
            (None, ssl.CERT_OPTIONAL),
        ],
    )
    async def test_cert_reqs_options(self, query_param, expected):
        uri = "rediss://?ssl_keyfile=./tests/tls/client.key&ssl_certfile=./tests/tls/client.crt"
        if query_param:
            uri += f"&ssl_cert_reqs={query_param}"
        pool = coredis.ConnectionPool.from_url(uri)
        assert (await pool.acquire()).ssl_context.verify_mode == expected


class TestConnection:
    async def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = coredis.Redis(db=9999)
        # an error should be raised on connect
        with pytest.raises(RedisError):
            await bad_connection.info()
        pool = bad_connection.connection_pool
        assert not pool._available_connections[0].is_connected

    async def test_busy_loading_from_pipeline(self):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        client = coredis.Redis()
        pipe = await client.pipeline()
        await pipe.create_request(
            b"DEBUG", b"ERROR", b"LOADING fake message", callback=lambda r, **k: r
        )
        with pytest.raises(RedisError):
            await pipe.execute()
        pool = client.connection_pool
        assert not pipe.connection
        assert len(pool._available_connections) == 1
        assert pool._available_connections[0]._transport

    def test_connect_from_url_tcp(self):
        connection = coredis.Redis.from_url("redis://localhost")
        pool = connection.connection_pool

        assert re.match("(.*)<(.*)<(.*)>>", repr(pool)).groups() == (
            "ConnectionPool",
            "Connection",
            "host=localhost,port=6379,db=0",
        )

    def test_connect_from_url_unix(self):
        connection = coredis.Redis.from_url("unix:///path/to/socket")
        pool = connection.connection_pool

        assert re.match("(.*)<(.*)<(.*)>>", repr(pool)).groups() == (
            "ConnectionPool",
            "UnixDomainSocketConnection",
            "path=/path/to/socket,db=0",
        )
