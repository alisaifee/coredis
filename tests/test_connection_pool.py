from __future__ import annotations

import re
import ssl

import pytest
from anyio import move_on_after, sleep

import coredis
from coredis._utils import query_param_to_bool
from coredis.connection import Connection, UnixDomainSocketConnection
from coredis.exceptions import (
    ConnectionError,
    RedisError,
)

pytestmark = pytest.mark.anyio


class TestConnectionPool:
    def get_pool(
        self,
        connection_class=Connection,
        connection_kwargs=None,
        max_connections=None,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = coredis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            blocking=False,
            **connection_kwargs,
        )
        return pool

    async def test_multiple_connections(self):
        pool = self.get_pool()
        async with pool:
            c1 = await pool.acquire(blocking=True)
            c2 = await pool.acquire(blocking=True)
            assert c1 != c2

    async def test_max_connections(self):
        pool = self.get_pool(max_connections=2)
        async with pool:
            await pool.acquire(blocking=True)
            await pool.acquire(blocking=True)
            with pytest.raises(ConnectionError):
                await pool.acquire(blocking=True)

    async def test_pool_disconnect(self):
        pool = self.get_pool(max_connections=3)
        async with pool:
            await pool.acquire(blocking=True)
            await pool.acquire(blocking=True)
            await pool.acquire(blocking=True)
        assert pool._connections == set()

    async def test_reuse_previously_released_connection(self):
        pool = self.get_pool()
        async with pool:
            c1 = await pool.acquire()
            c2 = await pool.acquire()
        assert c1 == c2

    def test_repr_contains_db_info_tcp(self):
        connection_kwargs = {"host": "localhost", "port": 6379, "db": 1}
        pool = self.get_pool(connection_kwargs=connection_kwargs)
        expected = "ConnectionPool<Connection<host=localhost,port=6379,db=1>>"
        assert repr(pool) == expected

    def test_repr_contains_db_info_unix(self):
        connection_kwargs = {"path": "/abc", "db": 1}
        pool = self.get_pool(
            connection_kwargs=connection_kwargs,
            connection_class=UnixDomainSocketConnection,
        )
        expected = "ConnectionPool<UnixDomainSocketConnection<path=/abc,db=1>>"
        assert repr(pool) == expected

    async def test_connection_idle_check(self):
        rs = coredis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            max_idle_time=0.2,
        )
        async with rs:
            await rs.info()
            assert len(rs.connection_pool._connections) >= 1
            await sleep(0.3)
            assert len(rs.connection_pool._connections) == 0


class TestBlockingConnectionPool:
    def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        connection_class=Connection,
        max_idle_time=None,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = coredis.ConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            blocking=True,
            max_idle_time=max_idle_time,
            **connection_kwargs,
        )

        return pool

    async def test_multiple_connections(self):
        pool = self.get_pool()
        async with pool:
            c1 = await pool.acquire(blocking=True)
            c2 = await pool.acquire(blocking=True)
            assert c1 != c2

    async def test_max_connections_timeout(self):
        pool = self.get_pool(max_connections=2)
        async with pool:
            with move_on_after(1) as scope:
                await pool.acquire(blocking=True)
                await pool.acquire(blocking=True)
                await pool.acquire(blocking=True)
            assert scope.cancelled_caught

    async def test_pool_disconnect(self):
        pool = self.get_pool()
        async with pool:
            await pool.acquire(blocking=True)
            await pool.acquire(blocking=True)
            await pool.acquire(blocking=True)
        assert pool._connections == set()

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
            connection_class=UnixDomainSocketConnection,
        )
        expected = "ConnectionPool<UnixDomainSocketConnection<path=/abc,db=1>>"
        assert repr(pool) == expected

    async def test_connection_idle_check(self):
        rs = coredis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            connection_pool=coredis.ConnectionPool(
                blocking=True, max_idle_time=0.2, host="127.0.01", port=6379
            ),
        )
        async with rs:
            await rs.info()
            assert len(rs.connection_pool._connections) >= 1
            await sleep(0.3)
            assert len(rs.connection_pool._connections) == 0


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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
        }

    def test_password(self):
        pool = coredis.ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": "",
            "password": "mypassword",
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
        }

    def test_db_as_argument(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 1,
            "username": None,
            "password": None,
            "max_idle_time": None,
        }

    def test_db_in_querystring(self):
        pool = coredis.ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == coredis.UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 2,
            "username": None,
            "password": None,
            "max_idle_time": None,
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
            "max_idle_time": None,
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
            "max_idle_time": None,
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
        conn = pool.connection_class(**pool.connection_kwargs)
        assert conn.ssl_context.verify_mode == expected


class TestConnection:
    async def test_on_connect_error(self):
        """
        An error in Connection.on_connect should disconnect from the server
        see for details: https://github.com/andymccurdy/redis-py/issues/368
        """
        # this assumes the Redis server being tested against doesn't have
        # 9999 databases ;)
        bad_connection = coredis.Redis(db=9999)
        with pytest.raises(Exception):
            await bad_connection.__aenter__()

    async def test_busy_loading_from_pipeline(self):
        """
        BusyLoadingErrors should be raised from a pipeline execution
        regardless of the raise_on_error flag.
        """
        client = coredis.Redis()
        async with client:
            async with client.pipeline() as pipe:
                pipe.create_request(
                    b"DEBUG", b"ERROR", b"LOADING fake message", callback=lambda r, **k: r
                )
                with pytest.raises(RedisError):
                    await pipe._execute()
                pool = client.connection_pool
                assert len(pool._connections) >= 1
                return

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
