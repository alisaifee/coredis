from __future__ import annotations

import asyncio
import os
from collections import deque
from unittest.mock import Mock, patch

import pytest

from coredis import Redis
from coredis.connection import ClusterConnection, Connection, UnixDomainSocketConnection
from coredis.exceptions import ConnectionError, RedisClusterException
from coredis.parser import Parser
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.pool.nodemanager import ManagedNode
from tests.conftest import targets


class DummyConnection(ClusterConnection):
    description_format = "DummyConnection<>"

    def __init__(self, host="localhost", port=7000, socket_timeout=None, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.awaiting_response = False
        self._parser = Parser()
        self._last_error = None
        self._transport = None
        self._read_flag = asyncio.Event()
        self._read_waiters = set()
        self._description_args = lambda: {}
        self._parse_task = None
        self._requests = deque()


class TestConnectionPool:
    async def get_pool(
        self,
        connection_kwargs=None,
        max_connections=None,
        max_connections_per_node=None,
        connection_class=DummyConnection,
        blocking=False,
        timeout=0,
    ):
        connection_kwargs = connection_kwargs or {}
        pool = ClusterConnectionPool(
            connection_class=connection_class,
            max_connections=max_connections,
            max_connections_per_node=max_connections_per_node,
            startup_nodes=[{"host": "127.0.0.1", "port": 7000}],
            blocking=blocking,
            timeout=timeout,
            **connection_kwargs,
        )
        await pool.initialize()

        return pool

    async def test_no_available_startup_nodes(self, redis_cluster):
        pool = ClusterConnectionPool(
            startup_nodes=[{"host": "foo", "port": 6379}, {"host": "bar", "port": 6379}]
        )
        with pytest.raises(RedisClusterException, match="Redis Cluster cannot be connected"):
            await pool.initialize()
        with pytest.raises(RedisClusterException, match="Cant reach a single startup node"):
            await pool.get_connection_by_slot(1)
        with pytest.raises(RedisClusterException, match="Cant reach a single startup node"):
            await pool.get_random_connection()

    async def test_in_use_not_exists(self, redis_cluster):
        """
        Test that if for some reason, the node that it tries to get the connectino for
        do not exists in the _in_use_connection variable.
        """
        pool = await self.get_pool()
        pool._in_use_connections = {}
        await pool.get_connection(b"pubsub", channel="foobar")

    async def test_connection_creation(self, redis_cluster):
        connection_kwargs = {"foo": "bar", "biz": "baz"}
        pool = await self.get_pool(connection_kwargs=connection_kwargs)
        connection = await pool.get_connection_by_node(
            ManagedNode(**{"host": "127.0.0.1", "port": 7000})
        )
        assert isinstance(connection, DummyConnection)

        for key in connection_kwargs:
            assert connection.kwargs[key] == connection_kwargs[key]

    async def test_multiple_connections(self, redis_cluster):
        pool = await self.get_pool()
        c1 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        c2 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        assert c1 != c2

    async def test_max_connections_too_low(self, redis_cluster):
        with pytest.warns(UserWarning, match="increased by 4 connections"):
            pool = await self.get_pool(max_connections=2)
        assert pool.max_connections == 6

    async def test_max_connections(self, redis_cluster):
        pool = await self.get_pool(max_connections=6)
        for port in range(7000, 7006):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": port}))
        with pytest.raises(ConnectionError):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))

    async def test_max_connections_blocking(self, redis_cluster):
        pool = await self.get_pool(max_connections=6, blocking=True, timeout=1)
        connections = []
        for port in range(7000, 7006):
            connections.append(
                await pool.get_connection_by_node(
                    ManagedNode(**{"host": "127.0.0.1", "port": port})
                )
            )
        with pytest.raises(ConnectionError):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        pool.release(connections[0])
        assert connections[0] == await pool.get_connection_by_node(
            ManagedNode(**{"host": "127.0.0.1", "port": 7000})
        )

    async def test_max_connections_per_node(self, redis_cluster):
        pool = await self.get_pool(max_connections=2, max_connections_per_node=True)
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        with pytest.raises(ConnectionError):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))

    async def test_max_connections_per_node_blocking(self, redis_cluster):
        pool = await self.get_pool(
            max_connections=2, max_connections_per_node=True, blocking=True, timeout=1
        )
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        with pytest.raises(ConnectionError):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))

    async def test_max_connections_default_setting(self):
        pool = await self.get_pool(max_connections=None)
        assert pool.max_connections == 2**31

    async def test_pool_disconnect(self):
        pool = await self.get_pool()
        c1 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        c2 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7001}))
        c3 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        pool.release(c3)
        pool.disconnect()
        assert not c1.is_connected
        assert not c2.is_connected
        assert not c3.is_connected

    async def test_reuse_previously_released_connection(self):
        pool = await self.get_pool()
        c1 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        pool.release(c1)
        c2 = await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))
        assert c1 == c2

    async def test_repr_contains_db_info_tcp(self, host_ip):
        """
        Note: init_slot_cache muts be set to false otherwise it will try to
              query the test server for data and then it can't be predicted reliably
        """
        connection_kwargs = {"host": "127.0.0.1", "port": 7000}
        pool = await self.get_pool(
            connection_kwargs=connection_kwargs, connection_class=ClusterConnection
        )
        expected = f"ClusterConnection<host={host_ip},port=7000>"
        assert expected in repr(pool)

    async def test_get_connection_by_key(self):
        """
        This test assumes that when hashing key 'foo' will be sent to server with port 7002
        """
        pool = await self.get_pool(connection_kwargs={})

        # Patch the call that is made inside the method to allow control of the returned
        # connection object
        with patch.object(
            ClusterConnectionPool, "get_connection_by_slot", autospec=True
        ) as pool_mock:

            async def side_effect(self, *args, **kwargs):
                return DummyConnection(port=1337)

            pool_mock.side_effect = side_effect

            connection = await pool.get_connection_by_key("foo")
            assert connection.port == 1337

        with pytest.raises(RedisClusterException) as ex:
            await pool.get_connection_by_key(None)
        assert str(ex.value).startswith("No way to dispatch this command to Redis Cluster."), True

    async def test_get_connection_by_slot(self):
        """
        This test assumes that when doing keyslot operation on "foo" it will return 12182
        """
        pool = await self.get_pool(connection_kwargs={})

        # Patch the call that is made inside the method to allow control of the returned
        # connection object
        with patch.object(
            ClusterConnectionPool, "get_connection_by_node", autospec=True
        ) as pool_mock:

            async def side_effect(self, *args, **kwargs):
                return DummyConnection(port=1337)

            pool_mock.side_effect = side_effect

            connection = await pool.get_connection_by_slot(12182)
            assert connection.port == 1337

        class AsyncMock(Mock):
            def __await__(self):
                future = asyncio.Future(loop=asyncio.get_event_loop())
                future.set_result(self)
                result = yield from future

                return result

        m = AsyncMock()
        pool.get_random_connection = m

        # If None value is provided then a random node should be tried/returned
        await pool.get_connection_by_slot(None)
        m.assert_called_once_with()

    async def test_get_connection_blocked(self):
        """
        Currently get_connection() should only be used by pubsub command.
        All other commands should be blocked and exception raised.
        """
        pool = await self.get_pool()

        with pytest.raises(RedisClusterException) as ex:
            await pool.get_connection("GET")
        assert str(ex.value).startswith("Only 'pubsub' commands can use get_connection()")

    async def test_master_node_by_slot(self):
        pool = await self.get_pool(connection_kwargs={})
        node = pool.get_primary_node_by_slot(0)
        node.port = 7000
        node = pool.get_primary_node_by_slot(12182)
        node.port = 7002

    async def test_connection_idle_check(self):
        pool = ClusterConnectionPool(
            startup_nodes=[dict(host="127.0.0.1", port=7000)],
            max_idle_time=0.2,
            idle_check_interval=0.1,
        )
        await pool.initialize()
        conn = await pool.get_connection_by_node(
            ManagedNode(
                **{
                    "host": "127.0.0.1",
                    "port": 7000,
                    "server_type": "primary",
                }
            )
        )
        name = conn.node.name
        assert len(pool._cluster_in_use_connections[name]) == 1
        pool.release(conn)
        assert len(pool._cluster_in_use_connections[name]) == 0
        assert pool._cluster_available_connections[name].qsize() == 1
        await asyncio.sleep(0.3)
        assert len(pool._cluster_in_use_connections[name]) == 0
        last_active_at = conn.last_active_at
        assert last_active_at == conn.last_active_at
        assert conn._transport is None

    @targets(
        "redis_cluster",
    )
    async def test_coverage_check_fail(self, client, user_client, _s):
        with pytest.warns(
            UserWarning,
            match="Unable to determine whether the cluster requires full coverage",
        ):
            no_perm_client = await user_client("testuser", "on", "+@all", "-CONFIG")
            assert _s("PONG") == await no_perm_client.ping()


class TestReadOnlyConnectionPool:
    async def get_pool(self, connection_kwargs=None, max_connections=None, startup_nodes=None):
        startup_nodes = startup_nodes or [{"host": "127.0.0.1", "port": 7000}]
        connection_kwargs = connection_kwargs or {}
        pool = ClusterConnectionPool(
            max_connections=max_connections,
            startup_nodes=startup_nodes,
            read_from_replicas=True,
            **connection_kwargs,
        )
        await pool.initialize()

        return pool

    async def test_repr_contains_db_info_readonly(self, host_ip):
        """
        Note: init_slot_cache must be set to false otherwise it will try to
              query the test server for data and then it can't be predicted reliably
        """
        pool = await self.get_pool(
            startup_nodes=[
                {"host": "127.0.0.1", "port": 7000},
                {"host": "127.0.0.2", "port": 7001},
            ],
        )
        assert f"ClusterConnection<host={host_ip},port=7001>" in repr(pool)
        assert f"ClusterConnection<host={host_ip},port=7000>" in repr(pool)

    async def test_max_connections(self):
        pool = await self.get_pool(max_connections=6)
        for port in range(7000, 7006):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": port}))
        with pytest.raises(ConnectionError):
            await pool.get_connection_by_node(ManagedNode(**{"host": "127.0.0.1", "port": 7000}))


class TestConnectionPoolURLParsing:
    def test_defaults(self):
        pool = ConnectionPool.from_url("redis://localhost")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_hostname(self):
        pool = ConnectionPool.from_url("redis://myhost")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "myhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_quoted_hostname(self):
        pool = ConnectionPool.from_url("redis://my %2F host %2B%3D+", decode_components=True)
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "my / host +=+",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_port(self):
        pool = ConnectionPool.from_url("redis://localhost:6380")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6380,
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_password(self):
        pool = ConnectionPool.from_url("redis://:mypassword@localhost")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": "",
            "password": "mypassword",
        }

    def test_quoted_password(self):
        pool = ConnectionPool.from_url(
            "redis://:%2Fmypass%2F%2B word%3D%24+@localhost", decode_components=True
        )
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "username": None,
            "password": "/mypass/+ word=$+",
        }

    def test_quoted_path(self):
        pool = ConnectionPool.from_url(
            "unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket",
            decode_components=True,
        )
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/my/path/to/../+_+=$ocket",
            "db": 0,
            "username": None,
            "password": "mypassword",
        }

    def test_db_as_argument(self):
        pool = ConnectionPool.from_url("redis://localhost", db="1")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 1,
            "username": None,
            "password": None,
        }

    def test_db_in_path(self):
        pool = ConnectionPool.from_url("redis://localhost/2", db="1")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 2,
            "username": None,
            "password": None,
        }

    def test_db_in_querystring(self):
        pool = ConnectionPool.from_url("redis://localhost/2?db=3", db="1")
        assert pool.connection_class == Connection
        assert pool.connection_kwargs == {
            "host": "localhost",
            "port": 6379,
            "db": 3,
            "username": None,
            "password": None,
        }

    def test_extra_querystring_options(self):
        pool = ConnectionPool.from_url("redis://localhost?a=1&b=2")
        assert pool.connection_class == Connection
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
        r = Redis.from_url("redis://myhost")
        assert r.connection_pool.connection_class == Connection
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
        pool = ConnectionPool.from_url("unix:///socket")
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": None,
            "password": None,
        }

    def test_password(self):
        pool = ConnectionPool.from_url("unix://:mypassword@/socket")
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": "",
            "password": "mypassword",
        }

    def test_db_as_argument(self):
        pool = ConnectionPool.from_url("unix:///socket", db=1)
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 1,
            "username": None,
            "password": None,
        }

    def test_db_in_querystring(self):
        pool = ConnectionPool.from_url("unix:///socket?db=2", db=1)
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 2,
            "username": None,
            "password": None,
        }

    def test_extra_querystring_options(self):
        pool = ConnectionPool.from_url("unix:///socket?a=1&b=2")
        assert pool.connection_class == UnixDomainSocketConnection
        assert pool.connection_kwargs == {
            "path": "/socket",
            "db": 0,
            "username": None,
            "password": None,
            "a": "1",
            "b": "2",
        }
