from __future__ import annotations

import anyio
import pytest

import coredis.connection
from coredis import ClusterConnectionPool, ConnectionPool, TCPConnection, UnixDomainSocketConnection
from coredis._concurrency import gather
from coredis.connection import ClusterConnection, TCPLocation, UnixDomainSocketLocation
from coredis.exceptions import ConnectionError
from coredis.patterns.cache import LRUCache
from tests.conftest import targets


class CommonPoolUrlParsingExamples:
    @pytest.mark.parametrize(
        "url, kwargs, username, password, decode_components",
        [
            ("redis://localhost", {}, None, None, False),
            ("redis://a:b@localhost", {}, "a", "b", False),
            ("redis://a:@localhost", {}, "a", None, False),
            ("redis://:b@localhost", {}, None, "b", False),
            ("redis://:b@localhost", {"username": "a"}, "a", "b", False),
            ("redis://:a%20b@localhost", {"username": "a"}, "a", "a b", True),
        ],
    )
    def test_authentication_parameters(
        self, pool_cls, url, kwargs, username, password, decode_components
    ):
        connection_args = pool_cls.from_url(url, decode_components=True, **kwargs).connection_kwargs
        assert connection_args.get("username") == username
        assert connection_args.get("password") == password

    @pytest.mark.parametrize(
        "url, kwargs, expected",
        [
            ("redis://localhost?stream_timeout=0.1", {}, {"stream_timeout": 0.1}),
            ("redis://localhost?connect_timeout=0.1", {}, {"connect_timeout": 0.1}),
            ("redis://localhost?max_idle_time=5", {}, {"max_idle_time": 5}),
            ("redis://localhost?noreply=1", {}, {"noreply": True}),
            ("redis://localhost?noreply=0", {}, {"noreply": False}),
            ("redis://localhost?noevict=1", {}, {"noevict": True}),
            ("redis://localhost?noevict=0", {}, {"noevict": False}),
            ("redis://localhost?notouch=1", {}, {"notouch": True}),
            ("redis://localhost?notouch=0", {}, {"notouch": False}),
        ],
    )
    def test_connection_parameters(self, pool_cls, url, kwargs, expected):
        connection_args = pool_cls.from_url(url, **kwargs).connection_kwargs
        for k, v in expected.items():
            assert connection_args[k] == v

    @pytest.mark.parametrize(
        "url, kwargs, db",
        [
            ("redis://localhost:1/2", {}, 2),
            (
                "redis://localhost:1?db=2",
                {},
                2,
            ),
            ("redis://localhost:6379", {}, None),
            ("redis://localhost:6379", {"db": 1}, 1),
        ],
    )
    def test_db(self, pool_cls, url, kwargs, db):
        connection_args = pool_cls.from_url(url, **kwargs).connection_kwargs
        assert connection_args.get("db") == db


class TestConnectionPoolUrlParsing(CommonPoolUrlParsingExamples):
    @pytest.fixture
    def pool_cls(self):
        return ConnectionPool

    @pytest.mark.parametrize(
        "url, kwargs, username, password",
        [
            ("unix://a:b@/var/tmp/redis.sock", {}, "a", "b"),
            ("unix://:b@/var/tmp/redis.sock", {}, None, "b"),
            ("unix://a:@/var/tmp/redis.sock", {}, "a", None),
        ],
    )
    def test_uds_authentication_parameters(self, pool_cls, url, kwargs, username, password):
        pool = pool_cls.from_url(url, **kwargs)
        assert pool.connection_class == UnixDomainSocketConnection
        connection_args = pool.connection_kwargs
        assert connection_args.get("username") == username
        assert connection_args.get("password") == password

    @pytest.mark.parametrize(
        "url, kwargs, expected",
        [
            (
                "redis://localhost?max_connections=5",
                {},
                {"max_connections": 5, "connection_class": TCPConnection},
            ),
            ("redis://localhost?timeout=5", {}, {"timeout": 5, "connection_class": TCPConnection}),
            (
                "unix://localhost?timeout=5",
                {},
                {"timeout": 5, "connection_class": UnixDomainSocketConnection},
            ),
        ],
    )
    def test_pool_parameters(self, pool_cls, url, kwargs, expected, mocker):
        spy = mocker.spy(pool_cls, "__init__")
        pool_cls.from_url(url, **kwargs)
        for k, v in expected.items():
            assert spy.call_args.kwargs[k] == v


class TestClusterConnectionPoolUrlParsing(CommonPoolUrlParsingExamples):
    @pytest.fixture
    def pool_cls(self):
        return ClusterConnectionPool

    @pytest.mark.parametrize(
        "url, kwargs, expected",
        [
            (
                "redis://localhost?max_connections=10",
                {},
                {"max_connections": 10},
            ),
            (
                "redis://localhost?max_connections_per_node=1",
                {},
                {"max_connections_per_node": 1},
            ),
            (
                "redis://localhost?timeout=1",
                {},
                {"timeout": 1},
            ),
            (
                "redis://localhost?read_from_replicas=1",
                {},
                {"read_from_replicas": True},
            ),
        ],
    )
    def test_cluster_pool_parameters(self, pool_cls, url, kwargs, expected, mocker):
        spy = mocker.spy(pool_cls, "__init__")
        pool_cls.from_url(url, **kwargs)
        for k, v in expected.items():
            assert spy.call_args.kwargs[k] == v


@targets("redis_basic")
class TestBasicPoolParameters:
    @pytest.mark.parametrize("client_arguments", [{"max_connections": 2}])
    async def test_max_connections(self, client, client_arguments, mocker):
        connect_tcp = mocker.spy(coredis.connection._tcp, "connect_tcp")
        await gather(*(client.blpop(["test"], timeout=1) for _ in range(3)))
        assert connect_tcp.call_count == 1

    @pytest.mark.parametrize("client_arguments", [{"max_connections": 2}])
    async def test_timeout(self, client, client_arguments, mocker):
        client.connection_pool.timeout = 1
        with pytest.RaisesGroup(TimeoutError):
            await gather(*(client.blpop(["test"], timeout=2) for _ in range(3)))


class TestBasicConnectionPoolConstruction:
    async def test_unintialized_pool(self, redis_basic_server):
        pool = coredis.ConnectionPool(location=TCPLocation(*redis_basic_server))
        with pytest.raises(RuntimeError, match="Connection pool is not initialized"):
            async with pool.acquire() as _:
                pass

        with pytest.raises(RuntimeError, match="Connection pool is not initialized"):
            await pool.get_connection()

    async def test_construction_with_tcp_location(self, redis_basic_server):
        async with coredis.ConnectionPool(location=TCPLocation(*redis_basic_server)) as pool:
            async with pool.acquire() as connection:
                assert isinstance(connection, TCPConnection)

    async def test_construction_with_host_port(self, redis_basic_server):
        async with coredis.ConnectionPool(
            host=redis_basic_server[0], port=redis_basic_server[1]
        ) as pool:
            async with pool.acquire() as connection:
                assert isinstance(connection, TCPConnection)

    async def test_construction_with_uds_location(self, redis_uds_server):
        async with coredis.ConnectionPool(
            location=UnixDomainSocketLocation(redis_uds_server)
        ) as pool:
            async with pool.acquire() as connection:
                assert isinstance(connection, UnixDomainSocketConnection)


@targets("redis_cluster")
class TestClusterPoolParameters:
    @pytest.mark.parametrize(
        "client_arguments", [{"max_connections": 2, "max_connections_per_node": True}]
    )
    async def test_max_connections(self, client, client_arguments, mocker):
        connect_tcp = mocker.spy(coredis.connection._tcp, "connect_tcp")
        await gather(*(client.blpop(["test"], timeout=1) for _ in range(3)))
        assert connect_tcp.call_count == 1

    @pytest.mark.parametrize(
        "client_arguments",
        [{"max_connections": 2, "max_connections_per_node": True, "pool_timeout": 1}],
    )
    async def test_timeout(self, client, client_arguments, mocker):
        with pytest.RaisesGroup(TimeoutError):
            await gather(*(client.blpop(["test"], timeout=2) for _ in range(3)))

    @pytest.mark.parametrize(
        "client_arguments",
        [{"read_from_replicas": True}],
    )
    async def test_read_from_replicas(self, client, client_arguments, mocker, _s):
        create_request = mocker.spy(client.connection_pool.connection_class, "create_request")
        await client.set("fubar", 1)
        assert _s(1) == await client.get("fubar")
        assert any(call.args[1] == b"READONLY" for call in create_request.call_args_list)


class TestClusterConnectionPoolConstruction:
    async def test_construction_with_startup_nodes(self, redis_cluster_server):
        async with coredis.ClusterConnectionPool(
            startup_nodes=[TCPLocation(*redis_cluster_server)]
        ) as pool:
            async with pool.acquire() as connection:
                assert isinstance(connection, ClusterConnection)


class TestCluserConnectionPoolLayoutCache:
    async def test_layout_initialization(self, redis_cluster_server):
        async with coredis.ClusterConnectionPool(
            startup_nodes=[TCPLocation(*redis_cluster_server)]
        ) as pool:
            assert pool.cluster_layout is not None
            assert pool.cluster_layout.nodes
            assert pool.cluster_layout.primaries
            assert pool.cluster_layout.replicas

    async def test_layout_refresh(self, redis_cluster_server, mocker):
        async with coredis.ClusterConnectionPool(
            startup_nodes=[TCPLocation(*redis_cluster_server)], reinitialize_steps=5
        ) as pool:
            refresh = mocker.spy(pool.cluster_layout, "_refresh")
            node = pool.cluster_layout.node_for_slot(1)

            [pool.cluster_layout.report_errors(node, ConnectionError()) for _ in range(5)]
            await anyio.sleep(0.1)
            assert refresh.call_count == 1

    async def test_garbage_collection(self, redis_cluster_server, mocker):
        cluster_slots = coredis.Redis.cluster_slots

        async def remove_replica_for_slot_1(self, *args, **kwargs):
            values = await cluster_slots(self, *args, **kwargs)
            slot_range, nodes = list(values.items())[0]
            values[slot_range] = nodes[:1]
            return values

        async with coredis.ClusterConnectionPool(
            startup_nodes=[TCPLocation(*redis_cluster_server)],
            reinitialize_steps=5,
            gc_interval=1,
        ) as pool:
            node = pool.cluster_layout.node_for_slot(1, primary=False)
            replica_connection = await pool.get_connection(node)
            pool.release(replica_connection)
            mocker.patch.object(coredis.Redis, "cluster_slots", new=remove_replica_for_slot_1)
            [pool.cluster_layout.report_errors(node, ConnectionError()) for _ in range(5)]
            assert replica_connection.usable
            await anyio.sleep(1)
            assert not replica_connection.usable


@targets("redis_basic", "redis_cluster")
class TestSharedConnectionPool:
    async def test_shared_pool(self, client, cloner, mocker):
        primary = await cloner(client)
        connect_tcp = mocker.spy(coredis.connection._tcp, "connect_tcp")
        async with primary:
            await primary.ping()
            assert connect_tcp.call_count > 0
            mocker.resetall()
            borrower = client.__class__(
                connection_pool=primary.connection_pool,
            )
            assert primary.connection_pool == borrower.connection_pool
            assert not client.connection_pool == borrower.connection_pool
            async with borrower:
                await borrower.ping()
                assert not connect_tcp.call_count

    async def test_concurrent_initialization(self, client, cloner, mocker):
        clone = await cloner(client)
        pool = clone.connection_pool

        async def ping(pool):
            async with client.__class__(connection_pool=pool) as c:
                await c.ping()

        ping_spy = mocker.spy(client.__class__, "ping")

        with pytest.raises(Exception) as exc_info:
            async with anyio.create_task_group() as tg:
                [tg.start_soon(ping, pool) for _ in range(10)]
        assert exc_info.group_contains(
            RuntimeError, match="Implicit concurrent connection pool sharing detected"
        )
        ping_spy.assert_called_once()

    @pytest.mark.parametrize("client_arguments", [{"cache": LRUCache()}])
    async def test_shared_cache(self, client, client_arguments, mocker):
        borrower = client.__class__(connection_pool=client.connection_pool)
        create_request = mocker.spy(client.connection_pool.connection_class, "create_request")
        await client.set("a", 1)
        assert borrower.connection_pool is client.connection_pool
        await client.get("a")
        async with borrower:
            await borrower.get("a")
        create_request.assert_called_once
