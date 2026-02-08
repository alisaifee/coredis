from __future__ import annotations

import pytest

import coredis.connection
from coredis import (
    ClusterConnectionPool,
    Connection,
    ConnectionPool,
    UnixDomainSocketConnection,
)
from coredis._concurrency import gather
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
        self, pool, url, kwargs, username, password, decode_components
    ):
        connection_args = pool.from_url(url, decode_components=True, **kwargs).connection_kwargs
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
    def test_connection_parameters(self, pool, url, kwargs, expected):
        connection_args = pool.from_url(url, **kwargs).connection_kwargs
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
    def test_db(self, pool, url, kwargs, db):
        connection_args = pool.from_url(url, **kwargs).connection_kwargs
        assert connection_args.get("db") == db


class TestConnectionPoolUrlParsing(CommonPoolUrlParsingExamples):
    @pytest.fixture
    def pool(self):
        return ConnectionPool

    @pytest.mark.parametrize(
        "url, kwargs, username, password",
        [
            ("unix://a:b@/var/tmp/redis.sock", {}, "a", "b"),
            ("unix://:b@/var/tmp/redis.sock", {}, None, "b"),
            ("unix://a:@/var/tmp/redis.sock", {}, "a", None),
        ],
    )
    def test_uds_authentication_parameters(self, pool, url, kwargs, username, password):
        connection_args = pool.from_url(url, **kwargs).connection_kwargs
        assert connection_args.get("username") == username
        assert connection_args.get("password") == password

    @pytest.mark.parametrize(
        "url, kwargs, expected",
        [
            (
                "redis://localhost?max_connections=5",
                {},
                {"max_connections": 5, "connection_class": Connection},
            ),
            ("redis://localhost?timeout=5", {}, {"timeout": 5, "connection_class": Connection}),
            (
                "unix://localhost?timeout=5",
                {},
                {"timeout": 5, "connection_class": UnixDomainSocketConnection},
            ),
        ],
    )
    def test_pool_parameters(self, pool, url, kwargs, expected, mocker):
        spy = mocker.spy(pool, "__init__")
        pool.from_url(url, **kwargs)
        for k, v in expected.items():
            assert spy.call_args.kwargs[k] == v


class TestClusterConnectionPoolUrlParsing(CommonPoolUrlParsingExamples):
    @pytest.fixture
    def pool(self):
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
    def test_cluster_pool_parameters(self, pool, url, kwargs, expected, mocker):
        spy = mocker.spy(pool, "__init__")
        pool.from_url(url, **kwargs)
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
        [{"max_connections": 2, "max_connections_per_node": True}],
    )
    async def test_timeout(self, client, client_arguments, mocker):
        client.connection_pool.timeout = 1
        with pytest.RaisesGroup(TimeoutError):
            await gather(*(client.blpop(["test"], timeout=2) for _ in range(3)))


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
