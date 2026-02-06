from __future__ import annotations

import pytest

from coredis import ClusterConnectionPool, Connection, ConnectionPool, UnixDomainSocketConnection


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
