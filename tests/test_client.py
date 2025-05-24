from __future__ import annotations

import asyncio
import ssl
from ssl import SSLError

import async_timeout
import pytest
from packaging.version import Version

import coredis
from coredis.exceptions import (
    AuthorizationError,
    ConnectionError,
    PersistenceError,
    ReplicationError,
    UnknownCommandError,
)
from coredis.typing import RedisCommand
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_ssl",
    "redis_ssl_no_client_auth",
    "dragonfly",
    "valkey",
    "redict",
)
class TestClient:
    @pytest.fixture(autouse=True)
    async def configure_client(self, client):
        client.verify_version = True
        await client.ping()

    async def test_server_version(self, client):
        assert isinstance(client.server_version, Version)
        await client.ping()
        assert isinstance(client.server_version, Version)

    async def test_unknown_command(self, client):
        with pytest.raises(UnknownCommandError):
            await client.execute_command(RedisCommand(b"BOGUS", ()))

    @pytest.mark.nodragonfly
    @pytest.mark.parametrize("client_arguments", [{"db": 1}])
    async def test_select_database(self, client, client_arguments):
        assert (await client.client_info())["db"] == 1

    @pytest.mark.nodragonfly
    @pytest.mark.parametrize("client_arguments", [{"client_name": "coredis"}])
    async def test_set_client_name(self, client, client_arguments):
        assert (await client.client_info())["name"] == "coredis"

    async def test_noreply_client(self, client, cloner, _s):
        noreply = await cloner(client, noreply=True)
        assert not await noreply.set("fubar", 1)
        await asyncio.sleep(0.01)
        assert await client.get("fubar") == _s("1")
        assert not await noreply.delete(["fubar"])
        await asyncio.sleep(0.01)
        assert not await client.get("fubar")
        assert not await noreply.ping()

    @pytest.mark.nodragonfly
    async def test_noreply_context(self, client, _s):
        with client.ignore_replies():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)

    @pytest.mark.min_server_version("7.1.240")
    @pytest.mark.nodragonfly
    async def test_ensure_persistence(self, client, _s):
        with client.ensure_persistence(1, 0, 2000):
            assert await client.set("fubar", 1)

        with pytest.raises(PersistenceError):
            with client.ensure_persistence(1, 1, 2000):
                assert await client.set("fubar", 1)
        assert await client.set("fubar", 1)

    async def test_decoding_context(self, client):
        await client.set("fubar", "A")
        with client.decoding(False):
            assert b"A" == await client.get("fubar")
            with client.decoding(True, encoding="cp424"):
                assert "א" == await client.get("fubar")

    async def test_blocking_task_cancellation(self, client, _s):
        awaitable = client.blpop(["nonexistent"], timeout=10)
        task = asyncio.ensure_future(awaitable)
        await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        async with async_timeout.timeout(0.1):
            assert _s("PONG") == await client.ping()

    @pytest.mark.nodragonfly
    async def test_concurrent_initialization(self, client, mocker):
        assert await client.client_kill(skipme=False)
        client.connection_pool.reset()
        connection = await client.connection_pool.get_connection(b"set", acquire=False)
        spy = mocker.spy(connection, "perform_handshake")
        await asyncio.gather(*[client.set(f"fubar{i}", bytes(2**16)) for i in range(10)])
        assert spy.call_count == 1


@targets(
    "redis_cluster",
    "redis_cluster_blocking",
)
class TestClusterClient:
    async def test_noreply_client(self, client, cloner, _s):
        noreply = await cloner(client, noreply=True)
        assert not await noreply.set("fubar", 1)
        await asyncio.sleep(0.01)
        assert await client.get("fubar") == _s("1")
        assert not await noreply.delete(["fubar"])
        await asyncio.sleep(0.01)
        assert not await client.get("fubar")

    async def test_noreply_context(self, client, _s):
        with client.ignore_replies():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)

    async def test_ensure_replication_unavailable(self, client, _s, user_client):
        no_perm_client = await user_client("testuser", "on", "allkeys", "+@all", "-WAIT")
        with pytest.raises(AuthorizationError):
            with no_perm_client.ensure_replication(1):
                assert await no_perm_client.set("fubar", 1)

    async def test_ensure_replication(self, client, _s):
        with client.ensure_replication(1):
            assert await client.set("fubar", 1)

        with pytest.raises(ReplicationError):
            with client.ensure_replication(2):
                assert await client.set("fubar", 1)
        assert await client.set("fubar", 1)

    @pytest.mark.min_server_version("7.1.240")
    async def test_ensure_persistence_unavailable(self, client, _s, user_client):
        no_perm_client = await user_client("testuser", "on", "allkeys", "+@all", "-WAITAOF")
        with pytest.raises(AuthorizationError):
            with no_perm_client.ensure_persistence(1, 1, 2000):
                await no_perm_client.set("fubar", 1)

    @pytest.mark.min_server_version("7.1.240")
    async def test_ensure_persistence(self, client, _s):
        with client.ensure_persistence(1, 1, 2000):
            assert await client.set("fubar", 1)

        with pytest.raises(PersistenceError):
            with client.ensure_persistence(1, 2, 2000):
                assert await client.set("fubar", 1)
        assert await client.set("fubar", 1)

    async def test_decoding_context(self, client):
        await client.set("fubar", "A")
        with client.decoding(False):
            assert b"A" == await client.get("fubar")
            with client.decoding(True, encoding="cp424"):
                assert "א" == await client.get("fubar")


class TestSSL:
    async def test_explicit_ssl_parameters(self, redis_ssl_server):
        client = coredis.Redis(
            port=8379,
            ssl=True,
            ssl_keyfile="./tests/tls/client.key",
            ssl_certfile="./tests/tls/client.crt",
            ssl_ca_certs="./tests/tls/ca.crt",
        )
        assert await client.ping() == b"PONG"

    async def test_explicit_ssl_context(self, redis_ssl_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
        client = coredis.Redis(
            port=8379,
            ssl_context=context,
        )
        assert await client.ping() == b"PONG"

    async def test_cluster_explicit_ssl_parameters(self, redis_ssl_cluster_server):
        client = coredis.RedisCluster(
            "localhost",
            port=8301,
            ssl=True,
            ssl_keyfile="./tests/tls/client.key",
            ssl_certfile="./tests/tls/client.crt",
            ssl_ca_certs="./tests/tls/ca.crt",
        )
        assert await client.ping() == b"PONG"

    async def test_cluster_explicit_ssl_context(self, redis_ssl_cluster_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
        client = coredis.RedisCluster(
            "localhost",
            8301,
            ssl_context=context,
        )
        assert await client.ping() == b"PONG"

    async def test_invalid_ssl_parameters(self, redis_ssl_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(
            certfile="./tests/tls/invalid-client.crt",
            keyfile="./tests/tls/invalid-client.key",
        )
        client = coredis.Redis(
            port=8379,
            ssl_context=context,
        )
        with pytest.raises(ConnectionError, match="decrypt error") as exc_info:
            await client.ping()
        assert isinstance(exc_info.value.__cause__, SSLError)

    async def test_ssl_no_verify_client(self, redis_ssl_server_no_client_auth):
        client = coredis.Redis(port=7379, ssl=True, ssl_cert_reqs="required")
        with pytest.raises(ConnectionError, match="certificate verify failed"):
            await client.ping()
        client = coredis.Redis(port=7379, ssl=True, ssl_cert_reqs="none")
        assert await client.ping() == b"PONG"


class TestFromUrl:
    async def test_basic_client(self, redis_basic_server):
        client = coredis.Redis.from_url(f"redis://{redis_basic_server[0]}:{redis_basic_server[1]}")
        assert b"PONG" == await client.ping()
        client = coredis.Redis.from_url(
            f"redis://{redis_basic_server[0]}:{redis_basic_server[1]}",
            decode_responses=True,
        )
        assert "PONG" == await client.ping()

    async def test_uds_client(self, redis_uds_server):
        client = coredis.Redis.from_url(f"redis://{redis_uds_server}")
        assert b"PONG" == await client.ping()
        client = coredis.Redis.from_url(f"redis://{redis_uds_server}", decode_responses=True)
        assert "PONG" == await client.ping()

    @pytest.mark.parametrize(
        "cert_reqs",
        [
            None,
            "none",
            "optional",
            "required",
        ],
    )
    async def test_ssl_client(self, redis_ssl_server, cert_reqs):
        storage_url = (
            f"rediss://{redis_ssl_server[0]}:{redis_ssl_server[1]}/"
            "?ssl_keyfile=./tests/tls/client.key"
            "&ssl_certfile=./tests/tls/client.crt"
            "&ssl_ca_certs=./tests/tls/ca.crt"
            "&ssl_check_hostname=false"
        )
        if cert_reqs is not None:
            storage_url += f"&ssl_cert_reqs={cert_reqs}"
        client = coredis.Redis.from_url(storage_url)
        assert b"PONG" == await client.ping()
        client = coredis.Redis.from_url(storage_url, decode_responses=True)
        assert "PONG" == await client.ping()

    async def test_cluster_client(self, redis_cluster_server):
        client = coredis.RedisCluster.from_url(
            f"redis://{redis_cluster_server[0]}:{redis_cluster_server[1]}"
        )
        assert b"PONG" == await client.ping()
        client = coredis.RedisCluster.from_url(
            f"redis://{redis_cluster_server[0]}:{redis_cluster_server[1]}",
            decode_responses=True,
        )
        assert "PONG" == await client.ping()

    @pytest.mark.parametrize(
        "cert_reqs",
        [
            None,
            "none",
            "optional",
            "required",
        ],
    )
    async def test_cluster_ssl_client(self, redis_ssl_cluster_server, cert_reqs):
        storage_url = (
            f"rediss://{redis_ssl_cluster_server[0]}:{redis_ssl_cluster_server[1]}/"
            "?ssl_keyfile=./tests/tls/client.key"
            "&ssl_certfile=./tests/tls/client.crt"
            "&ssl_ca_certs=./tests/tls/ca.crt"
            "&ssl_check_hostname=false"
        )
        if cert_reqs is not None:
            storage_url += f"&ssl_cert_reqs={cert_reqs}"
        client = coredis.RedisCluster.from_url(storage_url)
        assert b"PONG" == await client.ping()
        client = coredis.RedisCluster.from_url(storage_url, decode_responses=True)
        assert "PONG" == await client.ping()
