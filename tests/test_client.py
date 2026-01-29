from __future__ import annotations

import ssl
from ssl import SSLError

import anyio
import pytest
from anyio import create_task_group, fail_after, sleep
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

    @pytest.mark.parametrize("client_arguments", [{"db": 1}])
    @pytest.mark.nodragonfly
    async def test_select_database(self, client, client_arguments):
        assert (await client.client_info())["db"] == 1

    @pytest.mark.parametrize("client_arguments", [{"client_name": "coredis"}])
    @pytest.mark.nodragonfly
    async def test_set_client_name(self, client, client_arguments):
        assert (await client.client_info())["name"] == "coredis"

    async def test_noreply_client(self, client, cloner, _s):
        async with await cloner(client, noreply=True) as noreply:
            assert not await noreply.set("fubar", 1)
            await sleep(0.01)
            assert await client.get("fubar") == _s("1")
            assert not await noreply.delete(["fubar"])
            await sleep(0.01)
            assert not await client.get("fubar")
            assert not await noreply.ping()

    @pytest.mark.nodragonfly
    async def test_noreply_context(self, client, _s):
        with client.ignore_replies():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)

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
        cancelled = False

        async def _runner():
            nonlocal cancelled
            try:
                return await client.blpop(["nonexistent"], 10)
            except anyio.get_cancelled_exc_class():
                cancelled = True
                raise

        async with create_task_group() as tg:
            tg.start_soon(_runner)
            await sleep(0.5)
            tg.cancel_scope.cancel()
        assert cancelled
        with fail_after(0.1):
            assert _s("PONG") == await client.ping()

    @pytest.mark.parametrize("client_arguments", [{"stream_timeout": 0.01}])
    async def test_stream_timeout(self, client, client_arguments, _s):
        with pytest.raises(TimeoutError):
            await client.hset("hash", {bytes(k): k for k in range(4096)})


@targets(
    "redis_cluster",
)
class TestClusterClient:
    async def test_noreply_client(self, client, cloner, _s):
        async with await cloner(client, noreply=True) as noreply:
            assert not await noreply.set("fubar", 1)
            await sleep(0.01)
            assert await client.get("fubar") == _s("1")
            assert not await noreply.delete(["fubar"])
            await sleep(0.01)
            assert not await client.get("fubar")

    @pytest.mark.nodragonfly
    async def test_noreply_context(self, client, _s):
        with client.ignore_replies():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)

    async def test_ensure_replication_unavailable(self, client, _s, user_client):
        async with await user_client(
            "testuser", "on", "allkeys", "+@all", "-WAIT"
        ) as no_perm_client:
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

    async def test_ensure_persistence_unavailable(self, client, _s, user_client):
        async with await user_client(
            "testuser", "on", "allkeys", "+@all", "-WAITAOF"
        ) as no_perm_client:
            with pytest.raises(AuthorizationError):
                with no_perm_client.ensure_persistence(1, 1, 2000):
                    await no_perm_client.set("fubar", 1)

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
        async with coredis.Redis(
            port=8379,
            ssl=True,
            ssl_keyfile="./tests/tls/client.key",
            ssl_certfile="./tests/tls/client.crt",
            ssl_ca_certs="./tests/tls/ca.crt",
        ) as client:
            assert await client.ping() == b"PONG"

    async def test_explicit_ssl_context(self, redis_ssl_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
        async with coredis.Redis(
            port=8379,
            ssl_context=context,
        ) as client:
            assert await client.ping() == b"PONG"

    async def test_cluster_explicit_ssl_parameters(self, redis_ssl_cluster_server):
        async with coredis.RedisCluster(
            "localhost",
            port=8301,
            ssl=True,
            ssl_keyfile="./tests/tls/client.key",
            ssl_certfile="./tests/tls/client.crt",
            ssl_ca_certs="./tests/tls/ca.crt",
        ) as client:
            assert await client.ping() == b"PONG"

    async def test_cluster_explicit_ssl_context(self, redis_ssl_cluster_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
        async with coredis.RedisCluster(
            "localhost",
            8301,
            ssl_context=context,
        ) as client:
            assert await client.ping() == b"PONG"

    async def test_invalid_ssl_parameters(self, redis_ssl_server):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(
            certfile="./tests/tls/invalid-client.crt",
            keyfile="./tests/tls/invalid-client.key",
        )

        async with coredis.Redis(
            port=8379,
            ssl_context=context,
        ) as client:
            with pytest.raises(ConnectionError) as exc_info:
                await client.ping()
            assert isinstance(exc_info.value.__cause__, SSLError)

    async def test_ssl_no_verify_client(self, redis_ssl_server_no_client_auth):
        async with coredis.Redis(port=7379, ssl=True, ssl_cert_reqs="required") as client:
            with pytest.raises(ConnectionError) as exc_info:
                await client.ping()
            assert isinstance(exc_info.value.__cause__, SSLError)
        async with coredis.Redis(port=7379, ssl=True, ssl_cert_reqs="none") as client:
            assert await client.ping() == b"PONG"


class TestFromUrl:
    async def test_basic_client(self, redis_basic_server):
        async with coredis.Redis.from_url(
            f"redis://{redis_basic_server[0]}:{redis_basic_server[1]}"
        ) as client:
            assert b"PONG" == await client.ping()
        async with coredis.Redis.from_url(
            f"redis://{redis_basic_server[0]}:{redis_basic_server[1]}",
            decode_responses=True,
        ) as client:
            assert "PONG" == await client.ping()

    async def test_uds_client(self, redis_uds_server):
        async with coredis.Redis.from_url(f"unix://{redis_uds_server}") as client:
            assert b"PONG" == await client.ping()
        async with coredis.Redis.from_url(
            f"unix://{redis_uds_server}", decode_responses=True
        ) as client:
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
        async with coredis.Redis.from_url(storage_url) as client:
            assert b"PONG" == await client.ping()
        async with coredis.Redis.from_url(storage_url, decode_responses=True) as client:
            assert "PONG" == await client.ping()

    async def test_cluster_client(self, redis_cluster_server):
        async with coredis.RedisCluster.from_url(
            f"redis://{redis_cluster_server[0]}:{redis_cluster_server[1]}"
        ) as client:
            assert b"PONG" == await client.ping()
        async with coredis.RedisCluster.from_url(
            f"redis://{redis_cluster_server[0]}:{redis_cluster_server[1]}",
            decode_responses=True,
        ) as client:
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
        async with coredis.RedisCluster.from_url(storage_url) as client:
            assert b"PONG" == await client.ping()
        async with coredis.RedisCluster.from_url(storage_url, decode_responses=True) as client:
            assert "PONG" == await client.ping()
