from __future__ import annotations

import asyncio
from ssl import SSLContext, SSLError

import pytest
from packaging.version import Version

import coredis
from coredis.exceptions import CommandNotSupportedError, ConnectionError
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_resp2",
    "redis_basic_raw",
    "redis_basic_raw_resp2",
    "redis_ssl",
    "redis_ssl_resp2",
)
@pytest.mark.asyncio
class TestClient:
    @pytest.fixture(autouse=True)
    async def configure_client(self, client):
        client.verify_version = True
        await client.ping()

    async def test_custom_response_callback(self, client, _s):
        client.set_response_callback(_s("PING"), lambda r, **_: _s("BING"))
        assert await client.ping() == _s("BING")

    @pytest.mark.min_server_version("6.0.0")
    async def test_server_version(self, client):
        assert isinstance(client.server_version, Version)
        await client.ping()
        assert isinstance(client.server_version, Version)

    @pytest.mark.max_server_version("6.0.0")
    async def test_server_version_not_found(self, client):
        assert client.server_version is None
        await client.ping()
        assert client.server_version is None

    @pytest.mark.min_server_version("6.0.0")
    @pytest.mark.max_server_version("6.2.0")
    async def test_unsupported_command_6_0_x(self, client):
        await client.ping()
        with pytest.raises(CommandNotSupportedError):
            await client.getex("test")

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.max_server_version("6.2.9")
    async def test_unsupported_command_6_2_x(self, client):
        await client.ping()
        with pytest.raises(CommandNotSupportedError):
            await client.function_list()

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.parametrize("client_arguments", [({"db": 1})])
    async def test_select_database(self, client, client_arguments):
        assert (await client.client_info())["db"] == 1

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.parametrize("client_arguments", [({"client_name": "coredis"})])
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
        noreply.noreply = False
        assert _s("PONG") == await noreply.ping()

    async def test_noreply_context(self, client, _s):
        with client.ignore_reply():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)


@targets(
    "redis_cluster",
    "redis_cluster_resp2",
)
@pytest.mark.asyncio
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
        with client.ignore_reply():
            assert not await client.set("fubar", 1)
            assert not await client.get("fubar")
        assert await client.get("fubar") == _s(1)

    async def test_ensure_replication(self, client, _s):
        with client.ensure_replication(1):
            assert await client.set("fubar", 1)


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
        context = SSLContext()
        context.load_cert_chain(
            certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key"
        )
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
        context = SSLContext()
        context.load_cert_chain(
            certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key"
        )
        client = coredis.RedisCluster(
            "localhost",
            8301,
            ssl_context=context,
        )
        assert await client.ping() == b"PONG"

    async def test_invalid_ssl_parameters(self, redis_ssl_server):
        context = SSLContext()
        context.load_cert_chain(
            certfile="./tests/tls/invalid-client.crt",
            keyfile="./tests/tls/invalid-client.key",
        )
        client = coredis.Redis(
            port=8379,
            ssl_context=context,
        )
        with pytest.raises(ConnectionError, match="decrypt error") as exc_info:
            await client.ping() == b"PONG"
        assert isinstance(exc_info.value.__cause__, SSLError)
