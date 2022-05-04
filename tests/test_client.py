from __future__ import annotations

import pytest
from packaging.version import Version

from coredis import Redis
from coredis.exceptions import CommandNotSupportedError, ProtocolError
from coredis.utils import nativestr
from tests.conftest import targets


@targets("redis_basic", "redis_basic_resp3", "redis_basic_raw", "redis_basic_raw_resp3")
@pytest.mark.asyncio
class TestClient:
    @pytest.fixture(autouse=True)
    async def configure_client(self, client):
        client.verify_version = True
        await client.ping()

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
    @pytest.mark.max_server_version("7.0.0")
    async def test_deprecated_command(self, client, caplog):
        await client.ping()
        assert await client.set("a", 1)
        with pytest.warns(UserWarning) as warning:
            assert "1" == nativestr(await client.getset("a", 2))
        assert warning[0].message.args[0] == "Use set() with the get argument"

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.parametrize("client_arguments", [({"db": 1})])
    async def test_select_database(self, client, client_arguments):
        assert (await client.client_info())["db"] == 1

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.parametrize("client_arguments", [({"client_name": "coredis"})])
    async def test_set_client_name(self, client, client_arguments):
        assert (await client.client_info())["name"] == "coredis"


@pytest.mark.asyncio
@pytest.mark.min_server_version("6.0.0")
async def test_invalid_protocol_version(redis_basic):
    r = Redis(protocol_version=4)
    with pytest.raises(ProtocolError):
        await r.ping()
