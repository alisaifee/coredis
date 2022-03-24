import pytest
from packaging.version import Version

from tests.conftest import targets


@targets("redis_basic", "redis_basic_resp3")
@pytest.mark.asyncio
class TestClient:
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
