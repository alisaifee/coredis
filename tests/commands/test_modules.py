import pytest

from tests.conftest import targets


@pytest.mark.asyncio
@targets("redis_stack", "redis_stack_resp3")
class TestModules:
    async def test_modules_list(self, client):
        print(await client.module_list())
