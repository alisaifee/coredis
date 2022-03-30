from __future__ import annotations

import pytest

from tests.conftest import targets


@pytest.mark.asyncio
@targets("redis_stack", "redis_stack_resp3")
class TestModules:
    async def test_modules_list(self, client):
        module_info = await client.module_list()
        assert {"args", "name", "path", "ver"} & module_info[0].keys()
