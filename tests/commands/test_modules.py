from __future__ import annotations

import pytest

from tests.conftest import targets


@pytest.mark.asyncio
@targets(
    "redis_stack",
    "redis_stack_raw",
    "redis_stack_resp3",
    "redis_stack_raw_resp3",
)
class TestModules:
    async def test_modules_list(self, client, _s):
        module_info = await client.module_list()
        assert {_s("args"), _s("name"), _s("path"), _s("ver")} & module_info[0].keys()
