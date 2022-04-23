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
async def test_modules_list(client, _s):
    module_info = await client.module_list()
    assert {_s("args"), _s("name"), _s("path"), _s("ver")} & module_info[0].keys()


@pytest.mark.asyncio
@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp3",
    "redis_basic_raw_resp3",
)
async def test_no_modules(client):
    module_info = await client.module_list()
    assert module_info == ()
