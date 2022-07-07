from __future__ import annotations

import pytest

from coredis.tokens import PrefixToken
from tests.conftest import targets

pytestmark = pytest.mark.flaky


@pytest.mark.xfail
@targets(
    "redis_stack",
    "redis_stack_raw",
    "redis_stack_resp2",
    "redis_stack_raw_resp2",
)
async def test_modules_list(client, _s):
    module_info = await client.module_list()
    assert {_s("args"), _s("name"), _s("path"), _s("ver")} & module_info[0].keys()


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
)
async def test_no_modules(client):
    module_info = await client.module_list()
    assert module_info == ()


async def test_module_load(fake_redis):
    fake_redis.responses[b"MODULE LOAD"] = {
        ("/var/tmp/module.so",): b"OK",
        ("/var/tmp/module.so", "1"): b"OK",
    }
    assert await fake_redis.module_load("/var/tmp/module.so")
    assert await fake_redis.module_load("/var/tmp/module.so", "1")


async def test_module_unload(fake_redis):
    fake_redis.responses[b"MODULE UNLOAD"] = {("module",): b"OK"}
    assert await fake_redis.module_unload("module")


async def test_module_loadex(fake_redis):
    fake_redis.responses[b"MODULE LOADEX"] = {
        ("/var/tmp/module.so",): b"OK",
        ("/var/tmp/module.so", PrefixToken.CONFIG, "fu", "bar"): b"OK",
        ("/var/tmp/module.so", PrefixToken.ARGS, "1"): b"OK",
        (
            "/var/tmp/module.so",
            PrefixToken.CONFIG,
            "fu",
            "bar",
            PrefixToken.CONFIG,
            "bar",
            "fu",
        ): b"OK",
    }

    assert await fake_redis.module_loadex("/var/tmp/module.so")
    assert await fake_redis.module_loadex("/var/tmp/module.so", configs={"fu": "bar"})
    assert await fake_redis.module_loadex(
        "/var/tmp/module.so", configs={"fu": "bar", "bar": "fu"}
    )
    assert await fake_redis.module_loadex("/var/tmp/module.so", args=["1"])
