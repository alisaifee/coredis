from __future__ import annotations

import pytest

from coredis.exceptions import AuthenticationError, AuthorizationError, ResponseError
from tests.conftest import targets


@pytest.fixture(autouse=True, scope="function")
async def teardown(client):
    yield
    await client.acl_deluser(["test_user"])


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_auth",
    "redis_cluster",
    "redis_cluster_raw",
    "keydb",
)
@pytest.mark.min_server_version("6.0.0")
@pytest.mark.asyncio()
class TestACL:
    async def test_acl_cat(self, client, _s):
        assert {_s("keyspace")} & set(await client.acl_cat())
        assert {_s("keys")} & set(await client.acl_cat("keyspace"))

    @pytest.mark.min_server_version("7.0.0")
    async def test_acl_dryrun(self, client, _s):

        await client.acl_setuser("test_user", "+set", "~*")
        assert await client.acl_dryrun("test_user", "set", "foo", "bar")
        with pytest.raises(AuthorizationError):
            await client.acl_dryrun("test_user", "get", "foo")
        with pytest.raises(AuthorizationError):
            await client.acl_dryrun("test_user", "ping")

    async def test_acl_list(self, client, _s):
        assert _s("user default") in (await client.acl_list())[0]

    async def test_del_user(self, client, _s):
        assert 0 == await client.acl_deluser([_s("john"), "doe"])

    async def test_gen_pass(self, client, _s):
        assert len(await client.acl_genpass()) == 64
        assert len(await client.acl_genpass(4)) == 1

    async def test_acl_load(self, client):
        with pytest.raises(
            ResponseError, match="instance is not configured to use an ACL file"
        ):
            await client.acl_load()

    @pytest.mark.min_server_version("6.0.0")
    @pytest.mark.nocluster
    async def test_acl_log(self, client, _s):
        with pytest.warns(UserWarning):
            with pytest.raises(AuthenticationError):
                await client.auth("wrong", "wrong")
        log = await client.acl_log()
        assert len(log) == 1
        log = await client.acl_log(count=0)
        assert len(log) == 0
        await client.acl_log(reset=True)
        assert len(await client.acl_log()) == 0

    async def test_acl_save(self, client, _s):
        with pytest.raises(
            ResponseError,
            match="instance is not configured to use an ACL file",
        ):
            await client.acl_save()

    async def test_setuser(self, client, _s):
        assert await client.acl_setuser("default")

    async def test_users(self, client, _s):
        assert await client.acl_users() == (_s("default"),)

    async def test_whoami(self, client, _s):
        assert await client.acl_whoami() == _s("default")

    async def test_new_user(self, client, _s):
        try:
            assert await client.acl_setuser("test_user", "on")
            new_user = await client.acl_getuser("test_user")
            assert _s("on") in new_user[_s("flags")]
        finally:
            await client.acl_deluser(["test_user"])

    async def test_get_user(self, client, _s):
        default_user = await client.acl_getuser("default")
        assert _s("on") in default_user[_s("flags")]
