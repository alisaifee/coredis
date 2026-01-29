from __future__ import annotations

import anyio
import pytest
from exceptiongroup import catch

from coredis import PureToken
from coredis.client.basic import Redis
from coredis.exceptions import AuthenticationFailureError, ResponseError, UnblockedError
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_raw",
    "valkey",
    "redict",
)
class TestConnection:
    async def test_bgsave(self, client):
        await anyio.sleep(0.5)
        assert await client.bgsave()
        with pytest.raises(ResponseError, match="already in progress"):
            await client.bgsave()
        await anyio.sleep(0.5)
        assert await client.bgsave(schedule=True)

    async def test_ping(self, client, _s):
        resp = await client.ping()
        assert resp == _s("PONG")

    async def test_hello_no_args(self, client, _s):
        resp = await client.hello()
        assert resp[_s("server")] is not None

    async def test_hello_extended(self, client, _s):
        resp = await client.hello(3)
        assert resp[_s("proto")] == 3
        await client.hello(3, setname="coredis")
        assert await client.client_getname() == _s("coredis")
        with pytest.raises(AuthenticationFailureError):
            await client.hello(3, username="no", password="body")

    async def test_ping_custom_message(self, client, _s):
        resp = await client.ping(message="PANG")
        assert resp == _s("PANG")

    async def test_echo(self, client, _s):
        assert await client.echo("foo bar") == _s("foo bar")

    async def test_client_id(self, client, _s):
        id_ = await client.client_id()
        assert isinstance(id_, int)

    async def test_client_info(self, client, _s):
        info = await client.client_info()
        assert isinstance(info, dict)
        assert "addr" in info

    async def test_client_reply(self, client, _s):
        with pytest.raises(NotImplementedError):
            await client.client_reply(PureToken.ON)

    async def test_client_no_evict(self, client, _s):
        with pytest.warns(UserWarning):
            assert await client.client_no_evict(PureToken.ON)
            assert await client.client_no_evict(PureToken.OFF)

    async def test_client_no_touch(self, client, _s):
        with pytest.warns(UserWarning):
            assert await client.client_no_touch(PureToken.ON)
            assert await client.client_no_touch(PureToken.OFF)

    async def test_client_tracking(self, client, _s, cloner):
        async with await cloner(client) as clone:
            async with clone.connection_pool.acquire() as clone_connection:
                clone_id = clone_connection.client_id
                assert await client.client_tracking(PureToken.ON, redirect=clone_id, noloop=True)
                assert clone_id == await client.client_getredir()
                assert await client.client_tracking(PureToken.OFF)
                assert -1 == await client.client_getredir()
                with pytest.raises(ResponseError, match="does not exist"):
                    clients = await client.client_list()
                    invalid_client_id = max(c["id"] for c in clients) + 100
                    await client.client_tracking(PureToken.ON, redirect=invalid_client_id)
                assert await client.client_tracking(PureToken.ON, bcast=True, redirect=clone_id)
                assert await client.client_tracking(PureToken.OFF)
                assert await client.client_tracking(
                    PureToken.ON, "fu:", "bar:", bcast=True, redirect=clone_id
                )
                assert await client.client_tracking(PureToken.OFF)
                with pytest.raises(ResponseError, match="'fu' overlaps"):
                    assert await client.client_tracking(
                        PureToken.ON, "fu", "fuu", bcast=True, redirect=clone_id
                    )
                assert await client.client_tracking(PureToken.ON, optin=True, redirect=clone_id)
                with pytest.raises(ResponseError, match="in OPTOUT mode"):
                    await client.client_caching(PureToken.NO)
                assert await client.client_tracking(PureToken.ON, optin=True, redirect=clone_id)
                assert await client.client_caching(PureToken.YES)

                with pytest.raises(ResponseError, match="You can't switch"):
                    await client.client_tracking(PureToken.ON, optout=True, redirect=clone_id)
                assert await client.client_tracking(PureToken.OFF)
                assert await client.client_tracking(PureToken.ON, optout=True, redirect=clone_id)
                with pytest.raises(ResponseError, match="in OPTIN mode"):
                    await client.client_caching(PureToken.YES)
                assert await client.client_tracking(PureToken.ON, optout=True, redirect=clone_id)
                assert await client.client_caching(PureToken.NO)

    async def test_client_getredir(self, client, _s, cloner):
        assert await client.client_getredir() == -1
        clone = await cloner(client)
        async with clone:
            clone_id = (await clone.client_info())["id"]
            assert await client.client_tracking(PureToken.ON, redirect=clone_id)
            assert await client.client_getredir() == clone_id

    async def test_client_pause_unpause(self, client, _s, cloner):
        async with await cloner(client) as clone:
            assert await clone.client_pause(1000)
            with pytest.raises(TimeoutError):
                with anyio.fail_after(0.01):
                    await clone.ping()
            assert await client.client_unpause()
            assert await clone.ping() == _s("PONG")
            assert await clone.client_pause(1000, PureToken.WRITE)
            assert not await clone.get("fubar")
            with pytest.raises(TimeoutError):
                with anyio.fail_after(0.01):
                    await clone.set("fubar", 1)
            assert await client.client_unpause()
            assert await clone.set("fubar", 1)

    async def test_client_unblock(self, client: Redis, cloner):
        async with await cloner(client) as clone:
            client_id = await clone.client_id()

            async def unblock():
                await anyio.sleep(0.1)
                return await client.client_unblock(client_id, PureToken.ERROR)

            async def blocking():
                await clone.brpop(["notexist"], 1000)

            unblocked = False

            def unblocked_raised(_):
                nonlocal unblocked
                unblocked = True

            with catch({UnblockedError: unblocked_raised}):
                async with anyio.create_task_group() as tg:
                    tg.start_soon(blocking)
                    tg.start_soon(unblock)
            assert unblocked
            assert not await client.client_unblock(client_id, PureToken.ERROR)

    async def test_client_trackinginfo_no_tracking(self, client, _s):
        info = await client.client_trackinginfo()
        assert info[_s("flags")] == {_s("off")}

    async def test_client_trackinginfo_tracking_set(self, client, _s):
        resp = await client.client_tracking(PureToken.ON)
        assert resp
        info = await client.client_trackinginfo()
        assert info[_s("flags")] == {_s("on")}

    async def test_client_list(self, client, _s):
        clients = await client.client_list()
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]
        clients = await client.client_list(type_=PureToken.NORMAL)
        assert isinstance(clients[0], dict)

    async def test_client_list_with_specific_ids(self, client, _s):
        clients = await client.client_list()
        ids = [c["id"] for c in clients]
        assert ids
        refetch = await client.client_list(identifiers=ids)
        assert sorted(k["addr"] for k in refetch) == sorted(k["addr"] for k in clients)

    async def test_client_kill_fail(self, client, _s):
        with pytest.raises(ResponseError):
            await client.client_kill(ip_port="1.1.1.1:9999")

    async def test_client_kill_filter(self, client, cloner, _s):
        async with await cloner(client) as clone:
            clone_id = (await clone.client_info())["id"]
            assert await client.client_kill(identifier=clone_id) > 0
            with pytest.raises(ResponseError, match="No such user"):
                await client.client_kill(user="noexist")

            clone_addr = (await clone.client_info())["addr"]
            assert await client.client_kill(addr=clone_addr) == 1

    async def test_client_kill_filter_skip_me(self, client, cloner, _s):
        async with await cloner(client) as clone:
            my_id = (await client.client_info())["id"]
            clone_id = (await clone.client_info())["id"]
            laddr = (await client.client_info())["laddr"]
            resp = await client.client_kill(laddr=laddr, skipme=True)
            assert resp > 0
            await clone.ping()
            assert clone_id != (await clone.client_info())["id"]
            assert my_id == (await client.client_info())["id"]

    @pytest.mark.min_server_version("7.4.0")
    async def test_client_kill_filter_maxage(self, client, cloner, _s):
        async with await cloner(client) as clone:
            my_id = (await client.client_info())["id"]
            clone_id = (await clone.client_info())["id"]
            await anyio.sleep(1)
            assert await client.client_kill(maxage=1, skipme=False) >= 2
            assert clone_id != (await clone.client_info())["id"]
            assert my_id != (await client.client_info())["id"]

    async def test_client_list_after_client_setname(self, client, _s):
        with pytest.warns(UserWarning):
            await client.client_setname("cl=i=ent")
        clients = await client.client_list()
        assert "cl=i=ent" in [c["name"] for c in clients]

    async def test_client_list_after_client_setinfo(self, client, _s):
        with pytest.warns(UserWarning):
            await client.client_setinfo(lib_name="lolwut")
            await client.client_setinfo(lib_ver="12.12.12")
        clients = await client.client_list()
        assert ("lolwut", "12.12.12") in [(c["lib-name"], c["lib-ver"]) for c in clients]

    async def test_client_getname(self, client, _s):
        assert await client.client_getname() is None

    async def test_client_setname(self, client, _s):
        with pytest.warns(UserWarning):
            assert await client.client_setname("redis_py_test")
            assert await client.client_getname() == _s("redis_py_test")

    @pytest.mark.novalkey
    @pytest.mark.noredict
    async def test_client_pause(self, client, cloner):
        key = "key_should_expire"
        async with await cloner(client) as another_client:
            await client.set(key, "1", px=100)
            assert await client.client_pause(100)
            res = await another_client.get(key)
            assert not res

    async def test_select(self, client, _s):
        assert (await client.client_info())["db"] == 0
        with pytest.warns(UserWarning):
            assert await client.select(1)
        assert (await client.client_info())["db"] == 1

    async def test_reset(self, client, _s):
        assert (await client.client_info())["db"] == 0
        with pytest.warns(UserWarning):
            assert await client.select(1)
        assert (await client.client_info())["db"] == 1
        await client.reset()
        assert (await client.client_info())["db"] == 0
