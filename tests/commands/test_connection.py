from __future__ import annotations

import asyncio

import pytest

import coredis
from coredis import PureToken
from coredis.exceptions import AuthenticationFailureError, ResponseError, UnblockedError
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
)
@pytest.mark.asyncio()
class TestConnection:
    @pytest.mark.flaky
    async def test_bgsave(self, client):
        await asyncio.sleep(0.5)
        assert await client.bgsave()
        with pytest.raises(ResponseError, match="already in progress"):
            await client.bgsave()
        await asyncio.sleep(0.5)
        assert await client.bgsave(schedule=True)

    async def test_ping(self, client, _s):
        resp = await client.ping()
        assert resp == _s("PONG")

    @pytest.mark.min_server_version("6.2.0")
    async def test_hello_no_args(self, client, _s):
        resp = await client.hello()
        assert resp[_s("server")] == _s("redis")

    async def test_hello_extended(self, client, _s):
        resp = await client.hello(client.protocol_version)
        assert resp[_s("proto")] == client.protocol_version
        await client.hello(client.protocol_version, setname="coredis")
        assert await client.client_getname() == _s("coredis")
        with pytest.raises(AuthenticationFailureError):
            await client.hello(client.protocol_version, username="no", password="body")

    async def test_ping_custom_message(self, client, _s):
        resp = await client.ping(message="PANG")
        assert resp == _s("PANG")

    async def test_echo(self, client, _s):
        assert await client.echo("foo bar") == _s("foo bar")

    async def test_client_id(self, client, _s):
        id_ = await client.client_id()
        assert isinstance(id_, int)

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_info(self, client, _s):
        info = await client.client_info()
        assert isinstance(info, dict)
        assert "addr" in info

    async def test_client_reply(self, client, _s):
        with pytest.raises(NotImplementedError):
            await client.client_reply(PureToken.ON)

    @pytest.mark.min_server_version("7.0.0")
    async def test_client_no_evict(self, client, _s):
        assert await client.client_no_evict(PureToken.ON)
        assert await client.client_no_evict(PureToken.OFF)

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_tracking(self, client, _s, cloner):
        clone = await (await cloner(client)).connection_pool.get_connection("tracking")
        clone_id = clone.client_id
        assert await client.client_tracking(
            PureToken.ON, redirect=clone_id, noloop=True
        )
        assert clone_id == await client.client_getredir()
        assert await client.client_tracking(PureToken.OFF)
        assert -1 == await client.client_getredir()
        with pytest.raises(ResponseError, match="does not exist"):
            await client.client_tracking(PureToken.ON, redirect=1234)
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
        assert await client.client_tracking(
            PureToken.ON, optout=True, redirect=clone_id
        )
        with pytest.raises(ResponseError, match="in OPTIN mode"):
            await client.client_caching(PureToken.YES)
        assert await client.client_tracking(
            PureToken.ON, optout=True, redirect=clone_id
        )
        assert await client.client_caching(PureToken.NO)

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_getredir(self, client, _s, cloner):
        assert await client.client_getredir() == -1
        clone = await cloner(client)
        clone_id = (await clone.client_info())["id"]
        assert await client.client_tracking(PureToken.ON, redirect=clone_id)
        assert await client.client_getredir() == clone_id

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_pause_unpause(self, client, _s, cloner):
        clone = await cloner(client)
        assert await clone.client_pause(1000)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(clone.ping(), timeout=0.01)
        assert await client.client_unpause()
        assert await clone.ping() == _s("PONG")
        assert await clone.client_pause(1000, PureToken.WRITE)
        assert not await clone.get("fubar")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(clone.set("fubar", 1), timeout=0.01)
        assert await client.client_unpause()
        assert await clone.set("fubar", 1)

    @pytest.mark.xfail
    async def test_client_unblock(self, client, cloner, event_loop):
        clone = await cloner(client)
        client_id = await clone.client_id()

        async def unblock():
            await asyncio.sleep(0.1)
            return await client.client_unblock(client_id, PureToken.ERROR)

        sleeper = asyncio.create_task(clone.brpop(["notexist"], 1000))
        unblocker = asyncio.create_task(unblock())
        await asyncio.wait(
            [
                sleeper,
                unblocker,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert isinstance(sleeper.exception(), UnblockedError)
        assert unblocker.result()
        assert not await client.client_unblock(client_id, PureToken.ERROR)

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_trackinginfo_no_tracking(self, client, _s):
        info = await client.client_trackinginfo()
        assert info[_s("flags")] == {_s("off")}

    @pytest.mark.min_server_version("6.2.0")
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

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_list_with_specific_ids(self, client, _s):
        clients = await client.client_list()
        ids = [c["id"] for c in clients]
        assert ids
        refetch = await client.client_list(identifiers=ids)
        assert sorted(k["addr"] for k in refetch) == sorted(k["addr"] for k in clients)

    async def test_client_kill_fail(self, client, _s):
        with pytest.raises(ResponseError):
            await client.client_kill(ip_port="1.1.1.1:9999")

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_kill_filter(self, client, cloner, _s):
        clone = await cloner(client)
        clone_id = (await clone.client_info())["id"]
        assert await client.client_kill(identifier=clone_id) > 0
        with pytest.raises(ResponseError, match="No such user"):
            await client.client_kill(user="noexist") == 0

        clone_addr = (await clone.client_info())["addr"]
        assert await client.client_kill(type_=PureToken.PUBSUB) == 0
        assert await client.client_kill(addr=clone_addr) == 1

    @pytest.mark.min_server_version("6.2.0")
    async def test_client_kill_filter_skip_me(self, client, cloner, _s):
        clone = await cloner(client)
        my_id = (await client.client_info())["id"]
        clone_id = (await clone.client_info())["id"]
        laddr = (await client.client_info())["laddr"]
        resp = await client.client_kill(laddr=laddr, skipme=True)
        assert resp > 0
        await clone.ping()
        assert clone_id != (await clone.client_info())["id"]
        assert my_id == (await client.client_info())["id"]

    async def test_client_list_after_client_setname(self, client, _s):
        with pytest.warns(UserWarning):
            await client.client_setname("cl=i=ent")
        clients = await client.client_list()
        assert "cl=i=ent" in [c["name"] for c in clients]

    async def test_client_getname(self, client, _s):
        assert await client.client_getname() is None

    async def test_client_setname(self, client, _s):
        with pytest.warns(UserWarning):
            assert await client.client_setname("redis_py_test")
            assert await client.client_getname() == _s("redis_py_test")

    async def test_client_pause(self, client, event_loop):
        key = "key_should_expire"
        another_client = coredis.Redis(loop=event_loop)
        await client.set(key, "1", px=100)
        assert await client.client_pause(100)
        res = await another_client.get(key)
        assert not res

    @pytest.mark.min_server_version("6.2.0")
    async def test_select(self, client, _s):
        assert (await client.client_info())["db"] == 0
        assert await client.select(1)
        assert (await client.client_info())["db"] == 1

    @pytest.mark.min_server_version("6.2.0")
    async def test_reset(self, client, _s):
        assert (await client.client_info())["db"] == 0
        assert await client.select(1)
        assert (await client.client_info())["db"] == 1
        await client.reset()
        assert (await client.client_info())["db"] == 0
