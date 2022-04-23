from __future__ import annotations

import datetime
from types import TracebackType
from typing import Any

from wrapt import ObjectProxy

from coredis import PureToken
from coredis.client import AbstractRedis, AbstractRedisCluster
from coredis.commands.script import Script
from coredis.nodemanager import Node
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.typing import (
    AnyStr,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

class Pipeline(ObjectProxy, Generic[AnyStr]):  # type: ignore
    scripts: Set[Script[AnyStr]]
    @classmethod
    def proxy(
        cls,
        connection_pool: ConnectionPool,
        response_callbacks: Dict[bytes, Callable[..., Any]],
        transaction: Optional[bool],
    ) -> Pipeline[AnyStr]: ...
    async def copy(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        db: Optional[int] = ...,
        replace: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def delete(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def dump(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def exists(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def expire(
        self,
        key: Union[str, bytes],
        seconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def expireat(
        self,
        key: Union[str, bytes],
        unix_time_seconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def expiretime(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def keys(self, pattern: Union[str, bytes] = ...) -> Pipeline[AnyStr]: ...
    async def migrate(
        self,
        host: Union[str, bytes],
        port: int,
        destination_db: int,
        timeout: int,
        *keys: Union[str, bytes],
        copy: Optional[bool] = ...,
        replace: Optional[bool] = ...,
        auth: Optional[Union[str, bytes]] = ...,
        username: Optional[Union[str, bytes]] = ...,
        password: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def move(self, key: Union[str, bytes], db: int) -> Pipeline[AnyStr]: ...
    async def object_encoding(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def object_freq(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def object_idletime(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def object_refcount(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def persist(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def pexpire(
        self,
        key: Union[str, bytes],
        milliseconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def pexpireat(
        self,
        key: Union[str, bytes],
        unix_time_milliseconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def pexpiretime(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def pttl(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def randomkey(self) -> Pipeline[AnyStr]: ...
    async def rename(
        self, key: Union[str, bytes], newkey: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def renamenx(
        self, key: Union[str, bytes], newkey: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def restore(
        self,
        key: Union[str, bytes],
        ttl: int,
        serialized_value: bytes,
        replace: Optional[bool] = ...,
        absttl: Optional[bool] = ...,
        idletime: Optional[Union[int, datetime.timedelta]] = ...,
        freq: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def scan(
        self,
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
        type_: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def sort(
        self,
        key: Union[str, bytes],
        gets: Optional[Iterable[Union[str, bytes]]] = ...,
        by: Optional[Union[str, bytes]] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        alpha: Optional[bool] = ...,
        store: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def sort_ro(
        self,
        key: Union[str, bytes],
        gets: Optional[Iterable[Union[str, bytes]]] = ...,
        by: Optional[Union[str, bytes]] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        alpha: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def touch(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def ttl(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def type(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def unlink(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def wait(self, numreplicas: int, timeout: int) -> Pipeline[AnyStr]: ...
    async def append(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def decr(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def decrby(
        self, key: Union[str, bytes], decrement: int
    ) -> Pipeline[AnyStr]: ...
    async def get(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def getdel(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def getex(
        self,
        key: Union[str, bytes],
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        persist: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def getrange(
        self, key: Union[str, bytes], start: int, end: int
    ) -> Pipeline[AnyStr]: ...
    async def getset(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def incr(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def incrby(
        self, key: Union[str, bytes], increment: int
    ) -> Pipeline[AnyStr]: ...
    async def incrbyfloat(
        self, key: Union[str, bytes], increment: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def lcs(
        self,
        key1: Union[str, bytes],
        key2: Union[str, bytes],
        len_: Optional[bool] = ...,
        idx: Optional[bool] = ...,
        minmatchlen: Optional[int] = ...,
        withmatchlen: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def mget(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def mset(
        self, key_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def msetnx(
        self, key_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def psetex(
        self,
        key: Union[str, bytes],
        milliseconds: Union[int, datetime.timedelta],
        value: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def set(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes, int, float],
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        keepttl: Optional[bool] = ...,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        get: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def setex(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes, int, float],
        seconds: Union[int, datetime.timedelta],
    ) -> Pipeline[AnyStr]: ...
    async def setnx(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def setrange(
        self, key: Union[str, bytes], offset: int, value: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def strlen(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def substr(
        self, key: Union[str, bytes], start: int, end: int
    ) -> Pipeline[AnyStr]: ...
    async def bitcount(
        self,
        key: Union[str, bytes],
        start: Optional[int] = ...,
        end: Optional[int] = ...,
        index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def bitop(
        self,
        keys: Iterable[Union[str, bytes]],
        operation: Union[str, bytes],
        destkey: Union[str, bytes],
    ) -> Pipeline[AnyStr]: ...
    async def bitpos(
        self,
        key: Union[str, bytes],
        bit: int,
        start: Optional[int] = ...,
        end: Optional[int] = ...,
        end_index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def getbit(self, key: Union[str, bytes], offset: int) -> Pipeline[AnyStr]: ...
    async def setbit(
        self, key: Union[str, bytes], offset: int, value: int
    ) -> Pipeline[AnyStr]: ...
    async def hdel(
        self, key: Union[str, bytes], fields: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def hexists(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def hget(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def hgetall(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def hincrby(
        self, key: Union[str, bytes], field: Union[str, bytes], increment: int
    ) -> Pipeline[AnyStr]: ...
    async def hincrbyfloat(
        self,
        key: Union[str, bytes],
        field: Union[str, bytes],
        increment: Union[int, float],
    ) -> Pipeline[AnyStr]: ...
    async def hkeys(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def hlen(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def hmget(
        self, key: Union[str, bytes], fields: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def hmset(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def hrandfield(
        self,
        key: Union[str, bytes],
        count: Optional[int] = ...,
        withvalues: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def hscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def hset(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def hsetnx(
        self,
        key: Union[str, bytes],
        field: Union[str, bytes],
        value: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def hstrlen(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def hvals(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def blmove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
        timeout: Union[int, float],
    ) -> Pipeline[AnyStr]: ...
    async def blmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        timeout: Union[int, float],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def blpop(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def brpop(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def brpoplpush(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        timeout: Union[int, float],
    ) -> Pipeline[AnyStr]: ...
    async def lindex(self, key: Union[str, bytes], index: int) -> Pipeline[AnyStr]: ...
    async def linsert(
        self,
        key: Union[str, bytes],
        where: Literal[PureToken.BEFORE, PureToken.AFTER],
        pivot: Union[str, bytes, int, float],
        element: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def llen(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def lmove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
    ) -> Pipeline[AnyStr]: ...
    async def lmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def lpop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def lpos(
        self,
        key: Union[str, bytes],
        element: Union[str, bytes, int, float],
        rank: Optional[int] = ...,
        count: Optional[int] = ...,
        maxlen: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def lpush(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def lpushx(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def lrange(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> Pipeline[AnyStr]: ...
    async def lrem(
        self, key: Union[str, bytes], count: int, element: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def lset(
        self, key: Union[str, bytes], index: int, element: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def ltrim(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> Pipeline[AnyStr]: ...
    async def rpop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def rpoplpush(
        self, source: Union[str, bytes], destination: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def rpush(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def rpushx(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def sadd(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def scard(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def sdiff(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def sdiffstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def sinter(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def sintercard(
        self, keys: Iterable[Union[str, bytes]], limit: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def sinterstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def sismember(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def smembers(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def smismember(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def smove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        member: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def spop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def srandmember(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def srem(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def sscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def sunion(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def sunionstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def bzmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        timeout: Union[int, float],
        where: Literal[PureToken.MIN, PureToken.MAX],
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def bzpopmax(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def bzpopmin(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def zadd(
        self,
        key: Union[str, bytes],
        member_scores: Dict[Union[str, bytes], float],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        comparison: Optional[Literal[PureToken.GT, PureToken.LT]] = ...,
        change: Optional[bool] = ...,
        increment: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zcard(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def zcount(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def zdiff(
        self, keys: Iterable[Union[str, bytes]], withscores: Optional[bool] = ...
    ) -> Pipeline[AnyStr]: ...
    async def zdiffstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def zincrby(
        self,
        key: Union[str, bytes],
        member: Union[str, bytes, int, float],
        increment: int,
    ) -> Pipeline[AnyStr]: ...
    async def zinter(
        self,
        keys: Iterable[Union[str, bytes]],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
        withscores: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zintercard(
        self, keys: Iterable[Union[str, bytes]], limit: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def zinterstore(
        self,
        keys: Iterable[Union[str, bytes]],
        destination: Union[str, bytes],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zlexcount(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def zmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        where: Literal[PureToken.MIN, PureToken.MAX],
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zmscore(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def zpopmax(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def zpopmin(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def zrandmember(
        self,
        key: Union[str, bytes],
        count: Optional[int] = ...,
        withscores: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrange(
        self,
        key: Union[str, bytes],
        min_: Union[int, str, bytes, float],
        max_: Union[int, str, bytes, float],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = ...,
        rev: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        withscores: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrangebylex(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrangebyscore(
        self,
        key: Union[str, bytes],
        min_: Union[int, float],
        max_: Union[int, float],
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrangestore(
        self,
        dst: Union[str, bytes],
        src: Union[str, bytes],
        min_: Union[int, str, bytes, float],
        max_: Union[int, str, bytes, float],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = ...,
        rev: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrank(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def zrem(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def zremrangebylex(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def zremrangebyrank(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> Pipeline[AnyStr]: ...
    async def zremrangebyscore(
        self, key: Union[str, bytes], min_: Union[int, float], max_: Union[int, float]
    ) -> Pipeline[AnyStr]: ...
    async def zrevrange(
        self,
        key: Union[str, bytes],
        start: int,
        stop: int,
        withscores: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrevrangebylex(
        self,
        key: Union[str, bytes],
        max_: Union[str, bytes, int, float],
        min_: Union[str, bytes, int, float],
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrevrangebyscore(
        self,
        key: Union[str, bytes],
        max_: Union[int, float],
        min_: Union[int, float],
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zrevrank(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def zscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zscore(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def zunion(
        self,
        keys: Iterable[Union[str, bytes]],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
        withscores: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def zunionstore(
        self,
        keys: Iterable[Union[str, bytes]],
        destination: Union[str, bytes],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def pfadd(
        self, key: Union[str, bytes], *elements: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def pfcount(self, keys: Iterable[Union[str, bytes]]) -> Pipeline[AnyStr]: ...
    async def pfmerge(
        self, destkey: Union[str, bytes], sourcekeys: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def geoadd(
        self,
        key: Union[str, bytes],
        longitude_latitude_members: Iterable[
            Tuple[Union[int, float], Union[int, float], Union[str, bytes, int, float]]
        ],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        change: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def geodist(
        self,
        key: Union[str, bytes],
        member1: Union[str, bytes],
        member2: Union[str, bytes],
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def geohash(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def geopos(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def georadius(
        self,
        key: Union[str, bytes],
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        store: Optional[Union[str, bytes]] = ...,
        storedist: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def georadiusbymember(
        self,
        key: Union[str, bytes],
        member: Union[str, bytes, int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        store: Optional[Union[str, bytes]] = ...,
        storedist: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def geosearch(
        self,
        key: Union[str, bytes],
        member: Optional[Union[str, bytes, int, float]] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def geosearchstore(
        self,
        destination: Union[str, bytes],
        source: Union[str, bytes],
        member: Optional[Union[str, bytes, int, float]] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        storedist: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xack(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        identifiers: Iterable[Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def xadd(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        nomkstream: Optional[bool] = ...,
        trim_strategy: Optional[Literal[PureToken.MAXLEN, PureToken.MINID]] = ...,
        threshold: Optional[int] = ...,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = ...,
        limit: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xautoclaim(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        min_idle_time: Union[int, datetime.timedelta],
        start: Union[str, bytes, int, float],
        count: Optional[int] = ...,
        justid: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xclaim(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        min_idle_time: Union[int, datetime.timedelta],
        identifiers: Iterable[Union[str, bytes, int, float]],
        idle: Optional[int] = ...,
        time: Optional[Union[int, datetime.datetime]] = ...,
        retrycount: Optional[int] = ...,
        force: Optional[bool] = ...,
        justid: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xdel(
        self,
        key: Union[str, bytes],
        identifiers: Iterable[Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def xgroup_create(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        mkstream: Optional[bool] = ...,
        entriesread: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xgroup_createconsumer(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        consumername: Union[str, bytes],
    ) -> Pipeline[AnyStr]: ...
    async def xgroup_delconsumer(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        consumername: Union[str, bytes],
    ) -> Pipeline[AnyStr]: ...
    async def xgroup_destroy(
        self, key: Union[str, bytes], groupname: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def xgroup_setid(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        entriesread: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xinfo_consumers(
        self, key: Union[str, bytes], groupname: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def xinfo_groups(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def xinfo_stream(
        self,
        key: Union[str, bytes],
        full: Optional[bool] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xlen(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def xpending(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        start: Optional[Union[str, bytes, int, float]] = ...,
        end: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
        idle: Optional[int] = ...,
        consumer: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xrange(
        self,
        key: Union[str, bytes],
        start: Optional[Union[str, bytes, int, float]] = ...,
        end: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xread(
        self,
        streams: Dict[Union[str, bytes, int, float], Union[str, bytes, int, float]],
        count: Optional[int] = ...,
        block: Optional[Union[int, datetime.timedelta]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xreadgroup(
        self,
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        streams: Dict[Union[str, bytes, int, float], Union[str, bytes, int, float]],
        count: Optional[int] = ...,
        block: Optional[Union[int, datetime.timedelta]] = ...,
        noack: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xrevrange(
        self,
        key: Union[str, bytes],
        end: Optional[Union[str, bytes, int, float]] = ...,
        start: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def xtrim(
        self,
        key: Union[str, bytes],
        trim_strategy: Literal[PureToken.MAXLEN, PureToken.MINID],
        threshold: int,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = ...,
        limit: Optional[int] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def eval(
        self,
        script: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def evalsha(
        self,
        sha1: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def evalsha_ro(
        self,
        sha1: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def eval_ro(
        self,
        script: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def fcall(
        self,
        function: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def fcall_ro(
        self,
        function: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def function_delete(
        self, library_name: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def function_dump(self) -> Pipeline[AnyStr]: ...
    async def function_flush(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def function_kill(self) -> Pipeline[AnyStr]: ...
    async def function_list(
        self,
        libraryname: Optional[Union[str, bytes]] = ...,
        withcode: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def function_load(
        self, function_code: Union[str, bytes], replace: Optional[bool] = ...
    ) -> Pipeline[AnyStr]: ...
    async def function_restore(
        self,
        serialized_value: bytes,
        policy: Optional[
            Literal[PureToken.FLUSH, PureToken.APPEND, PureToken.REPLACE]
        ] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def function_stats(self) -> Pipeline[AnyStr]: ...
    async def script_debug(
        self, mode: Literal[PureToken.YES, PureToken.SYNC, PureToken.NO]
    ) -> Pipeline[AnyStr]: ...
    async def script_exists(
        self, sha1s: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def script_flush(
        self, sync_type: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def script_kill(self) -> Pipeline[AnyStr]: ...
    async def script_load(self, script: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def publish(
        self, channel: Union[str, bytes], message: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def pubsub_channels(
        self, pattern: Optional[Union[str, bytes]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def pubsub_numpat(self) -> Pipeline[AnyStr]: ...
    async def pubsub_numsub(self, *channels: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def acl_cat(
        self, categoryname: Optional[Union[str, bytes]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def acl_deluser(
        self, usernames: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def acl_dryrun(
        self,
        username: Union[str, bytes],
        command: Union[str, bytes],
        *args: Union[str, bytes, int, float],
    ) -> Pipeline[AnyStr]: ...
    async def acl_genpass(self, bits: Optional[int] = ...) -> Pipeline[AnyStr]: ...
    async def acl_getuser(self, username: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def acl_list(self) -> Pipeline[AnyStr]: ...
    async def acl_load(self) -> Pipeline[AnyStr]: ...
    async def acl_log(
        self, count: Optional[int] = ..., reset: Optional[bool] = ...
    ) -> Pipeline[AnyStr]: ...
    async def acl_save(self) -> Pipeline[AnyStr]: ...
    async def acl_setuser(
        self, username: Union[str, bytes], *rules: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def acl_users(self) -> Pipeline[AnyStr]: ...
    async def acl_whoami(self) -> Pipeline[AnyStr]: ...
    async def bgrewriteaof(self) -> Pipeline[AnyStr]: ...
    async def bgsave(self, schedule: Optional[bool] = ...) -> Pipeline[AnyStr]: ...
    async def command(self) -> Pipeline[AnyStr]: ...
    async def command_count(self) -> Pipeline[AnyStr]: ...
    async def command_docs(
        self, *command_names: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def command_getkeys(
        self,
        command: Union[str, bytes],
        arguments: Iterable[Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def command_getkeysandflags(
        self,
        command: Union[str, bytes],
        arguments: Iterable[Union[str, bytes, int, float]],
    ) -> Pipeline[AnyStr]: ...
    async def command_info(
        self, *command_names: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def command_list(
        self,
        module: Optional[Union[str, bytes]] = ...,
        aclcat: Optional[Union[str, bytes]] = ...,
        pattern: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def config_get(
        self, parameters: Iterable[Union[str, bytes]]
    ) -> Pipeline[AnyStr]: ...
    async def config_resetstat(self) -> Pipeline[AnyStr]: ...
    async def config_rewrite(self) -> Pipeline[AnyStr]: ...
    async def config_set(
        self, parameter_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> Pipeline[AnyStr]: ...
    async def dbsize(self) -> Pipeline[AnyStr]: ...
    async def failover(
        self,
        host: Optional[Union[str, bytes]] = ...,
        port: Optional[int] = ...,
        force: Optional[bool] = ...,
        abort: Optional[bool] = ...,
        timeout: Optional[Union[int, datetime.timedelta]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def flushall(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def flushdb(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def info(self, *sections: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def lastsave(self) -> Pipeline[AnyStr]: ...
    async def latency_doctor(self) -> Pipeline[AnyStr]: ...
    async def latency_graph(self, event: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def latency_histogram(
        self, *commands: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def latency_history(self, event: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def latency_latest(self) -> Pipeline[AnyStr]: ...
    async def latency_reset(self, *events: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def lolwut(self, version: Optional[int] = ...) -> Pipeline[AnyStr]: ...
    async def memory_doctor(self) -> Pipeline[AnyStr]: ...
    async def memory_malloc_stats(self) -> Pipeline[AnyStr]: ...
    async def memory_purge(self) -> Pipeline[AnyStr]: ...
    async def memory_stats(self) -> Pipeline[AnyStr]: ...
    async def memory_usage(
        self, key: Union[str, bytes], samples: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def module_list(self) -> Pipeline[AnyStr]: ...
    async def module_load(
        self, path: Union[str, bytes], *args: Union[str, bytes, int, float]
    ) -> Pipeline[AnyStr]: ...
    async def module_loadex(
        self,
        path: Union[str, bytes],
        configs: Optional[Dict[Union[str, bytes], Union[str, bytes, int, float]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def module_unload(self, name: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def replicaof(
        self, host: Optional[Union[str, bytes]] = ..., port: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def role(self) -> Pipeline[AnyStr]: ...
    async def save(self) -> Pipeline[AnyStr]: ...
    async def shutdown(
        self,
        nosave_save: Optional[Literal[PureToken.NOSAVE, PureToken.SAVE]] = ...,
        now: Optional[bool] = ...,
        force: Optional[bool] = ...,
        abort: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def slaveof(
        self, host: Optional[Union[str, bytes]] = ..., port: Optional[int] = ...
    ) -> Pipeline[AnyStr]: ...
    async def slowlog_get(self, count: Optional[int] = ...) -> Pipeline[AnyStr]: ...
    async def slowlog_len(self) -> Pipeline[AnyStr]: ...
    async def slowlog_reset(self) -> Pipeline[AnyStr]: ...
    async def swapdb(self, index1: int, index2: int) -> Pipeline[AnyStr]: ...
    async def time(self) -> Pipeline[AnyStr]: ...
    async def auth(
        self, password: Union[str, bytes], username: Optional[Union[str, bytes]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def client_caching(
        self, mode: Literal[PureToken.YES, PureToken.NO]
    ) -> Pipeline[AnyStr]: ...
    async def client_getname(self) -> Pipeline[AnyStr]: ...
    async def client_getredir(self) -> Pipeline[AnyStr]: ...
    async def client_id(self) -> Pipeline[AnyStr]: ...
    async def client_info(self) -> Pipeline[AnyStr]: ...
    async def client_kill(
        self,
        ip_port: Optional[Union[str, bytes]] = ...,
        identifier: Optional[int] = ...,
        type_: Optional[
            Literal[
                PureToken.NORMAL,
                PureToken.MASTER,
                PureToken.SLAVE,
                PureToken.REPLICA,
                PureToken.PUBSUB,
            ]
        ] = ...,
        user: Optional[Union[str, bytes]] = ...,
        addr: Optional[Union[str, bytes]] = ...,
        laddr: Optional[Union[str, bytes]] = ...,
        skipme: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def client_list(
        self,
        type_: Optional[
            Literal[
                PureToken.NORMAL, PureToken.MASTER, PureToken.REPLICA, PureToken.PUBSUB
            ]
        ] = ...,
        identifiers: Optional[Iterable[int]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def client_no_evict(
        self, enabled: Literal[PureToken.ON, PureToken.OFF]
    ) -> Pipeline[AnyStr]: ...
    async def client_pause(
        self,
        timeout: int,
        mode: Optional[Literal[PureToken.WRITE, PureToken.ALL]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def client_reply(
        self, mode: Literal[PureToken.ON, PureToken.OFF, PureToken.SKIP]
    ) -> Pipeline[AnyStr]: ...
    async def client_setname(
        self, connection_name: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def client_tracking(
        self,
        status: Literal[PureToken.ON, PureToken.OFF],
        *prefixes: Union[str, bytes],
        redirect: Optional[int] = ...,
        bcast: Optional[bool] = ...,
        optin: Optional[bool] = ...,
        optout: Optional[bool] = ...,
        noloop: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def client_trackinginfo(self) -> Pipeline[AnyStr]: ...
    async def client_unblock(
        self,
        client_id: int,
        timeout_error: Optional[Literal[PureToken.TIMEOUT, PureToken.ERROR]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def client_unpause(self) -> Pipeline[AnyStr]: ...
    async def echo(self, message: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def hello(
        self,
        protover: Optional[int] = ...,
        username: Optional[Union[str, bytes]] = ...,
        password: Optional[Union[str, bytes]] = ...,
        setname: Optional[Union[str, bytes]] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def ping(
        self, message: Optional[Union[str, bytes]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def quit(self) -> Pipeline[AnyStr]: ...
    async def reset(self) -> Pipeline[AnyStr]: ...
    async def select(self, index: int) -> Pipeline[AnyStr]: ...
    async def asking(self) -> Pipeline[AnyStr]: ...
    async def cluster_addslots(self, slots: Iterable[int]) -> Pipeline[AnyStr]: ...
    async def cluster_addslotsrange(
        self, slots: Iterable[Tuple[int, int]]
    ) -> Pipeline[AnyStr]: ...
    async def cluster_bumpepoch(self) -> Pipeline[AnyStr]: ...
    async def cluster_count_failure_reports(
        self, node_id: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def cluster_countkeysinslot(self, slot: int) -> Pipeline[AnyStr]: ...
    async def cluster_delslots(self, slots: Iterable[int]) -> Pipeline[AnyStr]: ...
    async def cluster_delslotsrange(
        self, slots: Iterable[Tuple[int, int]]
    ) -> Pipeline[AnyStr]: ...
    async def cluster_failover(
        self, options: Optional[Literal[PureToken.FORCE, PureToken.TAKEOVER]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def cluster_flushslots(self) -> Pipeline[AnyStr]: ...
    async def cluster_forget(self, node_id: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def cluster_getkeysinslot(
        self, slot: int, count: int
    ) -> Pipeline[AnyStr]: ...
    async def cluster_info(self) -> Pipeline[AnyStr]: ...
    async def cluster_keyslot(self, key: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def cluster_links(self) -> Pipeline[AnyStr]: ...
    async def cluster_meet(
        self, ip: Union[str, bytes], port: int
    ) -> Pipeline[AnyStr]: ...
    async def cluster_myid(self) -> Pipeline[AnyStr]: ...
    async def cluster_nodes(self) -> Pipeline[AnyStr]: ...
    async def cluster_replicas(
        self, node_id: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def cluster_replicate(
        self, node_id: Union[str, bytes]
    ) -> Pipeline[AnyStr]: ...
    async def cluster_reset(
        self, hard_soft: Optional[Literal[PureToken.HARD, PureToken.SOFT]] = ...
    ) -> Pipeline[AnyStr]: ...
    async def cluster_saveconfig(self) -> Pipeline[AnyStr]: ...
    async def cluster_set_config_epoch(self, config_epoch: int) -> Pipeline[AnyStr]: ...
    async def cluster_setslot(
        self,
        slot: int,
        importing: Optional[Union[str, bytes]] = ...,
        migrating: Optional[Union[str, bytes]] = ...,
        node: Optional[Union[str, bytes]] = ...,
        stable: Optional[bool] = ...,
    ) -> Pipeline[AnyStr]: ...
    async def cluster_shards(self) -> Pipeline[AnyStr]: ...
    async def cluster_slaves(self, node_id: Union[str, bytes]) -> Pipeline[AnyStr]: ...
    async def cluster_slots(self) -> Pipeline[AnyStr]: ...
    async def readonly(self) -> Pipeline[AnyStr]: ...
    async def readwrite(self) -> Pipeline[AnyStr]: ...
    async def watch(self, *keys: Union[str, bytes]) -> bool: ...
    async def unwatch(self) -> bool: ...
    def multi(self) -> None: ...
    async def execute(self, raise_on_error: bool = ...) -> Tuple[Any, ...]: ...
    async def __aenter__(self) -> "Pipeline[AnyStr]": ...
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None: ...

class ClusterPipeline(ObjectProxy, Generic[AnyStr]):  # type: ignore
    @classmethod
    def proxy(
        cls,
        connection_pool: ClusterConnectionPool,
        result_callbacks: Optional[Dict[bytes, Callable[..., Any]]] = ...,
        response_callbacks: Optional[Dict[bytes, Callable[..., Any]]] = ...,
        startup_nodes: Optional[List[Node]] = ...,
        transaction: Optional[bool] = ...,
        watches: Optional[Iterable[Union[str, bytes]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def copy(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        db: Optional[int] = ...,
        replace: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def delete(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def dump(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def exists(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def expire(
        self,
        key: Union[str, bytes],
        seconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def expireat(
        self,
        key: Union[str, bytes],
        unix_time_seconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def expiretime(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def keys(
        self, pattern: Union[str, bytes] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def migrate(
        self,
        host: Union[str, bytes],
        port: int,
        destination_db: int,
        timeout: int,
        *keys: Union[str, bytes],
        copy: Optional[bool] = ...,
        replace: Optional[bool] = ...,
        auth: Optional[Union[str, bytes]] = ...,
        username: Optional[Union[str, bytes]] = ...,
        password: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def move(
        self, key: Union[str, bytes], db: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def object_encoding(
        self, key: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def object_freq(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def object_idletime(
        self, key: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def object_refcount(
        self, key: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def persist(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def pexpire(
        self,
        key: Union[str, bytes],
        milliseconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def pexpireat(
        self,
        key: Union[str, bytes],
        unix_time_milliseconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def pexpiretime(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def pttl(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def randomkey(self) -> ClusterPipeline[AnyStr]: ...
    async def rename(
        self, key: Union[str, bytes], newkey: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def renamenx(
        self, key: Union[str, bytes], newkey: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def restore(
        self,
        key: Union[str, bytes],
        ttl: int,
        serialized_value: bytes,
        replace: Optional[bool] = ...,
        absttl: Optional[bool] = ...,
        idletime: Optional[Union[int, datetime.timedelta]] = ...,
        freq: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def scan(
        self,
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
        type_: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def sort(
        self,
        key: Union[str, bytes],
        gets: Optional[Iterable[Union[str, bytes]]] = ...,
        by: Optional[Union[str, bytes]] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        alpha: Optional[bool] = ...,
        store: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def sort_ro(
        self,
        key: Union[str, bytes],
        gets: Optional[Iterable[Union[str, bytes]]] = ...,
        by: Optional[Union[str, bytes]] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        alpha: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def touch(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def ttl(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def type(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def unlink(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def wait(self, numreplicas: int, timeout: int) -> ClusterPipeline[AnyStr]: ...
    async def append(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def decr(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def decrby(
        self, key: Union[str, bytes], decrement: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def get(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def getdel(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def getex(
        self,
        key: Union[str, bytes],
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        persist: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def getrange(
        self, key: Union[str, bytes], start: int, end: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def getset(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def incr(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def incrby(
        self, key: Union[str, bytes], increment: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def incrbyfloat(
        self, key: Union[str, bytes], increment: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def lcs(
        self,
        key1: Union[str, bytes],
        key2: Union[str, bytes],
        len_: Optional[bool] = ...,
        idx: Optional[bool] = ...,
        minmatchlen: Optional[int] = ...,
        withmatchlen: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def mget(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def mset(
        self, key_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def msetnx(
        self, key_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def psetex(
        self,
        key: Union[str, bytes],
        milliseconds: Union[int, datetime.timedelta],
        value: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def set(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes, int, float],
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        keepttl: Optional[bool] = ...,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        get: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def setex(
        self,
        key: Union[str, bytes],
        value: Union[str, bytes, int, float],
        seconds: Union[int, datetime.timedelta],
    ) -> ClusterPipeline[AnyStr]: ...
    async def setnx(
        self, key: Union[str, bytes], value: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def setrange(
        self, key: Union[str, bytes], offset: int, value: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def strlen(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def substr(
        self, key: Union[str, bytes], start: int, end: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def bitcount(
        self,
        key: Union[str, bytes],
        start: Optional[int] = ...,
        end: Optional[int] = ...,
        index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def bitop(
        self,
        keys: Iterable[Union[str, bytes]],
        operation: Union[str, bytes],
        destkey: Union[str, bytes],
    ) -> ClusterPipeline[AnyStr]: ...
    async def bitpos(
        self,
        key: Union[str, bytes],
        bit: int,
        start: Optional[int] = ...,
        end: Optional[int] = ...,
        end_index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def getbit(
        self, key: Union[str, bytes], offset: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def setbit(
        self, key: Union[str, bytes], offset: int, value: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def hdel(
        self, key: Union[str, bytes], fields: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def hexists(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def hget(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def hgetall(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def hincrby(
        self, key: Union[str, bytes], field: Union[str, bytes], increment: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def hincrbyfloat(
        self,
        key: Union[str, bytes],
        field: Union[str, bytes],
        increment: Union[int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def hkeys(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def hlen(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def hmget(
        self, key: Union[str, bytes], fields: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def hmset(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def hrandfield(
        self,
        key: Union[str, bytes],
        count: Optional[int] = ...,
        withvalues: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def hscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def hset(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def hsetnx(
        self,
        key: Union[str, bytes],
        field: Union[str, bytes],
        value: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def hstrlen(
        self, key: Union[str, bytes], field: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def hvals(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def blmove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
        timeout: Union[int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def blmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        timeout: Union[int, float],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def blpop(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def brpop(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def brpoplpush(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        timeout: Union[int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def lindex(
        self, key: Union[str, bytes], index: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def linsert(
        self,
        key: Union[str, bytes],
        where: Literal[PureToken.BEFORE, PureToken.AFTER],
        pivot: Union[str, bytes, int, float],
        element: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def llen(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def lmove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
    ) -> ClusterPipeline[AnyStr]: ...
    async def lmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def lpop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def lpos(
        self,
        key: Union[str, bytes],
        element: Union[str, bytes, int, float],
        rank: Optional[int] = ...,
        count: Optional[int] = ...,
        maxlen: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def lpush(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def lpushx(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def lrange(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def lrem(
        self, key: Union[str, bytes], count: int, element: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def lset(
        self, key: Union[str, bytes], index: int, element: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def ltrim(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def rpop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def rpoplpush(
        self, source: Union[str, bytes], destination: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def rpush(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def rpushx(
        self, key: Union[str, bytes], elements: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sadd(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def scard(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def sdiff(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sdiffstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sinter(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sintercard(
        self, keys: Iterable[Union[str, bytes]], limit: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def sinterstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sismember(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def smembers(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def smismember(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def smove(
        self,
        source: Union[str, bytes],
        destination: Union[str, bytes],
        member: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def spop(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def srandmember(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def srem(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def sunion(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def sunionstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def bzmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        timeout: Union[int, float],
        where: Literal[PureToken.MIN, PureToken.MAX],
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def bzpopmax(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def bzpopmin(
        self, keys: Iterable[Union[str, bytes]], timeout: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zadd(
        self,
        key: Union[str, bytes],
        member_scores: Dict[Union[str, bytes], float],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        comparison: Optional[Literal[PureToken.GT, PureToken.LT]] = ...,
        change: Optional[bool] = ...,
        increment: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zcard(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def zcount(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def zdiff(
        self, keys: Iterable[Union[str, bytes]], withscores: Optional[bool] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def zdiffstore(
        self, keys: Iterable[Union[str, bytes]], destination: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zincrby(
        self,
        key: Union[str, bytes],
        member: Union[str, bytes, int, float],
        increment: int,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zinter(
        self,
        keys: Iterable[Union[str, bytes]],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
        withscores: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zintercard(
        self, keys: Iterable[Union[str, bytes]], limit: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def zinterstore(
        self,
        keys: Iterable[Union[str, bytes]],
        destination: Union[str, bytes],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zlexcount(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def zmpop(
        self,
        keys: Iterable[Union[str, bytes]],
        where: Literal[PureToken.MIN, PureToken.MAX],
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zmscore(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zpopmax(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def zpopmin(
        self, key: Union[str, bytes], count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrandmember(
        self,
        key: Union[str, bytes],
        count: Optional[int] = ...,
        withscores: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrange(
        self,
        key: Union[str, bytes],
        min_: Union[int, str, bytes, float],
        max_: Union[int, str, bytes, float],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = ...,
        rev: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
        withscores: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrangebylex(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrangebyscore(
        self,
        key: Union[str, bytes],
        min_: Union[int, float],
        max_: Union[int, float],
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrangestore(
        self,
        dst: Union[str, bytes],
        src: Union[str, bytes],
        min_: Union[int, str, bytes, float],
        max_: Union[int, str, bytes, float],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = ...,
        rev: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrank(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrem(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zremrangebylex(
        self,
        key: Union[str, bytes],
        min_: Union[str, bytes, int, float],
        max_: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def zremrangebyrank(
        self, key: Union[str, bytes], start: int, stop: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def zremrangebyscore(
        self, key: Union[str, bytes], min_: Union[int, float], max_: Union[int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrevrange(
        self,
        key: Union[str, bytes],
        start: int,
        stop: int,
        withscores: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrevrangebylex(
        self,
        key: Union[str, bytes],
        max_: Union[str, bytes, int, float],
        min_: Union[str, bytes, int, float],
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrevrangebyscore(
        self,
        key: Union[str, bytes],
        max_: Union[int, float],
        min_: Union[int, float],
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zrevrank(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zscan(
        self,
        key: Union[str, bytes],
        cursor: Optional[int] = ...,
        match: Optional[Union[str, bytes]] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zscore(
        self, key: Union[str, bytes], member: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def zunion(
        self,
        keys: Iterable[Union[str, bytes]],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
        withscores: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def zunionstore(
        self,
        keys: Iterable[Union[str, bytes]],
        destination: Union[str, bytes],
        weights: Optional[Iterable[int]] = ...,
        aggregate: Optional[Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def pfadd(
        self, key: Union[str, bytes], *elements: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def pfcount(
        self, keys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def pfmerge(
        self, destkey: Union[str, bytes], sourcekeys: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def geoadd(
        self,
        key: Union[str, bytes],
        longitude_latitude_members: Iterable[
            Tuple[Union[int, float], Union[int, float], Union[str, bytes, int, float]]
        ],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        change: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def geodist(
        self,
        key: Union[str, bytes],
        member1: Union[str, bytes],
        member2: Union[str, bytes],
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def geohash(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def geopos(
        self, key: Union[str, bytes], members: Iterable[Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def georadius(
        self,
        key: Union[str, bytes],
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        store: Optional[Union[str, bytes]] = ...,
        storedist: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def georadiusbymember(
        self,
        key: Union[str, bytes],
        member: Union[str, bytes, int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        store: Optional[Union[str, bytes]] = ...,
        storedist: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def geosearch(
        self,
        key: Union[str, bytes],
        member: Optional[Union[str, bytes, int, float]] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def geosearchstore(
        self,
        destination: Union[str, bytes],
        source: Union[str, bytes],
        member: Optional[Union[str, bytes, int, float]] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        storedist: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xack(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        identifiers: Iterable[Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def xadd(
        self,
        key: Union[str, bytes],
        field_values: Dict[Union[str, bytes], Union[str, bytes, int, float]],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        nomkstream: Optional[bool] = ...,
        trim_strategy: Optional[Literal[PureToken.MAXLEN, PureToken.MINID]] = ...,
        threshold: Optional[int] = ...,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = ...,
        limit: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xautoclaim(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        min_idle_time: Union[int, datetime.timedelta],
        start: Union[str, bytes, int, float],
        count: Optional[int] = ...,
        justid: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xclaim(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        min_idle_time: Union[int, datetime.timedelta],
        identifiers: Iterable[Union[str, bytes, int, float]],
        idle: Optional[int] = ...,
        time: Optional[Union[int, datetime.datetime]] = ...,
        retrycount: Optional[int] = ...,
        force: Optional[bool] = ...,
        justid: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xdel(
        self,
        key: Union[str, bytes],
        identifiers: Iterable[Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def xgroup_create(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        mkstream: Optional[bool] = ...,
        entriesread: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xgroup_createconsumer(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        consumername: Union[str, bytes],
    ) -> ClusterPipeline[AnyStr]: ...
    async def xgroup_delconsumer(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        consumername: Union[str, bytes],
    ) -> ClusterPipeline[AnyStr]: ...
    async def xgroup_destroy(
        self, key: Union[str, bytes], groupname: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def xgroup_setid(
        self,
        key: Union[str, bytes],
        groupname: Union[str, bytes],
        identifier: Optional[Union[str, bytes, int, float]] = ...,
        entriesread: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xinfo_consumers(
        self, key: Union[str, bytes], groupname: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def xinfo_groups(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def xinfo_stream(
        self,
        key: Union[str, bytes],
        full: Optional[bool] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xlen(self, key: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def xpending(
        self,
        key: Union[str, bytes],
        group: Union[str, bytes],
        start: Optional[Union[str, bytes, int, float]] = ...,
        end: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
        idle: Optional[int] = ...,
        consumer: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xrange(
        self,
        key: Union[str, bytes],
        start: Optional[Union[str, bytes, int, float]] = ...,
        end: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xread(
        self,
        streams: Dict[Union[str, bytes, int, float], Union[str, bytes, int, float]],
        count: Optional[int] = ...,
        block: Optional[Union[int, datetime.timedelta]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xreadgroup(
        self,
        group: Union[str, bytes],
        consumer: Union[str, bytes],
        streams: Dict[Union[str, bytes, int, float], Union[str, bytes, int, float]],
        count: Optional[int] = ...,
        block: Optional[Union[int, datetime.timedelta]] = ...,
        noack: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xrevrange(
        self,
        key: Union[str, bytes],
        end: Optional[Union[str, bytes, int, float]] = ...,
        start: Optional[Union[str, bytes, int, float]] = ...,
        count: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def xtrim(
        self,
        key: Union[str, bytes],
        trim_strategy: Literal[PureToken.MAXLEN, PureToken.MINID],
        threshold: int,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = ...,
        limit: Optional[int] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def eval(
        self,
        script: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def evalsha(
        self,
        sha1: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def evalsha_ro(
        self,
        sha1: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def eval_ro(
        self,
        script: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def fcall(
        self,
        function: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def fcall_ro(
        self,
        function: Union[str, bytes],
        keys: Optional[Iterable[Union[str, bytes]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_delete(
        self, library_name: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_dump(self) -> ClusterPipeline[AnyStr]: ...
    async def function_flush(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_kill(self) -> ClusterPipeline[AnyStr]: ...
    async def function_list(
        self,
        libraryname: Optional[Union[str, bytes]] = ...,
        withcode: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_load(
        self, function_code: Union[str, bytes], replace: Optional[bool] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_restore(
        self,
        serialized_value: bytes,
        policy: Optional[
            Literal[PureToken.FLUSH, PureToken.APPEND, PureToken.REPLACE]
        ] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def function_stats(self) -> ClusterPipeline[AnyStr]: ...
    async def script_debug(
        self, mode: Literal[PureToken.YES, PureToken.SYNC, PureToken.NO]
    ) -> ClusterPipeline[AnyStr]: ...
    async def script_exists(
        self, sha1s: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def script_flush(
        self, sync_type: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def script_kill(self) -> ClusterPipeline[AnyStr]: ...
    async def script_load(
        self, script: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def publish(
        self, channel: Union[str, bytes], message: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def pubsub_channels(
        self, pattern: Optional[Union[str, bytes]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def pubsub_numpat(self) -> ClusterPipeline[AnyStr]: ...
    async def pubsub_numsub(
        self, *channels: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_cat(
        self, categoryname: Optional[Union[str, bytes]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_deluser(
        self, usernames: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_dryrun(
        self,
        username: Union[str, bytes],
        command: Union[str, bytes],
        *args: Union[str, bytes, int, float],
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_genpass(
        self, bits: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_getuser(
        self, username: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_list(self) -> ClusterPipeline[AnyStr]: ...
    async def acl_load(self) -> ClusterPipeline[AnyStr]: ...
    async def acl_log(
        self, count: Optional[int] = ..., reset: Optional[bool] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_save(self) -> ClusterPipeline[AnyStr]: ...
    async def acl_setuser(
        self, username: Union[str, bytes], *rules: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def acl_users(self) -> ClusterPipeline[AnyStr]: ...
    async def acl_whoami(self) -> ClusterPipeline[AnyStr]: ...
    async def bgrewriteaof(self) -> ClusterPipeline[AnyStr]: ...
    async def bgsave(
        self, schedule: Optional[bool] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def command(self) -> ClusterPipeline[AnyStr]: ...
    async def command_count(self) -> ClusterPipeline[AnyStr]: ...
    async def command_docs(
        self, *command_names: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def command_getkeys(
        self,
        command: Union[str, bytes],
        arguments: Iterable[Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def command_getkeysandflags(
        self,
        command: Union[str, bytes],
        arguments: Iterable[Union[str, bytes, int, float]],
    ) -> ClusterPipeline[AnyStr]: ...
    async def command_info(
        self, *command_names: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def command_list(
        self,
        module: Optional[Union[str, bytes]] = ...,
        aclcat: Optional[Union[str, bytes]] = ...,
        pattern: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def config_get(
        self, parameters: Iterable[Union[str, bytes]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def config_resetstat(self) -> ClusterPipeline[AnyStr]: ...
    async def config_rewrite(self) -> ClusterPipeline[AnyStr]: ...
    async def config_set(
        self, parameter_values: Dict[Union[str, bytes], Union[str, bytes, int, float]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def dbsize(self) -> ClusterPipeline[AnyStr]: ...
    async def failover(
        self,
        host: Optional[Union[str, bytes]] = ...,
        port: Optional[int] = ...,
        force: Optional[bool] = ...,
        abort: Optional[bool] = ...,
        timeout: Optional[Union[int, datetime.timedelta]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def flushall(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def flushdb(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def info(self, *sections: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def lastsave(self) -> ClusterPipeline[AnyStr]: ...
    async def latency_doctor(self) -> ClusterPipeline[AnyStr]: ...
    async def latency_graph(
        self, event: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def latency_histogram(
        self, *commands: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def latency_history(
        self, event: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def latency_latest(self) -> ClusterPipeline[AnyStr]: ...
    async def latency_reset(
        self, *events: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def lolwut(self, version: Optional[int] = ...) -> ClusterPipeline[AnyStr]: ...
    async def memory_doctor(self) -> ClusterPipeline[AnyStr]: ...
    async def memory_malloc_stats(self) -> ClusterPipeline[AnyStr]: ...
    async def memory_purge(self) -> ClusterPipeline[AnyStr]: ...
    async def memory_stats(self) -> ClusterPipeline[AnyStr]: ...
    async def memory_usage(
        self, key: Union[str, bytes], samples: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def module_list(self) -> ClusterPipeline[AnyStr]: ...
    async def module_load(
        self, path: Union[str, bytes], *args: Union[str, bytes, int, float]
    ) -> ClusterPipeline[AnyStr]: ...
    async def module_loadex(
        self,
        path: Union[str, bytes],
        configs: Optional[Dict[Union[str, bytes], Union[str, bytes, int, float]]] = ...,
        args: Optional[Iterable[Union[str, bytes, int, float]]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def module_unload(
        self, name: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def replicaof(
        self, host: Optional[Union[str, bytes]] = ..., port: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def role(self) -> ClusterPipeline[AnyStr]: ...
    async def save(self) -> ClusterPipeline[AnyStr]: ...
    async def shutdown(
        self,
        nosave_save: Optional[Literal[PureToken.NOSAVE, PureToken.SAVE]] = ...,
        now: Optional[bool] = ...,
        force: Optional[bool] = ...,
        abort: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def slaveof(
        self, host: Optional[Union[str, bytes]] = ..., port: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def slowlog_get(
        self, count: Optional[int] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def slowlog_len(self) -> ClusterPipeline[AnyStr]: ...
    async def slowlog_reset(self) -> ClusterPipeline[AnyStr]: ...
    async def swapdb(self, index1: int, index2: int) -> ClusterPipeline[AnyStr]: ...
    async def time(self) -> ClusterPipeline[AnyStr]: ...
    async def auth(
        self, password: Union[str, bytes], username: Optional[Union[str, bytes]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_caching(
        self, mode: Literal[PureToken.YES, PureToken.NO]
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_getname(self) -> ClusterPipeline[AnyStr]: ...
    async def client_getredir(self) -> ClusterPipeline[AnyStr]: ...
    async def client_id(self) -> ClusterPipeline[AnyStr]: ...
    async def client_info(self) -> ClusterPipeline[AnyStr]: ...
    async def client_kill(
        self,
        ip_port: Optional[Union[str, bytes]] = ...,
        identifier: Optional[int] = ...,
        type_: Optional[
            Literal[
                PureToken.NORMAL,
                PureToken.MASTER,
                PureToken.SLAVE,
                PureToken.REPLICA,
                PureToken.PUBSUB,
            ]
        ] = ...,
        user: Optional[Union[str, bytes]] = ...,
        addr: Optional[Union[str, bytes]] = ...,
        laddr: Optional[Union[str, bytes]] = ...,
        skipme: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_list(
        self,
        type_: Optional[
            Literal[
                PureToken.NORMAL, PureToken.MASTER, PureToken.REPLICA, PureToken.PUBSUB
            ]
        ] = ...,
        identifiers: Optional[Iterable[int]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_no_evict(
        self, enabled: Literal[PureToken.ON, PureToken.OFF]
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_pause(
        self,
        timeout: int,
        mode: Optional[Literal[PureToken.WRITE, PureToken.ALL]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_reply(
        self, mode: Literal[PureToken.ON, PureToken.OFF, PureToken.SKIP]
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_setname(
        self, connection_name: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_tracking(
        self,
        status: Literal[PureToken.ON, PureToken.OFF],
        *prefixes: Union[str, bytes],
        redirect: Optional[int] = ...,
        bcast: Optional[bool] = ...,
        optin: Optional[bool] = ...,
        optout: Optional[bool] = ...,
        noloop: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_trackinginfo(self) -> ClusterPipeline[AnyStr]: ...
    async def client_unblock(
        self,
        client_id: int,
        timeout_error: Optional[Literal[PureToken.TIMEOUT, PureToken.ERROR]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def client_unpause(self) -> ClusterPipeline[AnyStr]: ...
    async def echo(self, message: Union[str, bytes]) -> ClusterPipeline[AnyStr]: ...
    async def hello(
        self,
        protover: Optional[int] = ...,
        username: Optional[Union[str, bytes]] = ...,
        password: Optional[Union[str, bytes]] = ...,
        setname: Optional[Union[str, bytes]] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def ping(
        self, message: Optional[Union[str, bytes]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def quit(self) -> ClusterPipeline[AnyStr]: ...
    async def reset(self) -> ClusterPipeline[AnyStr]: ...
    async def select(self, index: int) -> ClusterPipeline[AnyStr]: ...
    async def asking(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_addslots(
        self, slots: Iterable[int]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_addslotsrange(
        self, slots: Iterable[Tuple[int, int]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_bumpepoch(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_count_failure_reports(
        self, node_id: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_countkeysinslot(self, slot: int) -> ClusterPipeline[AnyStr]: ...
    async def cluster_delslots(
        self, slots: Iterable[int]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_delslotsrange(
        self, slots: Iterable[Tuple[int, int]]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_failover(
        self, options: Optional[Literal[PureToken.FORCE, PureToken.TAKEOVER]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_flushslots(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_forget(
        self, node_id: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_getkeysinslot(
        self, slot: int, count: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_info(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_keyslot(
        self, key: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_links(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_meet(
        self, ip: Union[str, bytes], port: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_myid(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_nodes(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_replicas(
        self, node_id: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_replicate(
        self, node_id: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_reset(
        self, hard_soft: Optional[Literal[PureToken.HARD, PureToken.SOFT]] = ...
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_saveconfig(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_set_config_epoch(
        self, config_epoch: int
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_setslot(
        self,
        slot: int,
        importing: Optional[Union[str, bytes]] = ...,
        migrating: Optional[Union[str, bytes]] = ...,
        node: Optional[Union[str, bytes]] = ...,
        stable: Optional[bool] = ...,
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_shards(self) -> ClusterPipeline[AnyStr]: ...
    async def cluster_slaves(
        self, node_id: Union[str, bytes]
    ) -> ClusterPipeline[AnyStr]: ...
    async def cluster_slots(self) -> ClusterPipeline[AnyStr]: ...
    async def readonly(self) -> ClusterPipeline[AnyStr]: ...
    async def readwrite(self) -> ClusterPipeline[AnyStr]: ...
    async def watch(self, *keys: Union[str, bytes]) -> bool: ...
    async def unwatch(self) -> bool: ...
    def multi(self) -> None: ...
    async def execute(self, raise_on_error: bool = ...) -> Tuple[object, ...]: ...
    async def __aenter__(self) -> "ClusterPipeline[AnyStr]": ...
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None: ...
