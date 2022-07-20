from __future__ import annotations

import datetime
import itertools
from typing import overload

from deprecated.sphinx import versionadded

from coredis._utils import defaultvalue, dict_to_flat_list, tuples_to_flat_list
from coredis.commands import CommandMixin
from coredis.commands._utils import (
    normalized_milliseconds,
    normalized_seconds,
    normalized_time_milliseconds,
    normalized_time_seconds,
)
from coredis.commands._validators import (
    mutually_exclusive_parameters,
    mutually_inclusive_parameters,
)
from coredis.commands._wrappers import (
    CacheConfig,
    ClusterCommandConfig,
    RedirectUsage,
    redis_command,
)
from coredis.commands.bitfield import BitFieldOperation
from coredis.commands.constants import CommandGroup, CommandName, NodeFlag
from coredis.exceptions import (
    AuthorizationError,
    ConnectionError,
    DataError,
    RedisError,
)
from coredis.response._callbacks import (
    AnyStrCallback,
    BoolCallback,
    BoolsCallback,
    ClusterAlignedBoolsCombine,
    ClusterBoolCombine,
    ClusterEnsureConsistent,
    ClusterFirstNonException,
    ClusterMergeMapping,
    ClusterMergeSets,
    ClusterSum,
    DateTimeCallback,
    DictCallback,
    FloatCallback,
    IntCallback,
    ListCallback,
    NoopCallback,
    OptionalAnyStrCallback,
    OptionalFloatCallback,
    OptionalIntCallback,
    OptionalListCallback,
    SetCallback,
    SimpleStringCallback,
    SimpleStringOrIntCallback,
    TupleCallback,
)
from coredis.response._callbacks.acl import ACLLogCallback
from coredis.response._callbacks.cluster import (
    ClusterInfoCallback,
    ClusterLinksCallback,
    ClusterNodesCallback,
    ClusterShardsCallback,
    ClusterSlotsCallback,
)
from coredis.response._callbacks.command import (
    CommandCallback,
    CommandDocCallback,
    CommandKeyFlagCallback,
)
from coredis.response._callbacks.connection import ClientTrackingInfoCallback
from coredis.response._callbacks.geo import GeoCoordinatessCallback, GeoSearchCallback
from coredis.response._callbacks.hash import (
    HGetAllCallback,
    HRandFieldCallback,
    HScanCallback,
)
from coredis.response._callbacks.keys import ExpiryCallback, ScanCallback, SortCallback
from coredis.response._callbacks.module import ModuleInfoCallback
from coredis.response._callbacks.script import (
    FunctionListCallback,
    FunctionStatsCallback,
)
from coredis.response._callbacks.server import (
    ClientInfoCallback,
    ClientListCallback,
    DebugCallback,
    InfoCallback,
    LatencyCallback,
    LatencyHistogramCallback,
    RoleCallback,
    SlowlogCallback,
    TimeCallback,
)
from coredis.response._callbacks.sets import ItemOrSetCallback, SScanCallback
from coredis.response._callbacks.sorted_set import (
    BZPopCallback,
    ZAddCallback,
    ZMembersOrScoredMembers,
    ZMPopCallback,
    ZMScoreCallback,
    ZRandMemberCallback,
    ZScanCallback,
    ZSetScorePairCallback,
)
from coredis.response._callbacks.streams import (
    AutoClaimCallback,
    ClaimCallback,
    MultiStreamRangeCallback,
    PendingCallback,
    StreamInfoCallback,
    StreamRangeCallback,
    XInfoCallback,
)
from coredis.response._callbacks.strings import LCSCallback, StringSetCallback
from coredis.response.types import (
    ClientInfo,
    ClusterNode,
    ClusterNodeDetail,
    Command,
    GeoCoordinates,
    GeoSearchResult,
    LCSResult,
    LibraryDefinition,
    RoleInfo,
    ScoredMember,
    SlowLogInfo,
    StreamEntry,
    StreamInfo,
    StreamPending,
    StreamPendingExt,
)
from coredis.tokens import PrefixToken, PureToken
from coredis.typing import (
    AnyStr,
    CommandArgList,
    Dict,
    KeyT,
    List,
    Literal,
    Mapping,
    Optional,
    Parameters,
    ResponsePrimitive,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Union,
    ValueT,
)


class CoreCommands(CommandMixin[AnyStr]):
    @redis_command(CommandName.APPEND, group=CommandGroup.STRING)
    async def append(self, key: KeyT, value: ValueT) -> int:
        """
        Append a value to a key

        :return: the length of the string after the append operation.
        """

        return await self.execute_command(
            CommandName.APPEND, key, value, callback=IntCallback()
        )

    @redis_command(CommandName.DECR, group=CommandGroup.STRING)
    async def decr(self, key: KeyT) -> int:
        """
        Decrement the integer value of a key by one

        :return: the value of :paramref:`key` after the decrement
        """

        return await self.decrby(key, 1)

    @redis_command(CommandName.DECRBY, group=CommandGroup.STRING)
    async def decrby(self, key: KeyT, decrement: int) -> int:
        """
        Decrement the integer value of a key by the given number

        :return: the value of :paramref:`key` after the decrement
        """

        return await self.execute_command(
            CommandName.DECRBY, key, decrement, callback=IntCallback()
        )

    @redis_command(
        CommandName.GET,
        readonly=True,
        group=CommandGroup.STRING,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def get(self, key: KeyT) -> Optional[AnyStr]:
        """
        Get the value of a key

        :return: the value of :paramref:`key`, or ``None`` when :paramref:`key`
         does not exist.
        """

        return await self.execute_command(
            CommandName.GET, key, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.GETDEL,
        group=CommandGroup.STRING,
        version_introduced="6.2.0",
    )
    async def getdel(self, key: KeyT) -> Optional[AnyStr]:
        """
        Get the value of a key and delete the key


        :return: the value of :paramref:`key`, ``None`` when :paramref:`key`
         does not exist, or an error if the key's value type isn't a string.
        """

        return await self.execute_command(
            CommandName.GETDEL, key, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @mutually_exclusive_parameters("ex", "px", "exat", "pxat", "persist")
    @redis_command(
        CommandName.GETEX, group=CommandGroup.STRING, version_introduced="6.2.0"
    )
    async def getex(
        self,
        key: KeyT,
        ex: Optional[Union[int, datetime.timedelta]] = None,
        px: Optional[Union[int, datetime.timedelta]] = None,
        exat: Optional[Union[int, datetime.datetime]] = None,
        pxat: Optional[Union[int, datetime.datetime]] = None,
        persist: Optional[bool] = None,
    ) -> Optional[AnyStr]:
        """
        Get the value of a key and optionally set its expiration


        GETEX is similar to GET, but is a write command with
        additional options. All time parameters can be given as
        :class:`datetime.timedelta` or integers.

        :param key: name of the key
        :param ex: sets an expire flag on key :paramref:`key` for ``ex`` seconds.
        :param px: sets an expire flag on key :paramref:`key` for ``px`` milliseconds.
        :param exat: sets an expire flag on key :paramref:`key` for ``ex`` seconds,
         specified in unix time.
        :param pxat: sets an expire flag on key :paramref:`key` for ``ex`` milliseconds,
         specified in unix time.
        :param persist: remove the time to live associated with :paramref:`key`.

        :return: the value of :paramref:`key`, or ``None`` when :paramref:`key` does not exist.
        """

        pieces: CommandArgList = []

        if ex is not None:
            pieces.append("EX")
            pieces.append(normalized_seconds(ex))

        if px is not None:
            pieces.append("PX")
            pieces.append(normalized_milliseconds(px))

        if exat is not None:
            pieces.append("EXAT")
            pieces.append(normalized_time_seconds(exat))

        if pxat is not None:
            pieces.append("PXAT")
            pieces.append(normalized_time_milliseconds(pxat))

        if persist:
            pieces.append(PureToken.PERSIST)

        return await self.execute_command(
            CommandName.GETEX, key, *pieces, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.GETRANGE,
        readonly=True,
        group=CommandGroup.STRING,
        cache_config=CacheConfig(lambda *a, **k: a[0]),
    )
    async def getrange(self, key: KeyT, start: int, end: int) -> AnyStr:
        """
        Get a substring of the string stored at a key

        :return: The substring of the string value stored at :paramref:`key`,
         determined by the offsets ``start`` and ``end`` (both are inclusive)
        """

        return await self.execute_command(
            CommandName.GETRANGE, key, start, end, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.GETSET,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`set` with the get argument",
        group=CommandGroup.STRING,
    )
    async def getset(self, key: KeyT, value: ValueT) -> Optional[AnyStr]:
        """
        Set the string value of a key and return its old value

        :return: the old value stored at :paramref:`key`, or ``None`` when
         :paramref:`key` did not exist.
        """

        return await self.execute_command(
            CommandName.GETSET, key, value, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.INCR, group=CommandGroup.STRING)
    async def incr(self, key: KeyT) -> int:
        """
        Increment the integer value of a key by one

        :return: the value of :paramref:`key` after the increment.
         If no key exists, the value will be initialized as 1.
        """

        return await self.incrby(key, 1)

    @redis_command(CommandName.INCRBY, group=CommandGroup.STRING)
    async def incrby(self, key: KeyT, increment: int) -> int:
        """
        Increment the integer value of a key by the given amount

        :return: the value of :paramref:`key` after the increment
          If no key exists, the value will be initialized as ``increment``
        """

        return await self.execute_command(
            CommandName.INCRBY, key, increment, callback=IntCallback()
        )

    @redis_command(
        CommandName.INCRBYFLOAT,
        group=CommandGroup.STRING,
    )
    async def incrbyfloat(self, key: KeyT, increment: Union[int, float]) -> float:
        """
        Increment the float value of a key by the given amount

        :return: the value of :paramref:`key` after the increment.
         If no key exists, the value will be initialized as ``increment``
        """

        return await self.execute_command(
            CommandName.INCRBYFLOAT, key, increment, callback=FloatCallback()
        )

    @overload
    async def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
    ) -> AnyStr:
        ...

    @overload
    async def lcs(
        self, key1: Union[str, bytes], key2: Union[str, bytes], *, len_: Literal[True]
    ) -> int:
        ...

    @overload
    async def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
        *,
        idx: Literal[True],
        len_: Optional[bool] = ...,
        minmatchlen: Optional[int] = ...,
        withmatchlen: Optional[bool] = ...,
    ) -> LCSResult:
        ...

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LCS,
        version_introduced="7.0.0",
        group=CommandGroup.STRING,
        readonly=True,
    )
    async def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
        *,
        len_: Optional[bool] = None,
        idx: Optional[bool] = None,
        minmatchlen: Optional[int] = None,
        withmatchlen: Optional[bool] = None,
    ) -> Union[AnyStr, int, LCSResult]:
        """
        Find the longest common substring

        :return: The matched string if no other arguments are given.
         The returned values vary depending on different arguments.

         - If ``len_`` is provided the length of the longest match
         - If ``idx`` is ``True`` all the matches with the start/end positions
           of both keys. Optionally, if ``withmatchlen`` is ``True`` each match
           will contain the length of the match.

        """
        pieces: CommandArgList = [key1, key2]

        if len_ is not None:
            pieces.append(PureToken.LEN)

        if idx is not None:
            pieces.append(PureToken.IDX)

        if minmatchlen is not None:
            pieces.extend([PrefixToken.MINMATCHLEN, minmatchlen])

        if withmatchlen is not None:
            pieces.append(PureToken.WITHMATCHLEN)
        if idx is not None:
            return await self.execute_command(
                CommandName.LCS,
                *pieces,
                callback=LCSCallback[AnyStr](),
                **{
                    "len": len_,
                    "idx": idx,
                    "minmatchlen": minmatchlen,
                    "withmatchlen": withmatchlen,
                },
            )
        else:
            if len_ is not None:
                return await self.execute_command(
                    CommandName.LCS, *pieces, callback=IntCallback()
                )
            else:
                return await self.execute_command(
                    CommandName.LCS, *pieces, callback=AnyStrCallback[AnyStr]()
                )

    @redis_command(
        CommandName.MGET,
        readonly=True,
        group=CommandGroup.STRING,
    )
    async def mget(self, keys: Parameters[KeyT]) -> Tuple[Optional[AnyStr], ...]:
        """
        Returns values ordered identically to ``keys``
        """

        return await self.execute_command(
            CommandName.MGET, *keys, callback=TupleCallback[Optional[AnyStr]]()
        )

    @redis_command(
        CommandName.MSET,
        group=CommandGroup.STRING,
    )
    async def mset(self, key_values: Dict[KeyT, ValueT]) -> bool:
        """
        Sets multiple keys to multiple values
        """

        return await self.execute_command(
            CommandName.MSET,
            *dict_to_flat_list(key_values),
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.MSETNX, group=CommandGroup.STRING)
    async def msetnx(self, key_values: Dict[KeyT, ValueT]) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist

        :return: Whether all the keys were set
        """

        return await self.execute_command(
            CommandName.MSETNX, *dict_to_flat_list(key_values), callback=BoolCallback()
        )

    @redis_command(
        CommandName.PSETEX,
        group=CommandGroup.STRING,
    )
    async def psetex(
        self,
        key: KeyT,
        milliseconds: Union[int, datetime.timedelta],
        value: ValueT,
    ) -> bool:
        """
        Set the value and expiration in milliseconds of a key
        """

        if isinstance(milliseconds, datetime.timedelta):
            ms = int(milliseconds.microseconds / 1000)
            milliseconds = (
                milliseconds.seconds + milliseconds.days * 24 * 3600
            ) * 1000 + ms

        return await self.execute_command(
            CommandName.PSETEX,
            key,
            milliseconds,
            value,
            callback=SimpleStringCallback(),
        )

    @overload
    async def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        keepttl: Optional[bool] = ...,
    ) -> bool:
        ...

    @overload
    async def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = ...,
        get: Literal[True],
        ex: Optional[Union[int, datetime.timedelta]] = ...,
        px: Optional[Union[int, datetime.timedelta]] = ...,
        exat: Optional[Union[int, datetime.datetime]] = ...,
        pxat: Optional[Union[int, datetime.datetime]] = ...,
        keepttl: Optional[bool] = ...,
    ) -> Optional[AnyStr]:
        ...

    @mutually_exclusive_parameters("ex", "px", "exat", "pxat", "keepttl")
    @redis_command(
        CommandName.SET,
        group=CommandGroup.STRING,
        arguments={
            "exat": {"version_introduced": "6.2.0"},
            "pxat": {"version_introduced": "6.2.0"},
            "get": {"version_introduced": "6.2.0"},
        },
    )
    async def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
        get: Optional[bool] = None,
        ex: Optional[Union[int, datetime.timedelta]] = None,
        px: Optional[Union[int, datetime.timedelta]] = None,
        exat: Optional[Union[int, datetime.datetime]] = None,
        pxat: Optional[Union[int, datetime.datetime]] = None,
        keepttl: Optional[bool] = None,
    ) -> Optional[Union[AnyStr, bool]]:
        """
        Set the string value of a key

        :param condition: Condition to use when setting the key
        :param get: Return the old string stored at key, or nil if key did not exist.
         An error is returned and the command is aborted if the value stored at
         key is not a string.
        :param ex: Number of seconds to expire in
        :param px: Number of milliseconds to expire in
        :param exat: Expiry time with seconds granularity
        :param pxat: Expiry time with milliseconds granularity
        :param keepttl: Retain the time to live associated with the key

        :return: Whether the operation was performed successfully.

         .. warning:: If the command is issued with the ``get`` argument, the old string value
            stored at :paramref:`key` is return regardless of success or failure
            - except if the :paramref:`key` was not found.
        """
        pieces: CommandArgList = [key, value]

        if ex is not None:
            pieces.append("EX")
            pieces.append(normalized_seconds(ex))

        if px is not None:
            pieces.append("PX")
            pieces.append(normalized_milliseconds(px))

        if exat is not None:
            pieces.append("EXAT")
            pieces.append(normalized_time_seconds(exat))

        if pxat is not None:
            pieces.append("PXAT")
            pieces.append(normalized_time_milliseconds(pxat))

        if keepttl:
            pieces.append(PureToken.KEEPTTL)

        if get:
            pieces.append(PureToken.GET)

        if condition:
            pieces.append(condition)

        return await self.execute_command(
            CommandName.SET, *pieces, get=get, callback=StringSetCallback[AnyStr]()
        )

    @redis_command(
        CommandName.SETEX,
        group=CommandGroup.STRING,
    )
    async def setex(
        self,
        key: KeyT,
        value: ValueT,
        seconds: Union[int, datetime.timedelta],
    ) -> bool:
        """
        Set the value of key :paramref:`key` to ``value`` that expires in ``seconds``
        """

        return await self.execute_command(
            CommandName.SETEX,
            key,
            normalized_seconds(seconds),
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.SETNX, group=CommandGroup.STRING)
    async def setnx(self, key: KeyT, value: ValueT) -> bool:
        """
        Sets the value of key :paramref:`key` to ``value`` if key doesn't exist
        """

        return await self.execute_command(
            CommandName.SETNX, key, value, callback=BoolCallback()
        )

    @redis_command(CommandName.SETRANGE, group=CommandGroup.STRING)
    async def setrange(self, key: KeyT, offset: int, value: ValueT) -> int:
        """
        Overwrite bytes in the value of :paramref:`key` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.

        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        :return: the length of the string after it was modified by the command.
        """

        return await self.execute_command(
            CommandName.SETRANGE, key, offset, value, callback=IntCallback()
        )

    @redis_command(
        CommandName.STRLEN,
        readonly=True,
        group=CommandGroup.STRING,
        cache_config=CacheConfig(lambda *a: a[0]),
    )
    async def strlen(self, key: KeyT) -> int:
        """
        Get the length of the value stored in a key

        :return: the length of the string at :paramref:`key`, or ``0`` when :paramref:`key` does not
        """

        return await self.execute_command(
            CommandName.STRLEN, key, callback=IntCallback()
        )

    @redis_command(
        CommandName.SUBSTR,
        readonly=True,
        group=CommandGroup.STRING,
        version_deprecated="2.0.0",
        deprecation_reason="Use :meth:`getrange`",
        cache_config=CacheConfig(lambda *a: a[0]),
    )
    async def substr(self, key: KeyT, start: int, end: int) -> AnyStr:
        """
        Get a substring of the string stored at a key

        :return: the substring of the string value stored at key, determined by the offsets
         ``start`` and ``end`` (both are inclusive). Negative offsets can be used in order to
         provide an offset starting from the end of the string.
        """

        return await self.execute_command(
            CommandName.SUBSTR, key, start, end, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_ADDSLOTS,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_addslots(self, slots: Parameters[int]) -> bool:
        """
        Assign new hash slots to receiving node
        """

        return await self.execute_command(
            CommandName.CLUSTER_ADDSLOTS, *slots, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_ADDSLOTSRANGE,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
    )
    async def cluster_addslotsrange(self, slots: Parameters[Tuple[int, int]]) -> bool:
        """
        Assign new hash slots to receiving node
        """
        pieces: CommandArgList = []

        for slot in slots:
            pieces.extend(slot)

        return await self.execute_command(
            CommandName.CLUSTER_ADDSLOTSRANGE, *pieces, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.ASKING, group=CommandGroup.CLUSTER)
    async def asking(self) -> bool:
        """
        Sent by cluster clients after an -ASK redirect
        """

        return await self.execute_command(CommandName.ASKING, callback=BoolCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLUSTER_BUMPEPOCH, group=CommandGroup.CLUSTER)
    async def cluster_bumpepoch(self) -> AnyStr:
        """
        Advance the cluster config epoch

        :return: ``BUMPED`` if the epoch was incremented, or ``STILL``
         if the node already has the greatest config epoch in the cluster.
        """

        return await self.execute_command(
            CommandName.CLUSTER_BUMPEPOCH, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_COUNT_FAILURE_REPORTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_count_failure_reports(self, node_id: StringT) -> int:
        """
        Return the number of failure reports active for a given node

        """

        return await self.execute_command(
            CommandName.CLUSTER_COUNT_FAILURE_REPORTS,
            node_id,
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_COUNTKEYSINSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    async def cluster_countkeysinslot(self, slot: int) -> int:
        """
        Return the number of local keys in the specified hash slot
        """

        return await self.execute_command(
            CommandName.CLUSTER_COUNTKEYSINSLOT,
            slot,
            slot_id=slot,
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_DELSLOTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    async def cluster_delslots(self, slots: Parameters[int]) -> bool:
        """
        Set hash slots as unbound in the cluster.
        It determines by it self what node the slot is in and sends it there
        """
        return all(
            [
                await self.execute_command(
                    CommandName.CLUSTER_DELSLOTS,
                    slot,
                    slot_id=slot,
                    callback=SimpleStringCallback(),
                )
                for slot in slots
            ]
        )

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_DELSLOTSRANGE,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    async def cluster_delslotsrange(self, slots: Parameters[Tuple[int, int]]) -> bool:
        """
        Set hash slots as unbound in receiving node
        """

        return all(
            [
                await self.execute_command(
                    CommandName.CLUSTER_DELSLOTSRANGE,
                    slot_range[0],
                    slot_range[1],
                    slot_id=slot_range[0],
                    callback=SimpleStringCallback(),
                )
                for slot_range in slots
            ]
        )

    @redis_command(
        CommandName.CLUSTER_FAILOVER,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_failover(
        self,
        options: Optional[Literal[PureToken.FORCE, PureToken.TAKEOVER]] = None,
    ) -> bool:
        """
        Forces a replica to perform a manual failover of its master.
        """

        pieces: CommandArgList = []

        if options is not None:
            pieces.append(options)

        return await self.execute_command(
            CommandName.CLUSTER_FAILOVER, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLUSTER_FLUSHSLOTS,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_flushslots(self) -> bool:
        """
        Delete a node's own slots information
        """

        return await self.execute_command(
            CommandName.CLUSTER_FLUSHSLOTS, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_FORGET,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_forget(self, node_id: StringT) -> bool:
        """
        remove a node via its node ID from the set of known nodes
        of the Redis Cluster node receiving the command
        """

        return await self.execute_command(
            CommandName.CLUSTER_FORGET, node_id, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLUSTER_GETKEYSINSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    async def cluster_getkeysinslot(self, slot: int, count: int) -> Tuple[AnyStr, ...]:
        """
        Return local key names in the specified hash slot

        :return: :paramref:`count` key names

        """
        pieces: CommandArgList = [slot, count]

        return await self.execute_command(
            CommandName.CLUSTER_GETKEYSINSLOT,
            *pieces,
            slot_id=slot,
            callback=TupleCallback[AnyStr](),
        )

    @redis_command(
        CommandName.CLUSTER_INFO,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_info(self) -> Dict[str, str]:
        """
        Provides info about Redis Cluster node state
        """

        return await self.execute_command(
            CommandName.CLUSTER_INFO, callback=ClusterInfoCallback()
        )

    @redis_command(
        CommandName.CLUSTER_KEYSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_keyslot(self, key: KeyT) -> int:
        """
        Returns the hash slot of the specified key
        """

        return await self.execute_command(
            CommandName.CLUSTER_KEYSLOT, key, callback=IntCallback()
        )

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_LINKS,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
    )
    async def cluster_links(self) -> List[Dict[AnyStr, ResponsePrimitive]]:
        """
        Returns a list of all TCP links to and from peer nodes in cluster

        :return: A map of maps where each map contains various attributes
         and their values of a cluster link.

        """

        return await self.execute_command(
            CommandName.CLUSTER_LINKS, callback=ClusterLinksCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_MEET,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_meet(
        self, ip: StringT, port: int, cluster_bus_port: Optional[int] = None
    ) -> bool:
        """
        Force a node cluster to handshake with another node.
        """

        pieces: CommandArgList = [ip, port]
        if cluster_bus_port is not None:
            pieces.append(cluster_bus_port)
        return await self.execute_command(
            CommandName.CLUSTER_MEET, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.1")
    @redis_command(CommandName.CLUSTER_MYID, group=CommandGroup.CLUSTER)
    async def cluster_myid(self) -> AnyStr:
        """
        Return the node id
        """

        return await self.execute_command(
            CommandName.CLUSTER_MYID, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_NODES,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_nodes(self) -> List[ClusterNodeDetail]:
        """
        Get Cluster config for the node
        """

        return await self.execute_command(
            CommandName.CLUSTER_NODES, callback=ClusterNodesCallback()
        )

    @redis_command(
        CommandName.CLUSTER_REPLICATE,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_replicate(self, node_id: StringT) -> bool:
        """
        Reconfigure a node as a replica of the specified master node
        """

        return await self.execute_command(
            CommandName.CLUSTER_REPLICATE, node_id, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_RESET,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_reset(
        self,
        hard_soft: Optional[Literal[PureToken.HARD, PureToken.SOFT]] = None,
    ) -> bool:
        """
        Reset a Redis Cluster node
        """

        pieces: CommandArgList = []

        if hard_soft is not None:
            pieces.append(hard_soft)

        return await self.execute_command(
            CommandName.CLUSTER_RESET, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_SAVECONFIG,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    async def cluster_saveconfig(self) -> bool:
        """
        Forces the node to save cluster state on disk
        """

        return await self.execute_command(
            CommandName.CLUSTER_SAVECONFIG, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_SET_CONFIG_EPOCH,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_set_config_epoch(self, config_epoch: int) -> bool:
        """
        Set the configuration epoch in a new node
        """

        return await self.execute_command(
            CommandName.CLUSTER_SET_CONFIG_EPOCH,
            config_epoch,
            callback=SimpleStringCallback(),
        )

    @mutually_exclusive_parameters("importing", "migrating", "node", "stable")
    @redis_command(
        CommandName.CLUSTER_SETSLOT,
        group=CommandGroup.CLUSTER,
    )
    async def cluster_setslot(
        self,
        slot: int,
        *,
        importing: Optional[StringT] = None,
        migrating: Optional[StringT] = None,
        node: Optional[StringT] = None,
        stable: Optional[bool] = None,
    ) -> bool:
        """
        Bind a hash slot to a specific node
        """

        pieces: CommandArgList = []

        if importing is not None:
            pieces.extend([PrefixToken.IMPORTING, importing])

        if migrating is not None:
            pieces.extend([PrefixToken.MIGRATING, migrating])

        if node is not None:
            pieces.extend([PrefixToken.NODE, node])

        if stable is not None:
            pieces.append(PureToken.STABLE)

        return await self.execute_command(
            CommandName.CLUSTER_SETSLOT, slot, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_REPLICAS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_replicas(self, node_id: StringT) -> List[ClusterNodeDetail]:
        """
        List replica nodes of the specified master node
        """

        return await self.execute_command(
            CommandName.CLUSTER_REPLICAS, node_id, callback=ClusterNodesCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.CLUSTER_SHARDS,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_shards(
        self,
    ) -> List[Dict[AnyStr, Union[List[ValueT], Mapping[AnyStr, ValueT]]]]:
        """
        Get mapping of cluster slots to nodes
        """
        return await self.execute_command(
            CommandName.CLUSTER_SHARDS, callback=ClusterShardsCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_SLAVES,
        version_deprecated="5.0.0",
        deprecation_reason="Use :meth:`cluster_replicas`",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def cluster_slaves(self, node_id: StringT) -> List[ClusterNodeDetail]:
        """
        List replica nodes of the specified master node
        """

        return await self.execute_command(
            CommandName.CLUSTER_SLAVES, node_id, callback=ClusterNodesCallback()
        )

    @redis_command(
        CommandName.CLUSTER_SLOTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
        version_deprecated="7.0.0",
        deprecation_reason="Use :meth:`cluster_shards`",
    )
    async def cluster_slots(
        self,
    ) -> Dict[Tuple[int, int], Tuple[ClusterNode, ...]]:
        """
        Get mapping of Cluster slot to nodes
        """

        return await self.execute_command(
            CommandName.CLUSTER_SLOTS, callback=ClusterSlotsCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.READONLY,
        group=CommandGroup.CLUSTER,
    )
    async def readonly(self) -> bool:
        """
        Enables read queries for a connection to a cluster replica node
        """
        return await self.execute_command(
            CommandName.READONLY, callback=SimpleStringCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.READWRITE,
        group=CommandGroup.CLUSTER,
    )
    async def readwrite(self) -> bool:
        """
        Disables read queries for a connection to a cluster replica node
        """
        return await self.execute_command(
            CommandName.READWRITE, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.AUTH,
        group=CommandGroup.CONNECTION,
        arguments={"username": {"version_introduced": "6.0.0"}},
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.username` and :paramref:`Redis.password` "
                "arguments when initializing the client to ensure that all connections "
                "originating from this client are authenticated before being made available."
            ),
            True,
        ),
    )
    async def auth(self, password: StringT, username: Optional[StringT] = None) -> bool:
        """
        Authenticate to the server
        """
        pieces: CommandArgList = []
        pieces.append(password)

        if username is not None:
            pieces.append(username)

        return await self.execute_command(
            CommandName.AUTH, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.ECHO,
        group=CommandGroup.CONNECTION,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def echo(self, message: StringT) -> AnyStr:
        "Echo the string back from the server"

        return await self.execute_command(
            CommandName.ECHO, message, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.HELLO,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
    )
    async def hello(
        self,
        protover: Optional[int] = None,
        username: Optional[StringT] = None,
        password: Optional[StringT] = None,
        setname: Optional[StringT] = None,
    ) -> Dict[AnyStr, AnyStr]:
        """
        Handshake with Redis

        :return: a mapping of server properties.
        """
        pieces: CommandArgList = []

        if protover is not None:
            pieces.append(protover)

        if password:
            pieces.append("AUTH")
            pieces.append(username or "default")
            pieces.append(password)

        if setname is not None:
            pieces.append(PrefixToken.SETNAME)
            pieces.append(setname)

        return await self.execute_command(
            CommandName.HELLO,
            *pieces,
            callback=DictCallback[AnyStr, AnyStr](),
        )

    @redis_command(
        CommandName.PING,
        group=CommandGroup.CONNECTION,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def ping(self, message: Optional[StringT] = None) -> AnyStr:
        """
        Ping the server

        :return: ``PONG``, when no argument is provided else the
         :paramref:`message` provided
        """
        pieces: CommandArgList = []

        if message:
            pieces.append(message)

        return await self.execute_command(
            CommandName.PING, *pieces, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SELECT,
        group=CommandGroup.CONNECTION,
    )
    async def select(self, index: int) -> bool:
        """
        Change the selected database for the current connection
        """

        return await self.execute_command(
            CommandName.SELECT, index, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.QUIT,
        group=CommandGroup.CONNECTION,
    )
    async def quit(self) -> bool:
        """
        Close the connection
        """

        return await self.execute_command(
            CommandName.QUIT, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.RESET, version_introduced="6.2.0", group=CommandGroup.CONNECTION
    )
    async def reset(self) -> None:
        """
        Reset the connection
        """
        await self.execute_command(CommandName.RESET, callback=NoopCallback[AnyStr]())

    @redis_command(
        CommandName.GEOADD,
        group=CommandGroup.GEO,
        arguments={
            "condition": {"version_introduced": "6.2.0"},
            "change": {"version_introduced": "6.2.0"},
        },
    )
    async def geoadd(
        self,
        key: KeyT,
        longitude_latitude_members: Parameters[
            Tuple[Union[int, float], Union[int, float], ValueT]
        ],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
        change: Optional[bool] = None,
    ) -> int:
        """
        Add one or more geospatial items in the geospatial index represented
        using a sorted set

        :return: Number of elements added. If ``change`` is ``True`` the return
         is the number of elements that were changed.

        """
        pieces: CommandArgList = [key]

        if condition is not None:
            pieces.append(condition)

        if change is not None:
            pieces.append(PureToken.CHANGE)

        pieces.extend(tuples_to_flat_list(longitude_latitude_members))

        return await self.execute_command(
            CommandName.GEOADD, *pieces, callback=IntCallback()
        )

    @redis_command(
        CommandName.GEODIST,
        readonly=True,
        group=CommandGroup.GEO,
    )
    async def geodist(
        self,
        key: KeyT,
        member1: StringT,
        member2: StringT,
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
    ) -> Optional[float]:
        """
        Returns the distance between two members of a geospatial index

        :return: Distance in the unit specified by :paramref:`unit`
        """
        pieces: CommandArgList = [key, member1, member2]

        if unit:
            pieces.append(unit.lower())

        return await self.execute_command(
            CommandName.GEODIST, *pieces, callback=OptionalFloatCallback()
        )

    @redis_command(
        CommandName.GEOHASH,
        readonly=True,
        group=CommandGroup.GEO,
    )
    async def geohash(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> Tuple[AnyStr, ...]:
        """
        Returns members of a geospatial index as standard geohash strings
        """

        return await self.execute_command(
            CommandName.GEOHASH, key, *members, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.GEOPOS,
        readonly=True,
        group=CommandGroup.GEO,
    )
    async def geopos(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> Tuple[Optional[GeoCoordinates], ...]:
        """
        Returns longitude and latitude of members of a geospatial index

        :return: pairs of longitude/latitudes. Missing members are represented
         by ``None`` entries.
        """

        return await self.execute_command(
            CommandName.GEOPOS, key, *members, callback=GeoCoordinatessCallback()
        )

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
    ) -> Tuple[AnyStr, ...]:
        ...

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Literal[True],
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
    ) -> Tuple[GeoSearchResult, ...]:
        ...

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Optional[bool] = ...,
        withdist: Literal[True],
        withhash: Optional[bool] = ...,
    ) -> Tuple[GeoSearchResult, ...]:
        ...

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Literal[True],
    ) -> Tuple[GeoSearchResult, ...]:
        ...

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        store: KeyT,
    ) -> int:
        ...

    @overload
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Optional[bool] = ...,
        withdist: Optional[bool] = ...,
        withhash: Optional[bool] = ...,
        storedist: KeyT,
    ) -> int:
        ...

    @redis_command(
        CommandName.GEORADIUS,
        version_deprecated="6.2.0",
        deprecation_reason="""
        Use :meth:`geosearch` and :meth:`geosearchstore` with the radius argument
        """,
        group=CommandGroup.GEO,
        arguments={"any_": {"version_introduced": "6.2.0"}},
    )
    @mutually_exclusive_parameters("store", "storedist")
    @mutually_exclusive_parameters("store", ("withdist", "withhash", "withcoord"))
    @mutually_exclusive_parameters("storedist", ("withdist", "withhash", "withcoord"))
    @mutually_inclusive_parameters("any_", leaders=("count",))
    async def georadius(
        self,
        key: KeyT,
        longitude: Union[int, float],
        latitude: Union[int, float],
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Optional[bool] = None,
        withdist: Optional[bool] = None,
        withhash: Optional[bool] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        store: Optional[KeyT] = None,
        storedist: Optional[KeyT] = None,
    ) -> Union[int, Tuple[Union[AnyStr, GeoSearchResult], ...]]:
        """
        Query a geospatial index to fetch members within the borders of the area
        specified with center location at :paramref:`longitude` and :paramref:`latitude`
        and the maximum distance from the center (:paramref:`radius`).


        :return:

         - If no ``with{coord,dist,hash}`` options are provided the return
           is simply the names of places matched (optionally ordered if `order` is provided).
         - If any of the ``with{coord,dist,hash}`` options are set each result entry contains
           `(name, distance, geohash, coordinate pair)``
         - If a key for ``store`` or ``storedist`` is provided, the return is the count of places
           stored.
        """

        return await self._georadiusgeneric(
            CommandName.GEORADIUS,
            key,
            longitude,
            latitude,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            order=order,
            store=store,
            storedist=storedist,
            any_=any_,
        )

    @redis_command(
        CommandName.GEORADIUSBYMEMBER,
        version_deprecated="6.2.0",
        deprecation_reason="""
        Use :meth:`geosearch` and :meth:`geosearchstore` with the radius and member arguments
        """,
        group=CommandGroup.GEO,
    )
    @mutually_exclusive_parameters("store", "storedist")
    @mutually_exclusive_parameters("store", ("withdist", "withhash", "withcoord"))
    @mutually_exclusive_parameters("storedist", ("withdist", "withhash", "withcoord"))
    @mutually_inclusive_parameters("any_", leaders=("count",))
    async def georadiusbymember(
        self,
        key: KeyT,
        member: ValueT,
        radius: Union[int, float],
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        withcoord: Optional[bool] = None,
        withdist: Optional[bool] = None,
        withhash: Optional[bool] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        store: Optional[KeyT] = None,
        storedist: Optional[KeyT] = None,
    ) -> Union[int, Tuple[Union[AnyStr, GeoSearchResult], ...]]:
        """
        This command is exactly like :meth:`~Redis.georadius` with the sole difference
        that instead of searching from a coordinate, it searches from a member
        already existing in the index.

        :return:

         - If no ``with{coord,dist,hash}`` options are provided the return
           is simply the names of places matched (optionally ordered if `order` is provided).
         - If any of the ``with{coord,dist,hash}`` options are set each result entry contains
           `(name, distance, geohash, coordinate pair)``
         - If a key for ``store`` or ``storedist`` is provided, the return is the count of places
           stored.
        """

        return await self._georadiusgeneric(
            CommandName.GEORADIUSBYMEMBER,
            key,
            member,
            radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
            withhash=withhash,
            count=count,
            order=order,
            store=store,
            storedist=storedist,
            any_=any_,
        )

    async def _georadiusgeneric(
        self,
        command: Literal[
            CommandName.GEORADIUS,
            CommandName.GEORADIUSBYMEMBER,
        ],
        *args: ValueT,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: Optional[bool] = None,
        withdist: Optional[bool] = None,
        withhash: Optional[bool] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        store: Optional[KeyT] = None,
        storedist: Optional[KeyT] = None,
    ) -> Union[int, Tuple[Union[AnyStr, GeoSearchResult], ...]]:
        pieces: CommandArgList = list(args)
        options: Dict[str, ValueT] = {}
        if unit:
            pieces.append(unit.lower())

        if withdist:
            pieces.append(PureToken.WITHDIST)
            options["withdist"] = withdist
        if withcoord:
            pieces.append(PureToken.WITHCOORD)
            options["withcoord"] = withcoord
        if withhash:
            pieces.append(PureToken.WITHHASH)
            options["withhash"] = withhash

        if count is not None:
            pieces.extend(["COUNT", count])
            options["count"] = count

            if any_:
                pieces.append(PureToken.ANY)
                options["any_"] = any_

        if order:
            pieces.append(order)
            options["order"] = order

        if store:
            pieces.extend([PrefixToken.STORE, store])
            options["store"] = store

        if storedist:
            pieces.extend([PrefixToken.STOREDIST, storedist])
            options["storedist"] = storedist

        return await self.execute_command(
            command, *pieces, **options, callback=GeoSearchCallback[AnyStr]()
        )

    @mutually_inclusive_parameters("longitude", "latitude")
    @mutually_inclusive_parameters("radius", "circle_unit")
    @mutually_inclusive_parameters("width", "height", "box_unit")
    @mutually_inclusive_parameters("any_", leaders=("count",))
    @mutually_exclusive_parameters("member", ("longitude", "latitude"))
    @redis_command(
        CommandName.GEOSEARCH,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.GEO,
    )
    async def geosearch(
        self,
        key: KeyT,
        member: Optional[ValueT] = None,
        longitude: Optional[Union[int, float]] = None,
        latitude: Optional[Union[int, float]] = None,
        radius: Optional[Union[int, float]] = None,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
        width: Optional[Union[int, float]] = None,
        height: Optional[Union[int, float]] = None,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        withcoord: Optional[bool] = None,
        withdist: Optional[bool] = None,
        withhash: Optional[bool] = None,
    ) -> Union[int, Tuple[Union[AnyStr, GeoSearchResult], ...]]:
        """

        :return:

         - If no ``with{coord,dist,hash}`` options are provided the return
           is simply the names of places matched (optionally ordered if `order` is provided).
         - If any of the ``with{coord,dist,hash}`` options are set each result entry contains
           `(name, distance, geohash, coordinate pair)``
        """

        return await self._geosearchgeneric(
            CommandName.GEOSEARCH,
            key,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=circle_unit or box_unit,
            radius=radius,
            width=width,
            height=height,
            order=order,
            count=count,
            any_=any_,
            withcoord=withcoord,
            withdist=withdist,
            withhash=withhash,
            store=None,
            storedist=None,
        )

    @mutually_inclusive_parameters("longitude", "latitude")
    @mutually_inclusive_parameters("radius", "circle_unit")
    @mutually_inclusive_parameters("width", "height", "box_unit")
    @mutually_inclusive_parameters("any_", leaders=("count",))
    @mutually_exclusive_parameters("member", ("longitude", "latitude"))
    @redis_command(
        CommandName.GEOSEARCHSTORE, version_introduced="6.2.0", group=CommandGroup.GEO
    )
    async def geosearchstore(
        self,
        destination: KeyT,
        source: KeyT,
        member: Optional[ValueT] = None,
        longitude: Optional[Union[int, float]] = None,
        latitude: Optional[Union[int, float]] = None,
        radius: Optional[Union[int, float]] = None,
        circle_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
        width: Optional[Union[int, float]] = None,
        height: Optional[Union[int, float]] = None,
        box_unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        storedist: Optional[bool] = None,
    ) -> int:
        """
        :return: The number of elements stored in the resulting set
        """

        return await self._geosearchgeneric(
            CommandName.GEOSEARCHSTORE,
            destination,
            source,
            member=member,
            longitude=longitude,
            latitude=latitude,
            unit=circle_unit or box_unit,
            radius=radius,
            width=width,
            height=height,
            count=count,
            order=order,
            any_=any_,
            withcoord=None,
            withdist=None,
            withhash=None,
            store=None,
            storedist=storedist,
        )

    @overload
    async def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCH],
        *args: ValueT,
        member: Optional[ValueT] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        **kwargs: Optional[ValueT],
    ) -> Tuple[Union[AnyStr, GeoSearchResult], ...]:
        ...

    @overload
    async def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCHSTORE],
        *args: ValueT,
        member: Optional[ValueT] = ...,
        longitude: Optional[Union[int, float]] = ...,
        latitude: Optional[Union[int, float]] = ...,
        radius: Optional[Union[int, float]] = ...,
        width: Optional[Union[int, float]] = ...,
        height: Optional[Union[int, float]] = ...,
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = ...,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = ...,
        count: Optional[int] = ...,
        any_: Optional[bool] = ...,
        **kwargs: Optional[ValueT],
    ) -> int:
        ...

    async def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCH, CommandName.GEOSEARCHSTORE],
        *args: ValueT,
        member: Optional[ValueT] = None,
        longitude: Optional[Union[int, float]] = None,
        latitude: Optional[Union[int, float]] = None,
        radius: Optional[Union[int, float]] = None,
        width: Optional[Union[int, float]] = None,
        height: Optional[Union[int, float]] = None,
        unit: Optional[
            Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]
        ] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        count: Optional[int] = None,
        any_: Optional[bool] = None,
        **kwargs: Optional[ValueT],
    ) -> Union[int, Tuple[Union[AnyStr, GeoSearchResult], ...]]:
        pieces: CommandArgList = list(args)

        if member:
            pieces.extend([PrefixToken.FROMMEMBER, member])

        if longitude is not None and latitude is not None:
            pieces.extend([PrefixToken.FROMLONLAT, longitude, latitude])

        # BYRADIUS or BYBOX
        if unit is None:
            raise DataError("GEOSEARCH must have unit")

        if radius is not None:
            pieces.extend([PrefixToken.BYRADIUS, radius, unit.lower()])

        if width is not None and height is not None:
            pieces.extend([PrefixToken.BYBOX, width, height, unit.lower()])

        # sort
        if order:
            pieces.append(order)

        # count any
        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

            if any_:
                pieces.append(PureToken.ANY)

        # other properties

        for arg_name, byte_repr in (
            ("withdist", PureToken.WITHDIST),
            ("withcoord", PureToken.WITHCOORD),
            ("withhash", PureToken.WITHHASH),
            ("storedist", PureToken.STOREDIST),
        ):
            if kwargs[arg_name]:
                pieces.append(byte_repr)

        if command == CommandName.GEOSEARCHSTORE:
            return await self.execute_command(
                command, *pieces, **kwargs, callback=IntCallback()
            )
        else:
            return await self.execute_command(
                command, *pieces, **kwargs, callback=GeoSearchCallback[AnyStr]()
            )

    @redis_command(CommandName.HDEL, group=CommandGroup.HASH)
    async def hdel(self, key: KeyT, fields: Parameters[StringT]) -> int:
        """Deletes ``fields`` from hash :paramref:`key`"""

        return await self.execute_command(
            CommandName.HDEL, key, *fields, callback=IntCallback()
        )

    @redis_command(
        CommandName.HEXISTS,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hexists(self, key: KeyT, field: StringT) -> bool:
        """
        Returns a boolean indicating if ``field`` exists within hash :paramref:`key`
        """

        return await self.execute_command(
            CommandName.HEXISTS, key, field, callback=BoolCallback()
        )

    @redis_command(
        CommandName.HGET,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hget(self, key: KeyT, field: StringT) -> Optional[AnyStr]:
        """Returns the value of ``field`` within the hash :paramref:`key`"""

        return await self.execute_command(
            CommandName.HGET, key, field, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.HGETALL,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hgetall(self, key: KeyT) -> Dict[AnyStr, AnyStr]:
        """Returns a Python dict of the hash's name/value pairs"""

        return await self.execute_command(
            CommandName.HGETALL, key, callback=HGetAllCallback[AnyStr]()
        )

    @redis_command(CommandName.HINCRBY, group=CommandGroup.HASH)
    async def hincrby(self, key: KeyT, field: StringT, increment: int) -> int:
        """Increments the value of ``field`` in hash :paramref:`key` by ``increment``"""

        return await self.execute_command(
            CommandName.HINCRBY, key, field, increment, callback=IntCallback()
        )

    @redis_command(
        CommandName.HINCRBYFLOAT,
        group=CommandGroup.HASH,
    )
    async def hincrbyfloat(
        self, key: KeyT, field: StringT, increment: Union[int, float]
    ) -> float:
        """
        Increments the value of ``field`` in hash :paramref:`key` by floating
        ``increment``
        """

        return await self.execute_command(
            CommandName.HINCRBYFLOAT, key, field, increment, callback=FloatCallback()
        )

    @redis_command(
        CommandName.HKEYS,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hkeys(self, key: KeyT) -> Tuple[AnyStr, ...]:
        """Returns the list of keys within hash :paramref:`key`"""

        return await self.execute_command(
            CommandName.HKEYS, key, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.HLEN,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hlen(self, key: KeyT) -> int:
        """Returns the number of elements in hash :paramref:`key`"""

        return await self.execute_command(CommandName.HLEN, key, callback=IntCallback())

    @redis_command(CommandName.HSET, group=CommandGroup.HASH)
    async def hset(self, key: KeyT, field_values: Dict[StringT, ValueT]) -> int:
        """
        Sets ``field`` to ``value`` within hash :paramref:`key`

        :return: number of fields that were added
        """

        return await self.execute_command(
            CommandName.HSET,
            key,
            *dict_to_flat_list(field_values),
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.HSETNX,
        group=CommandGroup.HASH,
    )
    async def hsetnx(self, key: KeyT, field: StringT, value: ValueT) -> bool:
        """
        Sets ``field`` to ``value`` within hash :paramref:`key` if ``field`` does not
        exist.

        :return: whether the field was created
        """

        return await self.execute_command(
            CommandName.HSETNX, key, field, value, callback=BoolCallback()
        )

    @redis_command(
        CommandName.HMSET,
        group=CommandGroup.HASH,
        version_deprecated="4.0.0",
        deprecation_reason="Use :meth:`hset` with multiple field-value pairs",
    )
    async def hmset(self, key: KeyT, field_values: Dict[StringT, ValueT]) -> bool:
        """
        Sets key to value within hash :paramref:`key` for each corresponding
        key and value from the ``field_items`` dict.
        """

        pieces: CommandArgList = []

        for pair in field_values.items():
            pieces.extend(pair)

        return await self.execute_command(
            CommandName.HMSET, key, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.HMGET,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hmget(
        self, key: KeyT, fields: Parameters[StringT]
    ) -> Tuple[Optional[AnyStr], ...]:
        """Returns values ordered identically to ``fields``"""

        return await self.execute_command(
            CommandName.HMGET, key, *fields, callback=TupleCallback[Optional[AnyStr]]()
        )

    @redis_command(
        CommandName.HVALS,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hvals(self, key: KeyT) -> Tuple[AnyStr, ...]:
        """
        Get all the values in a hash

        :return: list of values in the hash, or an empty list when :paramref:`key` does not exist.
        """

        return await self.execute_command(
            CommandName.HVALS, key, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.HSCAN,
        readonly=True,
        group=CommandGroup.HASH,
    )
    async def hscan(
        self,
        key: KeyT,
        cursor: Optional[int] = 0,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> Tuple[int, Dict[AnyStr, AnyStr]]:

        """
        Incrementallys return key/value slices in a hash. Also returns a
        cursor pointing to the scan position.

        :param match: allows for filtering the keys by pattern
        :param count: allows for hint the minimum number of returns
        """
        pieces: CommandArgList = [key, cursor or "0"]

        if match is not None:
            pieces.extend([PrefixToken.MATCH, match])

        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

        return await self.execute_command(
            CommandName.HSCAN, *pieces, callback=HScanCallback[AnyStr]()
        )

    @redis_command(
        CommandName.HSTRLEN,
        readonly=True,
        group=CommandGroup.HASH,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def hstrlen(self, key: KeyT, field: StringT) -> int:
        """
        Get the length of the value of a hash field

        :return: the string length of the value associated with ``field``,
         or zero when ``field`` is not present in the hash or :paramref:`key` does not exist at all.
        """

        return await self.execute_command(
            CommandName.HSTRLEN, key, field, callback=IntCallback()
        )

    @overload
    async def hrandfield(
        self,
        key: KeyT,
        *,
        withvalues: Literal[True],
        count: int = ...,
    ) -> Dict[AnyStr, AnyStr]:
        ...

    @overload
    async def hrandfield(
        self,
        key: KeyT,
        *,
        count: int = ...,
    ) -> Tuple[AnyStr, ...]:
        ...

    @redis_command(
        CommandName.HRANDFIELD,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.HASH,
    )
    async def hrandfield(
        self,
        key: KeyT,
        *,
        count: Optional[int] = None,
        withvalues: Optional[bool] = None,
    ) -> Optional[Union[AnyStr, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]]:
        """
        Return a random field from the hash value stored at key.

        :return:  Without the additional :paramref:`count` argument, the command returns a randomly
         selected field, or ``None`` when :paramref:`key` does not exist.
         When the additional :paramref:`count` argument is passed, the command returns fields,
         or an empty tuple when :paramref:`key` does not exist.

         If ``withvalues``  is ``True``, the reply is a mapping of fields and
         their values from the hash.

        """
        params: CommandArgList = []
        options = {"withvalues": withvalues, "count": count}

        if count is not None:
            params.append(count)

        if withvalues:
            params.append(PureToken.WITHVALUES)
            options["withvalues"] = True

        return await self.execute_command(
            CommandName.HRANDFIELD,
            key,
            *params,
            **options,
            callback=HRandFieldCallback[AnyStr](),
        )

    @redis_command(
        CommandName.PFADD,
        group=CommandGroup.HYPERLOGLOG,
    )
    async def pfadd(self, key: KeyT, *elements: ValueT) -> bool:

        """
        Adds the specified elements to the specified HyperLogLog.

        :return: Whether atleast 1 HyperLogLog internal register was altered
        """
        pieces: CommandArgList = [key]

        if elements:
            pieces.extend(elements)

        return await self.execute_command(
            CommandName.PFADD, *pieces, callback=BoolCallback()
        )

    @redis_command(
        CommandName.PFCOUNT,
        readonly=True,
        group=CommandGroup.HYPERLOGLOG,
    )
    async def pfcount(self, keys: Parameters[KeyT]) -> int:
        """
        Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).

        :return: The approximated number of unique elements observed via :meth:`pfadd`.
        """

        return await self.execute_command(
            CommandName.PFCOUNT, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.PFMERGE,
        group=CommandGroup.HYPERLOGLOG,
    )
    async def pfmerge(self, destkey: KeyT, sourcekeys: Parameters[KeyT]) -> bool:
        """
        Merge N different HyperLogLogs into a single one
        """

        return await self.execute_command(
            CommandName.PFMERGE, destkey, *sourcekeys, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COPY,
        version_introduced="6.2.0",
        group=CommandGroup.GENERIC,
    )
    async def copy(
        self,
        source: KeyT,
        destination: KeyT,
        db: Optional[int] = None,
        replace: Optional[bool] = None,
    ) -> bool:
        """
        Copy a key
        """
        pieces: CommandArgList = []

        if db is not None:
            pieces.extend([PrefixToken.DB, db])

        if replace:
            pieces.append(PureToken.REPLACE)

        return await self.execute_command(
            CommandName.COPY, source, destination, *pieces, callback=BoolCallback()
        )

    @redis_command(
        CommandName.DEL,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
    )
    async def delete(self, keys: Parameters[KeyT]) -> int:
        """
        Delete one or more keys specified by ``keys``

        :return: The number of keys that were removed.
        """

        return await self.execute_command(
            CommandName.DEL, *keys, callback=IntCallback()
        )

    @redis_command(CommandName.DUMP, readonly=True, group=CommandGroup.GENERIC)
    async def dump(self, key: KeyT) -> bytes:
        """
        Return a serialized version of the value stored at the specified key.

        :return: the serialized value
        """

        return await self.execute_command(
            CommandName.DUMP, key, decode=False, callback=NoopCallback[bytes]()
        )

    @redis_command(
        CommandName.EXISTS,
        readonly=True,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
    )
    async def exists(self, keys: Parameters[KeyT]) -> int:
        """
        Determine if a key exists

        :return: the number of keys that exist from those specified as arguments.
        """

        return await self.execute_command(
            CommandName.EXISTS, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.EXPIRE,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
    )
    async def expire(
        self,
        key: KeyT,
        seconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = None,
    ) -> bool:
        """
        Set a key's time to live in seconds



        :return: if the timeout was set or not set.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        pieces: CommandArgList = [key, normalized_seconds(seconds)]

        if condition is not None:
            pieces.append(condition)

        return await self.execute_command(
            CommandName.EXPIRE, *pieces, callback=BoolCallback()
        )

    @redis_command(
        CommandName.EXPIREAT,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
    )
    async def expireat(
        self,
        key: KeyT,
        unix_time_seconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = None,
    ) -> bool:
        """
        Set the expiration for a key to a specific time


        :return: if the timeout was set or no.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.

        """

        pieces: CommandArgList = [key, normalized_time_seconds(unix_time_seconds)]

        if condition is not None:
            pieces.append(condition)

        return await self.execute_command(
            CommandName.EXPIREAT, *pieces, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EXPIRETIME,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        readonly=True,
    )
    async def expiretime(self, key: Union[str, bytes]) -> datetime.datetime:
        """
        Get the expiration Unix timestamp for a key

        :return: Expiration Unix timestamp in seconds, or a negative value in
         order to signal an error.

         * The command returns ``-1`` if the key exists but has no associated expiration time.
         * The command returns ``-2`` if the key does not exist.

        """

        return await self.execute_command(
            CommandName.EXPIRETIME, key, callback=ExpiryCallback()
        )

    @redis_command(
        CommandName.KEYS,
        readonly=True,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets(),
        ),
    )
    async def keys(self, pattern: StringT = "*") -> Set[AnyStr]:
        """
        Find all keys matching the given pattern

        :return: keys matching ``pattern``.
        """

        return await self.execute_command(
            CommandName.KEYS, pattern, callback=SetCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @mutually_inclusive_parameters("username", "password")
    @redis_command(CommandName.MIGRATE, group=CommandGroup.GENERIC)
    async def migrate(
        self,
        host: StringT,
        port: int,
        destination_db: int,
        timeout: int,
        *keys: KeyT,
        copy: Optional[bool] = None,
        replace: Optional[bool] = None,
        auth: Optional[StringT] = None,
        username: Optional[StringT] = None,
        password: Optional[StringT] = None,
    ) -> bool:
        """
        Atomically transfer key(s) from a Redis instance to another one.


        :return: If all keys were found in the source instance.
        """

        if not keys:
            raise DataError("MIGRATE requires at least one key")
        pieces: CommandArgList = []

        if copy:
            pieces.append(PureToken.COPY)

        if replace:
            pieces.append(PureToken.REPLACE)

        if auth:
            pieces.append("AUTH")
            pieces.append(auth)

        if username and password:
            pieces.append("AUTH2")
            pieces.append(username)
            pieces.append(password)

        pieces.append("KEYS")
        pieces.extend(keys)

        return await self.execute_command(
            CommandName.MIGRATE,
            host,
            port,
            b"",
            destination_db,
            timeout,
            *pieces,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.MOVE,
        group=CommandGroup.GENERIC,
    )
    async def move(self, key: KeyT, db: int) -> bool:
        """Move a key to another database"""

        return await self.execute_command(
            CommandName.MOVE, key, db, callback=BoolCallback()
        )

    @redis_command(
        CommandName.OBJECT_ENCODING, readonly=True, group=CommandGroup.GENERIC
    )
    async def object_encoding(self, key: KeyT) -> Optional[AnyStr]:
        """
        Return the internal encoding for the object stored at :paramref:`key`

        :return: the encoding of the object, or ``None`` if the key doesn't exist
        """

        return await self.execute_command(
            CommandName.OBJECT_ENCODING, key, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.OBJECT_FREQ, readonly=True, group=CommandGroup.GENERIC)
    async def object_freq(self, key: KeyT) -> int:
        """
        Return the logarithmic access frequency counter for the object
        stored at :paramref:`key`

        :return: The counter's value.
        """

        return await self.execute_command(
            CommandName.OBJECT_FREQ, key, callback=IntCallback()
        )

    @redis_command(
        CommandName.OBJECT_IDLETIME, readonly=True, group=CommandGroup.GENERIC
    )
    async def object_idletime(self, key: KeyT) -> int:
        """
        Return the time in seconds since the last access to the object
        stored at :paramref:`key`

        :return: The idle time in seconds.
        """

        return await self.execute_command(
            CommandName.OBJECT_IDLETIME, key, callback=IntCallback()
        )

    @redis_command(
        CommandName.OBJECT_REFCOUNT, readonly=True, group=CommandGroup.GENERIC
    )
    async def object_refcount(self, key: KeyT) -> int:
        """
        Return the reference count of the object stored at :paramref:`key`

        :return: The number of references.
        """

        return await self.execute_command(
            CommandName.OBJECT_REFCOUNT, key, callback=IntCallback()
        )

    @redis_command(
        CommandName.PERSIST,
        group=CommandGroup.GENERIC,
    )
    async def persist(self, key: KeyT) -> bool:
        """Removes an expiration on :paramref:`key`"""

        return await self.execute_command(
            CommandName.PERSIST, key, callback=BoolCallback()
        )

    @redis_command(
        CommandName.PEXPIRE,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
    )
    async def pexpire(
        self,
        key: KeyT,
        milliseconds: Union[int, datetime.timedelta],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = None,
    ) -> bool:
        """
        Set a key's time to live in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """
        pieces: CommandArgList = [key, normalized_milliseconds(milliseconds)]

        if condition is not None:
            pieces.append(condition)

        return await self.execute_command(
            CommandName.PEXPIRE, *pieces, callback=BoolCallback()
        )

    @redis_command(
        CommandName.PEXPIREAT,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
    )
    async def pexpireat(
        self,
        key: KeyT,
        unix_time_milliseconds: Union[int, datetime.datetime],
        condition: Optional[
            Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT]
        ] = None,
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        pieces: CommandArgList = [
            key,
            normalized_time_milliseconds(unix_time_milliseconds),
        ]

        if condition is not None:
            pieces.append(condition)

        return await self.execute_command(
            CommandName.PEXPIREAT, *pieces, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.PEXPIRETIME,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        readonly=True,
    )
    async def pexpiretime(self, key: Union[str, bytes]) -> datetime.datetime:
        """
        Get the expiration Unix timestamp for a key in milliseconds

        :return: Expiration Unix timestamp in milliseconds, or a negative value
         in order to signal an error

         * The command returns ``-1`` if the key exists but has no associated expiration time.
         * The command returns ``-2`` if the key does not exist.

        """

        return await self.execute_command(
            CommandName.PEXPIRETIME, key, unit="milliseconds", callback=ExpiryCallback()
        )

    @redis_command(CommandName.PTTL, readonly=True, group=CommandGroup.GENERIC)
    async def pttl(self, key: KeyT) -> int:
        """
        Returns the number of milliseconds until the key :paramref:`key` will expire

        :return: TTL in milliseconds, or a negative value in order to signal an error
        """

        return await self.execute_command(CommandName.PTTL, key, callback=IntCallback())

    @redis_command(
        CommandName.RANDOMKEY,
        readonly=True,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def randomkey(self) -> Optional[AnyStr]:
        """
        Returns the name of a random key

        :return: the random key, or ``None`` when the database is empty.
        """

        return await self.execute_command(
            CommandName.RANDOMKEY, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.RENAME,
        group=CommandGroup.GENERIC,
    )
    async def rename(self, key: KeyT, newkey: KeyT) -> bool:
        """
        Rekeys key :paramref:`key` to ``newkey``
        """

        return await self.execute_command(
            CommandName.RENAME, key, newkey, callback=BoolCallback()
        )

    @redis_command(
        CommandName.RENAMENX,
        group=CommandGroup.GENERIC,
    )
    async def renamenx(self, key: KeyT, newkey: KeyT) -> bool:
        """
        Rekeys key :paramref:`key` to ``newkey`` if ``newkey`` doesn't already exist

        :return: False when ``newkey`` already exists.
        """

        return await self.execute_command(
            CommandName.RENAMENX, key, newkey, callback=BoolCallback()
        )

    @redis_command(
        CommandName.RESTORE,
        group=CommandGroup.GENERIC,
    )
    async def restore(
        self,
        key: KeyT,
        ttl: Union[int, datetime.timedelta, datetime.datetime],
        serialized_value: bytes,
        replace: Optional[bool] = None,
        absttl: Optional[bool] = None,
        idletime: Optional[Union[int, datetime.timedelta]] = None,
        freq: Optional[int] = None,
    ) -> bool:
        """
        Create a key using the provided serialized value, previously obtained using DUMP.
        """
        params: CommandArgList = [
            key,
            normalized_milliseconds(ttl)  # type: ignore
            if not absttl
            else normalized_time_milliseconds(ttl),  # type: ignore
            serialized_value,
        ]

        if replace:
            params.append(PureToken.REPLACE)

        if absttl:
            params.append(PureToken.ABSTTL)

        if idletime is not None:
            params.extend(["IDLETIME", normalized_milliseconds(idletime)])

        if freq:
            params.extend(["FREQ", freq])

        return await self.execute_command(
            CommandName.RESTORE, *params, callback=SimpleStringCallback()
        )

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.SORT,
        group=CommandGroup.GENERIC,
    )
    async def sort(
        self,
        key: KeyT,
        gets: Optional[Parameters[KeyT]] = None,
        by: Optional[StringT] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        alpha: Optional[bool] = None,
        store: Optional[KeyT] = None,
    ) -> Union[Tuple[AnyStr, ...], int]:

        """
        Sort the elements in a list, set or sorted set

        :return: sorted elements.

         When the :paramref:`store` option is specified the command returns the number of
         sorted elements in the destination list.
        """

        pieces: CommandArgList = [key]
        options = {}

        if by is not None:
            pieces.append(PrefixToken.BY)
            pieces.append(by)

        if offset is not None and count is not None:
            pieces.append(PrefixToken.LIMIT)
            pieces.append(offset)
            pieces.append(count)

        for g in gets or []:
            pieces.append(PrefixToken.GET)
            pieces.append(g)

        if order:
            pieces.append(order)

        if alpha is not None:
            pieces.append(PureToken.SORTING)

        if store is not None:
            pieces.append(PrefixToken.STORE)
            pieces.append(store)
            options["store"] = True

        return await self.execute_command(
            CommandName.SORT, *pieces, **options, callback=SortCallback[AnyStr]()
        )

    @mutually_inclusive_parameters("offset", "count")
    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SORT_RO,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        readonly=True,
    )
    async def sort_ro(
        self,
        key: KeyT,
        gets: Optional[Parameters[KeyT]] = None,
        by: Optional[StringT] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        order: Optional[Literal[PureToken.ASC, PureToken.DESC]] = None,
        alpha: Optional[bool] = None,
    ) -> Tuple[AnyStr, ...]:
        """
        Sort the elements in a list, set or sorted set. Read-only variant of SORT.


        :return: sorted elements.

        """
        pieces: CommandArgList = [key]

        if by is not None:
            pieces.extend([PrefixToken.BY, by])

        if offset is not None and count is not None:
            pieces.extend([PrefixToken.LIMIT, offset, count])

        for g in gets or []:
            pieces.extend([PrefixToken.GET, g])

        if order:
            pieces.append(order)

        if alpha is not None:
            pieces.append(PureToken.SORTING)

        return await self.execute_command(
            CommandName.SORT_RO, *pieces, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.TOUCH,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
    )
    async def touch(self, keys: Parameters[KeyT]) -> int:
        """
        Alters the last access time of a key(s).
        Returns the number of existing keys specified.

        :return: The number of keys that were touched.
        """

        return await self.execute_command(
            CommandName.TOUCH, *keys, callback=IntCallback()
        )

    @redis_command(CommandName.TTL, readonly=True, group=CommandGroup.GENERIC)
    async def ttl(self, key: KeyT) -> int:
        """
        Get the time to live for a key in seconds

        :return: TTL in seconds, or a negative value in order to signal an error
        """

        return await self.execute_command(CommandName.TTL, key, callback=IntCallback())

    @redis_command(
        CommandName.TYPE,
        readonly=True,
        group=CommandGroup.GENERIC,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def type(self, key: KeyT) -> Optional[AnyStr]:
        """
        Determine the type stored at key

        :return: type of :paramref:`key`, or ``None`` when :paramref:`key` does not exist.
        """

        return await self.execute_command(
            CommandName.TYPE, key, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.UNLINK,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
    )
    async def unlink(self, keys: Parameters[KeyT]) -> int:
        """
        Delete a key asynchronously in another thread.
        Otherwise it is just as :meth:`delete`, but non blocking.

        :return: The number of keys that were unlinked.
        """

        return await self.execute_command(
            CommandName.UNLINK, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.WAIT,
        group=CommandGroup.GENERIC,
        redirect_usage=RedirectUsage(
            (
                "Use the :meth:`Redis.ensure_replication`  or "
                ":meth:`RedisCluster.ensure_replication` context managers to ensure "
                "a command is replicated to the number of replicas"
            ),
            False,
        ),
    )
    async def wait(self, numreplicas: int, timeout: int) -> int:
        """
        Wait for the synchronous replication of all the write commands sent in the context of
        the current connection

        :raises: :exc:`NotImplementedError`
        """

        raise NotImplementedError()

    @redis_command(
        CommandName.SCAN,
        readonly=True,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(route=NodeFlag.PRIMARIES),
    )
    async def scan(
        self,
        cursor: Optional[int] = 0,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
        type_: Optional[StringT] = None,
    ) -> Tuple[int, Tuple[AnyStr, ...]]:
        """
        Incrementally iterate the keys space
        """
        pieces: CommandArgList = [cursor or b"0"]

        if match is not None:
            pieces.extend([PrefixToken.MATCH, match])

        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

        if type_ is not None:
            pieces.extend([PrefixToken.TYPE, type_])

        return await self.execute_command(
            CommandName.SCAN, *pieces, callback=ScanCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BLMOVE, version_introduced="6.2.0", group=CommandGroup.LIST
    )
    async def blmove(
        self,
        source: KeyT,
        destination: KeyT,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
        timeout: Union[int, float],
    ) -> Optional[AnyStr]:
        """
        Pop an element from a list, push it to another list and return it;
        or block until one is available


        :return: the element being popped from :paramref:`source` and pushed to
         :paramref:`destination`
        """
        params: CommandArgList = [
            source,
            destination,
            wherefrom,
            whereto,
            timeout,
        ]

        return await self.execute_command(
            CommandName.BLMOVE, *params, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.BLMPOP, version_introduced="7.0.0", group=CommandGroup.LIST
    )
    async def blmpop(
        self,
        keys: Parameters[KeyT],
        timeout: Union[int, float],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = None,
    ) -> Optional[List[AnyStr]]:
        """
        Pop elements from the first non empty list, or block until one is available

        :return:

         - A ``None`` when no element could be popped, and timeout is reached.
         - A two-element array with the first element being the name of the key
           from which elements were popped, and the second element is an array of elements.

        """
        _keys: List[KeyT] = list(keys)
        pieces: CommandArgList = [timeout, len(_keys), *_keys, where]

        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

        return await self.execute_command(
            CommandName.BLMPOP, *pieces, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(CommandName.BLPOP, group=CommandGroup.LIST)
    async def blpop(
        self, keys: Parameters[KeyT], timeout: Union[int, float]
    ) -> Optional[List[AnyStr]]:
        """
        Remove and get the first element in a list, or block until one is available

        :return:

         - ``None`` when no element could be popped and the timeout expired.
         - A list with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return await self.execute_command(
            CommandName.BLPOP, *keys, timeout, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(CommandName.BRPOP, group=CommandGroup.LIST)
    async def brpop(
        self, keys: Parameters[KeyT], timeout: Union[int, float]
    ) -> Optional[List[AnyStr]]:
        """
        Remove and get the last element in a list, or block until one is available

        :return:

         - ``None`` when no element could be popped and the timeout expired.
         - A list with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return await self.execute_command(
            CommandName.BRPOP, *keys, timeout, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BRPOPLPUSH,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`blmove` with the `wherefrom` and `whereto` arguments",
        group=CommandGroup.LIST,
    )
    async def brpoplpush(
        self, source: KeyT, destination: KeyT, timeout: Union[int, float]
    ) -> Optional[AnyStr]:
        """
        Pop an element from a list, push it to another list and return it;
        or block until one is available

        :return: the element being popped from :paramref:`source` and pushed to
         :paramref:`destination`.
        """

        return await self.execute_command(
            CommandName.BRPOPLPUSH,
            source,
            destination,
            timeout,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.LINDEX,
        readonly=True,
        group=CommandGroup.LIST,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def lindex(self, key: KeyT, index: int) -> Optional[AnyStr]:
        """

        Get an element from a list by its index

        :return: the requested element, or ``None`` when ``index`` is out of range.
        """

        return await self.execute_command(
            CommandName.LINDEX, key, index, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.LINSERT, group=CommandGroup.LIST)
    async def linsert(
        self,
        key: KeyT,
        where: Literal[PureToken.AFTER, PureToken.BEFORE],
        pivot: ValueT,
        element: ValueT,
    ) -> int:
        """
        Inserts element in the list stored at key either before or after the reference
        value pivot.

        :return: the length of the list after the insert operation, or ``-1`` when
         the value pivot was not found.
        """

        return await self.execute_command(
            CommandName.LINSERT, key, where, pivot, element, callback=IntCallback()
        )

    @redis_command(
        CommandName.LLEN,
        readonly=True,
        group=CommandGroup.LIST,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def llen(self, key: KeyT) -> int:
        """
        :return: the length of the list at :paramref:`key`.
        """

        return await self.execute_command(CommandName.LLEN, key, callback=IntCallback())

    @redis_command(
        CommandName.LMOVE, version_introduced="6.2.0", group=CommandGroup.LIST
    )
    async def lmove(
        self,
        source: KeyT,
        destination: KeyT,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
    ) -> AnyStr:
        """
        Pop an element from a list, push it to another list and return it

        :return: the element being popped and pushed.
        """
        params = [source, destination, wherefrom, whereto]

        return await self.execute_command(
            CommandName.LMOVE, *params, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LMPOP, version_introduced="7.0.0", group=CommandGroup.LIST
    )
    async def lmpop(
        self,
        keys: Parameters[KeyT],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: Optional[int] = None,
    ) -> Optional[List[AnyStr]]:
        """
        Pop elements from the first non empty list

        :return:

         - A ```None``` when no element could be popped.
         - A two-element array with the first element being the name of the key
           from which elements were popped, and the second element is an array of elements.
        """
        _keys: List[KeyT] = list(keys)
        pieces: CommandArgList = [len(_keys), *_keys, where]

        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

        return await self.execute_command(
            CommandName.LMPOP, *pieces, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(
        CommandName.LPOP,
        group=CommandGroup.LIST,
        arguments={"count": {"version_introduced": "6.2.0"}},
    )
    async def lpop(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[AnyStr, List[AnyStr]]]:
        """
        Remove and get the first :paramref:`count` elements in a list

        :return: the value of the first element, or ``None`` when :paramref:`key` does not exist.
         If :paramref:`count` is provided the return is a list of popped elements,
         or ``None`` when :paramref:`key` does not exist.
        """
        pieces: CommandArgList = []

        if count is not None:
            pieces.append(count)

        if count is not None:
            return await self.execute_command(
                CommandName.LPOP, key, *pieces, callback=OptionalListCallback[AnyStr]()
            )
        return await self.execute_command(
            CommandName.LPOP, key, *pieces, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.LPOS,
        version_introduced="6.0.6",
        group=CommandGroup.LIST,
        readonly=True,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def lpos(
        self,
        key: KeyT,
        element: ValueT,
        rank: Optional[int] = None,
        count: Optional[int] = None,
        maxlen: Optional[int] = None,
    ) -> Optional[Union[int, List[int]]]:
        """

        Return the index of matching elements on a list


        :return: The command returns the integer representing the matching element,
         or ``None`` if there is no match.

         If the :paramref:`count` argument is given a list of integers representing
         the matching elements.
        """
        pieces: CommandArgList = [key, element]

        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])

        if rank is not None:
            pieces.extend([PrefixToken.RANK, rank])

        if maxlen is not None:
            pieces.extend([PrefixToken.MAXLEN, maxlen])

        if count is None:
            return await self.execute_command(
                CommandName.LPOS, *pieces, callback=OptionalIntCallback()
            )
        return await self.execute_command(
            CommandName.LPOS, *pieces, callback=OptionalListCallback[int]()
        )

    @redis_command(CommandName.LPUSH, group=CommandGroup.LIST)
    async def lpush(self, key: KeyT, elements: Parameters[ValueT]) -> int:
        """
        Prepend one or multiple elements to a list

        :return: the length of the list after the push operations.
        """

        return await self.execute_command(
            CommandName.LPUSH, key, *elements, callback=IntCallback()
        )

    @redis_command(CommandName.LPUSHX, group=CommandGroup.LIST)
    async def lpushx(self, key: KeyT, elements: Parameters[ValueT]) -> int:
        """
        Prepend an element to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return await self.execute_command(
            CommandName.LPUSHX, key, *elements, callback=IntCallback()
        )

    @redis_command(
        CommandName.LRANGE,
        readonly=True,
        group=CommandGroup.LIST,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def lrange(self, key: KeyT, start: int, stop: int) -> List[AnyStr]:
        """
        Get a range of elements from a list

        :return: list of elements in the specified range.
        """

        return await self.execute_command(
            CommandName.LRANGE, key, start, stop, callback=ListCallback[AnyStr]()
        )

    @redis_command(CommandName.LREM, group=CommandGroup.LIST)
    async def lrem(self, key: KeyT, count: int, element: ValueT) -> int:
        """
        Removes the first :paramref:`count` occurrences of elements equal to ``element``
        from the list stored at :paramref:`key`.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

        :return: the number of removed elements.
        """

        return await self.execute_command(
            CommandName.LREM, key, count, element, callback=IntCallback()
        )

    @redis_command(
        CommandName.LSET,
        group=CommandGroup.LIST,
    )
    async def lset(self, key: KeyT, index: int, element: ValueT) -> bool:
        """Sets ``index`` of list :paramref:`key` to ``element``"""

        return await self.execute_command(
            CommandName.LSET, key, index, element, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.LTRIM,
        group=CommandGroup.LIST,
    )
    async def ltrim(self, key: KeyT, start: int, stop: int) -> bool:
        """
        Trims the list :paramref:`key`, removing all values not within the slice
        between ``start`` and ``stop``

        ``start`` and ``stop`` can be negative numbers just like
        Python slicing notation
        """

        return await self.execute_command(
            CommandName.LTRIM, key, start, stop, callback=SimpleStringCallback()
        )

    @overload
    async def rpop(self, key: KeyT) -> Optional[AnyStr]:
        ...

    @overload
    async def rpop(self, key: KeyT, count: int) -> Optional[List[AnyStr]]:
        ...

    @redis_command(
        CommandName.RPOP,
        group=CommandGroup.LIST,
        arguments={"count": {"version_introduced": "6.2.0"}},
    )
    async def rpop(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[AnyStr, List[AnyStr]]]:
        """
        Remove and get the last elements in a list

        :return: When called without the :paramref:`count` argument the value
         of the last element, or ``None`` when :paramref:`key` does not exist.

         When called with the :paramref:`count` argument list of popped elements,
         or ``None`` when :paramref:`key` does not exist.
        """

        pieces: CommandArgList = []

        if count is not None:
            pieces.extend([count])

        if count is None:
            return await self.execute_command(
                CommandName.RPOP,
                key,
                *pieces,
                callback=OptionalAnyStrCallback[AnyStr](),
            )
        return await self.execute_command(
            CommandName.RPOP, key, *pieces, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(
        CommandName.RPOPLPUSH,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`lmove` with the wherefrom and whereto arguments",
        group=CommandGroup.LIST,
    )
    async def rpoplpush(self, source: KeyT, destination: KeyT) -> Optional[AnyStr]:
        """
        Remove the last element in a list, prepend it to another list and return it

        :return: the element being popped and pushed.
        """

        return await self.execute_command(
            CommandName.RPOPLPUSH,
            source,
            destination,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(CommandName.RPUSH, group=CommandGroup.LIST)
    async def rpush(self, key: KeyT, elements: Parameters[ValueT]) -> int:
        """
        Append an element(s) to a list

        :return: the length of the list after the push operation.
        """

        return await self.execute_command(
            CommandName.RPUSH, key, *elements, callback=IntCallback()
        )

    @redis_command(CommandName.RPUSHX, group=CommandGroup.LIST)
    async def rpushx(self, key: KeyT, elements: Parameters[ValueT]) -> int:
        """
        Append a element(s) to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return await self.execute_command(
            CommandName.RPUSHX, key, *elements, callback=IntCallback()
        )

    @redis_command(CommandName.SADD, group=CommandGroup.SET)
    async def sadd(self, key: KeyT, members: Parameters[ValueT]) -> int:
        """
        Add one or more members to a set

        :return: the number of elements that were added to the set, not including
         all the elements already present in the set.
        """

        return await self.execute_command(
            CommandName.SADD, key, *members, callback=IntCallback()
        )

    @redis_command(
        CommandName.SCARD,
        readonly=True,
        group=CommandGroup.SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def scard(self, key: KeyT) -> int:
        """
        Returns the number of members in the set

        :return the cardinality (number of elements) of the set, or ``0`` if :paramref:`key`
         does not exist.
        """

        return await self.execute_command(
            CommandName.SCARD, key, callback=IntCallback()
        )

    @redis_command(
        CommandName.SDIFF,
        readonly=True,
        group=CommandGroup.SET,
    )
    async def sdiff(self, keys: Parameters[KeyT]) -> Set[AnyStr]:
        """
        Subtract multiple sets

        :return: members of the resulting set.
        """

        return await self.execute_command(
            CommandName.SDIFF, *keys, callback=SetCallback[AnyStr]()
        )

    @redis_command(CommandName.SDIFFSTORE, group=CommandGroup.SET)
    async def sdiffstore(self, keys: Parameters[KeyT], destination: KeyT) -> int:
        """
        Subtract multiple sets and store the resulting set in a key

        """

        return await self.execute_command(
            CommandName.SDIFFSTORE, destination, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.SINTER,
        readonly=True,
        group=CommandGroup.SET,
    )
    async def sinter(self, keys: Parameters[KeyT]) -> Set[AnyStr]:
        """
        Intersect multiple sets

        :return: members of the resulting set
        """

        return await self.execute_command(
            CommandName.SINTER, *keys, callback=SetCallback[AnyStr]()
        )

    @redis_command(CommandName.SINTERSTORE, group=CommandGroup.SET)
    async def sinterstore(self, keys: Parameters[KeyT], destination: KeyT) -> int:
        """
        Intersect multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.
        """

        return await self.execute_command(
            CommandName.SINTERSTORE, destination, *keys, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SINTERCARD,
        version_introduced="7.0.0",
        group=CommandGroup.SET,
        readonly=True,
    )
    async def sintercard(
        self,
        keys: Parameters[KeyT],
        limit: Optional[int] = None,
    ) -> int:
        """
        Intersect multiple sets and return the cardinality of the result

        :return: The number of elements in the resulting intersection.
        """
        _keys: List[KeyT] = list(keys)

        pieces: CommandArgList = [len(_keys), *_keys]

        if limit is not None:
            pieces.extend(["LIMIT", limit])

        return await self.execute_command(
            CommandName.SINTERCARD, *pieces, callback=IntCallback()
        )

    @redis_command(
        CommandName.SISMEMBER,
        readonly=True,
        group=CommandGroup.SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def sismember(self, key: KeyT, member: ValueT) -> bool:
        """
        Determine if a given value is a member of a set

        :return: If the element is a member of the set. ``False`` if :paramref:`key` does not exist.
        """

        return await self.execute_command(
            CommandName.SISMEMBER, key, member, callback=BoolCallback()
        )

    @redis_command(
        CommandName.SMEMBERS,
        readonly=True,
        group=CommandGroup.SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def smembers(self, key: KeyT) -> Set[AnyStr]:
        """Returns all members of the set"""

        return await self.execute_command(
            CommandName.SMEMBERS, key, callback=SetCallback[AnyStr]()
        )

    @redis_command(
        CommandName.SMISMEMBER,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def smismember(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> Tuple[bool, ...]:
        """
        Returns the membership associated with the given elements for a set

        :return: tuple representing the membership of the given elements, in the same
         order as they are requested.
        """

        return await self.execute_command(
            CommandName.SMISMEMBER, key, *members, callback=BoolsCallback()
        )

    @redis_command(
        CommandName.SMOVE,
        group=CommandGroup.SET,
    )
    async def smove(self, source: KeyT, destination: KeyT, member: ValueT) -> bool:
        """
        Move a member from one set to another
        """

        return await self.execute_command(
            CommandName.SMOVE, source, destination, member, callback=BoolCallback()
        )

    @redis_command(
        CommandName.SPOP,
        group=CommandGroup.SET,
    )
    async def spop(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[AnyStr, Set[AnyStr]]]:
        """
        Remove and return one or multiple random members from a set

        :return: When called without the :paramref:`count` argument the removed member, or ``None``
         when :paramref:`key` does not exist.

         When called with the :paramref:`count` argument the removed members, or an empty array when
         :paramref:`key` does not exist.
        """

        if count is not None:
            return await self.execute_command(
                CommandName.SPOP,
                key,
                count,
                count=count,
                callback=SetCallback[AnyStr](),
            )
        else:
            return await self.execute_command(
                CommandName.SPOP, key, callback=AnyStrCallback[AnyStr]()
            )

    @redis_command(
        CommandName.SRANDMEMBER,
        readonly=True,
        group=CommandGroup.SET,
    )
    async def srandmember(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[AnyStr, Set[AnyStr]]]:
        """
        Get one or multiple random members from a set



        :return: without the additional :paramref:`count` argument, the command returns a  randomly
         selected element, or ``None`` when :paramref:`key` does not exist.

         When the additional :paramref:`count` argument is passed, the command returns elements,
         or an empty set when :paramref:`key` does not exist.
        """
        pieces: CommandArgList = []

        if count is not None:
            pieces.append(count)

        return await self.execute_command(
            CommandName.SRANDMEMBER,
            key,
            *pieces,
            count=count,
            callback=ItemOrSetCallback[AnyStr](),
        )

    @redis_command(CommandName.SREM, group=CommandGroup.SET)
    async def srem(self, key: KeyT, members: Parameters[ValueT]) -> int:
        """
        Remove one or more members from a set


        :return: the number of members that were removed from the set, not
         including non existing members.
        """

        return await self.execute_command(
            CommandName.SREM, key, *members, callback=IntCallback()
        )

    @redis_command(
        CommandName.SUNION,
        readonly=True,
        group=CommandGroup.SET,
    )
    async def sunion(self, keys: Parameters[KeyT]) -> Set[AnyStr]:
        """
        Add multiple sets

        :return: members of the resulting set.
        """

        return await self.execute_command(
            CommandName.SUNION, *keys, callback=SetCallback[AnyStr]()
        )

    @redis_command(CommandName.SUNIONSTORE, group=CommandGroup.SET)
    async def sunionstore(self, keys: Parameters[KeyT], destination: KeyT) -> int:
        """
        Add multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.

        """

        return await self.execute_command(
            CommandName.SUNIONSTORE, destination, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.SSCAN,
        readonly=True,
        group=CommandGroup.SET,
        cluster=ClusterCommandConfig(combine=ClusterEnsureConsistent()),
    )
    async def sscan(
        self,
        key: KeyT,
        cursor: Optional[int] = 0,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> Tuple[int, Set[AnyStr]]:
        """
        Incrementally returns subsets of elements in a set. Also returns a
        cursor pointing to the scan position.

        :param match: is for filtering the keys by pattern
        :param count: is for hint the minimum number of returns
        """
        pieces: CommandArgList = [key, cursor or "0"]

        if match is not None:
            pieces.extend(["MATCH", match])

        if count is not None:
            pieces.extend(["COUNT", count])

        return await self.execute_command(
            CommandName.SSCAN, *pieces, callback=SScanCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.BZMPOP,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
    )
    async def bzmpop(
        self,
        keys: Parameters[KeyT],
        timeout: Union[int, float],
        where: Literal[PureToken.MAX, PureToken.MIN],
        count: Optional[int] = None,
    ) -> Optional[Tuple[AnyStr, Tuple[ScoredMember, ...]]]:
        """
        Remove and return members with scores in a sorted set or block until one is available

        :return:

          - A ```None``` when no element could be popped.
          - A tuple of (name of key, popped (member, score) pairs)
        """
        _keys: List[KeyT] = list(keys)
        pieces: CommandArgList = [timeout, len(_keys), *_keys, where]

        if count is not None:
            pieces.extend(["COUNT", count])

        return await self.execute_command(
            CommandName.BZMPOP, *pieces, callback=ZMPopCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BZPOPMAX,
        group=CommandGroup.SORTED_SET,
    )
    async def bzpopmax(
        self, keys: Parameters[KeyT], timeout: Union[int, float]
    ) -> Optional[Tuple[AnyStr, AnyStr, float]]:
        """
        Remove and return the member with the highest score from one or more sorted sets,
        or block until one is available.

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """
        params: CommandArgList = []
        params.extend(keys)
        params.append(timeout)

        return await self.execute_command(
            CommandName.BZPOPMAX, *params, callback=BZPopCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BZPOPMIN,
        group=CommandGroup.SORTED_SET,
    )
    async def bzpopmin(
        self, keys: Parameters[KeyT], timeout: Union[int, float]
    ) -> Optional[Tuple[AnyStr, AnyStr, float]]:
        """
        Remove and return the member with the lowest score from one or more sorted sets,
        or block until one is available

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """

        params: CommandArgList = []
        params.extend(keys)
        params.append(timeout)

        return await self.execute_command(
            CommandName.BZPOPMIN, *params, callback=BZPopCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZADD,
        group=CommandGroup.SORTED_SET,
        arguments={"comparison": {"version_introduced": "6.2.0"}},
    )
    async def zadd(
        self,
        key: KeyT,
        member_scores: Dict[StringT, float],
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
        comparison: Optional[Literal[PureToken.GT, PureToken.LT]] = None,
        change: Optional[bool] = None,
        increment: Optional[bool] = None,
    ) -> Optional[Union[int, float]]:
        """
        Add one or more members to a sorted set, or update its score if it already exists

        :return:

         - When used without optional arguments, the number of elements added to the sorted set
           (excluding score updates).
         - If the ``change`` option is specified, the number of elements that were changed
           (added or updated).
         - If :paramref:`condition` is specified, the new score of :paramref:`member`
           (a double precision floating point number) represented as string
         - ``None`` if the operation is aborted

        """
        pieces: CommandArgList = []

        if change is not None:
            pieces.append(PureToken.CHANGE)

        if increment is not None:
            pieces.append(PureToken.INCREMENT)

        if condition:
            pieces.append(condition)

        if comparison:
            pieces.append(comparison)

        flat_member_scores = dict_to_flat_list(member_scores, reverse=True)
        pieces.extend(flat_member_scores)

        return await self.execute_command(
            CommandName.ZADD, key, *pieces, callback=ZAddCallback()
        )

    @redis_command(CommandName.ZCARD, readonly=True, group=CommandGroup.SORTED_SET)
    async def zcard(self, key: KeyT) -> int:
        """
        Get the number of members in a sorted set

        :return: the cardinality (number of elements) of the sorted set, or ``0``
         if the :paramref:`key` does not exist

        """

        return await self.execute_command(
            CommandName.ZCARD, key, callback=IntCallback()
        )

    @redis_command(CommandName.ZCOUNT, readonly=True, group=CommandGroup.SORTED_SET)
    async def zcount(
        self,
        key: KeyT,
        min_: ValueT,
        max_: ValueT,
    ) -> int:
        """
        Count the members in a sorted set with scores within the given values

        :return: the number of elements in the specified score range.
        """

        return await self.execute_command(
            CommandName.ZCOUNT, key, min_, max_, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZDIFF,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zdiff(
        self, keys: Parameters[KeyT], withscores: Optional[bool] = None
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """
        Subtract multiple sorted sets

        :return: the result of the difference (optionally with their scores, in case
         the ``withscores`` option is given).
        """
        pieces: CommandArgList = [len(list(keys)), *keys]

        if withscores:
            pieces.append(PureToken.WITHSCORES)

        return await self.execute_command(
            CommandName.ZDIFF,
            *pieces,
            withscores=withscores,
            callback=ZMembersOrScoredMembers[AnyStr](),
        )

    @redis_command(
        CommandName.ZDIFFSTORE,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zdiffstore(self, keys: Parameters[KeyT], destination: KeyT) -> int:
        """
        Subtract multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """
        pieces: CommandArgList = [len(list(keys)), *keys]

        return await self.execute_command(
            CommandName.ZDIFFSTORE, destination, *pieces, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZINCRBY,
        group=CommandGroup.SORTED_SET,
    )
    async def zincrby(self, key: KeyT, member: ValueT, increment: int) -> float:
        """
        Increment the score of a member in a sorted set

        :return: the new score of :paramref:`member`
        """

        return await self.execute_command(
            CommandName.ZINCRBY,
            key,
            increment,
            member,
            callback=FloatCallback(),
        )

    @redis_command(
        CommandName.ZINTER,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zinter(
        self,
        keys: Parameters[KeyT],
        weights: Optional[Parameters[int]] = None,
        aggregate: Optional[
            Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM]
        ] = None,
        withscores: Optional[bool] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Intersect multiple sorted sets

        :return: the result of intersection (optionally with their scores, in case
         the ``withscores`` option is given).

        """

        return await self._zaggregate(
            CommandName.ZINTER,
            keys,
            None,
            weights,
            aggregate,
            withscores=withscores,
        )

    @redis_command(CommandName.ZINTERSTORE, group=CommandGroup.SORTED_SET)
    async def zinterstore(
        self,
        keys: Parameters[KeyT],
        destination: KeyT,
        weights: Optional[Parameters[int]] = None,
        aggregate: Optional[
            Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM]
        ] = None,
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """

        return await self._zaggregate(
            CommandName.ZINTERSTORE, keys, destination, weights, aggregate
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ZINTERCARD,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
        readonly=True,
    )
    async def zintercard(
        self, keys: Parameters[KeyT], limit: Optional[int] = None
    ) -> int:
        """
        Intersect multiple sorted sets and return the cardinality of the result

        :return: The number of elements in the resulting intersection.

        """
        _keys: List[KeyT] = list(keys)
        pieces: CommandArgList = [len(_keys), *_keys]

        if limit is not None:
            pieces.extend(["LIMIT", limit])

        return await self.execute_command(
            CommandName.ZINTERCARD, *pieces, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZLEXCOUNT,
        readonly=True,
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zlexcount(self, key: KeyT, min_: ValueT, max_: ValueT) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range

        :return: the number of elements in the specified score range.
        """

        return await self.execute_command(
            CommandName.ZLEXCOUNT, key, min_, max_, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ZMPOP,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zmpop(
        self,
        keys: Parameters[KeyT],
        where: Literal[PureToken.MAX, PureToken.MIN],
        count: Optional[int] = None,
    ) -> Optional[Tuple[AnyStr, Tuple[ScoredMember, ...]]]:
        """
        Remove and return members with scores in a sorted set

        :return: A tuple of (name of key, popped (member, score) pairs)
        """
        _keys: List[KeyT] = list(keys)
        pieces: CommandArgList = [len(_keys), *_keys, where]

        if count is not None:
            pieces.extend(["COUNT", count])

        return await self.execute_command(
            CommandName.ZMPOP, *pieces, callback=ZMPopCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZMSCORE,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zmscore(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> Tuple[Optional[float], ...]:
        """
        Get the score associated with the given members in a sorted set

        :return: scores or ``None`` associated with the specified :paramref:`members`
         values (a double precision floating point number), represented as strings

        """

        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")

        return await self.execute_command(
            CommandName.ZMSCORE, key, *members, callback=ZMScoreCallback()
        )

    @redis_command(
        CommandName.ZPOPMAX,
        group=CommandGroup.SORTED_SET,
    )
    async def zpopmax(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[ScoredMember, Tuple[ScoredMember, ...]]]:
        """
        Remove and return members with the highest scores in a sorted set

        :return: popped elements and scores.
        """
        args = (count is not None) and [count] or []
        options = {"count": count}

        return await self.execute_command(
            CommandName.ZPOPMAX,
            key,
            *args,
            **options,
            callback=ZSetScorePairCallback[AnyStr](),
        )

    @redis_command(
        CommandName.ZPOPMIN,
        group=CommandGroup.SORTED_SET,
    )
    async def zpopmin(
        self, key: KeyT, count: Optional[int] = None
    ) -> Optional[Union[ScoredMember, Tuple[ScoredMember, ...]]]:
        """
        Remove and return members with the lowest scores in a sorted set

        :return: popped elements and scores.
        """
        args = (count is not None) and [count] or []
        options = {"count": count}

        return await self.execute_command(
            CommandName.ZPOPMIN,
            key,
            *args,
            callback=ZSetScorePairCallback[AnyStr](),
            **options,
        )

    @redis_command(
        CommandName.ZRANDMEMBER,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zrandmember(
        self,
        key: KeyT,
        count: Optional[int] = None,
        withscores: Optional[bool] = None,
    ) -> Optional[Union[AnyStr, List[AnyStr], Tuple[ScoredMember, ...]]]:
        """
        Get one or multiple random elements from a sorted set


        :return: without :paramref:`count`, the command returns a
         randomly selected element, or ``None`` when :paramref:`key` does not exist.

         If :paramref:`count` is passed the command returns the elements,
         or an empty tuple when :paramref:`key` does not exist.

         If :paramref:`withscores` argument is used, the return is a list elements
         and their scores from the sorted set.
        """
        params: CommandArgList = []
        options = {}

        if count is not None:
            params.append(count)
            options["count"] = count

        if withscores:
            params.append(PureToken.WITHSCORES)
            options["withscores"] = True

        return await self.execute_command(
            CommandName.ZRANDMEMBER,
            key,
            *params,
            callback=ZRandMemberCallback[AnyStr](),
            **options,
        )

    @overload
    async def zrange(
        self,
        key: KeyT,
        min_: Union[int, ValueT],
        max_: Union[int, ValueT],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Tuple[AnyStr, ...]:
        ...

    @overload
    async def zrange(
        self,
        key: KeyT,
        min_: Union[int, ValueT],
        max_: Union[int, ValueT],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        *,
        withscores: Literal[True],
    ) -> Tuple[ScoredMember, ...]:
        ...

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.ZRANGE,
        readonly=True,
        group=CommandGroup.SORTED_SET,
        arguments={
            "sortby": {"version_introduced": "6.2.0"},
            "rev": {"version_introduced": "6.2.0"},
            "offset": {"version_introduced": "6.2.0"},
            "count": {"version_introduced": "6.2.0"},
        },
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zrange(
        self,
        key: KeyT,
        min_: Union[int, ValueT],
        max_: Union[int, ValueT],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
        withscores: Optional[bool] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Return a range of members in a sorted set

        :return: elements in the specified range (optionally with their scores, in case
         :paramref:`withscores` is given).
        """

        return await self._zrange(
            CommandName.ZRANGE,
            key,
            min_,
            max_,
            None,
            rev,
            sortby,
            withscores,
            offset,
            count,
        )

    @redis_command(
        CommandName.ZRANGEBYLEX,
        readonly=True,
        version_deprecated="6.2.0",
        deprecation_reason=" Use :meth:`zrange` with the sortby=BYLEX argument",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    @mutually_inclusive_parameters("offset", "count")
    async def zrangebylex(
        self,
        key: KeyT,
        min_: ValueT,
        max_: ValueT,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Tuple[AnyStr, ...]:
        """

        Return a range of members in a sorted set, by lexicographical range

        :return: elements in the specified score range.
        """

        pieces: CommandArgList = [key, min_, max_]

        if offset is not None and count is not None:
            pieces.extend(["LIMIT", offset, count])

        return await self.execute_command(
            CommandName.ZRANGEBYLEX, *pieces, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZRANGEBYSCORE,
        readonly=True,
        version_deprecated="6.2.0",
        deprecation_reason=" Use :meth:`zrange` with the sortby=BYSCORE argument",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    @mutually_inclusive_parameters("offset", "count")
    async def zrangebyscore(
        self,
        key: KeyT,
        min_: Union[int, float],
        max_: Union[int, float],
        withscores: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Return a range of members in a sorted set, by score

        :return: elements in the specified score range (optionally with their scores).
        """

        pieces: CommandArgList = [key, min_, max_]

        if offset is not None and count is not None:
            pieces.extend([PrefixToken.LIMIT, offset, count])

        if withscores:
            pieces.append(PureToken.WITHSCORES)
        options = {"withscores": withscores}

        return await self.execute_command(
            CommandName.ZRANGEBYSCORE,
            *pieces,
            callback=ZMembersOrScoredMembers[AnyStr](),
            **options,
        )

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.ZRANGESTORE,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zrangestore(
        self,
        dst: KeyT,
        src: KeyT,
        min_: Union[int, ValueT],
        max_: Union[int, ValueT],
        sortby: Optional[Literal[PureToken.BYSCORE, PureToken.BYLEX]] = None,
        rev: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> int:
        """
        Store a range of members from sorted set into another key

        :return: the number of elements in the resulting sorted set
        """

        return await self._zrange(
            CommandName.ZRANGESTORE,
            src,
            min_,
            max_,
            dst,
            rev,
            sortby,
            False,
            offset,
            count,
        )

    @redis_command(
        CommandName.ZRANK,
        readonly=True,
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zrank(self, key: KeyT, member: ValueT) -> Optional[int]:
        """
        Determine the index of a member in a sorted set

        :return: the rank of :paramref:`member`
        """

        return await self.execute_command(
            CommandName.ZRANK, key, member, callback=OptionalIntCallback()
        )

    @redis_command(
        CommandName.ZREM,
        group=CommandGroup.SORTED_SET,
    )
    async def zrem(self, key: KeyT, members: Parameters[ValueT]) -> int:
        """
        Remove one or more members from a sorted set

        :return: The number of members removed from the sorted set, not including non existing
         members.
        """

        return await self.execute_command(
            CommandName.ZREM, key, *members, callback=IntCallback()
        )

    @redis_command(CommandName.ZREMRANGEBYLEX, group=CommandGroup.SORTED_SET)
    async def zremrangebylex(self, key: KeyT, min_: ValueT, max_: ValueT) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range

        :return: the number of elements removed.
        """

        return await self.execute_command(
            CommandName.ZREMRANGEBYLEX, key, min_, max_, callback=IntCallback()
        )

    @redis_command(CommandName.ZREMRANGEBYRANK, group=CommandGroup.SORTED_SET)
    async def zremrangebyrank(self, key: KeyT, start: int, stop: int) -> int:
        """
        Remove all members in a sorted set within the given indexes

        :return: the number of elements removed.
        """

        return await self.execute_command(
            CommandName.ZREMRANGEBYRANK, key, start, stop, callback=IntCallback()
        )

    @redis_command(CommandName.ZREMRANGEBYSCORE, group=CommandGroup.SORTED_SET)
    async def zremrangebyscore(
        self, key: KeyT, min_: Union[int, float], max_: Union[int, float]
    ) -> int:
        """
        Remove all members in a sorted set within the given scores

        :return: the number of elements removed.
        """

        return await self.execute_command(
            CommandName.ZREMRANGEBYSCORE, key, min_, max_, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZREVRANGE,
        readonly=True,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev argument",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zrevrange(
        self,
        key: KeyT,
        start: int,
        stop: int,
        withscores: Optional[bool] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Return a range of members in a sorted set, by index, with scores ordered from
        high to low

        :return: elements in the specified range (optionally with their scores).
        """
        pieces: CommandArgList = [key, start, stop]

        if withscores:
            pieces.append(PureToken.WITHSCORES)
        options = {"withscores": withscores}

        return await self.execute_command(
            CommandName.ZREVRANGE,
            *pieces,
            callback=ZMembersOrScoredMembers[AnyStr](),
            **options,
        )

    @redis_command(
        CommandName.ZREVRANGEBYLEX,
        readonly=True,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev and sort=BYLEX arguments",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    @mutually_inclusive_parameters("offset", "count")
    async def zrevrangebylex(
        self,
        key: KeyT,
        max_: ValueT,
        min_: ValueT,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Tuple[AnyStr, ...]:
        """

        Return a range of members in a sorted set, by lexicographical range, ordered from
        higher to lower strings.

        :return: elements in the specified score range
        """

        pieces: CommandArgList = [key, max_, min_]

        if offset is not None and count is not None:
            pieces.extend(["LIMIT", offset, count])

        return await self.execute_command(
            CommandName.ZREVRANGEBYLEX, *pieces, callback=TupleCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZREVRANGEBYSCORE,
        readonly=True,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev and sort=BYSCORE arguments",
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    @mutually_inclusive_parameters("offset", "count")
    async def zrevrangebyscore(
        self,
        key: KeyT,
        max_: Union[int, float],
        min_: Union[int, float],
        withscores: Optional[bool] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Return a range of members in a sorted set, by score, with scores ordered from high to low

        :return: elements in the specified score range (optionally with their scores)
        """

        pieces: CommandArgList = [key, max_, min_]

        if offset is not None and count is not None:
            pieces.extend(["LIMIT", offset, count])

        if withscores:
            pieces.append(PureToken.WITHSCORES)
        options = {"withscores": withscores}

        return await self.execute_command(
            CommandName.ZREVRANGEBYSCORE,
            *pieces,
            **options,
            callback=ZMembersOrScoredMembers[AnyStr](),
        )

    @redis_command(
        CommandName.ZREVRANK,
        readonly=True,
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zrevrank(self, key: KeyT, member: ValueT) -> Optional[int]:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low

        :return: the rank of :paramref:`member`
        """

        return await self.execute_command(
            CommandName.ZREVRANK, key, member, callback=OptionalIntCallback()
        )

    @redis_command(
        CommandName.ZSCAN,
        readonly=True,
        group=CommandGroup.SORTED_SET,
    )
    async def zscan(
        self,
        key: KeyT,
        cursor: Optional[int] = 0,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> Tuple[int, Tuple[ScoredMember, ...]]:
        """
        Incrementally iterate sorted sets elements and associated scores

        """
        pieces: CommandArgList = [key, cursor or "0"]

        if match is not None:
            pieces.extend(["MATCH", match])

        if count is not None:
            pieces.extend(["COUNT", count])

        return await self.execute_command(
            CommandName.ZSCAN, *pieces, callback=ZScanCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZSCORE,
        readonly=True,
        group=CommandGroup.SORTED_SET,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def zscore(self, key: KeyT, member: ValueT) -> Optional[float]:
        """
        Get the score associated with the given member in a sorted set

        :return: the score of :paramref:`member` (a double precision floating point number),
         represented as string or ``None`` if the member doesn't exist.
        """

        return await self.execute_command(
            CommandName.ZSCORE, key, member, callback=OptionalFloatCallback()
        )

    @redis_command(
        CommandName.ZUNION,
        readonly=True,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    async def zunion(
        self,
        keys: Parameters[KeyT],
        weights: Optional[Parameters[int]] = None,
        aggregate: Optional[
            Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]
        ] = None,
        withscores: Optional[bool] = None,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        """

        Add multiple sorted sets

        :return: the result of union (optionally with their scores, in case
         :paramref:`withscores` is given).
        """

        return await self._zaggregate(
            CommandName.ZUNION,
            keys,
            None,
            weights,
            aggregate,
            withscores=withscores,
        )

    @redis_command(CommandName.ZUNIONSTORE, group=CommandGroup.SORTED_SET)
    async def zunionstore(
        self,
        keys: Parameters[KeyT],
        destination: KeyT,
        weights: Optional[Parameters[int]] = None,
        aggregate: Optional[
            Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX]
        ] = None,
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """

        return await self._zaggregate(
            CommandName.ZUNIONSTORE, keys, destination, weights, aggregate
        )

    @overload
    async def _zrange(
        self,
        command: Literal[CommandName.ZRANGESTORE],
        key: KeyT,
        start: Union[int, ValueT],
        stop: Union[int, ValueT],
        dest: Optional[ValueT] = ...,
        rev: Optional[bool] = None,
        sortby: Optional[PureToken] = ...,
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> int:
        ...

    @overload
    async def _zrange(
        self,
        command: Literal[CommandName.ZRANGE],
        key: KeyT,
        start: Union[int, ValueT],
        stop: Union[int, ValueT],
        dest: Optional[ValueT] = ...,
        rev: Optional[bool] = None,
        sortby: Optional[PureToken] = ...,
        withscores: Optional[bool] = ...,
        offset: Optional[int] = ...,
        count: Optional[int] = ...,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        ...

    async def _zrange(
        self,
        command: Literal[CommandName.ZRANGE, CommandName.ZRANGESTORE],
        key: KeyT,
        start: Union[int, ValueT],
        stop: Union[int, ValueT],
        dest: Optional[ValueT] = None,
        rev: Optional[bool] = None,
        sortby: Optional[PureToken] = None,
        withscores: Optional[bool] = False,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> Union[int, Tuple[Union[AnyStr, ScoredMember], ...]]:
        pieces: CommandArgList = []

        if dest:
            pieces.append(dest)
        pieces.extend([key, start, stop])

        if sortby:
            pieces.append(sortby)

        if rev is not None:
            pieces.append(PureToken.REV)

        if offset is not None and count is not None:
            pieces.extend([PrefixToken.LIMIT, offset, count])

        if withscores:
            pieces.append(PureToken.WITHSCORES)
        options = {"withscores": withscores}

        if command == CommandName.ZRANGE:
            return await self.execute_command(
                command, *pieces, callback=ZMembersOrScoredMembers[AnyStr](), **options
            )
        else:
            return await self.execute_command(
                command, *pieces, callback=IntCallback(), **options
            )

    @overload
    async def _zaggregate(
        self,
        command: Literal[CommandName.ZUNIONSTORE, CommandName.ZINTERSTORE],
        keys: Parameters[KeyT],
        destination: Optional[KeyT] = ...,
        weights: Optional[Parameters[int]] = ...,
        aggregate: Optional[PureToken] = ...,
        withscores: Optional[bool] = ...,
    ) -> int:
        ...

    @overload
    async def _zaggregate(
        self,
        command: Literal[CommandName.ZUNION, CommandName.ZINTER],
        keys: Parameters[KeyT],
        destination: Optional[KeyT] = ...,
        weights: Optional[Parameters[int]] = ...,
        aggregate: Optional[PureToken] = ...,
        withscores: Optional[bool] = ...,
    ) -> Tuple[Union[AnyStr, ScoredMember], ...]:
        ...

    async def _zaggregate(
        self,
        command: Literal[
            CommandName.ZUNION,
            CommandName.ZUNIONSTORE,
            CommandName.ZINTER,
            CommandName.ZINTERSTORE,
        ],
        keys: Parameters[KeyT],
        destination: Optional[KeyT] = None,
        weights: Optional[Parameters[int]] = None,
        aggregate: Optional[PureToken] = None,
        withscores: Optional[bool] = None,
    ) -> Union[int, Tuple[Union[AnyStr, ScoredMember], ...]]:
        pieces: CommandArgList = []

        if destination:
            pieces.append(destination)
        pieces.append(len(list(keys)))
        pieces.extend(keys)
        options = {}

        if weights:
            pieces.append(PrefixToken.WEIGHTS)
            pieces.extend(weights)

        if aggregate:
            pieces.append(PrefixToken.AGGREGATE)
            pieces.append(aggregate)

        if withscores is not None:
            pieces.append(PureToken.WITHSCORES)
            options = {"withscores": True}

        if command in [CommandName.ZUNIONSTORE, CommandName.ZINTERSTORE]:
            return await self.execute_command(
                command, *pieces, callback=IntCallback(), **options
            )
        else:
            return await self.execute_command(
                command, *pieces, callback=ZMembersOrScoredMembers[AnyStr](), **options
            )

    @redis_command(CommandName.XACK, group=CommandGroup.STREAM)
    async def xack(
        self, key: KeyT, group: StringT, identifiers: Parameters[ValueT]
    ) -> int:
        """
        Marks a pending message as correctly processed,
        effectively removing it from the pending entries list of the consumer group.

        :return: number of messages successfully acknowledged,
         that is, the IDs we were actually able to resolve in the PEL.
        """

        return await self.execute_command(
            CommandName.XACK, key, group, *identifiers, callback=IntCallback()
        )

    @mutually_inclusive_parameters("trim_strategy", "threshold")
    @redis_command(
        CommandName.XADD,
        group=CommandGroup.STREAM,
        arguments={
            "nomkstream": {"version_introduced": "6.2.0"},
            "limit": {"version_introduced": "6.2.0"},
        },
    )
    async def xadd(
        self,
        key: KeyT,
        field_values: Dict[StringT, ValueT],
        identifier: Optional[ValueT] = None,
        nomkstream: Optional[bool] = None,
        trim_strategy: Optional[Literal[PureToken.MAXLEN, PureToken.MINID]] = None,
        threshold: Optional[int] = None,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = None,
        limit: Optional[int] = None,
    ) -> Optional[AnyStr]:
        """
        Appends a new entry to a stream

        :return: The identifier of the added entry. The identifier is the one auto-generated
         if ``*`` is passed as :paramref:`identifier`, otherwise the it justs returns
         the same identifier specified

         Returns ``None`` when used with :paramref:`nomkstream` and the key doesn't exist.

        """
        pieces: CommandArgList = []

        if nomkstream is not None:
            pieces.append(PureToken.NOMKSTREAM)

        if trim_strategy == PureToken.MAXLEN:
            pieces.append(trim_strategy)

            if trim_operator:
                pieces.append(trim_operator)

            if threshold is not None:
                pieces.append(threshold)

        if limit is not None:
            pieces.extend(["LIMIT", limit])

        pieces.append(identifier or PureToken.AUTO_ID)

        for kv in field_values.items():
            pieces.extend(list(kv))

        return await self.execute_command(
            CommandName.XADD, key, *pieces, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.XLEN, readonly=True, group=CommandGroup.STREAM)
    async def xlen(self, key: KeyT) -> int:
        """ """

        return await self.execute_command(CommandName.XLEN, key, callback=IntCallback())

    @redis_command(
        CommandName.XRANGE,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xrange(
        self,
        key: KeyT,
        start: Optional[ValueT] = None,
        end: Optional[ValueT] = None,
        count: Optional[int] = None,
    ) -> Tuple[StreamEntry, ...]:
        """
        Return a range of elements in a stream, with IDs matching the specified IDs interval
        """

        pieces: CommandArgList = [defaultvalue(start, "-"), defaultvalue(end, "+")]

        if count is not None:
            pieces.append("COUNT")
            pieces.append(count)

        return await self.execute_command(
            CommandName.XRANGE, key, *pieces, callback=StreamRangeCallback()
        )

    @redis_command(
        CommandName.XREVRANGE,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xrevrange(
        self,
        key: KeyT,
        end: Optional[ValueT] = None,
        start: Optional[ValueT] = None,
        count: Optional[int] = None,
    ) -> Tuple[StreamEntry, ...]:
        """
        Return a range of elements in a stream, with IDs matching the specified
        IDs interval, in reverse order (from greater to smaller IDs) compared to XRANGE
        """
        pieces: CommandArgList = [defaultvalue(end, "+"), defaultvalue(start, "-")]

        if count is not None:
            pieces.append("COUNT")
            pieces.append(count)

        return await self.execute_command(
            CommandName.XREVRANGE, key, *pieces, callback=StreamRangeCallback()
        )

    @redis_command(
        CommandName.XREAD,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xread(
        self,
        streams: Mapping[ValueT, ValueT],
        count: Optional[int] = None,
        block: Optional[Union[int, datetime.timedelta]] = None,
    ) -> Optional[Dict[AnyStr, Tuple[StreamEntry, ...]]]:
        """
        Return never seen elements in multiple streams, with IDs greater than
        the ones reported by the caller for each stream. Can block.

        :return: Mapping of streams to stream entries.
         Field and values are guaranteed to be reported in the same order they were
         added by :meth:`xadd`.

         When :paramref:`block` is used, on timeout ``None`` is returned.
        """
        pieces: CommandArgList = []

        if block is not None:
            pieces.append(PrefixToken.BLOCK)
            pieces.append(normalized_milliseconds(block))

        if count is not None:
            pieces.append(PrefixToken.COUNT)
            pieces.append(count)
        pieces.append(PrefixToken.STREAMS)
        ids: CommandArgList = []

        for partial_stream in streams.items():
            pieces.append(partial_stream[0])
            ids.append(partial_stream[1])
        pieces.extend(ids)

        return await self.execute_command(
            CommandName.XREAD, *pieces, callback=MultiStreamRangeCallback[AnyStr]()
        )

    @redis_command(
        CommandName.XREADGROUP,
        group=CommandGroup.STREAM,
    )
    async def xreadgroup(
        self,
        group: StringT,
        consumer: StringT,
        streams: Mapping[ValueT, ValueT],
        count: Optional[int] = None,
        block: Optional[Union[int, datetime.timedelta]] = None,
        noack: Optional[bool] = None,
    ) -> Optional[Dict[AnyStr, Tuple[StreamEntry, ...]]]:
        """ """
        pieces: CommandArgList = [PrefixToken.GROUP, group, consumer]

        if block is not None:
            pieces.append(PrefixToken.BLOCK)
            pieces.append(normalized_milliseconds(block))

        if count is not None:
            pieces.append(PrefixToken.COUNT)
            pieces.append(count)

        if noack:
            pieces.append(PureToken.NOACK)

        pieces.append(PrefixToken.STREAMS)
        ids: CommandArgList = []

        for partial_stream in streams.items():
            pieces.append(partial_stream[0])
            ids.append(partial_stream[1])
        pieces.extend(ids)
        return await self.execute_command(
            CommandName.XREADGROUP, *pieces, callback=MultiStreamRangeCallback[AnyStr]()
        )

    @mutually_inclusive_parameters("start", "end", "count")
    @redis_command(
        CommandName.XPENDING,
        readonly=True,
        group=CommandGroup.STREAM,
        arguments={"idle": {"version_introduced": "6.2.0"}},
    )
    async def xpending(
        self,
        key: KeyT,
        group: StringT,
        start: Optional[ValueT] = None,
        end: Optional[ValueT] = None,
        count: Optional[int] = None,
        idle: Optional[int] = None,
        consumer: Optional[StringT] = None,
    ) -> Union[Tuple[StreamPendingExt, ...], StreamPending]:
        """
        Return information and entries from a stream consumer group pending
        entries list, that are messages fetched but never acknowledged.
        """
        pieces: CommandArgList = [key, group]

        if idle is not None:
            pieces.extend([PrefixToken.IDLE, idle])

        if count is not None and end is not None and start is not None:
            pieces.extend([start, end, count])

        if consumer is not None:
            pieces.append(consumer)

        return await self.execute_command(
            CommandName.XPENDING, *pieces, count=count, callback=PendingCallback()
        )

    @mutually_inclusive_parameters("trim_strategy", "threshold")
    @redis_command(
        CommandName.XTRIM,
        group=CommandGroup.STREAM,
        arguments={"limit": {"version_introduced": "6.2.0"}},
    )
    async def xtrim(
        self,
        key: KeyT,
        trim_strategy: Literal[PureToken.MAXLEN, PureToken.MINID],
        threshold: int,
        trim_operator: Optional[
            Literal[PureToken.EQUAL, PureToken.APPROXIMATELY]
        ] = None,
        limit: Optional[int] = None,
    ) -> int:
        """ """
        pieces: CommandArgList = [trim_strategy]

        if trim_operator:
            pieces.append(trim_operator)

        pieces.append(threshold)

        if limit is not None:
            pieces.extend(["LIMIT", limit])

        return await self.execute_command(
            CommandName.XTRIM, key, *pieces, callback=IntCallback()
        )

    @redis_command(CommandName.XDEL, group=CommandGroup.STREAM)
    async def xdel(self, key: KeyT, identifiers: Parameters[ValueT]) -> int:
        """ """

        return await self.execute_command(
            CommandName.XDEL, key, *identifiers, callback=IntCallback()
        )

    @redis_command(
        CommandName.XINFO_CONSUMERS,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xinfo_consumers(
        self, key: KeyT, groupname: StringT
    ) -> Tuple[Dict[AnyStr, AnyStr], ...]:
        """
        Get list of all consumers that belong to :paramref:`groupname` of the
        stream stored at :paramref:`key`
        """

        return await self.execute_command(
            CommandName.XINFO_CONSUMERS,
            key,
            groupname,
            callback=XInfoCallback[AnyStr](),
        )

    @redis_command(
        CommandName.XINFO_GROUPS,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xinfo_groups(self, key: KeyT) -> Tuple[Dict[AnyStr, AnyStr], ...]:
        """
        Get list of all consumers groups of the stream stored at :paramref:`key`
        """

        return await self.execute_command(
            CommandName.XINFO_GROUPS, key, callback=XInfoCallback[AnyStr]()
        )

    @mutually_inclusive_parameters("count", leaders=["full"])
    @redis_command(
        CommandName.XINFO_STREAM,
        readonly=True,
        group=CommandGroup.STREAM,
    )
    async def xinfo_stream(
        self, key: KeyT, full: Optional[bool] = None, count: Optional[int] = None
    ) -> StreamInfo:
        """
        Get information about the stream stored at :paramref:`key`

        :param full: If specified the return will contained extended information
         about the stream (see :class:`StreamInfo`).
        :param count: restrict the number of `entries` returned when using :paramref:`full`
        """
        pieces: CommandArgList = []

        if full:
            pieces.append("FULL")

            if count is not None:
                pieces.extend(["COUNT", count])

        return await self.execute_command(
            CommandName.XINFO_STREAM,
            key,
            *pieces,
            full=full,
            callback=StreamInfoCallback(),
        )

    @redis_command(
        CommandName.XCLAIM,
        group=CommandGroup.STREAM,
    )
    async def xclaim(
        self,
        key: KeyT,
        group: StringT,
        consumer: StringT,
        min_idle_time: Union[int, datetime.timedelta],
        identifiers: Parameters[ValueT],
        idle: Optional[Union[int, datetime.timedelta]] = None,
        time: Optional[Union[int, datetime.datetime]] = None,
        retrycount: Optional[int] = None,
        force: Optional[bool] = None,
        justid: Optional[bool] = None,
    ) -> Union[Tuple[AnyStr, ...], Tuple[StreamEntry, ...]]:
        """
        Changes (or acquires) ownership of a message in a consumer group, as
        if the message was delivered to the specified consumer.
        """
        pieces: CommandArgList = [
            key,
            group,
            consumer,
            normalized_milliseconds(min_idle_time),
        ]
        pieces.extend(identifiers)

        if idle is not None:
            pieces.extend(["IDLE", normalized_milliseconds(idle)])

        if time is not None:
            pieces.extend(["TIME", normalized_time_milliseconds(time)])

        if retrycount is not None:
            pieces.extend(["RETRYCOUNT", retrycount])

        if force is not None:
            pieces.append(PureToken.FORCE)

        if justid is not None:
            pieces.append(PureToken.JUSTID)

        return await self.execute_command(
            CommandName.XCLAIM, *pieces, justid=justid, callback=ClaimCallback[AnyStr]()
        )

    @redis_command(
        CommandName.XGROUP_CREATE,
        arguments={"entriesread": {"version_introduced": "7.0.0"}},
        group=CommandGroup.STREAM,
    )
    async def xgroup_create(
        self,
        key: KeyT,
        groupname: StringT,
        identifier: Optional[ValueT] = None,
        mkstream: Optional[bool] = None,
        entriesread: Optional[int] = None,
    ) -> bool:
        """
        Create a consumer group.
        """
        pieces: CommandArgList = [key, groupname, identifier or PureToken.NEW_ID]

        if mkstream is not None:
            pieces.append(PureToken.MKSTREAM)

        if entriesread is not None:
            pieces.extend([PrefixToken.ENTRIESREAD, entriesread])

        return await self.execute_command(
            CommandName.XGROUP_CREATE, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.XGROUP_CREATECONSUMER,
        version_introduced="6.2.0",
        group=CommandGroup.STREAM,
    )
    async def xgroup_createconsumer(
        self, key: KeyT, groupname: StringT, consumername: StringT
    ) -> bool:
        """
        Create a consumer in a consumer group.

        :return: whether the consumer was created
        """
        pieces: CommandArgList = [key, groupname, consumername]

        return await self.execute_command(
            CommandName.XGROUP_CREATECONSUMER, *pieces, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.XGROUP_SETID,
        group=CommandGroup.STREAM,
        arguments={"entriesread": {"version_introduced": "7.0.0"}},
    )
    async def xgroup_setid(
        self,
        key: KeyT,
        groupname: StringT,
        identifier: Optional[ValueT] = None,
        entriesread: Optional[int] = None,
    ) -> bool:
        """
        Set a consumer group to an arbitrary last delivered ID value.
        """

        pieces: CommandArgList = [key, groupname, identifier or PureToken.NEW_ID]

        if entriesread is not None:
            pieces.extend([PrefixToken.ENTRIESREAD, entriesread])

        return await self.execute_command(
            CommandName.XGROUP_SETID, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.XGROUP_DESTROY, group=CommandGroup.STREAM)
    async def xgroup_destroy(self, key: KeyT, groupname: StringT) -> int:
        """
        Destroy a consumer group.

        :return: The number of destroyed consumer groups
        """

        return await self.execute_command(
            CommandName.XGROUP_DESTROY, key, groupname, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.XGROUP_DELCONSUMER, group=CommandGroup.STREAM)
    async def xgroup_delconsumer(
        self, key: KeyT, groupname: StringT, consumername: StringT
    ) -> int:
        """
        Delete a consumer from a consumer group.

        :return: The number of pending messages that the consumer had before it was deleted
        """

        return await self.execute_command(
            CommandName.XGROUP_DELCONSUMER,
            key,
            groupname,
            consumername,
            callback=IntCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.XAUTOCLAIM,
        version_introduced="6.2.0",
        group=CommandGroup.STREAM,
    )
    async def xautoclaim(
        self,
        key: KeyT,
        group: StringT,
        consumer: StringT,
        min_idle_time: Union[int, datetime.timedelta],
        start: ValueT,
        count: Optional[int] = None,
        justid: Optional[bool] = None,
    ) -> Union[
        Tuple[AnyStr, Tuple[AnyStr, ...]],
        Tuple[AnyStr, Tuple[StreamEntry, ...], Tuple[AnyStr, ...]],
    ]:

        """
        Changes (or acquires) ownership of messages in a consumer group, as if the messages were
        delivered to the specified consumer.

        :return: A dictionary with keys as stream identifiers and values containing
         all the successfully claimed messages in the same format as :meth:`xrange`

        """
        pieces: CommandArgList = [
            key,
            group,
            consumer,
            normalized_milliseconds(min_idle_time),
            start,
        ]

        if count is not None:
            pieces.extend(["COUNT", count])

        if justid is not None:
            pieces.append(PureToken.JUSTID)

        return await self.execute_command(
            CommandName.XAUTOCLAIM,
            *pieces,
            justid=justid,
            callback=AutoClaimCallback[AnyStr](),
        )

    @redis_command(
        CommandName.BITCOUNT,
        readonly=True,
        group=CommandGroup.BITMAP,
        arguments={"index_unit": {"version_introduced": "7.0.0"}},
    )
    @mutually_inclusive_parameters("start", "end")
    async def bitcount(
        self,
        key: KeyT,
        start: Optional[int] = None,
        end: Optional[int] = None,
        index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = None,
    ) -> int:
        """
        Returns the count of set bits in the value of :paramref:`key`.  Optional
        :paramref:`start` and :paramref:`end` parameters indicate which bytes to consider

        """
        params: CommandArgList = [key]

        if start is not None and end is not None:
            params.append(start)
            params.append(end)

        if index_unit is not None:
            params.append(index_unit)

        return await self.execute_command(
            CommandName.BITCOUNT, *params, callback=IntCallback()
        )

    def bitfield(self, key: KeyT) -> BitFieldOperation[AnyStr]:
        """
        :return: a :class:`~coredis.commands.bitfield.BitFieldOperation`
         instance to conveniently construct one or more bitfield operations on
         :paramref:`key`.
        """

        return BitFieldOperation[AnyStr](self, key)

    def bitfield_ro(self, key: KeyT) -> BitFieldOperation[AnyStr]:
        """

        :return: a :class:`~coredis.commands.bitfield.BitFieldOperation`
         instance to conveniently construct bitfield operations on a read only
         replica against :paramref:`key`.

        Raises :class:`ReadOnlyError` if a write operation is attempted
        """

        return BitFieldOperation[AnyStr](self, key, readonly=True)

    @redis_command(
        CommandName.BITOP,
        group=CommandGroup.BITMAP,
    )
    async def bitop(
        self, keys: Parameters[KeyT], operation: StringT, destkey: KeyT
    ) -> int:
        """
        Perform a bitwise operation using :paramref:`operation` between
        :paramref:`keys` and store the result in :paramref:`destkey`.
        """

        return await self.execute_command(
            CommandName.BITOP, operation, destkey, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.BITPOS,
        readonly=True,
        group=CommandGroup.BITMAP,
        arguments={"index_unit": {"version_introduced": "7.0.0"}},
    )
    @mutually_inclusive_parameters("end", leaders=("start",))
    async def bitpos(
        self,
        key: KeyT,
        bit: int,
        start: Optional[int] = None,
        end: Optional[int] = None,
        index_unit: Optional[Literal[PureToken.BIT, PureToken.BYTE]] = None,
    ) -> int:
        """

        Return the position of the first bit set to 1 or 0 in a string.
        :paramref:`start` and :paramref:`end` defines the search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.


        :return: The position of the first bit set to 1 or 0 according to the request.
         If we look for set bits (the bit argument is 1) and the string is empty or
         composed of just zero bytes, -1 is returned.

         If we look for clear bits (the bit argument is 0) and the string only contains
         bit set to 1, the function returns the first bit not part of the string on the right.

         So if the string is three bytes set to the value ``0xff`` the command ``BITPOS key 0`` will
         return 24, since up to bit 23 all the bits are 1.

         Basically, the function considers the right of the string as padded with
         zeros if you look for clear bits and specify no range or the ``start`` argument **only**.

         However, this behavior changes if you are looking for clear bits and
         specify a range with both ``start`` and ``end``.

         If no clear bit is found in the specified range, the function returns -1 as the user
         specified a clear range and there are no 0 bits in that range.
        """

        if bit not in (0, 1):
            raise RedisError("bit must be 0 or 1")
        params: CommandArgList = [key, bit]

        if start is not None:
            params.append(start)

        if start is not None and end is not None:
            params.append(end)

        if index_unit is not None:
            params.append(index_unit)

        return await self.execute_command(
            CommandName.BITPOS, *params, callback=IntCallback()
        )

    @redis_command(CommandName.GETBIT, readonly=True, group=CommandGroup.BITMAP)
    async def getbit(self, key: KeyT, offset: int) -> int:
        """
        Returns the bit value at offset in the string value stored at key

        :return: the bit value stored at :paramref:`offset`.
        """

        return await self.execute_command(
            CommandName.GETBIT, key, offset, callback=IntCallback()
        )

    @redis_command(CommandName.SETBIT, group=CommandGroup.BITMAP)
    async def setbit(self, key: KeyT, offset: int, value: int) -> int:
        """
        Flag the :paramref:`offset` in :paramref:`key` as :paramref:`value`.
        """
        value = value and 1 or 0

        return await self.execute_command(
            CommandName.SETBIT, key, offset, value, callback=IntCallback()
        )

    @redis_command(
        CommandName.PUBLISH,
        group=CommandGroup.PUBSUB,
    )
    async def publish(self, channel: StringT, message: ValueT) -> int:
        """
        Publish :paramref:`message` on :paramref:`channel`.

        :return: the number of subscribers the message was delivered to.
        """

        return await self.execute_command(
            CommandName.PUBLISH, channel, message, callback=IntCallback()
        )

    @versionadded(version="3.6.0")
    @redis_command(
        CommandName.SPUBLISH, group=CommandGroup.PUBSUB, version_introduced="7.0.0"
    )
    async def spublish(self, channel: StringT, message: ValueT) -> int:
        """
        Publish :paramref:`message` on shard :paramref:`channel`.

        :return: the number of shard subscribers the message was delivered to.

        .. note:: The number only represents subscribers listening to the exact
           node the message was published to, which means that if a subscriber
           is listening on a replica node, it will not be included in the count.
        """

        return await self.execute_command(
            CommandName.SPUBLISH, channel, message, callback=IntCallback()
        )

    @redis_command(
        CommandName.PUBSUB_CHANNELS,
        group=CommandGroup.PUBSUB,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeSets(),
        ),
    )
    async def pubsub_channels(self, pattern: Optional[StringT] = None) -> Set[AnyStr]:
        """
        Return channels that have at least one subscriber
        """

        return await self.execute_command(
            CommandName.PUBSUB_CHANNELS,
            pattern or b"*",
            callback=SetCallback[AnyStr](),
        )

    @versionadded(version="3.6.0")
    @redis_command(
        CommandName.PUBSUB_SHARDCHANNELS,
        group=CommandGroup.PUBSUB,
        version_introduced="7.0.0",
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeSets(),
        ),
    )
    async def pubsub_shardchannels(
        self, pattern: Optional[StringT] = None
    ) -> Set[AnyStr]:
        """
        Return shard channels that have at least one subscriber
        """

        return await self.execute_command(
            CommandName.PUBSUB_SHARDCHANNELS,
            pattern or b"*",
            callback=SetCallback[AnyStr](),
        )

    @redis_command(
        CommandName.PUBSUB_NUMPAT,
        group=CommandGroup.PUBSUB,
    )
    async def pubsub_numpat(self) -> int:
        """
        Get the count of unique patterns pattern subscriptions

        :return: the number of patterns all the clients are subscribed to.
        """

        return await self.execute_command(
            CommandName.PUBSUB_NUMPAT, callback=IntCallback()
        )

    @redis_command(
        CommandName.PUBSUB_NUMSUB,
        group=CommandGroup.PUBSUB,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeMapping[AnyStr, int](value_combine=sum),
        ),
    )
    async def pubsub_numsub(self, *channels: StringT) -> Dict[AnyStr, int]:
        """
        Get the count of subscribers for channels

        :return: Mapping of channels to number of subscribers per channel
        """
        pieces: CommandArgList = []

        if channels:
            pieces.extend(channels)

        return await self.execute_command(
            CommandName.PUBSUB_NUMSUB,
            *pieces,
            callback=DictCallback[AnyStr, int](),
        )

    @redis_command(
        CommandName.PUBSUB_SHARDNUMSUB,
        group=CommandGroup.PUBSUB,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeMapping[AnyStr, int](value_combine=sum),
        ),
    )
    async def pubsub_shardnumsub(self, *channels: StringT) -> Dict[AnyStr, int]:
        """
        Get the count of subscribers for shard channels

        :return: Ordered mapping of shard channels to number of subscribers per channel
        """
        pieces: CommandArgList = []

        if channels:
            pieces.extend(channels)

        return await self.execute_command(
            CommandName.PUBSUB_SHARDNUMSUB,
            *pieces,
            callback=DictCallback[AnyStr, int](),
        )

    async def _eval(
        self,
        command: Literal[CommandName.EVAL, CommandName.EVAL_RO],
        script: ValueT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        _keys: List[KeyT] = list(keys) if keys else []
        pieces: CommandArgList = [script, len(_keys), *_keys]

        if args:
            pieces.extend(args)

        return await self.execute_command(
            command, *pieces, callback=NoopCallback[ResponseType]()
        )

    @redis_command(
        CommandName.EVAL,
        group=CommandGroup.SCRIPTING,
    )
    async def eval(
        self,
        script: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Execute the Lua :paramref:`script` with the key names and argument values
        in :paramref:`keys` and :paramref:`args`.

        :return: The result of the script as redis returns it
        """

        return await self._eval(CommandName.EVAL, script, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EVAL_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        readonly=True,
    )
    async def eval_ro(
        self,
        script: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Read-only variant of :meth:`~Redis.eval` that cannot execute commands
        that modify data.

        :return: The result of the script as redis returns it
        """

        return await self._eval(CommandName.EVAL_RO, script, keys, args)

    async def _evalsha(
        self,
        command: Literal[CommandName.EVALSHA, CommandName.EVALSHA_RO],
        sha1: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        _keys: List[KeyT] = list(keys) if keys else []
        pieces: CommandArgList = [sha1, len(_keys), *_keys]

        if args:
            pieces.extend(args)

        return await self.execute_command(
            command, *pieces, callback=NoopCallback[ResponseType]()
        )

    @redis_command(CommandName.EVALSHA, group=CommandGroup.SCRIPTING)
    async def evalsha(
        self,
        sha1: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Execute the Lua script cached by it's :paramref:`sha` ref with the
        key names and argument values in :paramref:`keys` and :paramref:`args`.
        Evaluate a script from the server's cache by its :paramref:`sha1` digest.

        :return: The result of the script as redis returns it
        """

        return await self._evalsha(CommandName.EVALSHA, sha1, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EVALSHA_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        readonly=True,
    )
    async def evalsha_ro(
        self,
        sha1: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Read-only variant of :meth:`~Redis.evalsha` that cannot execute commands
        that modify data.

        :return: The result of the script as redis returns it
        """

        return await self._evalsha(CommandName.EVALSHA_RO, sha1, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SCRIPT_DEBUG,
        group=CommandGroup.SCRIPTING,
    )
    async def script_debug(
        self,
        mode: Literal[PureToken.NO, PureToken.SYNC, PureToken.YES],
    ) -> bool:
        """
        Set the debug mode for executed scripts

        :raises: :exc:`NotImplementedError`
        """

        raise NotImplementedError()

    @redis_command(
        CommandName.SCRIPT_EXISTS,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterAlignedBoolsCombine(),
        ),
    )
    async def script_exists(self, sha1s: Parameters[StringT]) -> Tuple[bool, ...]:
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as :paramref:`sha1s`.

        :return: tuple of boolean values indicating if each script
         exists in the cache.
        """

        return await self.execute_command(
            CommandName.SCRIPT_EXISTS, *sha1s, callback=BoolsCallback()
        )

    @redis_command(
        CommandName.SCRIPT_FLUSH,
        group=CommandGroup.SCRIPTING,
        arguments={"sync_type": {"version_introduced": "6.2.0"}},
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    async def script_flush(
        self,
        sync_type: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = None,
    ) -> bool:
        """
        Flushes all scripts from the script cache
        """
        pieces: CommandArgList = []

        if sync_type:
            pieces = [sync_type]

        return await self.execute_command(
            CommandName.SCRIPT_FLUSH, *pieces, callback=BoolCallback()
        )

    @redis_command(
        CommandName.SCRIPT_KILL,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterFirstNonException[bool](),
        ),
    )
    async def script_kill(self) -> bool:
        """
        Kills the currently executing Lua script
        """

        return await self.execute_command(
            CommandName.SCRIPT_KILL, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.SCRIPT_LOAD,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def script_load(self, script: StringT) -> AnyStr:
        """
        Loads a Lua :paramref:`script` into the script cache.

        :return: The SHA1 digest of the script added into the script cache
        """

        return await self.execute_command(
            CommandName.SCRIPT_LOAD, script, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FCALL,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
    )
    async def fcall(
        self,
        function: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Invoke a function
        """
        _keys: List[KeyT] = list(keys or [])
        pieces: CommandArgList = [function, len(_keys), *_keys, *(args or [])]

        return await self.execute_command(
            CommandName.FCALL, *pieces, callback=NoopCallback[ResponseType]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FCALL_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        readonly=True,
    )
    async def fcall_ro(
        self,
        function: StringT,
        keys: Optional[Parameters[KeyT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> ResponseType:
        """
        Read-only variant of :meth:`~coredis.Redis.fcall`
        """
        _keys: List[KeyT] = list(keys or [])
        pieces: CommandArgList = [function, len(_keys), *_keys, *(args or [])]

        return await self.execute_command(
            CommandName.FCALL_RO, *pieces, callback=NoopCallback[ResponseType]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_DELETE,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def function_delete(self, library_name: StringT) -> bool:
        """
        Delete a library and all its functions.
        """

        return await self.execute_command(
            CommandName.FUNCTION_DELETE, library_name, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_DUMP,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def function_dump(self) -> bytes:
        """
        Dump all functions into a serialized binary payload
        """

        return await self.execute_command(
            CommandName.FUNCTION_DUMP, decode=False, callback=NoopCallback[bytes]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_FLUSH,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def function_flush(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = None
    ) -> bool:
        """
        Delete all functions
        """
        pieces: CommandArgList = []

        if async_ is not None:
            pieces.append(async_)

        return await self.execute_command(
            CommandName.FUNCTION_FLUSH, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_KILL,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterFirstNonException[bool](),
        ),
    )
    async def function_kill(self) -> bool:
        """
        Kill the function currently in execution.
        """

        return await self.execute_command(
            CommandName.FUNCTION_KILL, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_LIST,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def function_list(
        self, libraryname: Optional[StringT] = None, withcode: Optional[bool] = None
    ) -> Mapping[str, LibraryDefinition]:
        """
        List information about the functions registered under
        :paramref:`libraryname`
        """
        pieces: CommandArgList = []

        if libraryname is not None:
            pieces.extend(["LIBRARYNAME", libraryname])

        if withcode:
            pieces.append(PureToken.WITHCODE)

        return await self.execute_command(
            CommandName.FUNCTION_LIST, *pieces, callback=FunctionListCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_LOAD,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def function_load(
        self,
        function_code: StringT,
        replace: Optional[bool] = None,
    ) -> AnyStr:
        """
        Load a library of functions.
        """
        pieces: CommandArgList = []

        if replace:
            pieces.append(PureToken.REPLACE)

        pieces.append(function_code)

        return await self.execute_command(
            CommandName.FUNCTION_LOAD, *pieces, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_RESTORE,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def function_restore(
        self,
        serialized_value: bytes,
        policy: Optional[
            Literal[PureToken.FLUSH, PureToken.APPEND, PureToken.REPLACE]
        ] = None,
    ) -> bool:
        """
        Restore all the functions on the given payload
        """
        pieces: CommandArgList = [serialized_value]

        if policy is not None:
            pieces.append(policy)

        return await self.execute_command(
            CommandName.FUNCTION_RESTORE, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_STATS,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    async def function_stats(
        self,
    ) -> Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]]:
        """
        Return information about the function currently running
        """

        return await self.execute_command(
            CommandName.FUNCTION_STATS, callback=FunctionStatsCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BGREWRITEAOF,
        group=CommandGroup.CONNECTION,
    )
    async def bgrewriteaof(self) -> bool:
        """Tell the Redis server to rewrite the AOF file from data in memory"""

        return await self.execute_command(
            CommandName.BGREWRITEAOF, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.BGSAVE,
        group=CommandGroup.CONNECTION,
    )
    async def bgsave(self, schedule: Optional[bool] = None) -> bool:
        """
        Tells the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        pieces: CommandArgList = []

        if schedule:
            pieces.append(PureToken.SCHEDULE)

        return await self.execute_command(
            CommandName.BGSAVE,
            *pieces,
            callback=SimpleStringCallback(
                ok_values={"Background saving started", "Background saving scheduled"}
            ),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_CACHING,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_caching(self, mode: Literal[PureToken.NO, PureToken.YES]) -> bool:
        """
        Instruct the server about tracking or not keys in the next request
        """
        pieces: CommandArgList = [mode]

        return await self.execute_command(
            CommandName.CLIENT_CACHING, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLIENT_KILL,
        group=CommandGroup.CONNECTION,
        arguments={"laddr": {"version_introduced": "6.2.0"}},
    )
    async def client_kill(
        self,
        ip_port: Optional[StringT] = None,
        identifier: Optional[int] = None,
        type_: Optional[
            Literal[
                PureToken.NORMAL,
                PureToken.MASTER,
                PureToken.SLAVE,
                PureToken.REPLICA,
                PureToken.PUBSUB,
            ]
        ] = None,
        user: Optional[StringT] = None,
        addr: Optional[StringT] = None,
        laddr: Optional[StringT] = None,
        skipme: Optional[bool] = None,
    ) -> Union[int, bool]:
        """
        Disconnects the client at :paramref:`ip_port`

        :return: ``True`` if the connection exists and has been closed
         or the number of clients killed.
        """

        pieces: CommandArgList = []

        if ip_port:
            pieces.append(ip_port)

        if identifier:
            pieces.extend(["ID", identifier])

        if type_:
            pieces.extend(["TYPE", type_])

        if user:
            pieces.extend(["USER", user])

        if addr:
            pieces.extend(["ADDR", addr])

        if laddr:
            pieces.extend(["LADDR", laddr])

        if skipme is not None:
            pieces.extend(["SKIPME", skipme and "yes" or "no"])

        return await self.execute_command(
            CommandName.CLIENT_KILL, *pieces, callback=SimpleStringOrIntCallback()
        )

    @redis_command(
        CommandName.CLIENT_LIST,
        group=CommandGroup.CONNECTION,
        arguments={
            "identifiers": {"version_introduced": "6.2.0"},
        },
    )
    async def client_list(
        self,
        type_: Optional[
            Literal[
                PureToken.MASTER, PureToken.NORMAL, PureToken.PUBSUB, PureToken.REPLICA
            ]
        ] = None,
        identifiers: Optional[Parameters[int]] = None,
    ) -> Tuple[ClientInfo, ...]:
        """
        Get client connections

        :return: a tuple of dictionaries containing client fields
        """

        pieces: CommandArgList = []

        if type_:
            pieces.extend(["TYPE", type_])

        if identifiers is not None:
            pieces.append("ID")
            pieces.extend(identifiers)

        return await self.execute_command(
            CommandName.CLIENT_LIST, *pieces, callback=ClientListCallback()
        )

    @redis_command(
        CommandName.CLIENT_GETNAME,
        group=CommandGroup.CONNECTION,
    )
    async def client_getname(self) -> Optional[AnyStr]:
        """
        Returns the current connection name

        :return: The connection name, or ``None`` if no name is set.
        """

        return await self.execute_command(
            CommandName.CLIENT_GETNAME, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLIENT_SETNAME,
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.client_name` argument when initializing the client "
                "to ensure the client name is consistent across all connections originating "
                "from the client"
            ),
            True,
        ),
    )
    async def client_setname(self, connection_name: StringT) -> bool:
        """
        Set the current connection name
        :return: If the connection name was successfully set.
        """

        return await self.execute_command(
            CommandName.CLIENT_SETNAME, connection_name, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLIENT_PAUSE,
        group=CommandGroup.CONNECTION,
        arguments={"mode": {"version_introduced": "6.2.0"}},
    )
    async def client_pause(
        self,
        timeout: int,
        mode: Optional[Literal[PureToken.WRITE, PureToken.ALL]] = None,
    ) -> bool:
        """
        Stop processing commands from clients for some time

        :return: The command returns ``True`` or raises an error if the timeout is invalid.
        """

        pieces: CommandArgList = [timeout]

        if mode is not None:
            pieces.append(mode)

        return await self.execute_command(
            CommandName.CLIENT_PAUSE, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_UNPAUSE,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_unpause(self) -> bool:
        """
        Resume processing of clients that were paused

        :return: The command returns ```True```
        """

        return await self.execute_command(
            CommandName.CLIENT_UNPAUSE, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLIENT_UNBLOCK, group=CommandGroup.CONNECTION)
    async def client_unblock(
        self,
        client_id: int,
        timeout_error: Optional[Literal[PureToken.TIMEOUT, PureToken.ERROR]] = None,
    ) -> bool:
        """
        Unblock a client blocked in a blocking command from a different connection

        :return: Whether the client was unblocked
        """
        pieces: CommandArgList = [client_id]

        if timeout_error is not None:
            pieces.append(timeout_error)

        return await self.execute_command(
            CommandName.CLIENT_UNBLOCK, *pieces, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_GETREDIR,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_getredir(self) -> int:
        """
        Get tracking notifications redirection client ID if any

        :return: the ID of the client we are redirecting the notifications to.
         The command returns ``-1`` if client tracking is not enabled,
         or ``0`` if client tracking is enabled but we are not redirecting the
         notifications to any client.
        """

        return await self.execute_command(
            CommandName.CLIENT_GETREDIR, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLIENT_ID, group=CommandGroup.CONNECTION)
    async def client_id(self) -> int:
        """
        Returns the client ID for the current connection

        :return: The id of the client.
        """

        return await self.execute_command(CommandName.CLIENT_ID, callback=IntCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_INFO,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_info(self) -> ClientInfo:
        """
        Returns information about the current client connection.
        """

        return await self.execute_command(
            CommandName.CLIENT_INFO, callback=ClientInfoCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_REPLY,
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.noreply` argument when initializing the client "
                "to ensure that all connections originating from this client disable "
                "or enable replies. You can also use the :meth:`Redis.ignore_reply` "
                "context manager to selectively execute certain commands without waiting "
                "for a reply"
            ),
            False,
        ),
    )
    async def client_reply(
        self, mode: Literal[PureToken.OFF, PureToken.ON, PureToken.SKIP]
    ) -> bool:
        """
        Instruct the server whether to reply to commands
        """
        raise NotImplementedError()

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_TRACKING,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_tracking(
        self,
        status: Literal[PureToken.OFF, PureToken.ON],
        *prefixes: StringT,
        redirect: Optional[int] = None,
        bcast: Optional[bool] = None,
        optin: Optional[bool] = None,
        optout: Optional[bool] = None,
        noloop: Optional[bool] = None,
    ) -> bool:

        """
        Enable or disable server assisted client side caching support

        :return: If the connection was successfully put in tracking mode or if the
         tracking mode was successfully disabled.
        """

        pieces: CommandArgList = [status]

        if prefixes:
            pieces.extend(
                itertools.chain(*zip([PrefixToken.PREFIX] * len(prefixes), prefixes))
            )
        if redirect is not None:
            pieces.extend([PrefixToken.REDIRECT, redirect])

        if bcast is not None:
            pieces.append(PureToken.BCAST)

        if optin is not None:
            pieces.append(PureToken.OPTIN)

        if optout is not None:
            pieces.append(PureToken.OPTOUT)

        if noloop is not None:
            pieces.append(PureToken.NOLOOP)

        return await self.execute_command(
            CommandName.CLIENT_TRACKING, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_TRACKINGINFO,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_trackinginfo(
        self,
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:
        """
        Return information about server assisted client side caching for the current connection

        :return: a mapping of tracking information sections and their respective values
        """

        return await self.execute_command(
            CommandName.CLIENT_TRACKINGINFO,
            callback=ClientTrackingInfoCallback[AnyStr](),
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.CLIENT_NO_EVICT,
        version_introduced="7.0.0",
        group=CommandGroup.CONNECTION,
    )
    async def client_no_evict(
        self, enabled: Literal[PureToken.ON, PureToken.OFF]
    ) -> bool:
        """
        Set client eviction mode for the current connection
        """
        return await self.execute_command(
            CommandName.CLIENT_NO_EVICT, enabled, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.DBSIZE,
        readonly=True,
        group=CommandGroup.SERVER,
    )
    async def dbsize(self) -> int:
        """Returns the number of keys in the current database"""

        return await self.execute_command(CommandName.DBSIZE, callback=IntCallback())

    @redis_command(
        CommandName.DEBUG_OBJECT,
        group=CommandGroup.SERVER,
    )
    async def debug_object(self, key: KeyT) -> Dict[str, Union[str, int]]:
        """Returns version specific meta information about a given key"""

        return await self.execute_command(
            CommandName.DEBUG_OBJECT, key, callback=DebugCallback()
        )

    @mutually_inclusive_parameters("host", "port")
    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.FAILOVER,
        version_introduced="6.2.0",
        group=CommandGroup.SERVER,
    )
    async def failover(
        self,
        host: Optional[StringT] = None,
        port: Optional[int] = None,
        force: Optional[bool] = None,
        abort: Optional[bool] = None,
        timeout: Optional[Union[int, datetime.timedelta]] = None,
    ) -> bool:
        """
        Start a coordinated failover between this server and one of its replicas.

        :return: `True` if the command was accepted and a coordinated failover
         is in progress.
        """
        pieces: CommandArgList = []

        if host and port:
            pieces.extend([PrefixToken.TO, host, port])

            if force is not None:
                pieces.append(PureToken.FORCE)

        if abort:
            pieces.append(PureToken.ABORT)

        if timeout is not None:
            pieces.append(PrefixToken.TIMEOUT)
            pieces.append(normalized_milliseconds(timeout))

        return await self.execute_command(
            CommandName.FAILOVER, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.FLUSHALL,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def flushall(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = None
    ) -> bool:
        """Deletes all keys in all databases on the current host"""
        pieces: CommandArgList = []

        if async_:
            pieces.append(async_)

        return await self.execute_command(
            CommandName.FLUSHALL, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.FLUSHDB,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def flushdb(
        self, async_: Optional[Literal[PureToken.ASYNC, PureToken.SYNC]] = None
    ) -> bool:
        """Deletes all keys in the current database"""
        pieces: CommandArgList = []

        if async_:
            pieces.append(async_)

        return await self.execute_command(
            CommandName.FLUSHDB, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.INFO,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def info(self, *sections: StringT) -> Dict[str, ResponseType]:
        """
        Get information and statistics about the server

        The :paramref:`sections` option can be used to select a specific section(s)
        of information
        """

        return await self.execute_command(
            CommandName.INFO, *sections, callback=InfoCallback()
        )

    @redis_command(
        CommandName.LASTSAVE,
        group=CommandGroup.SERVER,
    )
    async def lastsave(self) -> datetime.datetime:
        """
        Get the time of the last successful save to disk
        """

        return await self.execute_command(
            CommandName.LASTSAVE, callback=DateTimeCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_DOCTOR, group=CommandGroup.SERVER)
    async def latency_doctor(self) -> AnyStr:
        """
        Return a human readable latency analysis report.
        """

        return await self.execute_command(
            CommandName.LATENCY_DOCTOR, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_GRAPH, group=CommandGroup.SERVER)
    async def latency_graph(self, event: StringT) -> AnyStr:
        """
        Return a latency graph for the event.
        """

        return await self.execute_command(
            CommandName.LATENCY_GRAPH, event, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.LATENCY_HISTOGRAM,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
    )
    async def latency_histogram(
        self, *commands: Union[str, bytes]
    ) -> Dict[AnyStr, Dict[AnyStr, ValueT]]:
        """
        Return the cumulative distribution of latencies of a subset of commands or all.
        """

        return await self.execute_command(
            CommandName.LATENCY_HISTOGRAM,
            *commands,
            callback=LatencyHistogramCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LATENCY_HISTORY,
        group=CommandGroup.SERVER,
    )
    async def latency_history(self, event: StringT) -> Tuple[AnyStr, ...]:
        """
        Return timestamp-latency samples for the event.

        :return: The command returns a tuple where each element is a tuple
         representing the timestamp and the latency of the event.

        """
        pieces: CommandArgList = [event]

        return await self.execute_command(
            CommandName.LATENCY_HISTORY, *pieces, callback=TupleCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LATENCY_LATEST,
        group=CommandGroup.SERVER,
    )
    async def latency_latest(self) -> Dict[AnyStr, Tuple[int, int, int]]:
        """
        Return the latest latency samples for all events.

        :return: Mapping of event name to (timestamp, latest, all-time) triplet
        """

        return await self.execute_command(
            CommandName.LATENCY_LATEST,
            callback=LatencyCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_RESET, group=CommandGroup.SERVER)
    async def latency_reset(self, *events: StringT) -> int:
        """
        Reset latency data for one or more events.

        :return: the number of event time series that were reset.
        """
        pieces: CommandArgList = list(events) if events else []

        return await self.execute_command(
            CommandName.LATENCY_RESET, *pieces, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.MEMORY_DOCTOR, group=CommandGroup.SERVER)
    async def memory_doctor(self) -> AnyStr:
        """
        Outputs memory problems report
        """

        return await self.execute_command(
            CommandName.MEMORY_DOCTOR, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.MEMORY_MALLOC_STATS, group=CommandGroup.SERVER)
    async def memory_malloc_stats(self) -> AnyStr:
        """
        Show allocator internal stats
        :return: the memory allocator's internal statistics report
        """

        return await self.execute_command(
            CommandName.MEMORY_MALLOC_STATS, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.MEMORY_PURGE,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.ALL, combine=ClusterBoolCombine()),
    )
    async def memory_purge(self) -> bool:
        """
        Ask the allocator to release memory
        """

        return await self.execute_command(
            CommandName.MEMORY_PURGE, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.MEMORY_STATS,
        group=CommandGroup.SERVER,
    )
    async def memory_stats(self) -> Dict[AnyStr, ValueT]:
        """
        Show memory usage details
        :return: mapping of memory usage metrics and their values

        """

        return await self.execute_command(
            CommandName.MEMORY_STATS,
            callback=DictCallback[AnyStr, ValueT](),
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.MEMORY_USAGE, readonly=True, group=CommandGroup.SERVER)
    async def memory_usage(
        self, key: KeyT, *, samples: Optional[int] = None
    ) -> Optional[int]:
        """
        Estimate the memory usage of a key

        :return: the memory usage in bytes, or ``None`` when the key does not exist.

        """
        pieces: CommandArgList = []
        pieces.append(key)

        if samples is not None:
            pieces.extend([PrefixToken.SAMPLES, samples])

        return await self.execute_command(
            CommandName.MEMORY_USAGE, *pieces, callback=OptionalIntCallback()
        )

    @redis_command(
        CommandName.SAVE,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL, combine=ClusterEnsureConsistent()
        ),
    )
    async def save(self) -> bool:
        """
        Tells the Redis server to save its data to disk,
        blocking until the save is complete
        """

        return await self.execute_command(
            CommandName.SAVE, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.SHUTDOWN,
        group=CommandGroup.SERVER,
        arguments={
            "now": {"version_introduced": "7.0.0"},
            "force": {"version_introduced": "7.0.0"},
            "abort": {"version_introduced": "7.0.0"},
        },
    )
    async def shutdown(
        self,
        nosave_save: Optional[Literal[PureToken.NOSAVE, PureToken.SAVE]] = None,
        *,
        now: Optional[bool] = None,
        force: Optional[bool] = None,
        abort: Optional[bool] = None,
    ) -> bool:
        """Stops Redis server"""
        pieces: CommandArgList = []

        if nosave_save:
            pieces.append(nosave_save)

        if now is not None:
            pieces.append(PureToken.NOW)
        if force is not None:
            pieces.append(PureToken.FORCE)
        if abort is not None:
            pieces.append(PureToken.ABORT)

        try:
            response = await self.execute_command(
                CommandName.SHUTDOWN, *pieces, callback=SimpleStringCallback()
            )
            if abort is not None:
                return response
        except ConnectionError:
            # a ConnectionError here is expected
            return True

        if abort is None:
            raise RedisError("Unexpected error performing shutdown")

    @redis_command(
        CommandName.SLAVEOF,
        version_deprecated="5.0.0",
        deprecation_reason="Use :meth:`replicaof`",
        group=CommandGroup.SERVER,
    )
    async def slaveof(
        self, host: Optional[StringT] = None, port: Optional[int] = None
    ) -> bool:
        """
        Sets the server to be a replicated slave of the instance identified
        by the :paramref:`host` and :paramref:`port`.
        If called without arguments, the instance is promoted to a master instead.
        """

        if host is None and port is None:
            return await self.execute_command(
                CommandName.SLAVEOF, b"NO", b"ONE", callback=SimpleStringCallback()
            )
        assert host and port
        return await self.execute_command(
            CommandName.SLAVEOF, host, port, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.SLOWLOG_GET,
        group=CommandGroup.SERVER,
    )
    async def slowlog_get(self, count: Optional[int] = None) -> Tuple[SlowLogInfo, ...]:
        """
        Gets the entries from the slowlog. If :paramref:`count` is specified, get the
        most recent :paramref:`count` items.
        """
        pieces: CommandArgList = []

        if count is not None:
            pieces.append(count)

        return await self.execute_command(
            CommandName.SLOWLOG_GET, *pieces, callback=SlowlogCallback()
        )

    @redis_command(CommandName.SLOWLOG_LEN, group=CommandGroup.SERVER)
    async def slowlog_len(self) -> int:
        """Gets the number of items in the slowlog"""

        return await self.execute_command(
            CommandName.SLOWLOG_LEN, callback=IntCallback()
        )

    @redis_command(
        CommandName.SLOWLOG_RESET,
        group=CommandGroup.SERVER,
    )
    async def slowlog_reset(self) -> bool:
        """Removes all items in the slowlog"""

        return await self.execute_command(
            CommandName.SLOWLOG_RESET, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.TIME,
        group=CommandGroup.SERVER,
    )
    async def time(self) -> datetime.datetime:
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """

        return await self.execute_command(CommandName.TIME, callback=TimeCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.REPLICAOF,
        group=CommandGroup.SERVER,
    )
    async def replicaof(
        self, host: Optional[StringT] = None, port: Optional[int] = None
    ) -> bool:
        """
        Make the server a replica of another instance, or promote it as master.
        """

        if host is None and port is None:
            return await self.execute_command(
                CommandName.REPLICAOF, b"NO", b"ONE", callback=SimpleStringCallback()
            )
        assert host and port
        return await self.execute_command(
            CommandName.REPLICAOF, host, port, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.ROLE,
        group=CommandGroup.SERVER,
    )
    async def role(self) -> RoleInfo:
        """
        Provides information on the role of a Redis instance in the context of replication,
        by returning if the instance is currently a master, slave, or sentinel.
        The command also returns additional information about the state of the replication
        (if the role is master or slave)
        or the list of monitored master names (if the role is sentinel).
        """

        return await self.execute_command(CommandName.ROLE, callback=RoleCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SWAPDB,
        group=CommandGroup.SERVER,
    )
    async def swapdb(self, index1: int, index2: int) -> bool:
        """
        Swaps two Redis databases
        """
        pieces: CommandArgList = [index1, index2]

        return await self.execute_command(
            CommandName.SWAPDB, *pieces, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.LOLWUT, readonly=True, group=CommandGroup.SERVER)
    async def lolwut(self, version: Optional[int] = None) -> AnyStr:
        """
        Get the Redis version and a piece of generative computer art
        """
        pieces: CommandArgList = []

        if version is not None:
            pieces.append(version)

        return await self.execute_command(
            CommandName.LOLWUT, *pieces, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_CAT,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def acl_cat(
        self, categoryname: Optional[StringT] = None
    ) -> Tuple[AnyStr, ...]:
        """
        List the ACL categories or the commands inside a category


        :return: a list of ACL categories or a list of commands inside a given category.
         The command may return an error if an invalid category name is given as argument.

        """

        pieces: CommandArgList = []

        if categoryname:
            pieces.append(categoryname)

        return await self.execute_command(
            CommandName.ACL_CAT, *pieces, callback=TupleCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_DELUSER,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def acl_deluser(self, usernames: Parameters[StringT]) -> int:
        """
        Remove the specified ACL users and the associated rules


        :return: The number of users that were deleted.
         This number will not always match the number of arguments since
         certain users may not exist.
        """

        return await self.execute_command(
            CommandName.ACL_DELUSER, *usernames, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_DRYRUN,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    async def acl_dryrun(
        self, username: StringT, command: StringT, *args: ValueT
    ) -> bool:
        """
        Returns whether the user can execute the given command without executing the command.
        """
        pieces: CommandArgList = [username, command]

        if args:
            pieces.extend(args)

        return await self.execute_command(
            CommandName.ACL_DRYRUN,
            *pieces,
            callback=SimpleStringCallback(AuthorizationError),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_GENPASS,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def acl_genpass(self, bits: Optional[int] = None) -> AnyStr:
        """
        Generate a pseudorandom secure password to use for ACL users


        :return: by default 64 bytes string representing 256 bits of pseudorandom data.
         Otherwise if an argument if needed, the output string length is the number of
         specified bits (rounded to the next multiple of 4) divided by 4.
        """
        pieces: CommandArgList = []

        if bits is not None:
            pieces.append(bits)

        return await self.execute_command(
            CommandName.ACL_GENPASS, *pieces, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_GETUSER,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    async def acl_getuser(self, username: StringT) -> Dict[AnyStr, List[AnyStr]]:
        """
        Get the rules for a specific ACL user
        """

        return await self.execute_command(
            CommandName.ACL_GETUSER,
            username,
            callback=DictCallback[AnyStr, List[AnyStr]](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_LIST,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def acl_list(self) -> Tuple[AnyStr, ...]:
        """
        List the current ACL rules in ACL config file format
        """

        return await self.execute_command(
            CommandName.ACL_LIST, callback=TupleCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_LOAD,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL, combine=ClusterEnsureConsistent()
        ),
    )
    async def acl_load(self) -> bool:
        """
        Reload the ACLs from the configured ACL file

        :return: True if successful. The command may fail with an error for several reasons:

         - if the file is not readable
         - if there is an error inside the file, and in such case the error will be reported to
           the user in the error.
         - Finally the command will fail if the server is not configured to use an external
           ACL file.

        """

        return await self.execute_command(
            CommandName.ACL_LOAD, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @mutually_exclusive_parameters("count", "reset")
    @redis_command(
        CommandName.ACL_LOG,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
    )
    async def acl_log(
        self, count: Optional[int] = None, reset: Optional[bool] = None
    ) -> Union[bool, Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...]]:
        """
        List latest events denied because of ACLs in place

        :return: When called to show security events a list of ACL security events.
         When called with ``RESET`` True if the security log was cleared.

        """

        pieces: CommandArgList = []

        if count is not None:
            pieces.append(count)

        if reset is not None:
            pieces.append(PureToken.RESET)

        if reset:
            return await self.execute_command(
                CommandName.ACL_LOG, *pieces, callback=SimpleStringCallback()
            )
        else:
            return await self.execute_command(
                CommandName.ACL_LOG, *pieces, callback=ACLLogCallback[AnyStr]()
            )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_SAVE,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def acl_save(self) -> bool:
        """
        Save the current ACL rules in the configured ACL file

        :return: True if successful. The command may fail with an error for several reasons:
         - if the file cannot be written, or
         - if the server is not configured to use an external ACL file.

        """

        return await self.execute_command(
            CommandName.ACL_SAVE, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_SETUSER,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    async def acl_setuser(
        self,
        username: StringT,
        *rules: StringT,
    ) -> bool:
        """
        Modify or create the rules for a specific ACL user


        :return: True if successful. If the rules contain errors, the error is returned.
        """
        pieces: CommandArgList = []

        if rules:
            pieces.extend(rules)

        return await self.execute_command(
            CommandName.ACL_SETUSER, username, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_USERS,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def acl_users(self) -> Tuple[AnyStr, ...]:
        """
        List the username of all the configured ACL rules
        """

        return await self.execute_command(
            CommandName.ACL_USERS, callback=TupleCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_WHOAMI,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def acl_whoami(self) -> AnyStr:
        """
        Return the name of the user associated to the current connection


        :return: the username of the current connection.
        """

        return await self.execute_command(
            CommandName.ACL_WHOAMI, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command(self) -> Dict[str, Command]:
        """
        Get Redis command details

        :return: Mapping of command details.  Commands are returned
         in random order.
        """

        return await self.execute_command(
            CommandName.COMMAND, callback=CommandCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND_COUNT,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_count(self) -> int:
        """
        Get total number of Redis commands

        :return: number of commands returned by ``COMMAND``
        """

        return await self.execute_command(
            CommandName.COMMAND_COUNT, callback=IntCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.COMMAND_DOCS,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_docs(
        self, *command_names: StringT
    ) -> Dict[AnyStr, Dict[AnyStr, ResponseType]]:
        """
        Mapping of commands to a dictionary containing it's documentation
        """

        return await self.execute_command(
            CommandName.COMMAND_DOCS,
            *command_names,
            callback=CommandDocCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND_GETKEYS,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_getkeys(
        self, command: StringT, arguments: Parameters[ValueT]
    ) -> Tuple[AnyStr, ...]:
        """
        Extract keys given a full Redis command

        :return: Keys from your command.
        """

        return await self.execute_command(
            CommandName.COMMAND_GETKEYS,
            command,
            *arguments,
            callback=TupleCallback[AnyStr](),
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.COMMAND_GETKEYSANDFLAGS,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_getkeysandflags(
        self, command: StringT, arguments: Parameters[ValueT]
    ) -> Dict[AnyStr, Set[AnyStr]]:
        """
        Extract keys from a full Redis command and their usage flags.

        :return: Mapping of keys from your command to flags
        """

        return await self.execute_command(
            CommandName.COMMAND_GETKEYSANDFLAGS,
            command,
            *arguments,
            callback=CommandKeyFlagCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND_INFO,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_info(self, *command_names: StringT) -> Dict[str, Command]:
        """
        Get specific Redis command details, or all when no argument is given.

        :return: mapping of command details.

        """
        return await self.execute_command(
            CommandName.COMMAND_INFO, *command_names, callback=CommandCallback()
        )

    @mutually_exclusive_parameters("module", "aclcat", "pattern")
    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.COMMAND_LIST,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    async def command_list(
        self,
        module: Optional[StringT] = None,
        aclcat: Optional[StringT] = None,
        pattern: Optional[StringT] = None,
    ) -> Set[AnyStr]:
        """
        Get an array of Redis command names
        """
        pieces: CommandArgList = []

        if any([module, aclcat, pattern]):
            pieces.append(PrefixToken.FILTERBY)

        if module is not None:
            pieces.extend([PrefixToken.MODULE, module])

        if aclcat is not None:
            pieces.extend([PrefixToken.ACLCAT, aclcat])

        if pattern is not None:
            pieces.extend([PrefixToken.PATTERN, pattern])

        return await self.execute_command(
            CommandName.COMMAND_LIST, *pieces, callback=SetCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CONFIG_GET,
        group=CommandGroup.SERVER,
    )
    async def config_get(self, parameters: Parameters[StringT]) -> Dict[AnyStr, AnyStr]:
        """
        Get the values of configuration parameters
        """

        return await self.execute_command(
            CommandName.CONFIG_GET,
            *parameters,
            callback=DictCallback[AnyStr, AnyStr](),
        )

    @redis_command(
        CommandName.CONFIG_SET,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    async def config_set(self, parameter_values: Dict[StringT, ValueT]) -> bool:
        """Sets configuration parameters to the given values"""

        return await self.execute_command(
            CommandName.CONFIG_SET,
            *itertools.chain(*parameter_values.items()),
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.CONFIG_RESETSTAT,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    async def config_resetstat(self) -> bool:
        """Resets runtime statistics"""

        return await self.execute_command(
            CommandName.CONFIG_RESETSTAT, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.CONFIG_REWRITE, group=CommandGroup.SERVER)
    async def config_rewrite(self) -> bool:
        """
        Rewrites config file with the minimal change to reflect running config
        """

        return await self.execute_command(
            CommandName.CONFIG_REWRITE, callback=SimpleStringCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.MODULE_LIST,
        group=CommandGroup.SERVER,
    )
    async def module_list(self) -> Tuple[Dict[AnyStr, ResponsePrimitive], ...]:
        """
        List all modules loaded by the server

        :return: The loaded modules with each element represents a module
         containing a mapping with ``name`` and ``ver``
        """

        return await self.execute_command(
            CommandName.MODULE_LIST, callback=ModuleInfoCallback[AnyStr]()
        )

    @versionadded(version="3.2.0")
    @redis_command(CommandName.MODULE_LOAD, group=CommandGroup.SERVER)
    async def module_load(
        self, path: Union[str, bytes], *args: Union[str, bytes, int, float]
    ) -> bool:
        """
        Load a module
        """
        pieces: CommandArgList = [path]

        if args:
            pieces.extend(args)

        return await self.execute_command(
            CommandName.MODULE_LOAD, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.4.0")
    @redis_command(
        CommandName.MODULE_LOADEX, group=CommandGroup.SERVER, version_introduced="7.0.0"
    )
    async def module_loadex(
        self,
        path: Union[str, bytes],
        configs: Optional[Dict[StringT, ValueT]] = None,
        args: Optional[Parameters[ValueT]] = None,
    ) -> bool:
        """
        Loads a module from a dynamic library at runtime with configuration directives.
        """
        pieces: CommandArgList = [path]

        if configs:
            for pair in configs.items():
                pieces.append(PrefixToken.CONFIG)
                pieces.extend(pair)
        if args:
            pieces.append(PrefixToken.ARGS)
            pieces.extend(args)
        return await self.execute_command(
            CommandName.MODULE_LOADEX, *pieces, callback=SimpleStringCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(CommandName.MODULE_UNLOAD, group=CommandGroup.SERVER)
    async def module_unload(self, name: Union[str, bytes]) -> bool:
        """
        Unload a module
        """

        return await self.execute_command(
            CommandName.MODULE_UNLOAD, name, callback=SimpleStringCallback()
        )
