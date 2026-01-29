from __future__ import annotations

import datetime
import itertools
from typing import overload

from deprecated.sphinx import versionadded

from coredis._json import json
from coredis._utils import dict_to_flat_list, tuples_to_flat_list
from coredis.commands import CommandMixin
from coredis.commands._utils import (
    normalized_milliseconds,
    normalized_seconds,
    normalized_time_milliseconds,
    normalized_time_seconds,
)
from coredis.commands._validators import (
    ensure_iterable_valid,
    mutually_exclusive_parameters,
    mutually_inclusive_parameters,
)
from coredis.commands._wrappers import (
    ClusterCommandConfig,
    RedirectUsage,
    redis_command,
)
from coredis.commands.bitfield import BitFieldOperation
from coredis.commands.constants import CommandFlag, CommandGroup, CommandName, NodeFlag
from coredis.commands.request import CommandRequest
from coredis.exceptions import (
    AuthorizationError,
    DataError,
    RedisError,
)
from coredis.modules.response._callbacks.json import JsonCallback
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
    ItemOrTupleCallback,
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
    ZRankCallback,
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
from coredis.response._callbacks.vector_sets import (
    VEmbCallback,
    VInfoCallback,
    VLinksCallback,
    VSimCallback,
)
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
    VectorData,
)
from coredis.tokens import PrefixToken, PureToken
from coredis.typing import (
    AnyStr,
    CommandArgList,
    JsonType,
    KeyT,
    Literal,
    Mapping,
    Parameters,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
    StringT,
    ValueT,
)

# TODO: remove this once mypy can disambiguate class method names
#  from builtin types. ``set`` is a redis commands with
#  an associated method that clashes with the set[] type.
_Set = set


class CoreCommands(CommandMixin[AnyStr]):
    @redis_command(CommandName.APPEND, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def append(self, key: KeyT, value: ValueT) -> CommandRequest[int]:
        """
        Append a value to a key

        :return: the length of the string after the append operation.
        """

        return self.create_request(CommandName.APPEND, key, value, callback=IntCallback())

    @redis_command(CommandName.DECR, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def decr(self, key: KeyT) -> CommandRequest[int]:
        """
        Decrement the integer value of a key by one

        :return: the value of :paramref:`key` after the decrement
        """

        return self.decrby(key, 1)

    @redis_command(CommandName.DECRBY, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def decrby(self, key: KeyT, decrement: int) -> CommandRequest[int]:
        """
        Decrement the integer value of a key by the given number

        :return: the value of :paramref:`key` after the decrement
        """

        return self.create_request(CommandName.DECRBY, key, decrement, callback=IntCallback())

    @redis_command(
        CommandName.GET,
        group=CommandGroup.STRING,
        cacheable=True,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def get(self, key: KeyT) -> CommandRequest[AnyStr | None]:
        """
        Get the value of a key

        :return: the value of :paramref:`key`, or ``None`` when :paramref:`key`
         does not exist.
        """

        return self.create_request(
            CommandName.GET,
            key,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.GETDEL,
        group=CommandGroup.STRING,
        version_introduced="6.2.0",
        flags={CommandFlag.FAST},
    )
    def getdel(self, key: KeyT) -> CommandRequest[AnyStr | None]:
        """
        Get the value of a key and delete the key


        :return: the value of :paramref:`key`, ``None`` when :paramref:`key`
         does not exist, or an error if the key's value type isn't a string.
        """

        return self.create_request(
            CommandName.GETDEL, key, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @mutually_exclusive_parameters("ex", "px", "exat", "pxat", "persist")
    @redis_command(
        CommandName.GETEX,
        group=CommandGroup.STRING,
        version_introduced="6.2.0",
        flags={CommandFlag.FAST},
    )
    def getex(
        self,
        key: KeyT,
        ex: int | datetime.timedelta | None = None,
        px: int | datetime.timedelta | None = None,
        exat: int | datetime.datetime | None = None,
        pxat: int | datetime.datetime | None = None,
        persist: bool | None = None,
    ) -> CommandRequest[AnyStr | None]:
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

        command_arguments: CommandArgList = []

        if ex is not None:
            command_arguments.append(PrefixToken.EX)
            command_arguments.append(normalized_seconds(ex))

        if px is not None:
            command_arguments.append(PrefixToken.PX)
            command_arguments.append(normalized_milliseconds(px))

        if exat is not None:
            command_arguments.append(PrefixToken.EXAT)
            command_arguments.append(normalized_time_seconds(exat))

        if pxat is not None:
            command_arguments.append(PrefixToken.PXAT)
            command_arguments.append(normalized_time_milliseconds(pxat))

        if persist:
            command_arguments.append(PureToken.PERSIST)

        return self.create_request(
            CommandName.GETEX,
            key,
            *command_arguments,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.GETRANGE,
        group=CommandGroup.STRING,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def getrange(self, key: KeyT, start: int, end: int) -> CommandRequest[AnyStr]:
        """
        Get a substring of the string stored at a key

        :return: The substring of the string value stored at :paramref:`key`,
         determined by the offsets ``start`` and ``end`` (both are inclusive)
        """

        return self.create_request(
            CommandName.GETRANGE, key, start, end, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.GETSET,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`set` with the get argument",
        group=CommandGroup.STRING,
        flags={CommandFlag.FAST},
    )
    def getset(self, key: KeyT, value: ValueT) -> CommandRequest[AnyStr | None]:
        """
        Set the string value of a key and return its old value

        :return: the old value stored at :paramref:`key`, or ``None`` when
         :paramref:`key` did not exist.
        """

        return self.create_request(
            CommandName.GETSET, key, value, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.INCR, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def incr(self, key: KeyT) -> CommandRequest[int]:
        """
        Increment the integer value of a key by one

        :return: the value of :paramref:`key` after the increment.
         If no key exists, the value will be initialized as 1.
        """

        return self.create_request(
            CommandName.INCR,
            key,
            callback=IntCallback(),
        )

    @redis_command(CommandName.INCRBY, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def incrby(self, key: KeyT, increment: int) -> CommandRequest[int]:
        """
        Increment the integer value of a key by the given amount

        :return: the value of :paramref:`key` after the increment
          If no key exists, the value will be initialized as ``increment``
        """

        return self.create_request(CommandName.INCRBY, key, increment, callback=IntCallback())

    @redis_command(CommandName.INCRBYFLOAT, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def incrbyfloat(self, key: KeyT, increment: int | float) -> CommandRequest[float]:
        """
        Increment the float value of a key by the given amount

        :return: the value of :paramref:`key` after the increment.
         If no key exists, the value will be initialized as ``increment``
        """

        return self.create_request(
            CommandName.INCRBYFLOAT, key, increment, callback=FloatCallback()
        )

    @overload
    def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
    ) -> CommandRequest[AnyStr]: ...

    @overload
    def lcs(self, key1: KeyT, key2: KeyT, *, len_: Literal[True]) -> CommandRequest[int]: ...

    @overload
    def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
        *,
        idx: Literal[True],
        len_: bool | None = ...,
        minmatchlen: int | None = ...,
        withmatchlen: bool | None = ...,
    ) -> CommandRequest[LCSResult]: ...

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LCS,
        version_introduced="7.0.0",
        group=CommandGroup.STRING,
        flags={CommandFlag.READONLY},
    )
    def lcs(
        self,
        key1: KeyT,
        key2: KeyT,
        *,
        len_: bool | None = None,
        idx: bool | None = None,
        minmatchlen: int | None = None,
        withmatchlen: bool | None = None,
    ) -> CommandRequest[AnyStr] | CommandRequest[int] | CommandRequest[LCSResult]:
        """
        Find the longest common substring

        :return: The matched string if no other arguments are given.
         The returned values vary depending on different arguments.

         - If ``len_`` is provided the length of the longest match
         - If ``idx`` is ``True`` all the matches with the start/end positions
           of both keys. Optionally, if ``withmatchlen`` is ``True`` each match
           will contain the length of the match.

        """
        command_arguments: CommandArgList = [key1, key2]

        if len_ is not None:
            command_arguments.append(PureToken.LEN)

        if idx is not None:
            command_arguments.append(PureToken.IDX)

        if minmatchlen is not None:
            command_arguments.extend([PrefixToken.MINMATCHLEN, minmatchlen])

        if withmatchlen is not None:
            command_arguments.append(PureToken.WITHMATCHLEN)
        if idx is not None:
            return self.create_request(
                CommandName.LCS,
                *command_arguments,
                callback=LCSCallback(),
            )
        else:
            if len_ is not None:
                return self.create_request(
                    CommandName.LCS, *command_arguments, callback=IntCallback()
                )
            else:
                return self.create_request(
                    CommandName.LCS,
                    *command_arguments,
                    callback=AnyStrCallback[AnyStr](),
                )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.MGET,
        group=CommandGroup.STRING,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def mget(self, keys: Parameters[KeyT]) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """
        Returns values ordered identically to ``keys``
        """

        return self.create_request(CommandName.MGET, *keys, callback=TupleCallback[AnyStr | None]())

    @redis_command(
        CommandName.MSET,
        group=CommandGroup.STRING,
    )
    def mset(self, key_values: Mapping[KeyT, ValueT]) -> CommandRequest[bool]:
        """
        Sets multiple keys to multiple values
        """

        return self.create_request(
            CommandName.MSET,
            *dict_to_flat_list(key_values),
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.MSETNX, group=CommandGroup.STRING)
    def msetnx(self, key_values: Mapping[KeyT, ValueT]) -> CommandRequest[bool]:
        """
        Set multiple keys to multiple values, only if none of the keys exist

        :return: Whether all the keys were set
        """

        return self.create_request(
            CommandName.MSETNX, *dict_to_flat_list(key_values), callback=BoolCallback()
        )

    @redis_command(
        CommandName.PSETEX,
        group=CommandGroup.STRING,
    )
    def psetex(
        self,
        key: KeyT,
        milliseconds: int | datetime.timedelta,
        value: ValueT,
    ) -> CommandRequest[bool]:
        """
        Set the value and expiration in milliseconds of a key
        """

        return self.create_request(
            CommandName.PSETEX,
            key,
            normalized_milliseconds(milliseconds),
            value,
            callback=SimpleStringCallback(),
        )

    @overload
    def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Literal[PureToken.NX, PureToken.XX] | None = ...,
        ex: int | datetime.timedelta | None = ...,
        px: int | datetime.timedelta | None = ...,
        exat: int | datetime.datetime | None = ...,
        pxat: int | datetime.datetime | None = ...,
        keepttl: bool | None = ...,
    ) -> CommandRequest[bool]: ...

    @overload
    def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Literal[PureToken.NX, PureToken.XX] | None = ...,
        get: Literal[True],
        ex: int | datetime.timedelta | None = ...,
        px: int | datetime.timedelta | None = ...,
        exat: int | datetime.datetime | None = ...,
        pxat: int | datetime.datetime | None = ...,
        keepttl: bool | None = ...,
    ) -> CommandRequest[AnyStr | None]: ...

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
    def set(
        self,
        key: KeyT,
        value: ValueT,
        *,
        condition: Literal[PureToken.NX, PureToken.XX] | None = None,
        get: bool | None = None,
        ex: int | datetime.timedelta | None = None,
        px: int | datetime.timedelta | None = None,
        exat: int | datetime.datetime | None = None,
        pxat: int | datetime.datetime | None = None,
        keepttl: bool | None = None,
    ) -> CommandRequest[AnyStr | bool | None]:
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
        command_arguments: CommandArgList = [key, value]

        if ex is not None:
            command_arguments.append(PrefixToken.EX)
            command_arguments.append(normalized_seconds(ex))

        if px is not None:
            command_arguments.append(PrefixToken.PX)
            command_arguments.append(normalized_milliseconds(px))

        if exat is not None:
            command_arguments.append(PrefixToken.EXAT)
            command_arguments.append(normalized_time_seconds(exat))

        if pxat is not None:
            command_arguments.append(PrefixToken.PXAT)
            command_arguments.append(normalized_time_milliseconds(pxat))

        if keepttl:
            command_arguments.append(PureToken.KEEPTTL)

        if get:
            command_arguments.append(PureToken.GET)

        if condition:
            command_arguments.append(condition)

        return self.create_request(
            CommandName.SET,
            *command_arguments,
            callback=StringSetCallback[AnyStr](get=get),
        )

    @redis_command(
        CommandName.SETEX,
        group=CommandGroup.STRING,
    )
    def setex(
        self,
        key: KeyT,
        value: ValueT,
        seconds: int | datetime.timedelta,
    ) -> CommandRequest[bool]:
        """
        Set the value of key :paramref:`key` to ``value`` that expires in ``seconds``
        """

        return self.create_request(
            CommandName.SETEX,
            key,
            normalized_seconds(seconds),
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.SETNX, group=CommandGroup.STRING, flags={CommandFlag.FAST})
    def setnx(self, key: KeyT, value: ValueT) -> CommandRequest[bool]:
        """
        Sets the value of key :paramref:`key` to ``value`` if key doesn't exist
        """

        return self.create_request(CommandName.SETNX, key, value, callback=BoolCallback())

    @redis_command(CommandName.SETRANGE, group=CommandGroup.STRING)
    def setrange(self, key: KeyT, offset: int, value: ValueT) -> CommandRequest[int]:
        """
        Overwrite bytes in the value of :paramref:`key` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.

        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        :return: the length of the string after it was modified by the command.
        """

        return self.create_request(CommandName.SETRANGE, key, offset, value, callback=IntCallback())

    @redis_command(
        CommandName.STRLEN,
        group=CommandGroup.STRING,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def strlen(self, key: KeyT) -> CommandRequest[int]:
        """
        Get the length of the value stored in a key

        :return: the length of the string at :paramref:`key`, or ``0`` when :paramref:`key` does not
        """

        return self.create_request(CommandName.STRLEN, key, callback=IntCallback())

    @redis_command(
        CommandName.SUBSTR,
        group=CommandGroup.STRING,
        version_deprecated="2.0.0",
        deprecation_reason="Use :meth:`getrange`",
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def substr(self, key: KeyT, start: int, end: int) -> CommandRequest[AnyStr]:
        """
        Get a substring of the string stored at a key

        :return: the substring of the string value stored at key, determined by the offsets
         ``start`` and ``end`` (both are inclusive). Negative offsets can be used in order to
         provide an offset starting from the end of the string.
        """

        return self.create_request(
            CommandName.SUBSTR, key, start, end, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_ADDSLOTS,
        group=CommandGroup.CLUSTER,
    )
    def cluster_addslots(self, slots: Parameters[int]) -> CommandRequest[bool]:
        """
        Assign new hash slots to receiving node
        """

        return self.create_request(
            CommandName.CLUSTER_ADDSLOTS, *slots, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_ADDSLOTSRANGE,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
    )
    def cluster_addslotsrange(self, slots: Parameters[tuple[int, int]]) -> CommandRequest[bool]:
        """
        Assign new hash slots to receiving node
        """
        command_arguments: CommandArgList = []

        for slot in slots:
            command_arguments.extend(slot)

        return self.create_request(
            CommandName.CLUSTER_ADDSLOTSRANGE,
            *command_arguments,
            callback=BoolCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.ASKING, group=CommandGroup.CLUSTER, flags={CommandFlag.FAST})
    def asking(self) -> CommandRequest[bool]:
        """
        Sent by cluster clients after an -ASK redirect
        """

        return self.create_request(CommandName.ASKING, callback=BoolCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLUSTER_BUMPEPOCH, group=CommandGroup.CLUSTER)
    def cluster_bumpepoch(self) -> CommandRequest[AnyStr]:
        """
        Advance the cluster config epoch

        :return: ``BUMPED`` if the epoch was incremented, or ``STILL``
         if the node already has the greatest config epoch in the cluster.
        """

        return self.create_request(CommandName.CLUSTER_BUMPEPOCH, callback=AnyStrCallback[AnyStr]())

    @redis_command(
        CommandName.CLUSTER_COUNT_FAILURE_REPORTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_count_failure_reports(self, node_id: StringT) -> CommandRequest[int]:
        """
        Return the number of failure reports active for a given node

        """

        return self.create_request(
            CommandName.CLUSTER_COUNT_FAILURE_REPORTS,
            node_id,
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_COUNTKEYSINSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    def cluster_countkeysinslot(self, slot: int) -> CommandRequest[int]:
        """
        Return the number of local keys in the specified hash slot
        """

        return self.create_request(
            CommandName.CLUSTER_COUNTKEYSINSLOT,
            slot,
            execution_parameters={"slot_arguments_range": (0, 1)},
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_DELSLOTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.SLOT_ID,
            combine=ClusterBoolCombine(),
        ),
    )
    def cluster_delslots(self, slots: Parameters[int]) -> CommandRequest[bool]:
        """
        Set hash slots as unbound in the cluster.
        It determines by itself what node the slot is in and sends it there
        """
        return self.create_request(
            CommandName.CLUSTER_DELSLOTS,
            *slots,
            callback=SimpleStringCallback(),
            execution_parameters={"slot_arguments_range": (0, len(list(slots)))},
        )

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_DELSLOTSRANGE,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID, combine=ClusterBoolCombine()),
    )
    def cluster_delslotsrange(self, slots: Parameters[tuple[int, int]]) -> CommandRequest[bool]:
        """
        Set hash slots as unbound in receiving node
        """
        command_arguments: CommandArgList = list(itertools.chain(*slots))

        return self.create_request(
            CommandName.CLUSTER_DELSLOTSRANGE,
            *command_arguments,
            execution_parameters={"slot_arguments_range": (0, len(command_arguments))},
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_FAILOVER,
        group=CommandGroup.CLUSTER,
    )
    def cluster_failover(
        self,
        options: Literal[PureToken.FORCE, PureToken.TAKEOVER] | None = None,
    ) -> CommandRequest[bool]:
        """
        Forces a replica to perform a manual failover of its master.
        """

        command_arguments: CommandArgList = []

        if options is not None:
            command_arguments.append(options)

        return self.create_request(
            CommandName.CLUSTER_FAILOVER,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLUSTER_FLUSHSLOTS,
        group=CommandGroup.CLUSTER,
    )
    def cluster_flushslots(self) -> CommandRequest[bool]:
        """
        Delete a node's own slots information
        """

        return self.create_request(CommandName.CLUSTER_FLUSHSLOTS, callback=SimpleStringCallback())

    @redis_command(
        CommandName.CLUSTER_FORGET,
        group=CommandGroup.CLUSTER,
    )
    def cluster_forget(self, node_id: StringT) -> CommandRequest[bool]:
        """
        remove a node via its node ID from the set of known nodes
        of the Redis Cluster node receiving the command
        """

        return self.create_request(
            CommandName.CLUSTER_FORGET, node_id, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLUSTER_GETKEYSINSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.SLOT_ID),
    )
    def cluster_getkeysinslot(self, slot: int, count: int) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        Return local key names in the specified hash slot

        :return: :paramref:`count` key names

        """
        command_arguments: CommandArgList = [slot, count]

        return self.create_request(
            CommandName.CLUSTER_GETKEYSINSLOT,
            *command_arguments,
            execution_parameters={"slot_arguments_range": (0, 1)},
            callback=TupleCallback[AnyStr](),
        )

    @redis_command(
        CommandName.CLUSTER_INFO,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_info(self) -> CommandRequest[dict[str, str]]:
        """
        Provides info about Redis Cluster node state
        """

        return self.create_request(CommandName.CLUSTER_INFO, callback=ClusterInfoCallback())

    @redis_command(
        CommandName.CLUSTER_KEYSLOT,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_keyslot(self, key: KeyT) -> CommandRequest[int]:
        """
        Returns the hash slot of the specified key
        """

        return self.create_request(CommandName.CLUSTER_KEYSLOT, key, callback=IntCallback())

    @versionadded(version="3.1.1")
    @redis_command(
        CommandName.CLUSTER_LINKS,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
    )
    def cluster_links(self) -> CommandRequest[list[dict[AnyStr, ResponsePrimitive]]]:
        """
        Returns a list of all TCP links to and from peer nodes in cluster

        :return: A map of maps where each map contains various attributes
         and their values of a cluster link.

        """

        return self.create_request(
            CommandName.CLUSTER_LINKS, callback=ClusterLinksCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_MEET,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_meet(
        self, ip: StringT, port: int, cluster_bus_port: int | None = None
    ) -> CommandRequest[bool]:
        """
        Force a node cluster to handshake with another node.
        """

        command_arguments: CommandArgList = [ip, port]
        if cluster_bus_port is not None:
            command_arguments.append(cluster_bus_port)
        return self.create_request(
            CommandName.CLUSTER_MEET,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.1.1")
    @redis_command(CommandName.CLUSTER_MYID, group=CommandGroup.CLUSTER)
    def cluster_myid(self) -> CommandRequest[AnyStr]:
        """
        Return the node id
        """

        return self.create_request(CommandName.CLUSTER_MYID, callback=AnyStrCallback[AnyStr]())

    @redis_command(
        CommandName.CLUSTER_NODES,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_nodes(self) -> CommandRequest[list[ClusterNodeDetail]]:
        """
        Get Cluster config for the node
        """

        return self.create_request(CommandName.CLUSTER_NODES, callback=ClusterNodesCallback())

    @redis_command(
        CommandName.CLUSTER_REPLICATE,
        group=CommandGroup.CLUSTER,
    )
    def cluster_replicate(self, node_id: StringT) -> CommandRequest[bool]:
        """
        Reconfigure a node as a replica of the specified master node
        """

        return self.create_request(
            CommandName.CLUSTER_REPLICATE, node_id, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLUSTER_RESET,
        group=CommandGroup.CLUSTER,
    )
    def cluster_reset(
        self,
        hard_soft: Literal[PureToken.HARD, PureToken.SOFT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Reset a Redis Cluster node
        """

        command_arguments: CommandArgList = []

        if hard_soft is not None:
            command_arguments.append(hard_soft)

        return self.create_request(
            CommandName.CLUSTER_RESET,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_SAVECONFIG,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    def cluster_saveconfig(self) -> CommandRequest[bool]:
        """
        Forces the node to save cluster state on disk
        """

        return self.create_request(CommandName.CLUSTER_SAVECONFIG, callback=SimpleStringCallback())

    @redis_command(
        CommandName.CLUSTER_SET_CONFIG_EPOCH,
        group=CommandGroup.CLUSTER,
    )
    def cluster_set_config_epoch(self, config_epoch: int) -> CommandRequest[bool]:
        """
        Set the configuration epoch in a new node
        """

        return self.create_request(
            CommandName.CLUSTER_SET_CONFIG_EPOCH,
            config_epoch,
            callback=SimpleStringCallback(),
        )

    @mutually_exclusive_parameters("importing", "migrating", "node", "stable")
    @redis_command(
        CommandName.CLUSTER_SETSLOT,
        group=CommandGroup.CLUSTER,
    )
    def cluster_setslot(
        self,
        slot: int,
        *,
        importing: StringT | None = None,
        migrating: StringT | None = None,
        node: StringT | None = None,
        stable: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Bind a hash slot to a specific node
        """

        command_arguments: CommandArgList = [slot]

        if importing is not None:
            command_arguments.extend([PrefixToken.IMPORTING, importing])

        if migrating is not None:
            command_arguments.extend([PrefixToken.MIGRATING, migrating])

        if node is not None:
            command_arguments.extend([PrefixToken.NODE, node])

        if stable is not None:
            command_arguments.append(PureToken.STABLE)

        return self.create_request(
            CommandName.CLUSTER_SETSLOT,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.CLUSTER_REPLICAS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_replicas(self, node_id: StringT) -> CommandRequest[list[ClusterNodeDetail]]:
        """
        List replica nodes of the specified master node
        """

        return self.create_request(
            CommandName.CLUSTER_REPLICAS, node_id, callback=ClusterNodesCallback()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.CLUSTER_SHARDS,
        version_introduced="7.0.0",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_shards(
        self,
    ) -> CommandRequest[list[dict[AnyStr, list[RedisValueT] | Mapping[AnyStr, RedisValueT]]]]:
        """
        Get mapping of cluster slots to nodes
        """
        return self.create_request(
            CommandName.CLUSTER_SHARDS, callback=ClusterShardsCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLUSTER_SLAVES,
        version_deprecated="5.0.0",
        deprecation_reason="Use :meth:`cluster_replicas`",
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def cluster_slaves(self, node_id: StringT) -> CommandRequest[list[ClusterNodeDetail]]:
        """
        List replica nodes of the specified master node
        """

        return self.create_request(
            CommandName.CLUSTER_SLAVES, node_id, callback=ClusterNodesCallback()
        )

    @redis_command(
        CommandName.CLUSTER_SLOTS,
        group=CommandGroup.CLUSTER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
        version_deprecated="7.0.0",
        deprecation_reason="Use :meth:`cluster_shards`",
    )
    def cluster_slots(
        self,
    ) -> CommandRequest[dict[tuple[int, int], tuple[ClusterNode, ...]]]:
        """
        Get mapping of Cluster slot to nodes
        """

        return self.create_request(CommandName.CLUSTER_SLOTS, callback=ClusterSlotsCallback())

    @versionadded(version="3.2.0")
    @redis_command(CommandName.READONLY, group=CommandGroup.CLUSTER, flags={CommandFlag.FAST})
    def readonly(self) -> CommandRequest[bool]:
        """
        Enables read queries for a connection to a cluster replica node
        """
        return self.create_request(CommandName.READONLY, callback=SimpleStringCallback())

    @versionadded(version="3.2.0")
    @redis_command(CommandName.READWRITE, group=CommandGroup.CLUSTER, flags={CommandFlag.FAST})
    def readwrite(self) -> CommandRequest[bool]:
        """
        Disables read queries for a connection to a cluster replica node
        """
        return self.create_request(CommandName.READWRITE, callback=SimpleStringCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.AUTH,
        group=CommandGroup.CONNECTION,
        arguments={"username": {"version_introduced": "6.0.0"}},
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.username` and :paramref:`Redis.password` arguments when initializing the client to ensure that all connections originating from this client are authenticated before being made available."
            ),
            True,
        ),
        flags={CommandFlag.FAST},
    )
    def auth(self, password: StringT, username: StringT | None = None) -> CommandRequest[bool]:
        """
        Authenticate to the server
        """
        command_arguments: CommandArgList = []
        command_arguments.append(password)

        if username is not None:
            command_arguments.append(username)

        return self.create_request(
            CommandName.AUTH, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.ECHO,
        group=CommandGroup.CONNECTION,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
        flags={CommandFlag.FAST},
    )
    def echo(self, message: StringT) -> CommandRequest[AnyStr]:
        "Echo the string back from the server"

        return self.create_request(CommandName.ECHO, message, callback=AnyStrCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.HELLO,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
        flags={CommandFlag.FAST},
    )
    def hello(
        self,
        protover: int | None = None,
        username: StringT | None = None,
        password: StringT | None = None,
        setname: StringT | None = None,
    ) -> CommandRequest[dict[AnyStr, AnyStr]]:
        """
        Handshake with Redis

        :return: a mapping of server properties.
        """
        command_arguments: CommandArgList = []

        if protover is not None:
            command_arguments.append(protover)

        if password:
            command_arguments.append(PrefixToken.AUTH)
            command_arguments.append(username or "default")
            command_arguments.append(password)

        if setname is not None:
            command_arguments.append(PrefixToken.SETNAME)
            command_arguments.append(setname)

        return self.create_request(
            CommandName.HELLO,
            *command_arguments,
            callback=DictCallback[AnyStr, AnyStr](),
        )

    @redis_command(
        CommandName.PING,
        group=CommandGroup.CONNECTION,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
        flags={CommandFlag.FAST},
    )
    def ping(self, message: StringT | None = None) -> CommandRequest[AnyStr]:
        """
        Ping the server

        :return: ``PONG``, when no argument is provided else the
         :paramref:`message` provided
        """
        command_arguments: CommandArgList = []

        if message:
            command_arguments.append(message)

        return self.create_request(
            CommandName.PING, *command_arguments, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SELECT,
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use the `db` argument when initializing the client to ensure that all connections originating from this client use the desired database number"
            ),
            True,
        ),
        flags={CommandFlag.FAST},
    )
    def select(self, index: int) -> CommandRequest[bool]:
        """
        Change the selected database for the current connection
        """
        return self.create_request(CommandName.SELECT, index, callback=SimpleStringCallback())

    @redis_command(
        CommandName.QUIT,
        group=CommandGroup.CONNECTION,
        flags={CommandFlag.FAST},
        version_deprecated="7.1.240",
    )
    def quit(self) -> CommandRequest[bool]:
        """
        Close the connection
        """

        return self.create_request(CommandName.QUIT, callback=SimpleStringCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.RESET,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
        flags={CommandFlag.FAST},
    )
    def reset(self) -> CommandRequest[None]:
        """
        Reset the connection
        """
        return self.create_request(CommandName.RESET, callback=NoopCallback[None]())

    @redis_command(
        CommandName.GEOADD,
        group=CommandGroup.GEO,
        arguments={
            "condition": {"version_introduced": "6.2.0"},
            "change": {"version_introduced": "6.2.0"},
        },
    )
    def geoadd(
        self,
        key: KeyT,
        longitude_latitude_members: Parameters[tuple[int | float, int | float, ValueT]],
        condition: Literal[PureToken.NX, PureToken.XX] | None = None,
        change: bool | None = None,
    ) -> CommandRequest[int]:
        """
        Add one or more geospatial items in the geospatial index represented
        using a sorted set

        :return: Number of elements added. If ``change`` is ``True`` the return
         is the number of elements that were changed.

        """
        command_arguments: CommandArgList = [key]

        if condition is not None:
            command_arguments.append(condition)

        if change is not None:
            command_arguments.append(PureToken.CHANGE)

        command_arguments.extend(tuples_to_flat_list(longitude_latitude_members))

        return self.create_request(CommandName.GEOADD, *command_arguments, callback=IntCallback())

    @redis_command(
        CommandName.GEODIST,
        group=CommandGroup.GEO,
        flags={CommandFlag.READONLY},
    )
    def geodist(
        self,
        key: KeyT,
        member1: StringT,
        member2: StringT,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = None,
    ) -> CommandRequest[float | None]:
        """
        Returns the distance between two members of a geospatial index

        :return: Distance in the unit specified by :paramref:`unit`
        """
        command_arguments: CommandArgList = [key, member1, member2]

        if unit:
            command_arguments.append(unit.lower())

        return self.create_request(
            CommandName.GEODIST, *command_arguments, callback=OptionalFloatCallback()
        )

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.GEOHASH,
        group=CommandGroup.GEO,
        flags={CommandFlag.READONLY},
    )
    def geohash(self, key: KeyT, members: Parameters[ValueT]) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        Returns members of a geospatial index as standard geohash strings
        """

        return self.create_request(
            CommandName.GEOHASH, key, *members, callback=TupleCallback[AnyStr]()
        )

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.GEOPOS,
        group=CommandGroup.GEO,
        flags={CommandFlag.READONLY},
    )
    def geopos(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> CommandRequest[tuple[GeoCoordinates | None, ...]]:
        """
        Returns longitude and latitude of members of a geospatial index

        :return: pairs of longitude/latitudes. Missing members are represented
         by ``None`` entries.
        """

        return self.create_request(
            CommandName.GEOPOS, key, *members, callback=GeoCoordinatessCallback()
        )

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
    ) -> CommandRequest[tuple[AnyStr, ...]]: ...

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: Literal[True],
        withdist: bool | None = ...,
        withhash: bool | None = ...,
    ) -> CommandRequest[tuple[GeoSearchResult, ...]]: ...

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: bool | None = ...,
        withdist: Literal[True],
        withhash: bool | None = ...,
    ) -> CommandRequest[tuple[GeoSearchResult, ...]]: ...

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: bool | None = ...,
        withdist: bool | None = ...,
        withhash: Literal[True],
    ) -> CommandRequest[tuple[GeoSearchResult, ...]]: ...

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        store: KeyT,
    ) -> CommandRequest[int]: ...

    @overload
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: bool | None = ...,
        withdist: bool | None = ...,
        withhash: bool | None = ...,
        storedist: KeyT,
    ) -> CommandRequest[int]: ...

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
    def georadius(
        self,
        key: KeyT,
        longitude: int | float,
        latitude: int | float,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        *,
        withcoord: bool | None = None,
        withdist: bool | None = None,
        withhash: bool | None = None,
        count: int | None = None,
        any_: bool | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        store: KeyT | None = None,
        storedist: KeyT | None = None,
    ) -> CommandRequest[int | tuple[AnyStr | GeoSearchResult, ...]]:
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

        return self._georadiusgeneric(
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
    def georadiusbymember(
        self,
        key: KeyT,
        member: ValueT,
        radius: int | float,
        unit: Literal[PureToken.FT, PureToken.KM, PureToken.M, PureToken.MI],
        withcoord: bool | None = None,
        withdist: bool | None = None,
        withhash: bool | None = None,
        count: int | None = None,
        any_: bool | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        store: KeyT | None = None,
        storedist: KeyT | None = None,
    ) -> CommandRequest[int | tuple[AnyStr | GeoSearchResult, ...]]:
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

        return self._georadiusgeneric(
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

    def _georadiusgeneric(
        self,
        command: Literal[
            CommandName.GEORADIUS,
            CommandName.GEORADIUSBYMEMBER,
        ],
        *args: ValueT,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI],
        withcoord: bool | None = None,
        withdist: bool | None = None,
        withhash: bool | None = None,
        count: int | None = None,
        any_: bool | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        store: KeyT | None = None,
        storedist: KeyT | None = None,
    ) -> CommandRequest[int] | CommandRequest[tuple[AnyStr | GeoSearchResult, ...]]:
        command_arguments: CommandArgList = list(args)
        options: dict[str, ValueT] = {}
        if unit:
            command_arguments.append(unit.lower())

        if withdist:
            command_arguments.append(PureToken.WITHDIST)
            options["withdist"] = withdist
        if withcoord:
            command_arguments.append(PureToken.WITHCOORD)
            options["withcoord"] = withcoord
        if withhash:
            command_arguments.append(PureToken.WITHHASH)
            options["withhash"] = withhash

        if count is not None:
            command_arguments.extend(["COUNT", count])
            options["count"] = count

            if any_:
                command_arguments.append(PureToken.ANY)
                options["any_"] = any_

        if order:
            command_arguments.append(order)
            options["order"] = order

        if store:
            command_arguments.extend([PrefixToken.STORE, store])
            options["store"] = store

        if storedist:
            command_arguments.extend([PrefixToken.STOREDIST, storedist])
            options["storedist"] = storedist
        if store or storedist:
            return self.create_request(
                command,
                *command_arguments,
                callback=IntCallback(),
            )
        else:
            return self.create_request(
                command,
                *command_arguments,
                callback=GeoSearchCallback[AnyStr](**options),
            )

    @mutually_inclusive_parameters("longitude", "latitude")
    @mutually_inclusive_parameters("radius", "circle_unit")
    @mutually_inclusive_parameters("width", "height", "box_unit")
    @mutually_inclusive_parameters("any_", leaders=("count",))
    @mutually_exclusive_parameters("member", ("longitude", "latitude"))
    @redis_command(
        CommandName.GEOSEARCH,
        version_introduced="6.2.0",
        group=CommandGroup.GEO,
        flags={CommandFlag.READONLY},
    )
    def geosearch(
        self,
        key: KeyT,
        member: ValueT | None = None,
        longitude: int | float | None = None,
        latitude: int | float | None = None,
        radius: int | float | None = None,
        circle_unit: None | (Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]) = None,
        width: int | float | None = None,
        height: int | float | None = None,
        box_unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        count: int | None = None,
        any_: bool | None = None,
        withcoord: bool | None = None,
        withdist: bool | None = None,
        withhash: bool | None = None,
    ) -> CommandRequest[int | tuple[AnyStr | GeoSearchResult, ...]]:
        """

        :return:

         - If no ``with{coord,dist,hash}`` options are provided the return
           is simply the names of places matched (optionally ordered if `order` is provided).
         - If any of the ``with{coord,dist,hash}`` options are set each result entry contains
           `(name, distance, geohash, coordinate pair)``
        """

        return self._geosearchgeneric(
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
    @redis_command(CommandName.GEOSEARCHSTORE, version_introduced="6.2.0", group=CommandGroup.GEO)
    def geosearchstore(
        self,
        destination: KeyT,
        source: KeyT,
        member: ValueT | None = None,
        longitude: int | float | None = None,
        latitude: int | float | None = None,
        radius: int | float | None = None,
        circle_unit: None | (Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI]) = None,
        width: int | float | None = None,
        height: int | float | None = None,
        box_unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        count: int | None = None,
        any_: bool | None = None,
        storedist: bool | None = None,
    ) -> CommandRequest[int]:
        """
        :return: The number of elements stored in the resulting set
        """

        return self._geosearchgeneric(
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
    def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCH],
        *args: ValueT,
        member: ValueT | None = ...,
        longitude: int | float | None = ...,
        latitude: int | float | None = ...,
        radius: int | float | None = ...,
        width: int | float | None = ...,
        height: int | float | None = ...,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = ...,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = ...,
        count: int | None = ...,
        any_: bool | None = ...,
        **kwargs: ValueT | None,
    ) -> CommandRequest[tuple[AnyStr | GeoSearchResult, ...]]: ...

    @overload
    def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCHSTORE],
        *args: ValueT,
        member: ValueT | None = ...,
        longitude: int | float | None = ...,
        latitude: int | float | None = ...,
        radius: int | float | None = ...,
        width: int | float | None = ...,
        height: int | float | None = ...,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = ...,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = ...,
        count: int | None = ...,
        any_: bool | None = ...,
        **kwargs: ValueT | None,
    ) -> CommandRequest[int]: ...

    def _geosearchgeneric(
        self,
        command: Literal[CommandName.GEOSEARCH, CommandName.GEOSEARCHSTORE],
        *args: ValueT,
        member: ValueT | None = None,
        longitude: int | float | None = None,
        latitude: int | float | None = None,
        radius: int | float | None = None,
        width: int | float | None = None,
        height: int | float | None = None,
        unit: Literal[PureToken.M, PureToken.KM, PureToken.FT, PureToken.MI] | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        count: int | None = None,
        any_: bool | None = None,
        **kwargs: ValueT | None,
    ) -> CommandRequest[int] | CommandRequest[tuple[AnyStr | GeoSearchResult, ...]]:
        command_arguments: CommandArgList = list(args)

        if member:
            command_arguments.extend([PrefixToken.FROMMEMBER, member])

        if longitude is not None and latitude is not None:
            command_arguments.extend([PrefixToken.FROMLONLAT, longitude, latitude])

        # BYRADIUS or BYBOX
        if unit is None:
            raise DataError("GEOSEARCH must have unit")

        if radius is not None:
            command_arguments.extend([PrefixToken.BYRADIUS, radius, unit.lower()])

        if width is not None and height is not None:
            command_arguments.extend([PrefixToken.BYBOX, width, height, unit.lower()])

        # sort
        if order:
            command_arguments.append(order)

        # count any
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])

            if any_:
                command_arguments.append(PureToken.ANY)

        # other properties

        for arg_name, byte_repr in (
            ("withdist", PureToken.WITHDIST),
            ("withcoord", PureToken.WITHCOORD),
            ("withhash", PureToken.WITHHASH),
            ("storedist", PureToken.STOREDIST),
        ):
            if kwargs[arg_name]:
                command_arguments.append(byte_repr)

        if command == CommandName.GEOSEARCHSTORE:
            return self.create_request(command, *command_arguments, callback=IntCallback())
        else:
            return self.create_request(
                command,
                *command_arguments,
                callback=GeoSearchCallback[AnyStr](**kwargs),
            )

    @ensure_iterable_valid("fields")
    @redis_command(CommandName.HDEL, group=CommandGroup.HASH, flags={CommandFlag.FAST})
    def hdel(self, key: KeyT, fields: Parameters[StringT]) -> CommandRequest[int]:
        """Deletes ``fields`` from hash :paramref:`key`"""

        return self.create_request(CommandName.HDEL, key, *fields, callback=IntCallback())

    @redis_command(
        CommandName.HEXISTS,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def hexists(self, key: KeyT, field: StringT) -> CommandRequest[bool]:
        """
        Returns a boolean indicating if ``field`` exists within hash :paramref:`key`
        """

        return self.create_request(CommandName.HEXISTS, key, field, callback=BoolCallback())

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HEXPIRE, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hexpire(
        self,
        key: KeyT,
        seconds: int | datetime.timedelta,
        fields: Parameters[StringT],
        condition: Literal[PureToken.GT, PureToken.LT, PureToken.NX, PureToken.XX] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Set expiry for hash field using relative time to expire (seconds)
        """
        command_arguments: CommandArgList = [key, normalized_seconds(seconds)]

        if condition is not None:
            command_arguments.append(condition)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HEXPIRE, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HEXPIRETIME, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hexpiretime(
        self, key: KeyT, fields: Parameters[StringT]
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Returns the expiration time of a hash field as a Unix timestamp, in seconds.
        """
        command_arguments: CommandArgList = [key, PrefixToken.FIELDS, len(list(fields))]
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HEXPIRETIME, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HPEXPIRETIME, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hpexpiretime(
        self, key: KeyT, fields: Parameters[StringT]
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Returns the expiration time of a hash field as a Unix timestamp, in msec.
        """
        command_arguments: CommandArgList = [key, PrefixToken.FIELDS, len(list(fields))]

        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HPEXPIRETIME, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HPEXPIRE, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hpexpire(
        self,
        key: KeyT,
        milliseconds: int | datetime.timedelta,
        fields: Parameters[StringT],
        condition: Literal[PureToken.GT, PureToken.LT, PureToken.NX, PureToken.XX] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Set expiry for hash field using relative time to expire (milliseconds)
        """
        command_arguments: CommandArgList = [key, normalized_milliseconds(milliseconds)]

        if condition is not None:
            command_arguments.append(condition)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HPEXPIRE, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HEXPIREAT, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hexpireat(
        self,
        key: KeyT,
        unix_time_seconds: int | datetime.datetime,
        fields: Parameters[StringT],
        condition: Literal[PureToken.GT, PureToken.LT, PureToken.NX, PureToken.XX] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Set expiry for hash field using an absolute Unix timestamp (seconds)
        """
        command_arguments: CommandArgList = [key, normalized_time_seconds(unix_time_seconds)]
        if condition is not None:
            command_arguments.append(condition)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HEXPIREAT, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HPEXPIREAT, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hpexpireat(
        self,
        key: KeyT,
        unix_time_milliseconds: int | datetime.datetime,
        fields: Parameters[StringT],
        condition: Literal[PureToken.GT, PureToken.LT, PureToken.NX, PureToken.XX] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Set expiry for hash field using an absolute Unix timestamp (milliseconds)
        """
        command_arguments: CommandArgList = [
            key,
            normalized_time_milliseconds(unix_time_milliseconds),
        ]

        if condition is not None:
            command_arguments.append(condition)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HPEXPIREAT, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HPERSIST, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hpersist(self, key: KeyT, fields: Parameters[StringT]) -> CommandRequest[tuple[int, ...]]:
        """
        Removes the expiration time for each specified field
        """
        command_arguments: CommandArgList = [key, PrefixToken.FIELDS, len(list(fields))]
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HPERSIST, *command_arguments, callback=TupleCallback[int]()
        )

    @redis_command(
        CommandName.HGET,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def hget(self, key: KeyT, field: StringT) -> CommandRequest[AnyStr | None]:
        """Returns the value of ``field`` within the hash :paramref:`key`"""

        return self.create_request(
            CommandName.HGET, key, field, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.HGETALL,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def hgetall(self, key: KeyT) -> CommandRequest[dict[AnyStr, AnyStr]]:
        """Returns a Python dict of the hash's name/value pairs"""

        return self.create_request(CommandName.HGETALL, key, callback=HGetAllCallback[AnyStr]())

    @versionadded(version="5.0.0")
    @mutually_exclusive_parameters("ex", "px", "exat", "pxat", "persist", required=True)
    @redis_command(CommandName.HGETEX, version_introduced="8.0.0", group=CommandGroup.HASH)
    def hgetex(
        self,
        key: KeyT,
        fields: Parameters[StringT],
        ex: int | datetime.timedelta | None = None,
        px: int | datetime.timedelta | None = None,
        exat: int | datetime.datetime | None = None,
        pxat: int | datetime.datetime | None = None,
        persist: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """
        Get the value of one or more fields of a given hash key and optionally set their
        expiration time or time-to-live (TTL).

        :param key:  The key of the hash
        :param fields: The fields to get values for
        :param ex: Set the expiry of the fields to ``ex`` seconds
        :param px: Set the expiry of the fields to ``px`` milliseconds
        :param exat: Set the expiry of the fields to the specified time (in seconds)
        :param exat: Set the expiry of the fields to the specified time (in milliseconds)
        :param persist: Remove TTL from the fields
        :return: the values of each of the fields requested (Missing fields are returned
         as ``None``)
        """
        command_arguments: CommandArgList = [key]
        if ex is not None:
            command_arguments.extend([PrefixToken.EX, normalized_seconds(ex)])
        elif px is not None:
            command_arguments.extend([PrefixToken.PX, normalized_milliseconds(px)])
        if exat is not None:
            command_arguments.extend([PrefixToken.EXAT, normalized_time_seconds(exat)])
        elif pxat is not None:
            command_arguments.extend([PrefixToken.PXAT, normalized_time_milliseconds(pxat)])

        if persist is not None:
            command_arguments.append(PureToken.PERSIST)

        command_arguments.extend([PrefixToken.FIELDS, len(list(fields)), *fields])
        return self.create_request(
            CommandName.HGETEX, *command_arguments, callback=TupleCallback[AnyStr | None]()
        )

    @versionadded(version="5.0.0")
    @redis_command(CommandName.HGETDEL, version_introduced="8.0.0", group=CommandGroup.HASH)
    def hgetdel(
        self, key: KeyT, fields: Parameters[StringT]
    ) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """
        Get and delete the value of one or more fields of a given hash key. When the last field is deleted,
        the key will also be deleted.

        :param key: The key of the hash
        :param fields: The fields to get and delete
        :return: the values of the fields requested (Missing fields are returned
         as ``None``)
        """
        return self.create_request(
            CommandName.HGETDEL,
            key,
            PrefixToken.FIELDS,
            len(list(fields)),
            *fields,
            callback=TupleCallback[AnyStr | None](),
        )

    @redis_command(CommandName.HINCRBY, group=CommandGroup.HASH, flags={CommandFlag.FAST})
    def hincrby(self, key: KeyT, field: StringT, increment: int) -> CommandRequest[int]:
        """Increments the value of ``field`` in hash :paramref:`key` by ``increment``"""

        return self.create_request(
            CommandName.HINCRBY, key, field, increment, callback=IntCallback()
        )

    @redis_command(CommandName.HINCRBYFLOAT, group=CommandGroup.HASH, flags={CommandFlag.FAST})
    def hincrbyfloat(
        self, key: KeyT, field: StringT, increment: int | float
    ) -> CommandRequest[float]:
        """
        Increments the value of ``field`` in hash :paramref:`key` by floating
        ``increment``
        """

        return self.create_request(
            CommandName.HINCRBYFLOAT, key, field, increment, callback=FloatCallback()
        )

    @redis_command(
        CommandName.HKEYS,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def hkeys(self, key: KeyT) -> CommandRequest[tuple[AnyStr, ...]]:
        """Returns the list of keys within hash :paramref:`key`"""

        return self.create_request(CommandName.HKEYS, key, callback=TupleCallback[AnyStr]())

    @redis_command(
        CommandName.HLEN,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def hlen(self, key: KeyT) -> CommandRequest[int]:
        """Returns the number of elements in hash :paramref:`key`"""

        return self.create_request(CommandName.HLEN, key, callback=IntCallback())

    @redis_command(CommandName.HSET, group=CommandGroup.HASH, flags={CommandFlag.FAST})
    def hset(self, key: KeyT, field_values: Mapping[StringT, ValueT]) -> CommandRequest[int]:
        """
        Sets ``field`` to ``value`` within hash :paramref:`key`

        :return: number of fields that were added
        """

        return self.create_request(
            CommandName.HSET,
            key,
            *dict_to_flat_list(field_values),
            callback=IntCallback(),
        )

    @versionadded(version="5.0.0")
    @mutually_exclusive_parameters("ex", "px", "exat", "pxat", "keepttl", required=True)
    @redis_command(CommandName.HSETEX, version_introduced="8.0.0", group=CommandGroup.HASH)
    def hsetex(
        self,
        key: KeyT,
        field_values: Mapping[StringT, ValueT],
        condition: Literal[PureToken.FNX, PureToken.FXX] | None = None,
        ex: int | datetime.timedelta | None = None,
        px: int | datetime.timedelta | None = None,
        exat: int | datetime.datetime | None = None,
        pxat: int | datetime.datetime | None = None,
        keepttl: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Set the value of one or more fields of a given hash key, and optionally set their
        expiration time or time-to-live (TTL).

        :param key: The key of the hash
        :param field_values: Mapping of fields and the values to set
        :param condition: If ``FNX`` only set the fields if **none** of them exist,
         if ``FXX`` only set the fields if **all** of them already exists
        :param ex: Set the expiry of the fields to ``ex`` seconds
        :param px: Set the expiry of the fields to ``px`` milliseconds
        :param exat: Set the expiry of the fields to the specified time (in seconds)
        :param exat: Set the expiry of the fields to the specified time (in milliseconds)
        :param keepttl: Retain the TTL already associated with the fields
        :return: ``True`` if all the fields were successfully set
        """
        command_arguments: CommandArgList = [key]
        if condition is not None:
            command_arguments.append(condition)
        if ex is not None:
            command_arguments.extend([PrefixToken.EX, normalized_seconds(ex)])
        elif px is not None:
            command_arguments.extend([PrefixToken.PX, normalized_milliseconds(px)])
        if exat is not None:
            command_arguments.extend([PrefixToken.EXAT, normalized_time_seconds(exat)])
        elif pxat is not None:
            command_arguments.extend([PrefixToken.PXAT, normalized_time_milliseconds(pxat)])

        if keepttl is not None:
            command_arguments.append(PureToken.KEEPTTL)
        command_arguments.extend(
            [PrefixToken.FIELDS, len(field_values), *dict_to_flat_list(field_values)]
        )

        return self.create_request(
            CommandName.HSETEX,
            *command_arguments,
            callback=BoolCallback(),
        )

    @redis_command(CommandName.HSETNX, group=CommandGroup.HASH, flags={CommandFlag.FAST})
    def hsetnx(self, key: KeyT, field: StringT, value: ValueT) -> CommandRequest[bool]:
        """
        Sets ``field`` to ``value`` within hash :paramref:`key` if ``field`` does not
        exist.

        :return: whether the field was created
        """

        return self.create_request(CommandName.HSETNX, key, field, value, callback=BoolCallback())

    @redis_command(
        CommandName.HMSET,
        group=CommandGroup.HASH,
        version_deprecated="4.0.0",
        deprecation_reason="Use :meth:`hset` with multiple field-value pairs",
        flags={CommandFlag.FAST},
    )
    def hmset(self, key: KeyT, field_values: Mapping[StringT, ValueT]) -> CommandRequest[bool]:
        """
        Sets key to value within hash :paramref:`key` for each corresponding
        key and value from the ``field_values`` dict.
        """

        command_arguments: CommandArgList = [key]

        for pair in field_values.items():
            command_arguments.extend(pair)

        return self.create_request(
            CommandName.HMSET, *command_arguments, callback=SimpleStringCallback()
        )

    @ensure_iterable_valid("fields")
    @redis_command(
        CommandName.HMGET,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def hmget(
        self, key: KeyT, fields: Parameters[StringT]
    ) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """Returns values ordered identically to ``fields``"""

        return self.create_request(
            CommandName.HMGET, key, *fields, callback=TupleCallback[AnyStr | None]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HTTL, version_introduced="7.4.0", group=CommandGroup.HASH)
    def httl(self, key: KeyT, fields: Parameters[StringT]) -> CommandRequest[tuple[int, ...]]:
        """
        Returns the TTL in seconds of a hash field.
        """
        command_arguments: CommandArgList = []

        command_arguments.append(key)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HTTL, *command_arguments, callback=TupleCallback[int]()
        )

    @versionadded(version="4.18.0")
    @redis_command(CommandName.HPTTL, version_introduced="7.4.0", group=CommandGroup.HASH)
    def hpttl(self, key: KeyT, fields: Parameters[StringT]) -> CommandRequest[tuple[int, ...]]:
        """
        Returns the TTL in milliseconds of a hash field.
        """
        command_arguments: CommandArgList = []

        command_arguments.append(key)
        command_arguments.append(PrefixToken.FIELDS)
        command_arguments.append(len(list(fields)))
        command_arguments.extend(fields)

        return self.create_request(
            CommandName.HPTTL, *command_arguments, callback=TupleCallback[int]()
        )

    @redis_command(
        CommandName.HVALS,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def hvals(self, key: KeyT) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        Get all the values in a hash

        :return: list of values in the hash, or an empty list when :paramref:`key` does not exist.
        """

        return self.create_request(CommandName.HVALS, key, callback=TupleCallback[AnyStr]())

    @overload
    def hscan(
        self,
        key: KeyT,
        cursor: int | None = ...,
        match: StringT | None = ...,
        count: int | None = ...,
        *,
        novalues: Literal[True],
    ) -> CommandRequest[tuple[int, tuple[AnyStr, ...]]]: ...

    @overload
    def hscan(
        self,
        key: KeyT,
        cursor: int | None = None,
        match: StringT | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[int, dict[AnyStr, AnyStr]]]: ...
    @redis_command(
        CommandName.HSCAN,
        group=CommandGroup.HASH,
        flags={CommandFlag.READONLY},
        arguments={"novalues": {"version_introduced": "7.4.0"}},
    )
    def hscan(
        self,
        key: KeyT,
        cursor: int | None = None,
        match: StringT | None = None,
        count: int | None = None,
        novalues: bool | None = None,
    ) -> CommandRequest[tuple[int, dict[AnyStr, AnyStr] | tuple[AnyStr, ...]]]:
        """
        Incrementally return key/value slices in a hash. Also returns a
        cursor pointing to the scan position.

        :param match: allows for filtering the keys by pattern
        :param count: allows for hint the minimum number of returns
        :param novalues: when True only the field names are returned
        """
        command_arguments: CommandArgList = [key, cursor or "0"]

        if match is not None:
            command_arguments.extend([PrefixToken.MATCH, match])

        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if novalues is not None:
            command_arguments.append(PureToken.NOVALUES)

        return self.create_request(
            CommandName.HSCAN,
            *command_arguments,
            callback=HScanCallback[AnyStr](novalues=novalues),
        )

    @redis_command(
        CommandName.HSTRLEN,
        group=CommandGroup.HASH,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def hstrlen(self, key: KeyT, field: StringT) -> CommandRequest[int]:
        """
        Get the length of the value of a hash field

        :return: the string length of the value associated with ``field``,
         or zero when ``field`` is not present in the hash or :paramref:`key` does not exist at all.
        """

        return self.create_request(CommandName.HSTRLEN, key, field, callback=IntCallback())

    @overload
    def hrandfield(
        self,
        key: KeyT,
        *,
        withvalues: Literal[True],
        count: int = ...,
    ) -> CommandRequest[dict[AnyStr, AnyStr]]: ...

    @overload
    def hrandfield(
        self,
        key: KeyT,
        *,
        count: int = ...,
    ) -> CommandRequest[tuple[AnyStr, ...]]: ...

    @redis_command(
        CommandName.HRANDFIELD,
        version_introduced="6.2.0",
        group=CommandGroup.HASH,
        flags={CommandFlag.READONLY},
    )
    def hrandfield(
        self,
        key: KeyT,
        *,
        count: int | None = None,
        withvalues: bool | None = None,
    ) -> CommandRequest[AnyStr | tuple[AnyStr, ...] | dict[AnyStr, AnyStr] | None]:
        """
        Return a random field from the hash value stored at key.

        :return:  Without the additional :paramref:`count` argument, the command returns a randomly
         selected field, or ``None`` when :paramref:`key` does not exist.
         When the additional :paramref:`count` argument is passed, the command returns fields,
         or an empty tuple when :paramref:`key` does not exist.

         If ``withvalues``  is ``True``, the reply is a mapping of fields and
         their values from the hash.

        """
        command_arguments: CommandArgList = [key]
        options = {"withvalues": withvalues, "count": count}

        if count is not None:
            command_arguments.append(count)

        if withvalues:
            command_arguments.append(PureToken.WITHVALUES)
            options["withvalues"] = True

        return self.create_request(
            CommandName.HRANDFIELD,
            *command_arguments,
            callback=HRandFieldCallback[AnyStr](**options),
        )

    @redis_command(
        CommandName.PFADD,
        group=CommandGroup.HYPERLOGLOG,
        flags={CommandFlag.FAST},
    )
    def pfadd(self, key: KeyT, *elements: ValueT) -> CommandRequest[bool]:
        """
        Adds the specified elements to the specified HyperLogLog.

        :return: Whether atleast 1 HyperLogLog internal register was altered
        """
        command_arguments: CommandArgList = [key]

        if elements:
            command_arguments.extend(elements)

        return self.create_request(CommandName.PFADD, *command_arguments, callback=BoolCallback())

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.PFCOUNT,
        group=CommandGroup.HYPERLOGLOG,
        flags={CommandFlag.READONLY},
    )
    def pfcount(self, keys: Parameters[KeyT]) -> CommandRequest[int]:
        """
        Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).

        :return: The approximated number of unique elements observed via :meth:`pfadd`.
        """

        return self.create_request(CommandName.PFCOUNT, *keys, callback=IntCallback())

    @ensure_iterable_valid("sourcekeys")
    @redis_command(
        CommandName.PFMERGE,
        group=CommandGroup.HYPERLOGLOG,
    )
    def pfmerge(self, destkey: KeyT, sourcekeys: Parameters[KeyT]) -> CommandRequest[bool]:
        """
        Merge N different HyperLogLogs into a single one
        """

        return self.create_request(
            CommandName.PFMERGE, destkey, *sourcekeys, callback=SimpleStringCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COPY,
        version_introduced="6.2.0",
        group=CommandGroup.GENERIC,
    )
    def copy(
        self,
        source: KeyT,
        destination: KeyT,
        db: int | None = None,
        replace: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Copy a key
        """
        command_arguments: CommandArgList = [source, destination]

        if db is not None:
            command_arguments.extend([PrefixToken.DB, db])

        if replace:
            command_arguments.append(PureToken.REPLACE)

        return self.create_request(
            CommandName.COPY,
            *command_arguments,
            callback=BoolCallback(),
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.DEL,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
    )
    def delete(self, keys: Parameters[KeyT]) -> CommandRequest[int]:
        """
        Delete one or more keys specified by ``keys``

        :return: The number of keys that were removed.
        """

        return self.create_request(CommandName.DEL, *keys, callback=IntCallback())

    @redis_command(
        CommandName.DUMP,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def dump(self, key: KeyT) -> CommandRequest[bytes]:
        """
        Return a serialized version of the value stored at the specified key.

        :return: the serialized value
        """

        return self.create_request(
            CommandName.DUMP,
            key,
            execution_parameters={"decode": False},
            callback=NoopCallback[bytes](),
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.EXISTS,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def exists(self, keys: Parameters[KeyT]) -> CommandRequest[int]:
        """
        Determine if a key exists

        :return: the number of keys that exist from those specified as arguments.
        """

        return self.create_request(CommandName.EXISTS, *keys, callback=IntCallback())

    @redis_command(
        CommandName.EXPIRE,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.FAST},
    )
    def expire(
        self,
        key: KeyT,
        seconds: int | datetime.timedelta,
        condition: Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Set a key's time to live in seconds



        :return: if the timeout was set or not set.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        command_arguments: CommandArgList = [key, normalized_seconds(seconds)]

        if condition is not None:
            command_arguments.append(condition)

        return self.create_request(CommandName.EXPIRE, *command_arguments, callback=BoolCallback())

    @redis_command(
        CommandName.EXPIREAT,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.FAST},
    )
    def expireat(
        self,
        key: KeyT,
        unix_time_seconds: int | datetime.datetime,
        condition: Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Set the expiration for a key to a specific time


        :return: if the timeout was set or no.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.

        """

        command_arguments: CommandArgList = [
            key,
            normalized_time_seconds(unix_time_seconds),
        ]

        if condition is not None:
            command_arguments.append(condition)

        return self.create_request(
            CommandName.EXPIREAT, *command_arguments, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EXPIRETIME,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def expiretime(self, key: KeyT) -> CommandRequest[datetime.datetime]:
        """
        Get the expiration Unix timestamp for a key

        :return: Expiration Unix timestamp in seconds, or a negative value in
         order to signal an error.

         * The command returns ``-1`` if the key exists but has no associated expiration time.
         * The command returns ``-2`` if the key does not exist.

        """

        return self.create_request(CommandName.EXPIRETIME, key, callback=ExpiryCallback())

    @redis_command(
        CommandName.KEYS,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets(),
        ),
        flags={CommandFlag.READONLY},
    )
    def keys(self, pattern: StringT = "*") -> CommandRequest[_Set[AnyStr]]:
        """
        Find all keys matching the given pattern

        :return: keys matching ``pattern``.
        """

        return self.create_request(CommandName.KEYS, pattern, callback=SetCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @mutually_inclusive_parameters("username", "password")
    @redis_command(CommandName.MIGRATE, group=CommandGroup.GENERIC)
    def migrate(
        self,
        host: StringT,
        port: int,
        destination_db: int,
        timeout: int,
        *keys: KeyT,
        copy: bool | None = None,
        replace: bool | None = None,
        auth: StringT | None = None,
        username: StringT | None = None,
        password: StringT | None = None,
    ) -> CommandRequest[bool]:
        """
        Atomically transfer key(s) from a Redis instance to another one.


        :return: If all keys were found in the source instance.
        """

        if not keys:
            raise DataError("MIGRATE requires at least one key")
        command_arguments: CommandArgList = []

        if copy:
            command_arguments.append(PureToken.COPY)

        if replace:
            command_arguments.append(PureToken.REPLACE)

        if auth:
            command_arguments.append(PrefixToken.AUTH)
            command_arguments.append(auth)

        if username and password:
            command_arguments.append(PrefixToken.AUTH2)
            command_arguments.append(username)
            command_arguments.append(password)

        command_arguments.append(PrefixToken.KEYS)
        command_arguments.extend(keys)

        return self.create_request(
            CommandName.MIGRATE,
            host,
            port,
            b"",
            destination_db,
            timeout,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.MOVE, group=CommandGroup.GENERIC, flags={CommandFlag.FAST})
    def move(self, key: KeyT, db: int) -> CommandRequest[bool]:
        """Move a key to another database"""

        return self.create_request(CommandName.MOVE, key, db, callback=BoolCallback())

    @redis_command(
        CommandName.OBJECT_ENCODING,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def object_encoding(self, key: KeyT) -> CommandRequest[AnyStr | None]:
        """
        Return the internal encoding for the object stored at :paramref:`key`

        :return: the encoding of the object, or ``None`` if the key doesn't exist
        """

        return self.create_request(
            CommandName.OBJECT_ENCODING, key, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.OBJECT_FREQ,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def object_freq(self, key: KeyT) -> CommandRequest[int]:
        """
        Return the logarithmic access frequency counter for the object
        stored at :paramref:`key`

        :return: The counter's value.
        """

        return self.create_request(CommandName.OBJECT_FREQ, key, callback=IntCallback())

    @redis_command(
        CommandName.OBJECT_IDLETIME,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def object_idletime(self, key: KeyT) -> CommandRequest[int]:
        """
        Return the time in seconds since the last access to the object
        stored at :paramref:`key`

        :return: The idle time in seconds.
        """

        return self.create_request(CommandName.OBJECT_IDLETIME, key, callback=IntCallback())

    @redis_command(
        CommandName.OBJECT_REFCOUNT,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def object_refcount(self, key: KeyT) -> CommandRequest[int]:
        """
        Return the reference count of the object stored at :paramref:`key`

        :return: The number of references.
        """

        return self.create_request(CommandName.OBJECT_REFCOUNT, key, callback=IntCallback())

    @redis_command(CommandName.PERSIST, group=CommandGroup.GENERIC, flags={CommandFlag.FAST})
    def persist(self, key: KeyT) -> CommandRequest[bool]:
        """Removes an expiration on :paramref:`key`"""

        return self.create_request(CommandName.PERSIST, key, callback=BoolCallback())

    @redis_command(
        CommandName.PEXPIRE,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.FAST},
    )
    def pexpire(
        self,
        key: KeyT,
        milliseconds: int | datetime.timedelta,
        condition: Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Set a key's time to live in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """
        command_arguments: CommandArgList = [key, normalized_milliseconds(milliseconds)]

        if condition is not None:
            command_arguments.append(condition)

        return self.create_request(CommandName.PEXPIRE, *command_arguments, callback=BoolCallback())

    @redis_command(
        CommandName.PEXPIREAT,
        group=CommandGroup.GENERIC,
        arguments={"condition": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.FAST},
    )
    def pexpireat(
        self,
        key: KeyT,
        unix_time_milliseconds: int | datetime.datetime,
        condition: Literal[PureToken.NX, PureToken.XX, PureToken.GT, PureToken.LT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds

        :return: if the timeout was set or not.
         e.g. key doesn't exist, or operation skipped due to the provided arguments.
        """

        command_arguments: CommandArgList = [
            key,
            normalized_time_milliseconds(unix_time_milliseconds),
        ]

        if condition is not None:
            command_arguments.append(condition)

        return self.create_request(
            CommandName.PEXPIREAT, *command_arguments, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.PEXPIRETIME,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def pexpiretime(self, key: KeyT) -> CommandRequest[datetime.datetime]:
        """
        Get the expiration Unix timestamp for a key in milliseconds

        :return: Expiration Unix timestamp in milliseconds, or a negative value
         in order to signal an error

         * The command returns ``-1`` if the key exists but has no associated expiration time.
         * The command returns ``-2`` if the key does not exist.

        """

        return self.create_request(
            CommandName.PEXPIRETIME,
            key,
            callback=ExpiryCallback(unit="milliseconds"),
        )

    @redis_command(
        CommandName.PTTL,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def pttl(self, key: KeyT) -> CommandRequest[int]:
        """
        Returns the number of milliseconds until the key :paramref:`key` will expire

        :return: TTL in milliseconds, or a negative value in order to signal an error
        """

        return self.create_request(CommandName.PTTL, key, callback=IntCallback())

    @redis_command(
        CommandName.RANDOMKEY,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
        flags={CommandFlag.READONLY},
    )
    def randomkey(self) -> CommandRequest[AnyStr | None]:
        """
        Returns the name of a random key

        :return: the random key, or ``None`` when the database is empty.
        """

        return self.create_request(CommandName.RANDOMKEY, callback=OptionalAnyStrCallback[AnyStr]())

    @redis_command(
        CommandName.RENAME,
        group=CommandGroup.GENERIC,
    )
    def rename(self, key: KeyT, newkey: KeyT) -> CommandRequest[bool]:
        """
        Rekeys key :paramref:`key` to ``newkey``
        """

        return self.create_request(CommandName.RENAME, key, newkey, callback=BoolCallback())

    @redis_command(CommandName.RENAMENX, group=CommandGroup.GENERIC, flags={CommandFlag.FAST})
    def renamenx(self, key: KeyT, newkey: KeyT) -> CommandRequest[bool]:
        """
        Rekeys key :paramref:`key` to ``newkey`` if ``newkey`` doesn't already exist

        :return: False when ``newkey`` already exists.
        """

        return self.create_request(CommandName.RENAMENX, key, newkey, callback=BoolCallback())

    @redis_command(
        CommandName.RESTORE,
        group=CommandGroup.GENERIC,
    )
    def restore(
        self,
        key: KeyT,
        ttl: int | datetime.timedelta | datetime.datetime,
        serialized_value: bytes,
        replace: bool | None = None,
        absttl: bool | None = None,
        idletime: int | datetime.timedelta | None = None,
        freq: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Create a key using the provided serialized value, previously obtained using DUMP.
        """

        command_arguments: CommandArgList = [
            key,
            (
                normalized_milliseconds(ttl)  # type: ignore
                if not absttl
                else normalized_time_milliseconds(ttl)  # type: ignore
            ),
            serialized_value,
        ]

        if replace:
            command_arguments.append(PureToken.REPLACE)

        if absttl:
            command_arguments.append(PureToken.ABSTTL)

        if idletime is not None:
            command_arguments.extend(["IDLETIME", normalized_milliseconds(idletime)])

        if freq:
            command_arguments.extend(["FREQ", freq])

        return self.create_request(
            CommandName.RESTORE, *command_arguments, callback=SimpleStringCallback()
        )

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.SORT,
        group=CommandGroup.GENERIC,
    )
    def sort(
        self,
        key: KeyT,
        gets: Parameters[KeyT] | None = None,
        by: StringT | None = None,
        offset: int | None = None,
        count: int | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        alpha: bool | None = None,
        store: KeyT | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...] | int]:
        """
        Sort the elements in a list, set or sorted set

        :return: sorted elements.

         When the :paramref:`store` option is specified the command returns the number of
         sorted elements in the destination list.
        """

        command_arguments: CommandArgList = [key]
        options = {}

        if by is not None:
            command_arguments.append(PrefixToken.BY)
            command_arguments.append(by)

        if offset is not None and count is not None:
            command_arguments.append(PrefixToken.LIMIT)
            command_arguments.append(offset)
            command_arguments.append(count)

        for g in gets or []:
            command_arguments.append(PrefixToken.GET)
            command_arguments.append(g)

        if order:
            command_arguments.append(order)

        if alpha is not None:
            command_arguments.append(PureToken.SORTING)

        if store is not None:
            command_arguments.append(PrefixToken.STORE)
            command_arguments.append(store)
            options["store"] = True

        return self.create_request(
            CommandName.SORT,
            *command_arguments,
            callback=SortCallback[AnyStr](**options),
        )

    @mutually_inclusive_parameters("offset", "count")
    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SORT_RO,
        version_introduced="7.0.0",
        group=CommandGroup.GENERIC,
        flags={CommandFlag.READONLY},
    )
    def sort_ro(
        self,
        key: KeyT,
        gets: Parameters[KeyT] | None = None,
        by: StringT | None = None,
        offset: int | None = None,
        count: int | None = None,
        order: Literal[PureToken.ASC, PureToken.DESC] | None = None,
        alpha: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        Sort the elements in a list, set or sorted set. Read-only variant of SORT.


        :return: sorted elements.

        """
        command_arguments: CommandArgList = [key]

        if by is not None:
            command_arguments.extend([PrefixToken.BY, by])

        if offset is not None and count is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset, count])

        for g in gets or []:
            command_arguments.extend([PrefixToken.GET, g])

        if order:
            command_arguments.append(order)

        if alpha is not None:
            command_arguments.append(PureToken.SORTING)

        return self.create_request(
            CommandName.SORT_RO, *command_arguments, callback=TupleCallback[AnyStr]()
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.TOUCH,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def touch(self, keys: Parameters[KeyT]) -> CommandRequest[int]:
        """
        Alters the last access time of a key(s).
        Returns the number of existing keys specified.

        :return: The number of keys that were touched.
        """

        return self.create_request(CommandName.TOUCH, *keys, callback=IntCallback())

    @redis_command(
        CommandName.TTL,
        group=CommandGroup.GENERIC,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def ttl(self, key: KeyT) -> CommandRequest[int]:
        """
        Get the time to live for a key in seconds

        :return: TTL in seconds, or a negative value in order to signal an error
        """

        return self.create_request(CommandName.TTL, key, callback=IntCallback())

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.UNLINK,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(split=NodeFlag.PRIMARIES, combine=ClusterSum()),
        flags={CommandFlag.FAST},
    )
    def unlink(self, keys: Parameters[KeyT]) -> CommandRequest[int]:
        """
        Delete a key asynchronously in another thread.
        Otherwise it is just as :meth:`delete`, but non blocking.

        :return: The number of keys that were unlinked.
        """

        return self.create_request(CommandName.UNLINK, *keys, callback=IntCallback())

    @redis_command(
        CommandName.WAIT,
        group=CommandGroup.GENERIC,
        redirect_usage=RedirectUsage(
            (
                "Use the :meth:`Redis.ensure_replication`  or :meth:`RedisCluster.ensure_replication` context managers to ensure a command is replicated to the number of replicas"
            ),
            True,
        ),
    )
    def wait(self, numreplicas: int, timeout: int) -> CommandRequest[int]:
        """
        Wait for the synchronous replication of all the write commands sent in the context of
        the current connection

        :return: the number of replicas the write was replicated to
        """

        return self.create_request(CommandName.WAIT, numreplicas, timeout, callback=IntCallback())

    @versionadded(version="4.12.0")
    @redis_command(
        CommandName.WAITAOF,
        version_introduced="7.1.240",
        group=CommandGroup.GENERIC,
        redirect_usage=RedirectUsage(
            (
                "Use the :meth:`Redis.ensure_persistence`  or :meth:`RedisCluster.ensure_persistence` context managers to ensure a command is synced to the AOF of the number of local hosts or replicas"
            ),
            True,
        ),
    )
    def waitaof(
        self, numlocal: int, numreplicas: int, timeout: int
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Wait for all write commands sent in the context of the current connection to be synced
        to AOF of local host and/or replicas

        :return: a tuple of (numlocal, numreplicas) that the write commands were synced to
        """

        return self.create_request(
            CommandName.WAITAOF,
            numlocal,
            numreplicas,
            timeout,
            callback=TupleCallback[int](),
        )

    @redis_command(
        CommandName.SCAN,
        group=CommandGroup.GENERIC,
        cluster=ClusterCommandConfig(enabled=False),
        flags={CommandFlag.READONLY},
    )
    def scan(
        self,
        cursor: int | None = 0,
        match: StringT | None = None,
        count: int | None = None,
        type_: StringT | None = None,
    ) -> CommandRequest[tuple[int, tuple[AnyStr, ...]]]:
        """
        Incrementally iterate the keys space
        """
        command_arguments: CommandArgList = [cursor or b"0"]

        if match is not None:
            command_arguments.extend([PrefixToken.MATCH, match])

        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])

        if type_ is not None:
            command_arguments.extend([PrefixToken.TYPE, type_])

        return self.create_request(
            CommandName.SCAN, *command_arguments, callback=ScanCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BLMOVE,
        version_introduced="6.2.0",
        group=CommandGroup.LIST,
        flags={CommandFlag.BLOCKING},
    )
    def blmove(
        self,
        source: KeyT,
        destination: KeyT,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
        timeout: int | float,
    ) -> CommandRequest[AnyStr | None]:
        """
        Pop an element from a list, push it to another list and return it;
        or block until one is available


        :return: the element being popped from :paramref:`source` and pushed to
         :paramref:`destination`
        """
        command_arguments: CommandArgList = [
            source,
            destination,
            wherefrom,
            whereto,
            timeout,
        ]

        return self.create_request(
            CommandName.BLMOVE, *command_arguments, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.BLMPOP,
        version_introduced="7.0.0",
        group=CommandGroup.LIST,
        flags={CommandFlag.BLOCKING},
    )
    def blmpop(
        self,
        keys: Parameters[KeyT],
        timeout: int | float,
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: int | None = None,
    ) -> CommandRequest[list[AnyStr | list[AnyStr]] | None]:
        """
        Pop elements from the first non empty list, or block until one is available

        :return:

         - A ``None`` when no element could be popped, and timeout is reached.
         - A two-element array with the first element being the name of the key
           from which elements were popped, and the second element is an array of elements.

        """
        _keys: list[KeyT] = list(keys)
        command_arguments: CommandArgList = [timeout, len(_keys), *_keys, where]

        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])

        return self.create_request(
            CommandName.BLMPOP,
            *command_arguments,
            callback=OptionalListCallback[AnyStr | list[AnyStr]](),
        )

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.BLPOP, group=CommandGroup.LIST, flags={CommandFlag.BLOCKING})
    def blpop(
        self, keys: Parameters[KeyT], timeout: int | float
    ) -> CommandRequest[list[AnyStr] | None]:
        """
        Remove and get the first element in a list, or block until one is available

        :return:

         - ``None`` when no element could be popped and the timeout expired.
         - A list with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return self.create_request(
            CommandName.BLPOP, *keys, timeout, callback=OptionalListCallback[AnyStr]()
        )

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.BRPOP, group=CommandGroup.LIST, flags={CommandFlag.BLOCKING})
    def brpop(
        self, keys: Parameters[KeyT], timeout: int | float
    ) -> CommandRequest[list[AnyStr] | None]:
        """
        Remove and get the last element in a list, or block until one is available

        :return:

         - ``None`` when no element could be popped and the timeout expired.
         - A list with the first element being the name of the key
           where an element was popped and the second element being the value of the
           popped element.
        """

        return self.create_request(
            CommandName.BRPOP, *keys, timeout, callback=OptionalListCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BRPOPLPUSH,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`blmove` with the `wherefrom` and `whereto` arguments",
        group=CommandGroup.LIST,
        flags={CommandFlag.BLOCKING},
    )
    def brpoplpush(
        self, source: KeyT, destination: KeyT, timeout: int | float
    ) -> CommandRequest[AnyStr | None]:
        """
        Pop an element from a list, push it to another list and return it;
        or block until one is available

        :return: the element being popped from :paramref:`source` and pushed to
         :paramref:`destination`.
        """

        return self.create_request(
            CommandName.BRPOPLPUSH,
            source,
            destination,
            timeout,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.LINDEX,
        group=CommandGroup.LIST,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def lindex(self, key: KeyT, index: int) -> CommandRequest[AnyStr | None]:
        """

        Get an element from a list by its index

        :return: the requested element, or ``None`` when ``index`` is out of range.
        """

        return self.create_request(
            CommandName.LINDEX, key, index, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(CommandName.LINSERT, group=CommandGroup.LIST)
    def linsert(
        self,
        key: KeyT,
        where: Literal[PureToken.AFTER, PureToken.BEFORE],
        pivot: ValueT,
        element: ValueT,
    ) -> CommandRequest[int]:
        """
        Inserts element in the list stored at key either before or after the reference
        value pivot.

        :return: the length of the list after the insert operation, or ``-1`` when
         the value pivot was not found.
        """

        return self.create_request(
            CommandName.LINSERT, key, where, pivot, element, callback=IntCallback()
        )

    @redis_command(
        CommandName.LLEN,
        group=CommandGroup.LIST,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def llen(self, key: KeyT) -> CommandRequest[int]:
        """
        :return: the length of the list at :paramref:`key`.
        """

        return self.create_request(CommandName.LLEN, key, callback=IntCallback())

    @redis_command(CommandName.LMOVE, version_introduced="6.2.0", group=CommandGroup.LIST)
    def lmove(
        self,
        source: KeyT,
        destination: KeyT,
        wherefrom: Literal[PureToken.LEFT, PureToken.RIGHT],
        whereto: Literal[PureToken.LEFT, PureToken.RIGHT],
    ) -> CommandRequest[AnyStr | None]:
        """
        Pop an element from a list, push it to another list and return it

        :return: the element being popped and pushed.
        """
        command_arguments: CommandArgList = [source, destination, wherefrom, whereto]

        return self.create_request(
            CommandName.LMOVE, *command_arguments, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(CommandName.LMPOP, version_introduced="7.0.0", group=CommandGroup.LIST)
    def lmpop(
        self,
        keys: Parameters[KeyT],
        where: Literal[PureToken.LEFT, PureToken.RIGHT],
        count: int | None = None,
    ) -> CommandRequest[list[AnyStr | list[AnyStr]] | None]:
        """
        Pop elements from the first non empty list

        :return:

         - A ```None``` when no element could be popped.
         - A two-element array with the first element being the name of the key
           from which elements were popped, and the second element is an array of elements.
        """
        _keys: list[KeyT] = list(keys)
        command_arguments: CommandArgList = [len(_keys), *_keys, where]

        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])

        return self.create_request(
            CommandName.LMPOP,
            *command_arguments,
            callback=OptionalListCallback[AnyStr | list[AnyStr]](),
        )

    @overload
    def lpop(self, key: KeyT) -> CommandRequest[AnyStr | None]: ...

    @overload
    def lpop(self, key: KeyT, count: int) -> CommandRequest[list[AnyStr] | None]: ...

    @redis_command(
        CommandName.LPOP,
        group=CommandGroup.LIST,
        arguments={"count": {"version_introduced": "6.2.0"}},
        flags={CommandFlag.FAST},
    )
    def lpop(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[AnyStr | None] | CommandRequest[list[AnyStr] | None]:
        """
        Remove and get the first :paramref:`count` elements in a list

        :return: the value of the first element, or ``None`` when :paramref:`key` does not exist.
         If :paramref:`count` is provided the return is a list of popped elements,
         or ``None`` when :paramref:`key` does not exist.
        """
        command_arguments: CommandArgList = []

        if count is not None:
            command_arguments.append(count)

        if count is not None:
            return self.create_request(
                CommandName.LPOP,
                key,
                *command_arguments,
                callback=OptionalListCallback[AnyStr](),
            )
        return self.create_request(
            CommandName.LPOP,
            key,
            *command_arguments,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.LPOS,
        version_introduced="6.0.6",
        group=CommandGroup.LIST,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def lpos(
        self,
        key: KeyT,
        element: ValueT,
        rank: int | None = None,
        count: int | None = None,
        maxlen: int | None = None,
    ) -> CommandRequest[int | None] | CommandRequest[list[int] | None]:
        """

        Return the index of matching elements on a list


        :return: The command returns the integer representing the matching element,
         or ``None`` if there is no match.

         If the :paramref:`count` argument is given a list of integers representing
         the matching elements.
        """
        command_arguments: CommandArgList = [key, element]

        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])

        if rank is not None:
            command_arguments.extend([PrefixToken.RANK, rank])

        if maxlen is not None:
            command_arguments.extend([PrefixToken.MAXLEN, maxlen])

        if count is None:
            return self.create_request(
                CommandName.LPOS, *command_arguments, callback=OptionalIntCallback()
            )
        return self.create_request(
            CommandName.LPOS, *command_arguments, callback=OptionalListCallback[int]()
        )

    @ensure_iterable_valid("elements")
    @redis_command(CommandName.LPUSH, group=CommandGroup.LIST, flags={CommandFlag.FAST})
    def lpush(self, key: KeyT, elements: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Prepend one or multiple elements to a list

        :return: the length of the list after the push operations.
        """
        return self.create_request(CommandName.LPUSH, key, *elements, callback=IntCallback())

    @ensure_iterable_valid("elements")
    @redis_command(CommandName.LPUSHX, group=CommandGroup.LIST, flags={CommandFlag.FAST})
    def lpushx(self, key: KeyT, elements: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Prepend an element to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return self.create_request(CommandName.LPUSHX, key, *elements, callback=IntCallback())

    @redis_command(
        CommandName.LRANGE,
        group=CommandGroup.LIST,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def lrange(self, key: KeyT, start: int, stop: int) -> CommandRequest[list[AnyStr]]:
        """
        Get a range of elements from a list

        :return: list of elements in the specified range.
        """

        return self.create_request(
            CommandName.LRANGE, key, start, stop, callback=ListCallback[AnyStr]()
        )

    @redis_command(CommandName.LREM, group=CommandGroup.LIST)
    def lrem(self, key: KeyT, count: int, element: ValueT) -> CommandRequest[int]:
        """
        Removes the first :paramref:`count` occurrences of elements equal to ``element``
        from the list stored at :paramref:`key`.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.

        :return: the number of removed elements.
        """

        return self.create_request(CommandName.LREM, key, count, element, callback=IntCallback())

    @redis_command(
        CommandName.LSET,
        group=CommandGroup.LIST,
    )
    def lset(self, key: KeyT, index: int, element: ValueT) -> CommandRequest[bool]:
        """Sets ``index`` of list :paramref:`key` to ``element``"""

        return self.create_request(
            CommandName.LSET, key, index, element, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.LTRIM,
        group=CommandGroup.LIST,
    )
    def ltrim(self, key: KeyT, start: int, stop: int) -> CommandRequest[bool]:
        """
        Trims the list :paramref:`key`, removing all values not within the slice
        between ``start`` and ``stop``

        ``start`` and ``stop`` can be negative numbers just like
        Python slicing notation
        """

        return self.create_request(
            CommandName.LTRIM, key, start, stop, callback=SimpleStringCallback()
        )

    @overload
    def rpop(self, key: KeyT) -> CommandRequest[AnyStr | None]: ...

    @overload
    def rpop(self, key: KeyT, count: int) -> CommandRequest[list[AnyStr] | None]: ...

    @redis_command(
        CommandName.RPOP,
        group=CommandGroup.LIST,
        arguments={"count": {"version_introduced": "6.2.0"}},
        flags={CommandFlag.FAST},
    )
    def rpop(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[AnyStr | None] | CommandRequest[list[AnyStr] | None]:
        """
        Remove and get the last elements in a list

        :return: When called without the :paramref:`count` argument the value
         of the last element, or ``None`` when :paramref:`key` does not exist.

         When called with the :paramref:`count` argument list of popped elements,
         or ``None`` when :paramref:`key` does not exist.
        """

        command_arguments: CommandArgList = []

        if count is not None:
            command_arguments.extend([count])

        if count is None:
            return self.create_request(
                CommandName.RPOP,
                key,
                *command_arguments,
                callback=OptionalAnyStrCallback[AnyStr](),
            )
        return self.create_request(
            CommandName.RPOP,
            key,
            *command_arguments,
            callback=OptionalListCallback[AnyStr](),
        )

    @redis_command(
        CommandName.RPOPLPUSH,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`lmove` with the wherefrom and whereto arguments",
        group=CommandGroup.LIST,
    )
    def rpoplpush(self, source: KeyT, destination: KeyT) -> CommandRequest[AnyStr | None]:
        """
        Remove the last element in a list, prepend it to another list and return it

        :return: the element being popped and pushed.
        """

        return self.create_request(
            CommandName.RPOPLPUSH,
            source,
            destination,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @ensure_iterable_valid("elements")
    @redis_command(
        CommandName.RPUSH,
        group=CommandGroup.LIST,
        flags={CommandFlag.FAST},
    )
    def rpush(self, key: KeyT, elements: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Append an element(s) to a list

        :return: the length of the list after the push operation.
        """

        return self.create_request(CommandName.RPUSH, key, *elements, callback=IntCallback())

    @ensure_iterable_valid("elements")
    @redis_command(
        CommandName.RPUSHX,
        group=CommandGroup.LIST,
        flags={CommandFlag.FAST},
    )
    def rpushx(self, key: KeyT, elements: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Append a element(s) to a list, only if the list exists

        :return: the length of the list after the push operation.
        """

        return self.create_request(CommandName.RPUSHX, key, *elements, callback=IntCallback())

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.SADD,
        group=CommandGroup.SET,
        flags={CommandFlag.FAST},
    )
    def sadd(self, key: KeyT, members: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Add one or more members to a set

        :return: the number of elements that were added to the set, not including
         all the elements already present in the set.
        """

        return self.create_request(CommandName.SADD, key, *members, callback=IntCallback())

    @redis_command(
        CommandName.SCARD,
        group=CommandGroup.SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def scard(self, key: KeyT) -> CommandRequest[int]:
        """
        Returns the number of members in the set

        :return the cardinality (number of elements) of the set, or ``0`` if :paramref:`key`
         does not exist.
        """

        return self.create_request(CommandName.SCARD, key, callback=IntCallback())

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.SDIFF,
        group=CommandGroup.SET,
        flags={CommandFlag.READONLY},
    )
    def sdiff(self, keys: Parameters[KeyT]) -> CommandRequest[_Set[AnyStr]]:
        """
        Subtract multiple sets

        :return: members of the resulting set.
        """

        return self.create_request(CommandName.SDIFF, *keys, callback=SetCallback[AnyStr]())

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.SDIFFSTORE, group=CommandGroup.SET)
    def sdiffstore(self, keys: Parameters[KeyT], destination: KeyT) -> CommandRequest[int]:
        """
        Subtract multiple sets and store the resulting set in a key

        """

        return self.create_request(
            CommandName.SDIFFSTORE, destination, *keys, callback=IntCallback()
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.SINTER,
        group=CommandGroup.SET,
        flags={CommandFlag.READONLY},
    )
    def sinter(self, keys: Parameters[KeyT]) -> CommandRequest[_Set[AnyStr]]:
        """
        Intersect multiple sets

        :return: members of the resulting set
        """

        return self.create_request(CommandName.SINTER, *keys, callback=SetCallback[AnyStr]())

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.SINTERSTORE, group=CommandGroup.SET)
    def sinterstore(self, keys: Parameters[KeyT], destination: KeyT) -> CommandRequest[int]:
        """
        Intersect multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.
        """

        return self.create_request(
            CommandName.SINTERSTORE, destination, *keys, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.SINTERCARD,
        version_introduced="7.0.0",
        group=CommandGroup.SET,
        flags={CommandFlag.READONLY},
    )
    def sintercard(
        self,
        keys: Parameters[KeyT],
        limit: int | None = None,
    ) -> CommandRequest[int]:
        """
        Intersect multiple sets and return the cardinality of the result

        :return: The number of elements in the resulting intersection.
        """
        _keys: list[KeyT] = list(keys)

        command_arguments: CommandArgList = [len(_keys), *_keys]

        if limit is not None:
            command_arguments.extend(["LIMIT", limit])

        return self.create_request(
            CommandName.SINTERCARD, *command_arguments, callback=IntCallback()
        )

    @redis_command(
        CommandName.SISMEMBER,
        group=CommandGroup.SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def sismember(self, key: KeyT, member: ValueT) -> CommandRequest[bool]:
        """
        Determine if a given value is a member of a set

        :return: If the element is a member of the set. ``False`` if :paramref:`key` does not exist.
        """

        return self.create_request(CommandName.SISMEMBER, key, member, callback=BoolCallback())

    @redis_command(
        CommandName.SMEMBERS,
        group=CommandGroup.SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def smembers(self, key: KeyT) -> CommandRequest[_Set[AnyStr]]:
        """Returns all members of the set"""

        return self.create_request(CommandName.SMEMBERS, key, callback=SetCallback[AnyStr]())

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.SMISMEMBER,
        version_introduced="6.2.0",
        group=CommandGroup.SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def smismember(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> CommandRequest[tuple[bool, ...]]:
        """
        Returns the membership associated with the given elements for a set

        :return: tuple representing the membership of the given elements, in the same
         order as they are requested.
        """

        return self.create_request(CommandName.SMISMEMBER, key, *members, callback=BoolsCallback())

    @redis_command(
        CommandName.SMOVE,
        group=CommandGroup.SET,
        flags={CommandFlag.FAST},
    )
    def smove(self, source: KeyT, destination: KeyT, member: ValueT) -> CommandRequest[bool]:
        """
        Move a member from one set to another
        """

        return self.create_request(
            CommandName.SMOVE, source, destination, member, callback=BoolCallback()
        )

    @redis_command(
        CommandName.SPOP,
        group=CommandGroup.SET,
        flags={CommandFlag.FAST},
    )
    def spop(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[AnyStr] | CommandRequest[_Set[AnyStr] | None]:
        """
        Remove and return one or multiple random members from a set

        :return: When called without the :paramref:`count` argument the removed member, or ``None``
         when :paramref:`key` does not exist.

         When called with the :paramref:`count` argument the removed members, or an empty array when
         :paramref:`key` does not exist.
        """

        if count is not None:
            return self.create_request(
                CommandName.SPOP,
                key,
                count,
                callback=SetCallback[AnyStr](count=count),
            )
        else:
            return self.create_request(CommandName.SPOP, key, callback=AnyStrCallback[AnyStr]())

    @redis_command(
        CommandName.SRANDMEMBER,
        group=CommandGroup.SET,
        flags={CommandFlag.READONLY},
    )
    def srandmember(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[AnyStr | _Set[AnyStr]]:
        """
        Get one or multiple random members from a set



        :return: without the additional :paramref:`count` argument, the command returns a  randomly
         selected element, or ``None`` when :paramref:`key` does not exist.

         When the additional :paramref:`count` argument is passed, the command returns elements,
         or an empty set when :paramref:`key` does not exist.
        """
        command_arguments: CommandArgList = []

        if count is not None:
            command_arguments.append(count)

        return self.create_request(
            CommandName.SRANDMEMBER,
            key,
            *command_arguments,
            callback=ItemOrSetCallback[AnyStr](count=count),
        )

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.SREM,
        group=CommandGroup.SET,
        flags={CommandFlag.FAST},
    )
    def srem(self, key: KeyT, members: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Remove one or more members from a set


        :return: the number of members that were removed from the set, not
         including non existing members.
        """

        return self.create_request(CommandName.SREM, key, *members, callback=IntCallback())

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.SUNION,
        group=CommandGroup.SET,
        flags={CommandFlag.READONLY},
    )
    def sunion(self, keys: Parameters[KeyT]) -> CommandRequest[_Set[AnyStr]]:
        """
        Add multiple sets

        :return: members of the resulting set.
        """

        return self.create_request(CommandName.SUNION, *keys, callback=SetCallback[AnyStr]())

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.SUNIONSTORE, group=CommandGroup.SET)
    def sunionstore(self, keys: Parameters[KeyT], destination: KeyT) -> CommandRequest[int]:
        """
        Add multiple sets and store the resulting set in a key

        :return: the number of elements in the resulting set.

        """

        return self.create_request(
            CommandName.SUNIONSTORE, destination, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.SSCAN,
        group=CommandGroup.SET,
        cluster=ClusterCommandConfig(combine=ClusterEnsureConsistent()),
        flags={CommandFlag.READONLY},
    )
    def sscan(
        self,
        key: KeyT,
        cursor: int | None = 0,
        match: StringT | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[int, _Set[AnyStr]]]:
        """
        Incrementally returns subsets of elements in a set. Also returns a
        cursor pointing to the scan position.

        :param match: is for filtering the keys by pattern
        :param count: is for hint the minimum number of returns
        """
        command_arguments: CommandArgList = [key, cursor or "0"]

        if match is not None:
            command_arguments.extend(["MATCH", match])

        if count is not None:
            command_arguments.extend(["COUNT", count])

        return self.create_request(
            CommandName.SSCAN, *command_arguments, callback=SScanCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.BZMPOP,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.BLOCKING},
    )
    def bzmpop(
        self,
        keys: Parameters[KeyT],
        timeout: int | float,
        where: Literal[PureToken.MAX, PureToken.MIN],
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr, tuple[ScoredMember, ...]] | None]:
        """
        Remove and return members with scores in a sorted set or block until one is available

        :return:

          - A ```None``` when no element could be popped.
          - A tuple of (name of key, popped (member, score) pairs)
        """
        _keys: list[KeyT] = list(keys)
        command_arguments: CommandArgList = [timeout, len(_keys), *_keys, where]

        if count is not None:
            command_arguments.extend(["COUNT", count])

        return self.create_request(
            CommandName.BZMPOP, *command_arguments, callback=ZMPopCallback[AnyStr]()
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.BZPOPMAX,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.FAST, CommandFlag.BLOCKING},
    )
    def bzpopmax(
        self, keys: Parameters[KeyT], timeout: int | float
    ) -> CommandRequest[tuple[AnyStr, AnyStr, float] | None]:
        """
        Remove and return the member with the highest score from one or more sorted sets,
        or block until one is available.

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """
        command_arguments: CommandArgList = [*keys, timeout]

        return self.create_request(
            CommandName.BZPOPMAX, *command_arguments, callback=BZPopCallback[AnyStr]()
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.BZPOPMIN,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.FAST, CommandFlag.BLOCKING},
    )
    def bzpopmin(
        self, keys: Parameters[KeyT], timeout: int | float
    ) -> CommandRequest[tuple[AnyStr, AnyStr, float] | None]:
        """
        Remove and return the member with the lowest score from one or more sorted sets,
        or block until one is available

        :return: A triplet with the first element being the name of the key
         where a member was popped, the second element is the popped member itself,
         and the third element is the score of the popped element.
        """

        command_arguments: CommandArgList = [*keys, timeout]

        return self.create_request(
            CommandName.BZPOPMIN, *command_arguments, callback=BZPopCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZADD,
        group=CommandGroup.SORTED_SET,
        arguments={"comparison": {"version_introduced": "6.2.0"}},
        flags={CommandFlag.FAST},
    )
    def zadd(
        self,
        key: KeyT,
        member_scores: Mapping[StringT, int | float],
        condition: Literal[PureToken.NX, PureToken.XX] | None = None,
        comparison: Literal[PureToken.GT, PureToken.LT] | None = None,
        change: bool | None = None,
        increment: bool | None = None,
    ) -> CommandRequest[int | float]:
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
        command_arguments: CommandArgList = []

        if change is not None:
            command_arguments.append(PureToken.CHANGE)

        if increment is not None:
            command_arguments.append(PureToken.INCREMENT)

        if condition:
            command_arguments.append(condition)

        if comparison:
            command_arguments.append(comparison)

        flat_member_scores = dict_to_flat_list(member_scores, reverse=True)
        command_arguments.extend(flat_member_scores)

        return self.create_request(
            CommandName.ZADD, key, *command_arguments, callback=ZAddCallback()
        )

    @redis_command(
        CommandName.ZCARD,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zcard(self, key: KeyT) -> CommandRequest[int]:
        """
        Get the number of members in a sorted set

        :return: the cardinality (number of elements) of the sorted set, or ``0``
         if the :paramref:`key` does not exist

        """

        return self.create_request(CommandName.ZCARD, key, callback=IntCallback())

    @redis_command(
        CommandName.ZCOUNT,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zcount(
        self,
        key: KeyT,
        min_: ValueT,
        max_: ValueT,
    ) -> CommandRequest[int]:
        """
        Count the members in a sorted set with scores within the given values

        :return: the number of elements in the specified score range.
        """

        return self.create_request(CommandName.ZCOUNT, key, min_, max_, callback=IntCallback())

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZDIFF,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zdiff(
        self, keys: Parameters[KeyT], withscores: bool | None = None
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """
        Subtract multiple sorted sets

        :return: the result of the difference (optionally with their scores, in case
         the ``withscores`` option is given).
        """
        command_arguments: CommandArgList = [len(list(keys)), *keys]

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)

        return self.create_request(
            CommandName.ZDIFF,
            *command_arguments,
            callback=ZMembersOrScoredMembers[AnyStr](withscores=withscores),
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZDIFFSTORE,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    def zdiffstore(self, keys: Parameters[KeyT], destination: KeyT) -> CommandRequest[int]:
        """
        Subtract multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """
        command_arguments: CommandArgList = [len(list(keys)), *keys]

        return self.create_request(
            CommandName.ZDIFFSTORE,
            destination,
            *command_arguments,
            callback=IntCallback(),
        )

    @redis_command(
        CommandName.ZINCRBY,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.FAST},
    )
    def zincrby(self, key: KeyT, member: ValueT, increment: int) -> CommandRequest[float]:
        """
        Increment the score of a member in a sorted set

        :return: the new score of :paramref:`member`
        """

        return self.create_request(
            CommandName.ZINCRBY,
            key,
            increment,
            member,
            callback=FloatCallback(),
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZINTER,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zinter(
        self,
        keys: Parameters[KeyT],
        weights: Parameters[int] | None = None,
        aggregate: Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM] | None = None,
        withscores: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Intersect multiple sorted sets

        :return: the result of intersection (optionally with their scores, in case
         the ``withscores`` option is given).

        """

        return self._zaggregate(
            CommandName.ZINTER, keys, weights=weights, aggregate=aggregate, withscores=withscores
        )

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.ZINTERSTORE, group=CommandGroup.SORTED_SET)
    def zinterstore(
        self,
        keys: Parameters[KeyT],
        destination: KeyT,
        weights: Parameters[int] | None = None,
        aggregate: Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM] | None = None,
    ) -> CommandRequest[int]:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """

        return self._zaggregate(
            CommandName.ZINTERSTORE,
            keys,
            destination=destination,
            weights=weights,
            aggregate=aggregate,
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZINTERCARD,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zintercard(self, keys: Parameters[KeyT], limit: int | None = None) -> CommandRequest[int]:
        """
        Intersect multiple sorted sets and return the cardinality of the result

        :return: The number of elements in the resulting intersection.

        """
        _keys: list[KeyT] = list(keys)
        command_arguments: CommandArgList = [len(_keys), *_keys]

        if limit is not None:
            command_arguments.extend(["LIMIT", limit])

        return self.create_request(
            CommandName.ZINTERCARD, *command_arguments, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZLEXCOUNT,
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zlexcount(self, key: KeyT, min_: ValueT, max_: ValueT) -> CommandRequest[int]:
        """
        Count the number of members in a sorted set between a given lexicographical range

        :return: the number of elements in the specified score range.
        """

        return self.create_request(CommandName.ZLEXCOUNT, key, min_, max_, callback=IntCallback())

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZMPOP,
        version_introduced="7.0.0",
        group=CommandGroup.SORTED_SET,
    )
    def zmpop(
        self,
        keys: Parameters[KeyT],
        where: Literal[PureToken.MAX, PureToken.MIN],
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr, tuple[ScoredMember, ...]] | None]:
        """
        Remove and return members with scores in a sorted set

        :return: A tuple of (name of key, popped (member, score) pairs)
        """
        _keys: list[KeyT] = list(keys)
        command_arguments: CommandArgList = [len(_keys), *_keys, where]

        if count is not None:
            command_arguments.extend(["COUNT", count])

        return self.create_request(
            CommandName.ZMPOP, *command_arguments, callback=ZMPopCallback[AnyStr]()
        )

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.ZMSCORE,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zmscore(
        self, key: KeyT, members: Parameters[ValueT]
    ) -> CommandRequest[tuple[float | None, ...]]:
        """
        Get the score associated with the given members in a sorted set

        :return: scores or ``None`` associated with the specified :paramref:`members`
         values (a double precision floating point number), represented as strings

        """

        if not members:
            raise DataError("ZMSCORE members must be a non-empty list")

        return self.create_request(CommandName.ZMSCORE, key, *members, callback=ZMScoreCallback())

    @redis_command(
        CommandName.ZPOPMAX,
        group=CommandGroup.SORTED_SET,
    )
    def zpopmax(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[ScoredMember | tuple[ScoredMember, ...] | None]:
        """
        Remove and return members with the highest scores in a sorted set

        :return: popped elements and scores.
        """
        args = (count is not None) and [count] or []
        return self.create_request(
            CommandName.ZPOPMAX,
            key,
            *args,
            callback=ZSetScorePairCallback[AnyStr](count=count),
        )

    @redis_command(
        CommandName.ZPOPMIN,
        group=CommandGroup.SORTED_SET,
    )
    def zpopmin(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[ScoredMember | tuple[ScoredMember, ...] | None]:
        """
        Remove and return members with the lowest scores in a sorted set

        :return: popped elements and scores.
        """
        args = (count is not None) and [count] or []

        return self.create_request(
            CommandName.ZPOPMIN,
            key,
            *args,
            callback=ZSetScorePairCallback[AnyStr](count=count),
        )

    @redis_command(
        CommandName.ZRANDMEMBER,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zrandmember(
        self,
        key: KeyT,
        count: int | None = None,
        withscores: bool | None = None,
    ) -> CommandRequest[AnyStr | tuple[AnyStr, ...] | tuple[ScoredMember, ...] | None]:
        """
        Get one or multiple random elements from a sorted set


        :return: without :paramref:`count`, the command returns a
         randomly selected element, or ``None`` when :paramref:`key` does not exist.

         If :paramref:`count` is passed the command returns the elements,
         or an empty tuple when :paramref:`key` does not exist.

         If :paramref:`withscores` argument is used, the return is a list elements
         and their scores from the sorted set.
        """
        command_arguments: CommandArgList = [key]
        options = {}

        if count is not None:
            command_arguments.append(count)
            options["count"] = count

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
            options["withscores"] = True

        return self.create_request(
            CommandName.ZRANDMEMBER,
            *command_arguments,
            callback=ZRandMemberCallback[AnyStr](**options),
        )

    @overload
    def zrange(
        self,
        key: KeyT,
        min_: int | ValueT,
        max_: int | ValueT,
        sortby: Literal[PureToken.BYSCORE, PureToken.BYLEX] | None = None,
        rev: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...]]: ...

    @overload
    def zrange(
        self,
        key: KeyT,
        min_: int | ValueT,
        max_: int | ValueT,
        sortby: Literal[PureToken.BYSCORE, PureToken.BYLEX] | None = None,
        rev: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
        *,
        withscores: Literal[True],
    ) -> CommandRequest[tuple[ScoredMember, ...]]: ...

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.ZRANGE,
        group=CommandGroup.SORTED_SET,
        arguments={
            "sortby": {"version_introduced": "6.2.0"},
            "rev": {"version_introduced": "6.2.0"},
            "offset": {"version_introduced": "6.2.0"},
            "count": {"version_introduced": "6.2.0"},
        },
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def zrange(
        self,
        key: KeyT,
        min_: int | ValueT,
        max_: int | ValueT,
        sortby: Literal[PureToken.BYSCORE, PureToken.BYLEX] | None = None,
        rev: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
        withscores: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Return a range of members in a sorted set

        :return: elements in the specified range (optionally with their scores, in case
         :paramref:`withscores` is given).
        """

        return self._zrange(
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
        version_deprecated="6.2.0",
        deprecation_reason=" Use :meth:`zrange` with the sortby=BYLEX argument",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("offset", "count")
    def zrangebylex(
        self,
        key: KeyT,
        min_: ValueT,
        max_: ValueT,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...]]:
        """

        Return a range of members in a sorted set, by lexicographical range

        :return: elements in the specified score range.
        """

        command_arguments: CommandArgList = [key, min_, max_]

        if offset is not None and count is not None:
            command_arguments.extend(["LIMIT", offset, count])

        return self.create_request(
            CommandName.ZRANGEBYLEX,
            *command_arguments,
            callback=TupleCallback[AnyStr](),
        )

    @redis_command(
        CommandName.ZRANGEBYSCORE,
        version_deprecated="6.2.0",
        deprecation_reason=" Use :meth:`zrange` with the sortby=BYSCORE argument",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("offset", "count")
    def zrangebyscore(
        self,
        key: KeyT,
        min_: int | float,
        max_: int | float,
        withscores: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Return a range of members in a sorted set, by score

        :return: elements in the specified score range (optionally with their scores).
        """

        command_arguments: CommandArgList = [key, min_, max_]

        if offset is not None and count is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset, count])

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
        options = {"withscores": withscores}

        return self.create_request(
            CommandName.ZRANGEBYSCORE,
            *command_arguments,
            callback=ZMembersOrScoredMembers[AnyStr](**options),
        )

    @mutually_inclusive_parameters("offset", "count")
    @redis_command(
        CommandName.ZRANGESTORE,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
    )
    def zrangestore(
        self,
        dst: KeyT,
        src: KeyT,
        min_: int | ValueT,
        max_: int | ValueT,
        sortby: Literal[PureToken.BYSCORE, PureToken.BYLEX] | None = None,
        rev: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[int]:
        """
        Store a range of members from sorted set into another key

        :return: the number of elements in the resulting sorted set
        """

        return self._zrange(
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
        arguments={"withscore": {"version_introduced": "7.1.240"}},
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zrank(
        self, key: KeyT, member: ValueT, withscore: bool | None = None
    ) -> CommandRequest[int | tuple[int, float] | None]:
        """
        Determine the index of a member in a sorted set

        :return: the rank of :paramref:`member`. If :paramref:`withscore` is `True`
         the return is a tuple of (rank, score).
        """
        command_arguments: CommandArgList = [key, member]

        if withscore:
            command_arguments.append(PureToken.WITHSCORE)

        return self.create_request(
            CommandName.ZRANK,
            *command_arguments,
            callback=ZRankCallback(withscore=withscore),
        )

    @ensure_iterable_valid("members")
    @redis_command(
        CommandName.ZREM,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.FAST},
    )
    def zrem(self, key: KeyT, members: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Remove one or more members from a sorted set

        :return: The number of members removed from the sorted set, not including non existing
         members.
        """

        return self.create_request(CommandName.ZREM, key, *members, callback=IntCallback())

    @redis_command(CommandName.ZREMRANGEBYLEX, group=CommandGroup.SORTED_SET)
    def zremrangebylex(self, key: KeyT, min_: ValueT, max_: ValueT) -> CommandRequest[int]:
        """
        Remove all members in a sorted set between the given lexicographical range

        :return: the number of elements removed.
        """

        return self.create_request(
            CommandName.ZREMRANGEBYLEX, key, min_, max_, callback=IntCallback()
        )

    @redis_command(CommandName.ZREMRANGEBYRANK, group=CommandGroup.SORTED_SET)
    def zremrangebyrank(self, key: KeyT, start: int, stop: int) -> CommandRequest[int]:
        """
        Remove all members in a sorted set within the given indexes

        :return: the number of elements removed.
        """

        return self.create_request(
            CommandName.ZREMRANGEBYRANK, key, start, stop, callback=IntCallback()
        )

    @redis_command(CommandName.ZREMRANGEBYSCORE, group=CommandGroup.SORTED_SET)
    def zremrangebyscore(
        self, key: KeyT, min_: int | float, max_: int | float
    ) -> CommandRequest[int]:
        """
        Remove all members in a sorted set within the given scores

        :return: the number of elements removed.
        """

        return self.create_request(
            CommandName.ZREMRANGEBYSCORE, key, min_, max_, callback=IntCallback()
        )

    @redis_command(
        CommandName.ZREVRANGE,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev argument",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def zrevrange(
        self,
        key: KeyT,
        start: int,
        stop: int,
        withscores: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Return a range of members in a sorted set, by index, with scores ordered from
        high to low

        :return: elements in the specified range (optionally with their scores).
        """
        command_arguments: CommandArgList = [key, start, stop]

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)

        return self.create_request(
            CommandName.ZREVRANGE,
            *command_arguments,
            callback=ZMembersOrScoredMembers[AnyStr](withscores=withscores),
        )

    @redis_command(
        CommandName.ZREVRANGEBYLEX,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev and sort=BYLEX arguments",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("offset", "count")
    def zrevrangebylex(
        self,
        key: KeyT,
        max_: ValueT,
        min_: ValueT,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...]]:
        """

        Return a range of members in a sorted set, by lexicographical range, ordered from
        higher to lower strings.

        :return: elements in the specified score range
        """

        command_arguments: CommandArgList = [key, max_, min_]

        if offset is not None and count is not None:
            command_arguments.extend(["LIMIT", offset, count])

        return self.create_request(
            CommandName.ZREVRANGEBYLEX,
            *command_arguments,
            callback=TupleCallback[AnyStr](),
        )

    @redis_command(
        CommandName.ZREVRANGEBYSCORE,
        version_deprecated="6.2.0",
        deprecation_reason="Use :meth:`zrange` with the rev and sort=BYSCORE arguments",
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("offset", "count")
    def zrevrangebyscore(
        self,
        key: KeyT,
        max_: int | float,
        min_: int | float,
        withscores: bool | None = None,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Return a range of members in a sorted set, by score, with scores ordered from high to low

        :return: elements in the specified score range (optionally with their scores)
        """

        command_arguments: CommandArgList = [key, max_, min_]

        if offset is not None and count is not None:
            command_arguments.extend(["LIMIT", offset, count])

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)

        return self.create_request(
            CommandName.ZREVRANGEBYSCORE,
            *command_arguments,
            callback=ZMembersOrScoredMembers[AnyStr](withscores=withscores),
        )

    @redis_command(
        CommandName.ZREVRANK,
        arguments={"withscore": {"version_introduced": "7.1.240"}},
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zrevrank(
        self, key: KeyT, member: ValueT, withscore: bool | None = None
    ) -> CommandRequest[int | tuple[int, float] | None]:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low

        :return: the rank of :paramref:`member`. If :paramref:`withscore` is `True`
         the return is a tuple of (rank, score).
        """
        command_arguments: CommandArgList = [key, member]
        if withscore:
            command_arguments.append(PureToken.WITHSCORE)
        return self.create_request(
            CommandName.ZREVRANK,
            *command_arguments,
            callback=ZRankCallback(withscore=withscore),
        )

    @redis_command(
        CommandName.ZSCAN,
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zscan(
        self,
        key: KeyT,
        cursor: int | None = 0,
        match: StringT | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[int, tuple[ScoredMember, ...]]]:
        """
        Incrementally iterate sorted sets elements and associated scores

        """
        command_arguments: CommandArgList = [key, cursor or "0"]

        if match is not None:
            command_arguments.extend(["MATCH", match])

        if count is not None:
            command_arguments.extend(["COUNT", count])

        return self.create_request(
            CommandName.ZSCAN, *command_arguments, callback=ZScanCallback[AnyStr]()
        )

    @redis_command(
        CommandName.ZSCORE,
        group=CommandGroup.SORTED_SET,
        cacheable=True,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def zscore(self, key: KeyT, member: ValueT) -> CommandRequest[float | None]:
        """
        Get the score associated with the given member in a sorted set

        :return: the score of :paramref:`member` (a double precision floating point number),
         or ``None`` if the member doesn't exist.
        """

        return self.create_request(
            CommandName.ZSCORE, key, member, callback=OptionalFloatCallback()
        )

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.ZUNION,
        version_introduced="6.2.0",
        group=CommandGroup.SORTED_SET,
        flags={CommandFlag.READONLY},
    )
    def zunion(
        self,
        keys: Parameters[KeyT],
        weights: Parameters[int] | None = None,
        aggregate: Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX] | None = None,
        withscores: bool | None = None,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        """

        Add multiple sorted sets

        :return: the result of union (optionally with their scores, in case
         :paramref:`withscores` is given).
        """

        return self._zaggregate(
            CommandName.ZUNION,
            keys,
            weights=weights,
            aggregate=aggregate,
            withscores=withscores,
        )

    @ensure_iterable_valid("keys")
    @redis_command(CommandName.ZUNIONSTORE, group=CommandGroup.SORTED_SET)
    def zunionstore(
        self,
        keys: Parameters[KeyT],
        destination: KeyT,
        weights: Parameters[int] | None = None,
        aggregate: Literal[PureToken.SUM, PureToken.MIN, PureToken.MAX] | None = None,
    ) -> CommandRequest[int]:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key

        :return: the number of elements in the resulting sorted set at :paramref:`destination`.
        """

        return self._zaggregate(
            CommandName.ZUNIONSTORE,
            keys,
            destination=destination,
            weights=weights,
            aggregate=aggregate,
        )

    @overload
    def _zrange(
        self,
        command: Literal[CommandName.ZRANGESTORE],
        key: KeyT,
        start: int | ValueT,
        stop: int | ValueT,
        dest: ValueT | None = ...,
        rev: bool | None = None,
        sortby: PureToken | None = ...,
        withscores: bool | None = ...,
        offset: int | None = ...,
        count: int | None = ...,
    ) -> CommandRequest[int]: ...

    @overload
    def _zrange(
        self,
        command: Literal[CommandName.ZRANGE],
        key: KeyT,
        start: int | ValueT,
        stop: int | ValueT,
        dest: ValueT | None = ...,
        rev: bool | None = None,
        sortby: PureToken | None = ...,
        withscores: bool | None = ...,
        offset: int | None = ...,
        count: int | None = ...,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]: ...

    def _zrange(
        self,
        command: Literal[CommandName.ZRANGE, CommandName.ZRANGESTORE],
        key: KeyT,
        start: int | ValueT,
        stop: int | ValueT,
        dest: ValueT | None = None,
        rev: bool | None = None,
        sortby: PureToken | None = None,
        withscores: bool | None = False,
        offset: int | None = None,
        count: int | None = None,
    ) -> CommandRequest[int] | CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        command_arguments: CommandArgList = []

        if dest:
            command_arguments.append(dest)
        command_arguments.extend([key, start, stop])

        if sortby:
            command_arguments.append(sortby)

        if rev is not None:
            command_arguments.append(PureToken.REV)

        if offset is not None and count is not None:
            command_arguments.extend([PrefixToken.LIMIT, offset, count])

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)

        if command == CommandName.ZRANGE:
            return self.create_request(
                command,
                *command_arguments,
                callback=ZMembersOrScoredMembers[AnyStr](withscores=withscores),
            )
        else:
            return self.create_request(command, *command_arguments, callback=IntCallback())

    @overload
    def _zaggregate(
        self,
        command: Literal[
            CommandName.ZUNIONSTORE,
            CommandName.ZINTERSTORE,
        ],
        keys: Parameters[KeyT],
        *,
        destination: KeyT | None = ...,
        weights: Parameters[int] | None = ...,
        aggregate: Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM] | None = ...,
        withscores: bool | None = ...,
    ) -> CommandRequest[int]: ...

    @overload
    def _zaggregate(
        self,
        command: Literal[
            CommandName.ZUNION,
            CommandName.ZINTER,
        ],
        keys: Parameters[KeyT],
        *,
        destination: KeyT | None = ...,
        weights: Parameters[int] | None = ...,
        aggregate: Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM] | None = ...,
        withscores: bool | None = ...,
    ) -> CommandRequest[tuple[AnyStr | ScoredMember, ...]]: ...

    def _zaggregate(
        self,
        command: Literal[
            CommandName.ZUNION,
            CommandName.ZUNIONSTORE,
            CommandName.ZINTER,
            CommandName.ZINTERSTORE,
        ],
        keys: Parameters[KeyT],
        *,
        destination: KeyT | None = None,
        weights: Parameters[int] | None = None,
        aggregate: Literal[PureToken.MAX, PureToken.MIN, PureToken.SUM] | None = None,
        withscores: bool | None = None,
    ) -> CommandRequest[int] | CommandRequest[tuple[AnyStr | ScoredMember, ...]]:
        command_arguments: CommandArgList = []

        if destination:
            command_arguments.append(destination)
        command_arguments.append(len(list(keys)))
        command_arguments.extend(keys)
        options = {}

        if weights:
            command_arguments.append(PrefixToken.WEIGHTS)
            command_arguments.extend(weights)

        if aggregate:
            command_arguments.append(PrefixToken.AGGREGATE)
            command_arguments.append(aggregate)

        if withscores is not None:
            command_arguments.append(PureToken.WITHSCORES)
            options = {"withscores": True}

        if command in [CommandName.ZUNIONSTORE, CommandName.ZINTERSTORE]:
            return self.create_request(
                command,
                *command_arguments,
                callback=IntCallback(),
            )
        else:
            return self.create_request(
                command,
                *command_arguments,
                callback=ZMembersOrScoredMembers[AnyStr](**options),
            )

    @ensure_iterable_valid("identifiers")
    @redis_command(
        CommandName.XACK,
        group=CommandGroup.STREAM,
        flags={CommandFlag.FAST},
    )
    def xack(
        self, key: KeyT, group: StringT, identifiers: Parameters[ValueT]
    ) -> CommandRequest[int]:
        """
        Marks a pending message as correctly processed,
        effectively removing it from the pending entries list of the consumer group.

        :return: number of messages successfully acknowledged,
         that is, the IDs we were actually able to resolve in the PEL.
        """

        return self.create_request(
            CommandName.XACK, key, group, *identifiers, callback=IntCallback()
        )

    @versionadded(version="5.2.0")
    @ensure_iterable_valid("identifiers")
    @redis_command(
        CommandName.XACKDEL,
        version_introduced="8.2.0",
        group=CommandGroup.STREAM,
        flags={CommandFlag.FAST},
    )
    def xackdel(
        self,
        key: KeyT,
        group: StringT,
        identifiers: Parameters[ValueT],
        condition: Literal[PureToken.KEEPREF, PureToken.DELREF, PureToken.ACKED] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Acknowledges and conditionally deletes one or multiple entries (messages) for a stream
        consumer group at the specified key.

        """
        command_arguments: CommandArgList = [key, group]
        if condition is not None:
            command_arguments.append(condition)

        command_arguments.extend([PrefixToken.IDS, len(list(identifiers)), *identifiers])
        return self.create_request(
            CommandName.XACKDEL, *command_arguments, callback=TupleCallback[int]()
        )

    @mutually_inclusive_parameters("trim_strategy", "threshold")
    @redis_command(
        CommandName.XADD,
        group=CommandGroup.STREAM,
        arguments={
            "nomkstream": {"version_introduced": "6.2.0"},
            "limit": {"version_introduced": "6.2.0"},
            "condition": {"version_introduced": "8.2.0"},
        },
        flags={CommandFlag.FAST},
    )
    def xadd(
        self,
        key: KeyT,
        field_values: Mapping[StringT, ValueT],
        identifier: ValueT | None = None,
        nomkstream: bool | None = None,
        trim_strategy: Literal[PureToken.MAXLEN, PureToken.MINID] | None = None,
        threshold: int | None = None,
        trim_operator: Literal[PureToken.EQUAL, PureToken.APPROXIMATELY] | None = None,
        limit: int | None = None,
        condition: Literal[PureToken.KEEPREF, PureToken.DELREF, PureToken.ACKED] | None = None,
    ) -> CommandRequest[AnyStr | None]:
        """
        Appends a new entry to a stream

        :return: The identifier of the added entry. The identifier is the one auto-generated
         if ``*`` is passed as :paramref:`identifier`, otherwise the it justs returns
         the same identifier specified

         Returns ``None`` when used with :paramref:`nomkstream` and the key doesn't exist.

        """
        command_arguments: CommandArgList = []

        if nomkstream is not None:
            command_arguments.append(PureToken.NOMKSTREAM)
        if condition is not None:
            command_arguments.append(condition)
        if trim_strategy == PureToken.MAXLEN:
            command_arguments.append(trim_strategy)

            if trim_operator:
                command_arguments.append(trim_operator)

            if threshold is not None:
                command_arguments.append(threshold)

        if limit is not None:
            command_arguments.extend(["LIMIT", limit])

        command_arguments.append(identifier or PureToken.AUTO_ID)

        for kv in field_values.items():
            command_arguments.extend(list(kv))

        return self.create_request(
            CommandName.XADD,
            key,
            *command_arguments,
            callback=OptionalAnyStrCallback[AnyStr](),
        )

    @redis_command(
        CommandName.XLEN,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def xlen(self, key: KeyT) -> CommandRequest[int]:
        """ """

        return self.create_request(CommandName.XLEN, key, callback=IntCallback())

    @redis_command(
        CommandName.XRANGE,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY},
    )
    def xrange(
        self,
        key: KeyT,
        start: ValueT | None = None,
        end: ValueT | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[StreamEntry, ...]]:
        """
        Return a range of elements in a stream, with IDs matching the specified IDs interval
        """

        command_arguments: CommandArgList = [
            start if start is not None else "-",
            end if end is not None else "+",
        ]

        if count is not None:
            command_arguments.append(PrefixToken.COUNT)
            command_arguments.append(count)

        return self.create_request(
            CommandName.XRANGE, key, *command_arguments, callback=StreamRangeCallback()
        )

    @redis_command(
        CommandName.XREVRANGE,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY},
    )
    def xrevrange(
        self,
        key: KeyT,
        end: ValueT | None = None,
        start: ValueT | None = None,
        count: int | None = None,
    ) -> CommandRequest[tuple[StreamEntry, ...]]:
        """
        Return a range of elements in a stream, with IDs matching the specified
        IDs interval, in reverse order (from greater to smaller IDs) compared to XRANGE
        """
        command_arguments: CommandArgList = [
            end if end is not None else "+",
            start if start is not None else "-",
        ]

        if count is not None:
            command_arguments.append(PrefixToken.COUNT)
            command_arguments.append(count)

        return self.create_request(
            CommandName.XREVRANGE,
            key,
            *command_arguments,
            callback=StreamRangeCallback(),
        )

    @redis_command(
        CommandName.XREAD,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY, CommandFlag.BLOCKING},
    )
    def xread(
        self,
        streams: Mapping[ValueT, ValueT],
        count: int | None = None,
        block: int | datetime.timedelta | None = None,
    ) -> CommandRequest[dict[AnyStr, tuple[StreamEntry, ...]] | None]:
        """
        Return never seen elements in multiple streams, with IDs greater than
        the ones reported by the caller for each stream. Can block.

        :return: Mapping of streams to stream entries.
         Field and values are guaranteed to be reported in the same order they were
         added by :meth:`xadd`.

         When :paramref:`block` is used, on timeout ``None`` is returned.
        """
        command_arguments: CommandArgList = []

        if block is not None:
            command_arguments.append(PrefixToken.BLOCK)
            command_arguments.append(normalized_milliseconds(block))

        if count is not None:
            command_arguments.append(PrefixToken.COUNT)
            command_arguments.append(count)
        command_arguments.append(PrefixToken.STREAMS)
        ids: CommandArgList = []

        for partial_stream in streams.items():
            command_arguments.append(partial_stream[0])
            ids.append(partial_stream[1])
        command_arguments.extend(ids)

        return self.create_request(
            CommandName.XREAD,
            *command_arguments,
            callback=MultiStreamRangeCallback[AnyStr](),
        )

    @redis_command(CommandName.XREADGROUP, group=CommandGroup.STREAM, flags={CommandFlag.BLOCKING})
    def xreadgroup(
        self,
        group: StringT,
        consumer: StringT,
        streams: Mapping[ValueT, ValueT],
        count: int | None = None,
        block: int | datetime.timedelta | None = None,
        noack: bool | None = None,
    ) -> CommandRequest[dict[AnyStr, tuple[StreamEntry, ...]] | None]:
        """ """
        command_arguments: CommandArgList = [PrefixToken.GROUP, group, consumer]

        if block is not None:
            command_arguments.append(PrefixToken.BLOCK)
            command_arguments.append(normalized_milliseconds(block))

        if count is not None:
            command_arguments.append(PrefixToken.COUNT)
            command_arguments.append(count)

        if noack:
            command_arguments.append(PureToken.NOACK)

        command_arguments.append(PrefixToken.STREAMS)
        ids: CommandArgList = []

        for partial_stream in streams.items():
            command_arguments.append(partial_stream[0])
            ids.append(partial_stream[1])
        command_arguments.extend(ids)
        return self.create_request(
            CommandName.XREADGROUP,
            *command_arguments,
            callback=MultiStreamRangeCallback[AnyStr](),
        )

    @mutually_inclusive_parameters("start", "end", "count")
    @redis_command(
        CommandName.XPENDING,
        group=CommandGroup.STREAM,
        arguments={"idle": {"version_introduced": "6.2.0"}},
        flags={CommandFlag.READONLY},
    )
    def xpending(
        self,
        key: KeyT,
        group: StringT,
        start: ValueT | None = None,
        end: ValueT | None = None,
        count: int | None = None,
        idle: int | None = None,
        consumer: StringT | None = None,
    ) -> CommandRequest[tuple[StreamPendingExt, ...] | StreamPending]:
        """
        Return information and entries from a stream consumer group pending
        entries list, that are messages fetched but never acknowledged.
        """
        command_arguments: CommandArgList = [key, group]

        if idle is not None:
            command_arguments.extend([PrefixToken.IDLE, idle])

        if count is not None and end is not None and start is not None:
            command_arguments.extend([start, end, count])

        if consumer is not None:
            command_arguments.append(consumer)

        return self.create_request(
            CommandName.XPENDING,
            *command_arguments,
            callback=PendingCallback(count=count),
        )

    @mutually_inclusive_parameters("trim_strategy", "threshold")
    @redis_command(
        CommandName.XTRIM,
        group=CommandGroup.STREAM,
        arguments={
            "limit": {"version_introduced": "6.2.0"},
            "condition": {"version_introduced": "8.2.0"},
        },
    )
    def xtrim(
        self,
        key: KeyT,
        trim_strategy: Literal[PureToken.MAXLEN, PureToken.MINID],
        threshold: int,
        trim_operator: Literal[PureToken.EQUAL, PureToken.APPROXIMATELY] | None = None,
        limit: int | None = None,
        condition: Literal[PureToken.KEEPREF, PureToken.DELREF, PureToken.ACKED] | None = None,
    ) -> CommandRequest[int]:
        """ """
        command_arguments: CommandArgList = [trim_strategy]

        if trim_operator:
            command_arguments.append(trim_operator)

        command_arguments.append(threshold)

        if limit is not None:
            command_arguments.extend(["LIMIT", limit])
        if condition is not None:
            command_arguments.append(condition)

        return self.create_request(
            CommandName.XTRIM, key, *command_arguments, callback=IntCallback()
        )

    @ensure_iterable_valid("identifiers")
    @redis_command(
        CommandName.XDEL,
        group=CommandGroup.STREAM,
        flags={CommandFlag.FAST},
    )
    def xdel(self, key: KeyT, identifiers: Parameters[ValueT]) -> CommandRequest[int]:
        """
        Removes the specified entries from a stream, and returns the number of entries deleted
        """

        return self.create_request(CommandName.XDEL, key, *identifiers, callback=IntCallback())

    @versionadded(version="5.2.0")
    @ensure_iterable_valid("identifiers")
    @redis_command(
        CommandName.XDELEX,
        version_introduced="8.2",
        group=CommandGroup.STREAM,
        flags={CommandFlag.FAST},
    )
    def xdelex(
        self,
        key: KeyT,
        identifiers: Parameters[ValueT],
        condition: Literal[PureToken.KEEPREF, PureToken.DELREF, PureToken.ACKED] | None = None,
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Deletes one or multiple entries from the stream at the specified key.
        """
        command_arguments: CommandArgList = [key]
        if condition is not None:
            command_arguments.append(condition)
        command_arguments.extend([PrefixToken.IDS, len(list(identifiers)), *identifiers])

        return self.create_request(
            CommandName.XDELEX, *command_arguments, callback=TupleCallback[int]()
        )

    @redis_command(
        CommandName.XINFO_CONSUMERS,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY},
    )
    def xinfo_consumers(
        self, key: KeyT, groupname: StringT
    ) -> CommandRequest[tuple[dict[AnyStr, AnyStr], ...]]:
        """
        Get list of all consumers that belong to :paramref:`groupname` of the
        stream stored at :paramref:`key`
        """

        return self.create_request(
            CommandName.XINFO_CONSUMERS,
            key,
            groupname,
            callback=XInfoCallback[AnyStr](),
        )

    @redis_command(
        CommandName.XINFO_GROUPS,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY},
    )
    def xinfo_groups(self, key: KeyT) -> CommandRequest[tuple[dict[AnyStr, AnyStr], ...]]:
        """
        Get list of all consumers groups of the stream stored at :paramref:`key`
        """

        return self.create_request(CommandName.XINFO_GROUPS, key, callback=XInfoCallback[AnyStr]())

    @mutually_inclusive_parameters("count", leaders=["full"])
    @redis_command(
        CommandName.XINFO_STREAM,
        group=CommandGroup.STREAM,
        flags={CommandFlag.READONLY},
    )
    def xinfo_stream(
        self, key: KeyT, full: bool | None = None, count: int | None = None
    ) -> CommandRequest[StreamInfo]:
        """
        Get information about the stream stored at :paramref:`key`

        :param full: If specified the return will contained extended information
         about the stream ( see: :class:`coredis.response.types.StreamInfo` ).
        :param count: restrict the number of `entries` returned when using :paramref:`full`
        """
        command_arguments: CommandArgList = []

        if full:
            command_arguments.append(PureToken.FULL)

            if count is not None:
                command_arguments.extend([PrefixToken.COUNT, count])

        return self.create_request(
            CommandName.XINFO_STREAM,
            key,
            *command_arguments,
            callback=StreamInfoCallback(full=full),
        )

    @ensure_iterable_valid("identifiers")
    @redis_command(
        CommandName.XCLAIM,
        group=CommandGroup.STREAM,
        flags={CommandFlag.FAST},
    )
    def xclaim(
        self,
        key: KeyT,
        group: StringT,
        consumer: StringT,
        min_idle_time: int | datetime.timedelta,
        identifiers: Parameters[ValueT],
        idle: int | datetime.timedelta | None = None,
        time: int | datetime.datetime | None = None,
        retrycount: int | None = None,
        force: bool | None = None,
        justid: bool | None = None,
        lastid: ValueT | None = None,
    ) -> CommandRequest[tuple[AnyStr, ...] | tuple[StreamEntry, ...]]:
        """
        Changes (or acquires) ownership of a message in a consumer group, as
        if the message was delivered to the specified consumer.
        """
        command_arguments: CommandArgList = [
            key,
            group,
            consumer,
            normalized_milliseconds(min_idle_time),
        ]
        command_arguments.extend(identifiers)

        if idle is not None:
            command_arguments.extend([PrefixToken.IDLE, normalized_milliseconds(idle)])

        if time is not None:
            command_arguments.extend([PrefixToken.TIME, normalized_time_milliseconds(time)])

        if retrycount is not None:
            command_arguments.extend([PrefixToken.RETRYCOUNT, retrycount])

        if force is not None:
            command_arguments.append(PureToken.FORCE)

        if justid is not None:
            command_arguments.append(PureToken.JUSTID)

        if lastid is not None:
            command_arguments.extend([PrefixToken.LASTID, lastid])
        return self.create_request(
            CommandName.XCLAIM,
            *command_arguments,
            callback=ClaimCallback[AnyStr](justid=justid),
        )

    @redis_command(
        CommandName.XGROUP_CREATE,
        arguments={"entriesread": {"version_introduced": "7.0.0"}},
        group=CommandGroup.STREAM,
    )
    def xgroup_create(
        self,
        key: KeyT,
        groupname: StringT,
        identifier: ValueT | None = None,
        mkstream: bool | None = None,
        entriesread: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Create a consumer group.
        """
        command_arguments: CommandArgList = [
            key,
            groupname,
            identifier or PureToken.NEW_ID,
        ]

        if mkstream is not None:
            command_arguments.append(PureToken.MKSTREAM)

        if entriesread is not None:
            command_arguments.extend([PrefixToken.ENTRIESREAD, entriesread])

        return self.create_request(
            CommandName.XGROUP_CREATE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.XGROUP_CREATECONSUMER,
        version_introduced="6.2.0",
        group=CommandGroup.STREAM,
    )
    def xgroup_createconsumer(
        self, key: KeyT, groupname: StringT, consumername: StringT
    ) -> CommandRequest[bool]:
        """
        Create a consumer in a consumer group.

        :return: whether the consumer was created
        """
        command_arguments: CommandArgList = [key, groupname, consumername]

        return self.create_request(
            CommandName.XGROUP_CREATECONSUMER,
            *command_arguments,
            callback=BoolCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.XGROUP_SETID,
        group=CommandGroup.STREAM,
        arguments={"entriesread": {"version_introduced": "7.0.0"}},
    )
    def xgroup_setid(
        self,
        key: KeyT,
        groupname: StringT,
        identifier: ValueT | None = None,
        entriesread: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Set a consumer group to an arbitrary last delivered ID value.
        """

        command_arguments: CommandArgList = [
            key,
            groupname,
            identifier or PureToken.NEW_ID,
        ]

        if entriesread is not None:
            command_arguments.extend([PrefixToken.ENTRIESREAD, entriesread])

        return self.create_request(
            CommandName.XGROUP_SETID,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.XGROUP_DESTROY, group=CommandGroup.STREAM)
    def xgroup_destroy(self, key: KeyT, groupname: StringT) -> CommandRequest[int]:
        """
        Destroy a consumer group.

        :return: The number of destroyed consumer groups
        """

        return self.create_request(
            CommandName.XGROUP_DESTROY, key, groupname, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.XGROUP_DELCONSUMER, group=CommandGroup.STREAM)
    def xgroup_delconsumer(
        self, key: KeyT, groupname: StringT, consumername: StringT
    ) -> CommandRequest[int]:
        """
        Delete a consumer from a consumer group.

        :return: The number of pending messages that the consumer had before it was deleted
        """

        return self.create_request(
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
        flags={CommandFlag.FAST},
    )
    def xautoclaim(
        self,
        key: KeyT,
        group: StringT,
        consumer: StringT,
        min_idle_time: int | datetime.timedelta,
        start: ValueT,
        count: int | None = None,
        justid: bool | None = None,
    ) -> CommandRequest[
        tuple[AnyStr, tuple[AnyStr, ...]]
        | tuple[AnyStr, tuple[StreamEntry, ...], tuple[AnyStr, ...]]
    ]:
        """
        Changes (or acquires) ownership of messages in a consumer group, as if the messages were
        delivered to the specified consumer.

        :return: A dictionary with keys as stream identifiers and values containing
         all the successfully claimed messages in the same format as :meth:`xrange`

        """
        command_arguments: CommandArgList = [
            key,
            group,
            consumer,
            normalized_milliseconds(min_idle_time),
            start,
        ]

        if count is not None:
            command_arguments.extend(["COUNT", count])

        if justid is not None:
            command_arguments.append(PureToken.JUSTID)

        return self.create_request(
            CommandName.XAUTOCLAIM,
            *command_arguments,
            callback=AutoClaimCallback[AnyStr](justid=justid),
        )

    @redis_command(
        CommandName.BITCOUNT,
        group=CommandGroup.BITMAP,
        arguments={"index_unit": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("start", "end")
    def bitcount(
        self,
        key: KeyT,
        start: int | None = None,
        end: int | None = None,
        index_unit: Literal[PureToken.BIT, PureToken.BYTE] | None = None,
    ) -> CommandRequest[int]:
        """
        Returns the count of set bits in the value of :paramref:`key`.  Optional
        :paramref:`start` and :paramref:`end` parameters indicate which bytes to consider

        """
        command_arguments: CommandArgList = [key]

        if start is not None and end is not None:
            command_arguments.append(start)
            command_arguments.append(end)

        if index_unit is not None:
            command_arguments.append(index_unit)

        return self.create_request(CommandName.BITCOUNT, *command_arguments, callback=IntCallback())

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

    @ensure_iterable_valid("keys")
    @redis_command(
        CommandName.BITOP,
        group=CommandGroup.BITMAP,
    )
    def bitop(
        self, keys: Parameters[KeyT], operation: StringT, destkey: KeyT
    ) -> CommandRequest[int]:
        """
        Perform a bitwise operation using :paramref:`operation` between
        :paramref:`keys` and store the result in :paramref:`destkey`.
        """

        return self.create_request(
            CommandName.BITOP, operation, destkey, *keys, callback=IntCallback()
        )

    @redis_command(
        CommandName.BITPOS,
        group=CommandGroup.BITMAP,
        arguments={"index_unit": {"version_introduced": "7.0.0"}},
        flags={CommandFlag.READONLY},
    )
    @mutually_inclusive_parameters("end", leaders=("start",))
    def bitpos(
        self,
        key: KeyT,
        bit: int,
        start: int | None = None,
        end: int | None = None,
        index_unit: Literal[PureToken.BIT, PureToken.BYTE] | None = None,
    ) -> CommandRequest[int]:
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
        command_arguments: CommandArgList = [key, bit]

        if start is not None:
            command_arguments.append(start)

        if start is not None and end is not None:
            command_arguments.append(end)

        if index_unit is not None:
            command_arguments.append(index_unit)

        return self.create_request(CommandName.BITPOS, *command_arguments, callback=IntCallback())

    @redis_command(
        CommandName.GETBIT,
        group=CommandGroup.BITMAP,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def getbit(self, key: KeyT, offset: int) -> CommandRequest[int]:
        """
        Returns the bit value at offset in the string value stored at key

        :return: the bit value stored at :paramref:`offset`.
        """

        return self.create_request(CommandName.GETBIT, key, offset, callback=IntCallback())

    @redis_command(CommandName.SETBIT, group=CommandGroup.BITMAP)
    def setbit(self, key: KeyT, offset: int, value: int) -> CommandRequest[int]:
        """
        Flag the :paramref:`offset` in :paramref:`key` as :paramref:`value`.
        """
        value = value and 1 or 0

        return self.create_request(CommandName.SETBIT, key, offset, value, callback=IntCallback())

    @redis_command(
        CommandName.PUBLISH,
        group=CommandGroup.PUBSUB,
    )
    def publish(self, channel: StringT, message: ValueT) -> CommandRequest[int]:
        """
        Publish :paramref:`message` on :paramref:`channel`.

        :return: the number of subscribers the message was delivered to.
        """

        return self.create_request(CommandName.PUBLISH, channel, message, callback=IntCallback())

    @versionadded(version="3.6.0")
    @redis_command(CommandName.SPUBLISH, group=CommandGroup.PUBSUB, version_introduced="7.0.0")
    def spublish(self, channel: StringT, message: ValueT) -> CommandRequest[int]:
        """
        Publish :paramref:`message` on shard :paramref:`channel`.

        :return: the number of shard subscribers the message was delivered to.

        .. note:: The number only represents subscribers listening to the exact
           node the message was published to, which means that if a subscriber
           is listening on a replica node, it will not be included in the count.
        """

        return self.create_request(CommandName.SPUBLISH, channel, message, callback=IntCallback())

    @redis_command(
        CommandName.PUBSUB_CHANNELS,
        group=CommandGroup.PUBSUB,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeSets(),
        ),
    )
    def pubsub_channels(self, pattern: StringT | None = None) -> CommandRequest[_Set[AnyStr]]:
        """
        Return channels that have at least one subscriber
        """

        return self.create_request(
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
    def pubsub_shardchannels(self, pattern: StringT | None = None) -> CommandRequest[_Set[AnyStr]]:
        """
        Return shard channels that have at least one subscriber
        """

        return self.create_request(
            CommandName.PUBSUB_SHARDCHANNELS,
            pattern or b"*",
            callback=SetCallback[AnyStr](),
        )

    @redis_command(
        CommandName.PUBSUB_NUMPAT,
        group=CommandGroup.PUBSUB,
    )
    def pubsub_numpat(self) -> CommandRequest[int]:
        """
        Get the count of unique patterns pattern subscriptions

        :return: the number of patterns all the clients are subscribed to.
        """

        return self.create_request(CommandName.PUBSUB_NUMPAT, callback=IntCallback())

    @redis_command(
        CommandName.PUBSUB_NUMSUB,
        group=CommandGroup.PUBSUB,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterMergeMapping[AnyStr, int](value_combine=sum),
        ),
    )
    def pubsub_numsub(self, *channels: StringT) -> CommandRequest[dict[AnyStr, int]]:
        """
        Get the count of subscribers for channels

        :return: Mapping of channels to number of subscribers per channel
        """
        command_arguments: CommandArgList = []

        if channels:
            command_arguments.extend(channels)

        return self.create_request(
            CommandName.PUBSUB_NUMSUB,
            *command_arguments,
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
    def pubsub_shardnumsub(self, *channels: StringT) -> CommandRequest[dict[AnyStr, int]]:
        """
        Get the count of subscribers for shard channels

        :return: Ordered mapping of shard channels to number of subscribers per channel
        """
        command_arguments: CommandArgList = []

        if channels:
            command_arguments.extend(channels)

        return self.create_request(
            CommandName.PUBSUB_SHARDNUMSUB,
            *command_arguments,
            callback=DictCallback[AnyStr, int](),
        )

    def _eval(
        self,
        command: Literal[CommandName.EVAL, CommandName.EVAL_RO],
        script: ValueT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        _keys: list[KeyT] = list(keys) if keys else []
        command_arguments: CommandArgList = [script, len(_keys), *_keys]

        if args:
            command_arguments.extend(args)

        return self.create_request(
            command, *command_arguments, callback=NoopCallback[ResponseType]()
        )

    @redis_command(
        CommandName.EVAL,
        group=CommandGroup.SCRIPTING,
    )
    def eval(
        self,
        script: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Execute the Lua :paramref:`script` with the key names and argument values
        in :paramref:`keys` and :paramref:`args`.

        :return: The result of the script as redis returns it
        """

        return self._eval(CommandName.EVAL, script, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EVAL_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        flags={CommandFlag.READONLY},
    )
    def eval_ro(
        self,
        script: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Read-only variant of :meth:`~Redis.eval` that cannot execute commands
        that modify data.

        :return: The result of the script as redis returns it
        """

        return self._eval(CommandName.EVAL_RO, script, keys, args)

    def _evalsha(
        self,
        command: Literal[CommandName.EVALSHA, CommandName.EVALSHA_RO],
        sha1: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        _keys: list[KeyT] = list(keys) if keys else []
        command_arguments: CommandArgList = [sha1, len(_keys), *_keys]

        if args:
            command_arguments.extend(args)

        return self.create_request(command, *command_arguments, callback=NoopCallback())

    @redis_command(CommandName.EVALSHA, group=CommandGroup.SCRIPTING)
    def evalsha(
        self,
        sha1: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Execute the Lua script cached by it's :paramref:`sha` ref with the
        key names and argument values in :paramref:`keys` and :paramref:`args`.
        Evaluate a script from the server's cache by its :paramref:`sha1` digest.

        :return: The result of the script as redis returns it
        """

        return self._evalsha(CommandName.EVALSHA, sha1, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.EVALSHA_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        flags={CommandFlag.READONLY},
    )
    def evalsha_ro(
        self,
        sha1: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Read-only variant of :meth:`~Redis.evalsha` that cannot execute commands
        that modify data.

        :return: The result of the script as redis returns it
        """

        return self._evalsha(CommandName.EVALSHA_RO, sha1, keys, args)

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.SCRIPT_DEBUG,
        group=CommandGroup.SCRIPTING,
    )
    def script_debug(
        self,
        mode: Literal[PureToken.NO, PureToken.SYNC, PureToken.YES],
    ) -> CommandRequest[bool]:
        """
        Set the debug mode for executed scripts

        :raises: :exc:`NotImplementedError`
        """

        raise NotImplementedError()

    @ensure_iterable_valid("sha1s")
    @redis_command(
        CommandName.SCRIPT_EXISTS,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterAlignedBoolsCombine(),
        ),
    )
    def script_exists(self, sha1s: Parameters[StringT]) -> CommandRequest[tuple[bool, ...]]:
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as :paramref:`sha1s`.

        :return: tuple of boolean values indicating if each script
         exists in the cache.
        """

        return self.create_request(CommandName.SCRIPT_EXISTS, *sha1s, callback=BoolsCallback())

    @redis_command(
        CommandName.SCRIPT_FLUSH,
        group=CommandGroup.SCRIPTING,
        arguments={"sync_type": {"version_introduced": "6.2.0"}},
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    def script_flush(
        self,
        sync_type: Literal[PureToken.ASYNC, PureToken.SYNC] | None = None,
    ) -> CommandRequest[bool]:
        """
        Flushes all scripts from the script cache
        """
        command_arguments: CommandArgList = []

        if sync_type:
            command_arguments = [sync_type]

        return self.create_request(
            CommandName.SCRIPT_FLUSH, *command_arguments, callback=BoolCallback()
        )

    @redis_command(
        CommandName.SCRIPT_KILL,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterFirstNonException[bool](),
        ),
    )
    def script_kill(self) -> CommandRequest[bool]:
        """
        Kills the currently executing Lua script
        """

        return self.create_request(CommandName.SCRIPT_KILL, callback=SimpleStringCallback())

    @redis_command(
        CommandName.SCRIPT_LOAD,
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterEnsureConsistent(),
        ),
    )
    def script_load(self, script: StringT) -> CommandRequest[AnyStr]:
        """
        Loads a Lua :paramref:`script` into the script cache.

        :return: The SHA1 digest of the script added into the script cache
        """
        return self.create_request(
            CommandName.SCRIPT_LOAD, script, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FCALL,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
    )
    def fcall(
        self,
        function: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Invoke a function
        """
        _keys: list[KeyT] = list(keys or [])
        command_arguments: CommandArgList = [
            function,
            len(_keys),
            *_keys,
            *(args or []),
        ]

        return self.create_request(CommandName.FCALL, *command_arguments, callback=NoopCallback())

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FCALL_RO,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        flags={CommandFlag.READONLY},
    )
    def fcall_ro(
        self,
        function: StringT,
        keys: Parameters[KeyT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[ResponseType]:
        """
        Read-only variant of :meth:`~coredis.Redis.fcall`
        """
        _keys: list[KeyT] = list(keys or [])
        command_arguments: CommandArgList = [
            function,
            len(_keys),
            *_keys,
            *(args or []),
        ]

        return self.create_request(
            CommandName.FCALL_RO, *command_arguments, callback=NoopCallback()
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
    def function_delete(self, library_name: StringT) -> CommandRequest[bool]:
        """
        Delete a library and all its functions.
        """

        return self.create_request(
            CommandName.FUNCTION_DELETE, library_name, callback=SimpleStringCallback()
        )

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_DUMP,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def function_dump(self) -> CommandRequest[bytes]:
        """
        Dump all functions into a serialized binary payload
        """

        return self.create_request(
            CommandName.FUNCTION_DUMP,
            execution_parameters={"decode": False},
            callback=NoopCallback[bytes](),
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
    def function_flush(
        self, async_: Literal[PureToken.ASYNC, PureToken.SYNC] | None = None
    ) -> CommandRequest[bool]:
        """
        Delete all functions
        """
        command_arguments: CommandArgList = []

        if async_ is not None:
            command_arguments.append(async_)

        return self.create_request(
            CommandName.FUNCTION_FLUSH,
            *command_arguments,
            callback=SimpleStringCallback(),
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
    def function_kill(self) -> CommandRequest[bool]:
        """
        Kill the function currently in execution.
        """

        return self.create_request(CommandName.FUNCTION_KILL, callback=SimpleStringCallback())

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.FUNCTION_LIST,
        version_introduced="7.0.0",
        group=CommandGroup.SCRIPTING,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def function_list(
        self, libraryname: StringT | None = None, withcode: bool | None = None
    ) -> CommandRequest[Mapping[AnyStr, LibraryDefinition]]:
        """
        List information about the functions registered under
        :paramref:`libraryname`
        """
        command_arguments: CommandArgList = []

        if libraryname is not None:
            command_arguments.extend(["LIBRARYNAME", libraryname])

        if withcode:
            command_arguments.append(PureToken.WITHCODE)

        return self.create_request(
            CommandName.FUNCTION_LIST,
            *command_arguments,
            callback=FunctionListCallback[AnyStr](),
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
    def function_load(
        self,
        function_code: StringT,
        replace: bool | None = None,
    ) -> CommandRequest[AnyStr]:
        """
        Load a library of functions.
        """
        command_arguments: CommandArgList = []

        if replace:
            command_arguments.append(PureToken.REPLACE)

        command_arguments.append(function_code)

        return self.create_request(
            CommandName.FUNCTION_LOAD,
            *command_arguments,
            callback=AnyStrCallback[AnyStr](),
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
    def function_restore(
        self,
        serialized_value: bytes,
        policy: Literal[PureToken.FLUSH, PureToken.APPEND, PureToken.REPLACE] | None = None,
    ) -> CommandRequest[bool]:
        """
        Restore all the functions on the given payload
        """
        command_arguments: CommandArgList = [serialized_value]

        if policy is not None:
            command_arguments.append(policy)

        return self.create_request(
            CommandName.FUNCTION_RESTORE,
            *command_arguments,
            callback=SimpleStringCallback(),
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
    def function_stats(
        self,
    ) -> CommandRequest[
        dict[AnyStr, AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None]
    ]:
        """
        Return information about the function currently running
        """

        return self.create_request(
            CommandName.FUNCTION_STATS, callback=FunctionStatsCallback[AnyStr]()
        )

    @redis_command(
        CommandName.BGREWRITEAOF,
        group=CommandGroup.CONNECTION,
    )
    def bgrewriteaof(self) -> CommandRequest[bool]:
        """Tell the Redis server to rewrite the AOF file from data in memory"""

        return self.create_request(CommandName.BGREWRITEAOF, callback=SimpleStringCallback())

    @redis_command(
        CommandName.BGSAVE,
        group=CommandGroup.CONNECTION,
    )
    def bgsave(self, schedule: bool | None = None) -> CommandRequest[bool]:
        """
        Tells the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        command_arguments: CommandArgList = []

        if schedule:
            command_arguments.append(PureToken.SCHEDULE)

        return self.create_request(
            CommandName.BGSAVE,
            *command_arguments,
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
    def client_caching(self, mode: Literal[PureToken.NO, PureToken.YES]) -> CommandRequest[bool]:
        """
        Instruct the server about tracking or not keys in the next request
        """
        command_arguments: CommandArgList = [mode]

        return self.create_request(
            CommandName.CLIENT_CACHING,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.CLIENT_KILL,
        group=CommandGroup.CONNECTION,
        arguments={
            "laddr": {"version_introduced": "6.2.0"},
            "maxage": {"version_introduced": "7.4.0"},
        },
    )
    def client_kill(
        self,
        ip_port: StringT | None = None,
        identifier: int | None = None,
        type_: None
        | (
            Literal[
                PureToken.NORMAL,
                PureToken.MASTER,
                PureToken.SLAVE,
                PureToken.REPLICA,
                PureToken.PUBSUB,
            ]
        ) = None,
        user: StringT | None = None,
        addr: StringT | None = None,
        laddr: StringT | None = None,
        skipme: bool | None = None,
        maxage: int | None = None,
    ) -> CommandRequest[int | bool]:
        """
        Disconnects the client at :paramref:`ip_port`

        :return: ``True`` if the connection exists and has been closed
         or the number of clients killed.
        """

        command_arguments: CommandArgList = []

        if ip_port:
            command_arguments.append(ip_port)

        if identifier:
            command_arguments.extend([PrefixToken.IDENTIFIER, identifier])

        if type_:
            command_arguments.extend([PrefixToken.TYPE, type_])

        if user:
            command_arguments.extend([PrefixToken.USER, user])

        if addr:
            command_arguments.extend([PrefixToken.ADDR, addr])

        if laddr:
            command_arguments.extend([PrefixToken.LADDR, laddr])

        if maxage:
            command_arguments.extend([PrefixToken.MAXAGE, maxage])
        if skipme is not None:
            command_arguments.extend([PrefixToken.SKIPME, skipme and "yes" or "no"])

        return self.create_request(
            CommandName.CLIENT_KILL,
            *command_arguments,
            callback=SimpleStringOrIntCallback(),
        )

    @redis_command(
        CommandName.CLIENT_LIST,
        group=CommandGroup.CONNECTION,
        arguments={
            "identifiers": {"version_introduced": "6.2.0"},
        },
    )
    def client_list(
        self,
        type_: None
        | (Literal[PureToken.MASTER, PureToken.NORMAL, PureToken.PUBSUB, PureToken.REPLICA]) = None,
        identifiers: Parameters[int] | None = None,
    ) -> CommandRequest[tuple[ClientInfo, ...]]:
        """
        Get client connections

        :return: a tuple of dictionaries containing client fields
        """

        command_arguments: CommandArgList = []

        if type_:
            command_arguments.extend([PrefixToken.TYPE, type_])

        if identifiers is not None:
            command_arguments.append(PrefixToken.IDENTIFIER)
            command_arguments.extend(identifiers)

        return self.create_request(
            CommandName.CLIENT_LIST, *command_arguments, callback=ClientListCallback()
        )

    @redis_command(
        CommandName.CLIENT_GETNAME,
        group=CommandGroup.CONNECTION,
    )
    def client_getname(self) -> CommandRequest[AnyStr | None]:
        """
        Returns the current connection name

        :return: The connection name, or ``None`` if no name is set.
        """

        return self.create_request(
            CommandName.CLIENT_GETNAME, callback=OptionalAnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.CLIENT_SETNAME,
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.client_name` argument when initializing the client to ensure the client name is consistent across all connections originating from the client"
            ),
            True,
        ),
    )
    def client_setname(self, connection_name: StringT) -> CommandRequest[bool]:
        """
        Set the current connection name
        :return: If the connection name was successfully set.
        """

        return self.create_request(
            CommandName.CLIENT_SETNAME, connection_name, callback=SimpleStringCallback()
        )

    @mutually_exclusive_parameters("lib_name", "lib_ver")
    @versionadded(version="4.12.0")
    @redis_command(
        CommandName.CLIENT_SETINFO,
        version_introduced="7.1.240",
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Coredis sets the library name and version by default during the handshake phase.Explicitly calling this command will only apply to the connection from the pool that was used to send it and not for subsequent commands"
            ),
            True,
        ),
    )
    def client_setinfo(
        self,
        lib_name: StringT | None = None,
        lib_ver: StringT | None = None,
    ) -> CommandRequest[bool]:
        """
        Set client or connection specific info

        :param lib_name: name of the library
        :param lib_ver: version of the library
        """
        command_arguments: CommandArgList = []
        if lib_name:
            command_arguments.extend([PrefixToken.LIB_NAME, lib_name])
        if lib_ver:
            command_arguments.extend([PrefixToken.LIB_VER, lib_ver])

        return self.create_request(
            CommandName.CLIENT_SETINFO, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.CLIENT_PAUSE,
        group=CommandGroup.CONNECTION,
        arguments={"mode": {"version_introduced": "6.2.0"}},
    )
    def client_pause(
        self,
        timeout: int,
        mode: Literal[PureToken.WRITE, PureToken.ALL] | None = None,
    ) -> CommandRequest[bool]:
        """
        Stop processing commands from clients for some time

        :return: The command returns ``True`` or raises an error if the timeout is invalid.
        """

        command_arguments: CommandArgList = [timeout]

        if mode is not None:
            command_arguments.append(mode)

        return self.create_request(
            CommandName.CLIENT_PAUSE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_UNPAUSE,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    def client_unpause(self) -> CommandRequest[bool]:
        """
        Resume processing of clients that were paused

        :return: The command returns ```True```
        """

        return self.create_request(CommandName.CLIENT_UNPAUSE, callback=SimpleStringCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLIENT_UNBLOCK, group=CommandGroup.CONNECTION)
    def client_unblock(
        self,
        client_id: int,
        timeout_error: Literal[PureToken.TIMEOUT, PureToken.ERROR] | None = None,
    ) -> CommandRequest[bool]:
        """
        Unblock a client blocked in a blocking command from a different connection

        :return: Whether the client was unblocked
        """
        command_arguments: CommandArgList = [client_id]

        if timeout_error is not None:
            command_arguments.append(timeout_error)

        return self.create_request(
            CommandName.CLIENT_UNBLOCK, *command_arguments, callback=BoolCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_GETREDIR,
        version_introduced="6.0.0",
        group=CommandGroup.CONNECTION,
    )
    def client_getredir(self) -> CommandRequest[int]:
        """
        Get tracking notifications redirection client ID if any

        :return: the ID of the client we are redirecting the notifications to.
         The command returns ``-1`` if client tracking is not enabled,
         or ``0`` if client tracking is enabled but we are not redirecting the
         notifications to any client.
        """

        return self.create_request(CommandName.CLIENT_GETREDIR, callback=IntCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.CLIENT_ID, group=CommandGroup.CONNECTION)
    def client_id(self) -> CommandRequest[int]:
        """
        Returns the client ID for the current connection

        :return: The id of the client.
        """

        return self.create_request(CommandName.CLIENT_ID, callback=IntCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_INFO,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    def client_info(self) -> CommandRequest[ClientInfo]:
        """
        Returns information about the current client connection.
        """
        return self.create_request(CommandName.CLIENT_INFO, callback=ClientInfoCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_REPLY,
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use the :paramref:`Redis.noreply` argument when initializing the client to ensure that all connections originating from this client disable or enable replies. You can also use the :meth:`Redis.ignore_replies` context manager to selectively execute certain commands without waiting for a reply"
            ),
            False,
        ),
    )
    def client_reply(
        self, mode: Literal[PureToken.OFF, PureToken.ON, PureToken.SKIP]
    ) -> CommandRequest[bool]:
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
    def client_tracking(
        self,
        status: Literal[PureToken.OFF, PureToken.ON],
        *prefixes: StringT,
        redirect: int | None = None,
        bcast: bool | None = None,
        optin: bool | None = None,
        optout: bool | None = None,
        noloop: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Enable or disable server assisted client side caching support

        :return: If the connection was successfully put in tracking mode or if the
         tracking mode was successfully disabled.
        """

        command_arguments: CommandArgList = [status]

        if prefixes:
            command_arguments.extend(
                itertools.chain(*zip([PrefixToken.PREFIX] * len(prefixes), prefixes))
            )
        if redirect is not None:
            command_arguments.extend([PrefixToken.REDIRECT, redirect])

        if bcast is not None:
            command_arguments.append(PureToken.BCAST)

        if optin is not None:
            command_arguments.append(PureToken.OPTIN)

        if optout is not None:
            command_arguments.append(PureToken.OPTOUT)

        if noloop is not None:
            command_arguments.append(PureToken.NOLOOP)

        return self.create_request(
            CommandName.CLIENT_TRACKING,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.CLIENT_TRACKINGINFO,
        version_introduced="6.2.0",
        group=CommandGroup.CONNECTION,
    )
    def client_trackinginfo(
        self,
    ) -> CommandRequest[dict[AnyStr, AnyStr | _Set[AnyStr] | list[AnyStr]]]:
        """
        Return information about server assisted client side caching for the current connection

        :return: a mapping of tracking information sections and their respective values
        """

        return self.create_request(
            CommandName.CLIENT_TRACKINGINFO,
            callback=ClientTrackingInfoCallback[AnyStr](),
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.CLIENT_NO_EVICT,
        version_introduced="7.0.0",
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use :paramref:`Redis.noevict` argument when initializing the client to ensure that all connections originating from this client use the desired mode"
            ),
            True,
        ),
    )
    def client_no_evict(
        self, enabled: Literal[PureToken.ON, PureToken.OFF]
    ) -> CommandRequest[bool]:
        """
        Set client eviction mode for the current connection
        """
        return self.create_request(
            CommandName.CLIENT_NO_EVICT, enabled, callback=SimpleStringCallback()
        )

    @versionadded(version="4.12.0")
    @redis_command(
        CommandName.CLIENT_NO_TOUCH,
        version_introduced="7.1.240",
        group=CommandGroup.CONNECTION,
        redirect_usage=RedirectUsage(
            (
                "Use :paramref:`Redis.notouch` argument when initializing the client to ensure that all connections originating from this client use the desired mode"
            ),
            True,
        ),
    )
    def client_no_touch(
        self, enabled: Literal[PureToken.OFF, PureToken.ON]
    ) -> CommandRequest[bool]:
        """
        Controls whether commands sent by the client will alter the LRU/LFU of the keys they access.
        """
        return self.create_request(
            CommandName.CLIENT_NO_TOUCH, enabled, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.DBSIZE,
        group=CommandGroup.SERVER,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def dbsize(self) -> CommandRequest[int]:
        """Returns the number of keys in the current database"""

        return self.create_request(CommandName.DBSIZE, callback=IntCallback())

    @redis_command(
        CommandName.DEBUG_OBJECT,
        group=CommandGroup.SERVER,
    )
    def debug_object(self, key: KeyT) -> CommandRequest[dict[str, str | int]]:
        """Returns version specific meta information about a given key"""

        return self.create_request(CommandName.DEBUG_OBJECT, key, callback=DebugCallback())

    @mutually_inclusive_parameters("host", "port")
    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.FAILOVER,
        version_introduced="6.2.0",
        group=CommandGroup.SERVER,
    )
    def failover(
        self,
        host: StringT | None = None,
        port: int | None = None,
        force: bool | None = None,
        abort: bool | None = None,
        timeout: int | datetime.timedelta | None = None,
    ) -> CommandRequest[bool]:
        """
        Start a coordinated failover between this server and one of its replicas.

        :return: `True` if the command was accepted and a coordinated failover
         is in progress.
        """
        command_arguments: CommandArgList = []

        if host and port:
            command_arguments.extend([PrefixToken.TO, host, port])

            if force is not None:
                command_arguments.append(PureToken.FORCE)

        if abort:
            command_arguments.append(PureToken.ABORT)

        if timeout is not None:
            command_arguments.append(PrefixToken.TIMEOUT)
            command_arguments.append(normalized_milliseconds(timeout))

        return self.create_request(
            CommandName.FAILOVER, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.FLUSHALL,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    def flushall(
        self, async_: Literal[PureToken.ASYNC, PureToken.SYNC] | None = None
    ) -> CommandRequest[bool]:
        """Deletes all keys in all databases on the current host"""
        command_arguments: CommandArgList = []

        if async_:
            command_arguments.append(async_)

        return self.create_request(
            CommandName.FLUSHALL, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.FLUSHDB,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterEnsureConsistent(),
        ),
    )
    def flushdb(
        self, async_: Literal[PureToken.ASYNC, PureToken.SYNC] | None = None
    ) -> CommandRequest[bool]:
        """Deletes all keys in the current database"""
        command_arguments: CommandArgList = []

        if async_:
            command_arguments.append(async_)

        return self.create_request(
            CommandName.FLUSHDB, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.INFO,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def info(
        self,
        *sections: StringT,
    ) -> CommandRequest[dict[str, ResponseType]]:
        """
        Get information and statistics about the server

        The :paramref:`sections` option can be used to select a specific section(s)
        of information.

        """

        return self.create_request(
            CommandName.INFO,
            *sections,
            callback=InfoCallback(),
        )

    @redis_command(CommandName.LASTSAVE, group=CommandGroup.SERVER, flags={CommandFlag.FAST})
    def lastsave(self) -> CommandRequest[datetime.datetime]:
        """
        Get the time of the last successful save to disk
        """

        return self.create_request(CommandName.LASTSAVE, callback=DateTimeCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_DOCTOR, group=CommandGroup.SERVER)
    def latency_doctor(self) -> CommandRequest[AnyStr]:
        """
        Return a human readable latency analysis report.
        """

        return self.create_request(CommandName.LATENCY_DOCTOR, callback=AnyStrCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_GRAPH, group=CommandGroup.SERVER)
    def latency_graph(self, event: StringT) -> CommandRequest[AnyStr]:
        """
        Return a latency graph for the event.
        """

        return self.create_request(
            CommandName.LATENCY_GRAPH, event, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.LATENCY_HISTOGRAM,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
    )
    def latency_histogram(
        self, *commands: StringT
    ) -> CommandRequest[dict[AnyStr, dict[AnyStr, RedisValueT]]]:
        """
        Return the cumulative distribution of latencies of a subset of commands or all.
        """

        return self.create_request(
            CommandName.LATENCY_HISTOGRAM,
            *commands,
            callback=LatencyHistogramCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LATENCY_HISTORY,
        group=CommandGroup.SERVER,
    )
    def latency_history(self, event: StringT) -> CommandRequest[tuple[list[int], ...]]:
        """
        Return timestamp-latency samples for the event.

        :return: The command returns a tuple where each element is a tuple
         representing the timestamp and the latency of the event.

        """
        command_arguments: CommandArgList = [event]

        return self.create_request(
            CommandName.LATENCY_HISTORY,
            *command_arguments,
            callback=TupleCallback[list[int]](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.LATENCY_LATEST,
        group=CommandGroup.SERVER,
    )
    def latency_latest(self) -> CommandRequest[dict[AnyStr, tuple[int, int, int]]]:
        """
        Return the latest latency samples for all events.

        :return: Mapping of event name to (timestamp, latest, all-time) triplet
        """

        return self.create_request(
            CommandName.LATENCY_LATEST,
            callback=LatencyCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.LATENCY_RESET, group=CommandGroup.SERVER)
    def latency_reset(self, *events: StringT) -> CommandRequest[int]:
        """
        Reset latency data for one or more events.

        :return: the number of event time series that were reset.
        """
        command_arguments: CommandArgList = list(events) if events else []

        return self.create_request(
            CommandName.LATENCY_RESET, *command_arguments, callback=IntCallback()
        )

    @versionadded(version="3.0.0")
    @redis_command(CommandName.MEMORY_DOCTOR, group=CommandGroup.SERVER)
    def memory_doctor(self) -> CommandRequest[AnyStr]:
        """
        Outputs memory problems report
        """

        return self.create_request(CommandName.MEMORY_DOCTOR, callback=AnyStrCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.MEMORY_MALLOC_STATS, group=CommandGroup.SERVER)
    def memory_malloc_stats(self) -> CommandRequest[AnyStr]:
        """
        Show allocator internal stats
        :return: the memory allocator's internal statistics report
        """

        return self.create_request(
            CommandName.MEMORY_MALLOC_STATS, callback=AnyStrCallback[AnyStr]()
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.MEMORY_PURGE,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.ALL, combine=ClusterBoolCombine()),
    )
    def memory_purge(self) -> CommandRequest[bool]:
        """
        Ask the allocator to release memory
        """

        return self.create_request(CommandName.MEMORY_PURGE, callback=SimpleStringCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.MEMORY_STATS,
        group=CommandGroup.SERVER,
    )
    def memory_stats(self) -> CommandRequest[dict[AnyStr, ValueT]]:
        """
        Show memory usage details
        :return: mapping of memory usage metrics and their values

        """

        return self.create_request(
            CommandName.MEMORY_STATS,
            callback=DictCallback[AnyStr, ValueT](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.MEMORY_USAGE,
        group=CommandGroup.SERVER,
        flags={CommandFlag.READONLY},
    )
    def memory_usage(self, key: KeyT, *, samples: int | None = None) -> CommandRequest[int | None]:
        """
        Estimate the memory usage of a key

        :return: the memory usage in bytes, or ``None`` when the key does not exist.

        """
        command_arguments: CommandArgList = []
        command_arguments.append(key)

        if samples is not None:
            command_arguments.extend([PrefixToken.SAMPLES, samples])

        return self.create_request(
            CommandName.MEMORY_USAGE, *command_arguments, callback=OptionalIntCallback()
        )

    @redis_command(
        CommandName.SAVE,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.ALL, combine=ClusterEnsureConsistent()),
    )
    def save(self) -> CommandRequest[bool]:
        """
        Tells the Redis server to save its data to disk,
        blocking until the save is complete
        """

        return self.create_request(CommandName.SAVE, callback=SimpleStringCallback())

    @redis_command(
        CommandName.SHUTDOWN,
        group=CommandGroup.SERVER,
        arguments={
            "now": {"version_introduced": "7.0.0"},
            "force": {"version_introduced": "7.0.0"},
            "abort": {"version_introduced": "7.0.0"},
        },
    )
    def shutdown(
        self,
        nosave_save: Literal[PureToken.NOSAVE, PureToken.SAVE] | None = None,
        *,
        now: bool | None = None,
        force: bool | None = None,
        abort: bool | None = None,
    ) -> CommandRequest[bool]:
        """Stops Redis server"""
        command_arguments: CommandArgList = []

        if nosave_save:
            command_arguments.append(nosave_save)

        if now is not None:
            command_arguments.append(PureToken.NOW)
        if force is not None:
            command_arguments.append(PureToken.FORCE)
        if abort is not None:
            command_arguments.append(PureToken.ABORT)

        return self.create_request(
            CommandName.SHUTDOWN,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.SLAVEOF,
        version_deprecated="5.0.0",
        deprecation_reason="Use :meth:`replicaof`",
        group=CommandGroup.SERVER,
    )
    def slaveof(self, host: StringT | None = None, port: int | None = None) -> CommandRequest[bool]:
        """
        Sets the server to be a replicated slave of the instance identified
        by the :paramref:`host` and :paramref:`port`.
        If called without arguments, the instance is promoted to a master instead.
        """

        if host is None and port is None:
            return self.create_request(
                CommandName.SLAVEOF, b"NO", b"ONE", callback=SimpleStringCallback()
            )
        assert host and port
        return self.create_request(CommandName.SLAVEOF, host, port, callback=SimpleStringCallback())

    @redis_command(
        CommandName.SLOWLOG_GET,
        group=CommandGroup.SERVER,
    )
    def slowlog_get(self, count: int | None = None) -> CommandRequest[tuple[SlowLogInfo, ...]]:
        """
        Gets the entries from the slowlog. If :paramref:`count` is specified, get the
        most recent :paramref:`count` items.
        """
        command_arguments: CommandArgList = []

        if count is not None:
            command_arguments.append(count)

        return self.create_request(
            CommandName.SLOWLOG_GET, *command_arguments, callback=SlowlogCallback()
        )

    @redis_command(CommandName.SLOWLOG_LEN, group=CommandGroup.SERVER)
    def slowlog_len(self) -> CommandRequest[int]:
        """Gets the number of items in the slowlog"""

        return self.create_request(CommandName.SLOWLOG_LEN, callback=IntCallback())

    @redis_command(
        CommandName.SLOWLOG_RESET,
        group=CommandGroup.SERVER,
    )
    def slowlog_reset(self) -> CommandRequest[bool]:
        """Removes all items in the slowlog"""

        return self.create_request(CommandName.SLOWLOG_RESET, callback=SimpleStringCallback())

    @redis_command(CommandName.TIME, group=CommandGroup.SERVER, flags={CommandFlag.FAST})
    def time(self) -> CommandRequest[datetime.datetime]:
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """

        return self.create_request(CommandName.TIME, callback=TimeCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.REPLICAOF,
        group=CommandGroup.SERVER,
    )
    def replicaof(
        self, host: StringT | None = None, port: int | None = None
    ) -> CommandRequest[bool]:
        """
        Make the server a replica of another instance, or promote it as master.
        """

        if host is None and port is None:
            return self.create_request(
                CommandName.REPLICAOF, b"NO", b"ONE", callback=SimpleStringCallback()
            )
        assert host and port
        return self.create_request(
            CommandName.REPLICAOF, host, port, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.ROLE, group=CommandGroup.SERVER, flags={CommandFlag.FAST})
    def role(self) -> CommandRequest[RoleInfo]:
        """
        Provides information on the role of a Redis instance in the context of replication,
        by returning if the instance is currently a master, slave, or sentinel.
        The command also returns additional information about the state of the replication
        (if the role is master or slave)
        or the list of monitored master names (if the role is sentinel).
        """

        return self.create_request(CommandName.ROLE, callback=RoleCallback())

    @versionadded(version="3.0.0")
    @redis_command(CommandName.SWAPDB, group=CommandGroup.SERVER, flags={CommandFlag.FAST})
    def swapdb(self, index1: int, index2: int) -> CommandRequest[bool]:
        """
        Swaps two Redis databases
        """
        command_arguments: CommandArgList = [index1, index2]

        return self.create_request(
            CommandName.SWAPDB, *command_arguments, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.LOLWUT,
        group=CommandGroup.SERVER,
        flags={CommandFlag.READONLY, CommandFlag.FAST},
    )
    def lolwut(self, version: int | None = None) -> CommandRequest[str]:
        """
        Get the Redis version and a piece of generative computer art
        """
        command_arguments: CommandArgList = []

        if version is not None:
            command_arguments.extend([PrefixToken.VERSION, version])

        return self.create_request(
            CommandName.LOLWUT,
            *command_arguments,
            callback=NoopCallback[str](),
            execution_parameters={"decode": True},
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_CAT,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def acl_cat(self, categoryname: StringT | None = None) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        List the ACL categories or the commands inside a category


        :return: a list of ACL categories or a list of commands inside a given category.
         The command may return an error if an invalid category name is given as argument.

        """

        command_arguments: CommandArgList = []

        if categoryname:
            command_arguments.append(categoryname)

        return self.create_request(
            CommandName.ACL_CAT, *command_arguments, callback=TupleCallback[AnyStr]()
        )

    @ensure_iterable_valid("usernames")
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
    def acl_deluser(self, usernames: Parameters[StringT]) -> CommandRequest[int]:
        """
        Remove the specified ACL users and the associated rules


        :return: The number of users that were deleted.
         This number will not always match the number of arguments since
         certain users may not exist.
        """

        return self.create_request(CommandName.ACL_DELUSER, *usernames, callback=IntCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_DRYRUN,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.RANDOM,
        ),
    )
    def acl_dryrun(
        self, username: StringT, command: StringT, *args: ValueT
    ) -> CommandRequest[bool]:
        """
        Returns whether the user can execute the given command without executing the command.
        """
        command_arguments: CommandArgList = [username, command]

        if args:
            command_arguments.extend(args)

        return self.create_request(
            CommandName.ACL_DRYRUN,
            *command_arguments,
            callback=SimpleStringCallback(AuthorizationError),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_GENPASS,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def acl_genpass(self, bits: int | None = None) -> CommandRequest[AnyStr]:
        """
        Generate a pseudorandom secure password to use for ACL users


        :return: by default 64 bytes string representing 256 bits of pseudorandom data.
         Otherwise if an argument if needed, the output string length is the number of
         specified bits (rounded to the next multiple of 4) divided by 4.
        """
        command_arguments: CommandArgList = []

        if bits is not None:
            command_arguments.append(bits)

        return self.create_request(
            CommandName.ACL_GENPASS,
            *command_arguments,
            callback=AnyStrCallback[AnyStr](),
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
    def acl_getuser(
        self, username: StringT
    ) -> CommandRequest[dict[AnyStr, list[AnyStr] | _Set[AnyStr]]]:
        """
        Get the rules for a specific ACL user
        """

        return self.create_request(
            CommandName.ACL_GETUSER,
            username,
            callback=DictCallback[AnyStr, list[AnyStr] | _Set[AnyStr]](),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_LIST,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def acl_list(self) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        List the current ACL rules in ACL config file format
        """

        return self.create_request(CommandName.ACL_LIST, callback=TupleCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_LOAD,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.ALL, combine=ClusterEnsureConsistent()),
    )
    def acl_load(self) -> CommandRequest[bool]:
        """
        Reload the ACLs from the configured ACL file

        :return: True if successful. The command may fail with an error for several reasons:

         - if the file is not readable
         - if there is an error inside the file, and in such case the error will be reported to
           the user in the error.
         - Finally the command will fail if the server is not configured to use an external
           ACL file.

        """

        return self.create_request(CommandName.ACL_LOAD, callback=SimpleStringCallback())

    @versionadded(version="3.0.0")
    @mutually_exclusive_parameters("count", "reset")
    @redis_command(
        CommandName.ACL_LOG,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
    )
    def acl_log(
        self, count: int | None = None, reset: bool | None = None
    ) -> CommandRequest[bool] | CommandRequest[tuple[dict[AnyStr, ResponsePrimitive] | None, ...]]:
        """
        List latest events denied because of ACLs in place

        :return: When called to show security events a list of ACL security events.
         When called with ``RESET`` True if the security log was cleared.

        """

        command_arguments: CommandArgList = []

        if count is not None:
            command_arguments.append(count)

        if reset is not None:
            command_arguments.append(PureToken.RESET)

        if reset:
            return self.create_request(
                CommandName.ACL_LOG, *command_arguments, callback=SimpleStringCallback()
            )
        else:
            return self.create_request(
                CommandName.ACL_LOG,
                *command_arguments,
                callback=ACLLogCallback[AnyStr](),
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
    def acl_save(self) -> CommandRequest[bool]:
        """
        Save the current ACL rules in the configured ACL file

        :return: True if successful. The command may fail with an error for several reasons:
         - if the file cannot be written, or
         - if the server is not configured to use an external ACL file.

        """

        return self.create_request(CommandName.ACL_SAVE, callback=SimpleStringCallback())

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
    def acl_setuser(
        self,
        username: StringT,
        *rules: StringT,
    ) -> CommandRequest[bool]:
        """
        Modify or create the rules for a specific ACL user


        :return: True if successful. If the rules contain errors, the error is returned.
        """
        command_arguments: CommandArgList = [username]

        if rules:
            command_arguments.extend(rules)

        return self.create_request(
            CommandName.ACL_SETUSER,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_USERS,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def acl_users(self) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        List the username of all the configured ACL rules
        """

        return self.create_request(CommandName.ACL_USERS, callback=TupleCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.ACL_WHOAMI,
        version_introduced="6.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def acl_whoami(self) -> CommandRequest[AnyStr]:
        """
        Return the name of the user associated to the current connection


        :return: the username of the current connection.
        """

        return self.create_request(CommandName.ACL_WHOAMI, callback=AnyStrCallback[AnyStr]())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def command(self) -> CommandRequest[dict[str, Command]]:
        """
        Get Redis command details

        :return: Mapping of command details.  Commands are returned
         in random order.
        """

        return self.create_request(CommandName.COMMAND, callback=CommandCallback())

    @versionadded(version="3.0.0")
    @redis_command(
        CommandName.COMMAND_COUNT,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def command_count(self) -> CommandRequest[int]:
        """
        Get total number of Redis commands

        :return: number of commands returned by ``COMMAND``
        """

        return self.create_request(CommandName.COMMAND_COUNT, callback=IntCallback())

    @versionadded(version="3.1.0")
    @redis_command(
        CommandName.COMMAND_DOCS,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def command_docs(
        self, *command_names: StringT
    ) -> CommandRequest[dict[AnyStr, dict[AnyStr, ResponseType]]]:
        """
        Mapping of commands to a dictionary containing it's documentation
        """

        return self.create_request(
            CommandName.COMMAND_DOCS,
            *command_names,
            callback=CommandDocCallback[AnyStr](),
        )

    @versionadded(version="3.0.0")
    @ensure_iterable_valid("arguments")
    @redis_command(
        CommandName.COMMAND_GETKEYS,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def command_getkeys(
        self, command: StringT, arguments: Parameters[ValueT]
    ) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        Extract keys given a full Redis command

        :return: Keys from your command.
        """

        return self.create_request(
            CommandName.COMMAND_GETKEYS,
            command,
            *arguments,
            callback=TupleCallback[AnyStr](),
        )

    @versionadded(version="3.1.0")
    @ensure_iterable_valid("arguments")
    @redis_command(
        CommandName.COMMAND_GETKEYSANDFLAGS,
        version_introduced="7.0.0",
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def command_getkeysandflags(
        self, command: StringT, arguments: Parameters[ValueT]
    ) -> CommandRequest[dict[AnyStr, _Set[AnyStr]]]:
        """
        Extract keys from a full Redis command and their usage flags.

        :return: Mapping of keys from your command to flags
        """

        return self.create_request(
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
    def command_info(self, *command_names: StringT) -> CommandRequest[dict[str, Command]]:
        """
        Get specific Redis command details, or all when no argument is given.

        :return: mapping of command details.

        """
        return self.create_request(
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
    def command_list(
        self,
        module: StringT | None = None,
        aclcat: StringT | None = None,
        pattern: StringT | None = None,
    ) -> CommandRequest[_Set[AnyStr]]:
        """
        Get an array of Redis command names
        """
        command_arguments: CommandArgList = []

        if any([module, aclcat, pattern]):
            command_arguments.append(PrefixToken.FILTERBY)

        if module is not None:
            command_arguments.extend([PrefixToken.MODULE, module])

        if aclcat is not None:
            command_arguments.extend([PrefixToken.ACLCAT, aclcat])

        if pattern is not None:
            command_arguments.extend([PrefixToken.PATTERN, pattern])

        return self.create_request(
            CommandName.COMMAND_LIST, *command_arguments, callback=SetCallback[AnyStr]()
        )

    @ensure_iterable_valid("parameters")
    @redis_command(
        CommandName.CONFIG_GET,
        group=CommandGroup.SERVER,
    )
    def config_get(self, parameters: Parameters[StringT]) -> CommandRequest[dict[AnyStr, AnyStr]]:
        """
        Get the values of configuration parameters
        """

        return self.create_request(
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
    def config_set(self, parameter_values: Mapping[StringT, ValueT]) -> CommandRequest[bool]:
        """Sets configuration parameters to the given values"""

        return self.create_request(
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
    def config_resetstat(self) -> CommandRequest[bool]:
        """Resets runtime statistics"""

        return self.create_request(CommandName.CONFIG_RESETSTAT, callback=SimpleStringCallback())

    @redis_command(CommandName.CONFIG_REWRITE, group=CommandGroup.SERVER)
    def config_rewrite(self) -> CommandRequest[bool]:
        """
        Rewrites config file with the minimal change to reflect running config
        """

        return self.create_request(CommandName.CONFIG_REWRITE, callback=SimpleStringCallback())

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.MODULE_LIST,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(route=NodeFlag.RANDOM),
    )
    def module_list(self) -> CommandRequest[tuple[dict[AnyStr, ResponsePrimitive], ...]]:
        """
        List all modules loaded by the server

        :return: The loaded modules with each element represents a module
         containing a mapping with ``name`` and ``ver``
        """

        return self.create_request(CommandName.MODULE_LIST, callback=ModuleInfoCallback[AnyStr]())

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.MODULE_LOAD,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    def module_load(self, path: StringT, *args: str | bytes | int | float) -> CommandRequest[bool]:
        """
        Load a module
        """
        command_arguments: CommandArgList = [path]

        if args:
            command_arguments.extend(args)

        return self.create_request(
            CommandName.MODULE_LOAD, *command_arguments, callback=SimpleStringCallback()
        )

    @versionadded(version="3.4.0")
    @redis_command(
        CommandName.MODULE_LOADEX,
        group=CommandGroup.SERVER,
        version_introduced="7.0.0",
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    def module_loadex(
        self,
        path: StringT,
        configs: dict[StringT, ValueT] | None = None,
        args: Parameters[ValueT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Loads a module from a dynamic library at runtime with configuration directives.
        """
        command_arguments: CommandArgList = [path]

        if configs:
            for pair in configs.items():
                command_arguments.append(PrefixToken.CONFIG)
                command_arguments.extend(pair)
        if args:
            command_arguments.append(PrefixToken.ARGS)
            command_arguments.extend(args)
        return self.create_request(
            CommandName.MODULE_LOADEX,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @versionadded(version="3.2.0")
    @redis_command(
        CommandName.MODULE_UNLOAD,
        group=CommandGroup.SERVER,
        cluster=ClusterCommandConfig(
            route=NodeFlag.ALL,
            combine=ClusterBoolCombine(),
        ),
    )
    def module_unload(self, name: StringT) -> CommandRequest[bool]:
        """
        Unload a module
        """

        return self.create_request(CommandName.MODULE_UNLOAD, name, callback=SimpleStringCallback())

    @redis_command(
        CommandName.TYPE,
        group=CommandGroup.GENERIC,
        cacheable=True,
        flags={CommandFlag.FAST, CommandFlag.READONLY},
    )
    def type(self, key: KeyT) -> CommandRequest[AnyStr | None]:
        """
        Determine the type stored at key

        :return: type of :paramref:`key`, or ``None`` when :paramref:`key` does not exist.
        """

        return self.create_request(CommandName.TYPE, key, callback=OptionalAnyStrCallback[AnyStr]())

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VADD, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vadd(
        self,
        key: KeyT,
        element: ValueT,
        values: Parameters[float | int] | bytes,
        reduce: int | None = None,
        cas: bool | None = None,
        quantization: Literal[PureToken.BIN, PureToken.NOQUANT, PureToken.Q8] | None = None,
        ef: int | None = None,
        attributes: JsonType | None = None,
        numlinks: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Add a new element into the vector set specified by key

        :param key: The key containing the vector set
        :param element: The name of the element being added to the vector set
        :param values: either a byte representation of a 32-bit floating point (FP32) blob of values
         or a sequence of doubles representing the vector.
        :param reduce: dimensions to reduce the vector values to
        :param cas: whether to add using check-and-set
        :param quantization: The quantization type to use
        :param ef: exploration factor to use when connecting the element to the existing graph
        :param attributes: json attributes to associate with the element
        :param numlinks: maximum number of connections each node in the graph will have with other
         nodes.

        :return: ``True`` if the element was successfully added to the vector set
        """
        command_arguments: CommandArgList = [key]
        if reduce is not None:
            command_arguments.extend([PureToken.REDUCE, reduce])

        if isinstance(values, bytes):
            command_arguments.extend([PureToken.FP32, values])
        else:
            command_arguments.extend([PureToken.VALUES, len(list(values)), *values])

        command_arguments.append(element)
        if cas is not None:
            command_arguments.append(PureToken.CAS)
        if quantization is not None:
            command_arguments.append(quantization)
        if ef is not None:
            command_arguments.extend([PrefixToken.EF, ef])
        if attributes:
            command_arguments.extend([PrefixToken.SETATTR, json.dumps(attributes)])
        if numlinks:
            command_arguments.extend([PrefixToken.M, numlinks])

        return self.create_request(CommandName.VADD, *command_arguments, callback=BoolCallback())

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VREM, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vrem(self, key: KeyT, element: ValueT) -> CommandRequest[bool]:
        """
        Remove an element from a vector set.

        :param key: The key containing the vector set
        :param element: The element to remove

        :return: ``True`` if the element was successfully deleted from the set
        """

        return self.create_request(CommandName.VREM, key, element, callback=BoolCallback())

    @overload
    def vsim(
        self,
        key: KeyT,
        *,
        element: StringT | None = ...,
        values: Parameters[float] | bytes | None = ...,
        count: int | None = ...,
        epsilon: float | None = ...,
        ef: int | None = ...,
        filter: StringT | None = ...,
        filter_ef: int | None = ...,
        truth: bool | None = ...,
    ) -> CommandRequest[tuple[AnyStr, ...]]: ...
    @overload
    def vsim(
        self,
        key: KeyT,
        *,
        element: StringT | None = ...,
        values: Parameters[float] | bytes | None = ...,
        withscores: Literal[True],
        count: int | None = ...,
        epsilon: float | None = ...,
        ef: int | None = ...,
        filter: StringT | None = ...,
        filter_ef: int | None = ...,
        truth: bool | None = ...,
    ) -> CommandRequest[dict[AnyStr, float]]: ...
    @overload
    def vsim(
        self,
        key: KeyT,
        *,
        element: StringT | None = ...,
        values: Parameters[float] | bytes | None = ...,
        withattribs: Literal[True],
        count: int | None = ...,
        epsilon: float | None = ...,
        ef: int | None = ...,
        filter: StringT | None = ...,
        filter_ef: int | None = ...,
        truth: bool | None = ...,
    ) -> CommandRequest[dict[AnyStr, JsonType]]: ...
    @overload
    def vsim(
        self,
        key: KeyT,
        *,
        element: StringT | None = ...,
        values: Parameters[float] | bytes | None = ...,
        withscores: Literal[True],
        withattribs: Literal[True],
        count: int | None = ...,
        epsilon: float | None = ...,
        ef: int | None = ...,
        filter: StringT | None = ...,
        filter_ef: int | None = ...,
        truth: bool | None = ...,
    ) -> CommandRequest[dict[AnyStr, tuple[float, JsonType]]]: ...

    @versionadded(version="5.0.0")
    @mutually_exclusive_parameters("values", "element", required=True)
    @redis_command(
        CommandName.VSIM,
        version_introduced="8.0.0",
        group=CommandGroup.VECTOR_SET,
        arguments={
            "withattribs": {"version_introduced": "8.2"},
            "epsilon": {"version_introduced": "8.2"},
        },
    )
    def vsim(
        self,
        key: KeyT,
        *,
        element: StringT | None = None,
        values: Parameters[float] | bytes | None = None,
        withscores: bool | None = None,
        withattribs: bool | None = None,
        count: int | None = None,
        epsilon: float | None = None,
        ef: int | None = None,
        filter: StringT | None = None,
        filter_ef: int | None = None,
        truth: bool | None = None,
    ) -> CommandRequest[
        tuple[AnyStr, ...]
        | dict[AnyStr, float]
        | dict[AnyStr, JsonType]
        | dict[AnyStr, tuple[float, JsonType]]
    ]:
        """
        Return elements similar to a given vector or element

        :param key: The key containing the vector set
        :param element: An existing element to find similar elements for
        :param values: either a byte representation of a 32-bit floating point (FP32) blob of values
         or a sequence of doubles representing the vector to use as the similarity reference.
        :param withscores: whether to return similarity scores for each result
        :param withattribs: whether to include attributes for for each result
        :param count: number of results to limit to
        :param epsilon: distance threshold; results with distance greater than this are
         excluded.
        :param ef: Search exploration factor
        :param filter: Expression to restrict matching elements
        :param filter_ef: limits the number of filtering attempts
        :param truth: forces an exact linear scan of all elements bypassing the HSNW graph
        :return: the matching elements or a mapping of the matching elements to their scores
         if :paramref:`withscores` is ``True`` and/or their attributes if :paramref:`withattribs`
         is ``True``
        """
        command_arguments: CommandArgList = [key]
        if values is not None:
            if isinstance(values, bytes):
                command_arguments.extend([PureToken.FP32, values])
            else:
                _values: list[float] = list(values)
                command_arguments.extend([PureToken.VALUES, len(_values), *_values])
        if element:
            command_arguments.extend([PureToken.ELE, element])

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
        if withattribs:
            command_arguments.append(PureToken.WITHATTRIBS)
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if ef is not None:
            command_arguments.extend([PrefixToken.EF, ef])
        if epsilon is not None:
            command_arguments.extend([PrefixToken.EPSILON, epsilon])
        if filter is not None:
            command_arguments.extend([PrefixToken.FILTER, filter])
        if filter_ef is not None:
            command_arguments.extend([PrefixToken.FILTER_EF, filter_ef])
        if truth:
            command_arguments.append(PureToken.TRUTH)
        return self.create_request(
            CommandName.VSIM,
            *command_arguments,
            callback=VSimCallback[AnyStr](withscores=withscores, withattribs=withattribs),
        )

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VDIM, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vdim(self, key: KeyT) -> CommandRequest[int]:
        """
        Return the dimension of vectors in the vector set

        :param key: The key containing the vector set
        :return: The dimensions of the vectors in the vector set
        """
        return self.create_request(CommandName.VDIM, key, callback=IntCallback())

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VCARD, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vcard(self, key: KeyT) -> CommandRequest[int]:
        """
        Return the number of elements in a vector set

        :param key: The key containing the vector set
        :return: The number of elements in the vector set

        """
        return self.create_request(CommandName.VCARD, key, callback=IntCallback())

    @overload
    def vemb(self, key: KeyT, element: StringT) -> CommandRequest[tuple[float, ...] | None]: ...
    @overload
    def vemb(
        self, key: KeyT, element: StringT, raw: Literal[True]
    ) -> CommandRequest[VectorData | None]: ...

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VEMB, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vemb(
        self, key: KeyT, element: StringT, raw: bool | None = None
    ) -> CommandRequest[tuple[float, ...] | VectorData | None]:
        """
        Return the vector associated with an element

        :param key: The key containing the vector set
        :param element: The name of the vector to retrieve
        :param raw: Whether to return the raw result

        :return: A tuple of floating point values representing the vector.
         if :paramref:`raw` is ``True`` the raw vector along with its metadata
         will be returned as a :class:`~coredis.response.types.VectorData` mapping.
        """
        command_arguments: CommandArgList = [key, element]
        if raw:
            command_arguments.append(PureToken.RAW)
        return self.create_request(
            CommandName.VEMB,
            *command_arguments,
            execution_parameters={"decode": not raw},
            callback=VEmbCallback(raw=raw),
        )

    @overload
    def vlinks(
        self, key: KeyT, element: StringT
    ) -> CommandRequest[tuple[tuple[AnyStr, ...], ...] | None]: ...
    @overload
    def vlinks(
        self, key: KeyT, element: StringT, withscores: Literal[True]
    ) -> CommandRequest[tuple[dict[AnyStr, float], ...] | None]: ...

    @versionadded(version="5.0.0")
    @redis_command(
        CommandName.VLINKS,
        version_introduced="8.0.0",
        group=CommandGroup.VECTOR_SET,
    )
    def vlinks(
        self, key: KeyT, element: StringT, withscores: bool | None = None
    ) -> CommandRequest[tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None]:
        """
        Return the neighbors of an element at each layer in the HNSW graph

        :param key: The key containing the vector set
        :param element: The element to find neighbours for
        :param withscores: Whether to return scores with neighbours

        :return: A tuple containing tuples of connected neighbours for each layer of the HSNW graph.
         If :paramref:`withscores` is ``True`` each layer will instead be a mapping
         of neighbours to scores. The last entry of the top level tuple is the lowest layer of
         the HSNW graph.
        """
        command_arguments: CommandArgList = [key, element]

        if withscores:
            command_arguments.append(PureToken.WITHSCORES)
        return self.create_request(
            CommandName.VLINKS,
            *command_arguments,
            callback=VLinksCallback[AnyStr](withscores=withscores),
        )

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VINFO, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vinfo(self, key: KeyT) -> CommandRequest[dict[AnyStr, AnyStr | int] | None]:
        """
        Return information about a vector set

        :param key: The key containing the vector set
        :return: mapping of attributes and values describing the vector set

        """
        return self.create_request(CommandName.VINFO, key, callback=VInfoCallback[AnyStr]())

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VSETATTR, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vsetattr(self, key: KeyT, element: StringT, attributes: JsonType) -> CommandRequest[bool]:
        """
        Associate or remove the JSON attributes of elements

        :param key: The key containing the vector set
        :param element: The element to set the attributes for
        :param attributes: JSON serializable values to set as attributes


        :return: ``True`` if the attributes were successfully set
        """
        command_arguments: CommandArgList = [key, element]
        command_arguments.append(json.dumps(attributes))

        return self.create_request(
            CommandName.VSETATTR, *command_arguments, callback=BoolCallback()
        )

    @versionadded(version="5.0.0")
    @redis_command(CommandName.VGETATTR, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET)
    def vgetattr(self, key: KeyT, element: StringT) -> CommandRequest[JsonType]:
        """
        Retrieve the JSON attributes of elements

        :param key: The key containing the vector set
        :param element: The element to get the attributes for

        :return: the attributes of the element or None if they don't
         exist.
        """
        command_arguments: CommandArgList = [key, element]
        return self.create_request(
            CommandName.VGETATTR, *command_arguments, callback=JsonCallback()
        )

    @overload
    def vrandmember(self, key: KeyT) -> CommandRequest[AnyStr | None]: ...
    @overload
    def vrandmember(self, key: KeyT, count: int) -> CommandRequest[tuple[AnyStr | None, ...]]: ...
    @versionadded(version="5.0.0")
    @redis_command(
        CommandName.VRANDMEMBER, version_introduced="8.0.0", group=CommandGroup.VECTOR_SET
    )
    def vrandmember(
        self, key: KeyT, count: int | None = None
    ) -> CommandRequest[tuple[AnyStr | None, ...] | AnyStr | None]:
        """
        Return one or multiple random members from a vector set

        :param key: The key containing the vector set
        :param count: The number of random elements to return


        :return: A random element from the set or a list of
         random elements if :paramref:`count` is specificied.
         If :paramref:`count` is negative, duplicates may occur.
        """
        command_arguments: CommandArgList = [key]
        if count is not None:
            command_arguments.append(count)

        return self.create_request(
            CommandName.VRANDMEMBER,
            *command_arguments,
            callback=ItemOrTupleCallback[AnyStr | None](),
        )

    @versionadded(version="6.0.0")
    @redis_command(CommandName.VRANGE, version_introduced="8.4.0", group=CommandGroup.VECTOR_SET)
    def vrange(
        self, key: KeyT, start: StringT, end: StringT, count: int | None = None
    ) -> CommandRequest[tuple[AnyStr, ...]]:
        """
        :param key: The key containing the vector set
        :param start: The starting point of the lexicographical range
        :param end: The ending point of the lexicographical range.
        :param count: Maximum number of elements to return. If negative,
         all elements in the range will be returned.

        :return: The elements in lexicographical order within the range
        """
        command_arguments: CommandArgList = [key, start, end]
        if count is not None:
            command_arguments.append(count)

        return self.create_request(
            CommandName.VRANGE,
            *command_arguments,
            callback=TupleCallback[AnyStr](),
        )

    @versionadded(version="5.2.0")
    @redis_command(CommandName.VISMEMBER, version_introduced="8.2.0", group=CommandGroup.VECTOR_SET)
    def vismember(self, key: KeyT, element: StringT) -> CommandRequest[bool]:
        """
        Check if an element exists in a vector set

        :param key: The key containing the vector set
        :param element: The element to check for membership


        """
        return self.create_request(
            CommandName.VISMEMBER,
            key,
            element,
            callback=BoolCallback(),
        )
