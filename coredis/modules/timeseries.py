from __future__ import annotations

import itertools
from datetime import datetime
from typing import List

from coredis.typing import (
    AnyStr,
    CommandArgList,
    Dict,
    KeyT,
    Literal,
    Optional,
    Parameters,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Union,
    ValueT,
)

from .._utils import dict_to_flat_list
from ..commands._utils import normalized_time_milliseconds
from ..commands._validators import (
    mutually_exclusive_parameters,
    mutually_inclusive_parameters,
)
from ..commands._wrappers import ClusterCommandConfig
from ..commands.constants import CommandGroup, CommandName, NodeFlag
from ..response._callbacks import (
    ClusterMergeSets,
    IntCallback,
    SetCallback,
    SimpleStringCallback,
    TupleCallback,
)
from ..tokens import PrefixToken, PureToken
from .base import ModuleGroup, module_command
from .response._callbacks.timeseries import (
    ClusterMergeTimeSeries,
    SampleCallback,
    SamplesCallback,
    TimeSeriesCallback,
    TimeSeriesInfoCallback,
    TimeSeriesMultiCallback,
)


def normalized_timestamp(ts: Union[int, datetime, StringT]) -> Union[StringT, int]:
    if isinstance(ts, (bytes, str)):
        return ts
    return normalized_time_milliseconds(ts)


class TimeSeries(ModuleGroup[AnyStr]):
    MODULE = "timeseries"

    @module_command(
        CommandName.TS_CREATE, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def create(
        self,
        key: KeyT,
        retention: Optional[int] = None,
        encoding: Optional[
            Literal[PureToken.COMPRESSED, PureToken.UNCOMPRESSED]
        ] = None,
        chunk_size: Optional[int] = None,
        duplicate_policy: Optional[
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ] = None,
        labels: Optional[Dict[StringT, ValueT]] = None,
    ) -> bool:
        """
        Create a new time series
        """
        pieces: CommandArgList = [key]
        if retention is not None:
            pieces.extend([PrefixToken.RETENTION, retention])
        if encoding:
            pieces.extend([PrefixToken.ENCODING, encoding])
        if chunk_size is not None:
            pieces.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy is not None:
            pieces.extend([PrefixToken.DUPLICATE_POLICY, duplicate_policy])
        if labels:
            pieces.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        return await self.execute_module_command(
            CommandName.TS_CREATE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TS_DEL, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def delete(
        self,
        key: KeyT,
        fromtimestamp: Union[int, datetime, StringT],
        totimestamp: Union[int, datetime, StringT],
    ) -> int:
        """
        Delete all samples between two timestamps for a given time series
        """
        return await self.execute_module_command(
            CommandName.TS_DEL,
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
            callback=IntCallback(),
        )

    @module_command(
        CommandName.TS_ALTER, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def alter(
        self,
        key: KeyT,
        labels: Optional[Dict[StringT, StringT]] = None,
        retention: Optional[int] = None,
        chunk_size: Optional[int] = None,
        duplicate_policy: Optional[
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ] = None,
    ) -> bool:
        """
        Update the retention, chunk size, duplicate policy, and labels of an existing time series
        """
        pieces: CommandArgList = [key]
        if labels:
            pieces.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        if retention is not None:
            pieces.extend([PrefixToken.RETENTION, retention])
        if chunk_size is not None:
            pieces.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy:
            pieces.extend([PrefixToken.DUPLICATE_POLICY, duplicate_policy])
        return await self.execute_module_command(
            CommandName.TS_ALTER, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TS_ADD, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def add(
        self,
        key: KeyT,
        timestamp: Union[int, datetime, StringT],
        value: Union[int, float],
        retention: Optional[int] = None,
        encoding: Optional[
            Literal[PureToken.COMPRESSED, PureToken.UNCOMPRESSED]
        ] = None,
        chunk_size: Optional[int] = None,
        duplicate_policy: Optional[
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ] = None,
        labels: Optional[Dict[StringT, ValueT]] = None,
    ) -> int:
        """
        Append a sample to a time series
        """
        pieces: CommandArgList = [
            key,
            normalized_timestamp(timestamp),
            value,
        ]
        if retention is not None:
            pieces.extend([PrefixToken.RETENTION, retention])
        if encoding:
            pieces.extend([PrefixToken.ENCODING, encoding])
        if chunk_size is not None:
            pieces.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy:
            pieces.extend([PrefixToken.ON_DUPLICATE, duplicate_policy])
        if labels:
            pieces.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        return await self.execute_module_command(
            CommandName.TS_ADD, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_MADD,
        group=CommandGroup.TIMESERIES,
        module="timeseries",
    )
    async def madd(self, ktvs: Parameters[Tuple[AnyStr, int, int]]) -> Tuple[int, ...]:
        """
        Append new samples to one or more time series
        """
        pieces: CommandArgList = list(itertools.chain(*ktvs))

        return await self.execute_module_command(
            CommandName.TS_MADD, *pieces, callback=TupleCallback[int]()
        )

    @module_command(
        CommandName.TS_INCRBY, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def incrby(
        self,
        key: KeyT,
        value: Union[int, float],
        labels: Optional[Dict[StringT, ValueT]] = None,
        timestamp: Optional[Union[datetime, int, StringT]] = None,
        retention: Optional[int] = None,
        uncompressed: Optional[bool] = None,
        chunk_size: Optional[int] = None,
    ) -> int:
        """
        Increase the value of the sample with the maximal existing timestamp,
        or create a new sample with a value equal to the value of the sample with
        the maximal existing timestamp with a given increment
        """
        pieces: CommandArgList = [key, value]
        if timestamp:
            pieces.extend([PrefixToken.TIMESTAMP, normalized_timestamp(timestamp)])
        if retention:
            pieces.extend([PrefixToken.RETENTION, retention])
        if uncompressed:
            pieces.append(PureToken.UNCOMPRESSED)
        if chunk_size:
            pieces.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if labels:
            pieces.extend(
                [PrefixToken.LABELS, *dict_to_flat_list(labels)]  # type: ignore
            )

        return await self.execute_module_command(
            CommandName.TS_INCRBY, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_DECRBY, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def decrby(
        self,
        key: KeyT,
        value: Union[int, float],
        labels: Optional[Dict[StringT, ValueT]] = None,
        timestamp: Optional[Union[datetime, int, StringT]] = None,
        retention: Optional[int] = None,
        uncompressed: Optional[bool] = None,
        chunk_size: Optional[int] = None,
    ) -> int:
        """
        Decrease the value of the sample with the maximal existing timestamp,
        or create a new sample with a value equal to the value of the sample with
        the maximal existing timestamp with a given decrement
        """
        pieces: CommandArgList = [key, value]

        if timestamp:
            pieces.extend([PrefixToken.TIMESTAMP, normalized_timestamp(timestamp)])
        if retention:
            pieces.extend([PrefixToken.RETENTION, retention])
        if uncompressed:
            pieces.append(PureToken.UNCOMPRESSED)
        if chunk_size:
            pieces.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if labels:
            pieces.extend(
                [PrefixToken.LABELS, *dict_to_flat_list(labels)]  # type: ignore
            )
        return await self.execute_module_command(
            CommandName.TS_DECRBY, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_CREATERULE, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def createrule(
        self,
        source: KeyT,
        destination: KeyT,
        aggregation: Literal[
            PureToken.AVG,
            PureToken.COUNT,
            PureToken.FIRST,
            PureToken.LAST,
            PureToken.MAX,
            PureToken.MIN,
            PureToken.RANGE,
            PureToken.STD_P,
            PureToken.STD_S,
            PureToken.SUM,
            PureToken.TWA,
            PureToken.VAR_P,
            PureToken.VAR_S,
        ],
        bucketduration: int,
        aligntimestamp: Optional[int] = None,
    ) -> bool:
        """
        Create a compaction rule
        """
        pieces: CommandArgList = [source, destination]
        pieces.extend([PrefixToken.AGGREGATION, aggregation, bucketduration])
        if aligntimestamp is not None:
            pieces.append(aligntimestamp)
        return await self.execute_module_command(
            CommandName.TS_CREATERULE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TS_DELETERULE, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def deleterule(self, source: KeyT, destination: KeyT) -> bool:
        """
        Delete a compaction rule

        """
        pieces: CommandArgList = [source, destination]

        return await self.execute_module_command(
            CommandName.TS_DELETERULE, *pieces, callback=SimpleStringCallback()
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @module_command(
        CommandName.TS_RANGE, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def range(
        self,
        key: KeyT,
        fromtimestamp: Union[datetime, int, StringT],
        totimestamp: Union[datetime, int, StringT],
        filter_by_ts: Optional[Parameters[int]] = None,
        latest: Optional[StringT] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        count: Optional[int] = None,
        aggregator: Optional[
            Literal[
                PureToken.AVG,
                PureToken.COUNT,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.RANGE,
                PureToken.STD_P,
                PureToken.STD_S,
                PureToken.SUM,
                PureToken.TWA,
                PureToken.VAR_P,
                PureToken.VAR_S,
            ]
        ] = None,
        bucketduration: Optional[int] = None,
        align: Optional[Union[int, StringT]] = None,
        buckettimestamp: Optional[StringT] = None,
        empty: Optional[bool] = None,
    ) -> Union[Tuple[Tuple[int, float], ...], Tuple[()]]:
        """
        Query a range in forward direction
        """
        pieces: CommandArgList = [
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            pieces.append(b"LATEST")
        if filter_by_ts:
            _ts: List[int] = list(filter_by_ts)
            pieces.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            pieces.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])
        if aggregator and bucketduration is not None:
            if align is not None:
                pieces.extend([PrefixToken.ALIGN, align])
            pieces.extend([PrefixToken.AGGREGATION, aggregator, bucketduration])
            if buckettimestamp is not None:
                pieces.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
                if empty is not None:
                    pieces.append(PureToken.EMPTY)

        return await self.execute_module_command(
            CommandName.TS_RANGE, *pieces, callback=SamplesCallback()
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @module_command(
        CommandName.TS_REVRANGE, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def revrange(
        self,
        key: KeyT,
        fromtimestamp: Union[int, datetime, StringT],
        totimestamp: Union[int, datetime, StringT],
        filter_by_ts: Optional[Parameters[int]] = None,
        latest: Optional[StringT] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        count: Optional[int] = None,
        aggregator: Optional[
            Literal[
                PureToken.AVG,
                PureToken.COUNT,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.RANGE,
                PureToken.STD_P,
                PureToken.STD_S,
                PureToken.SUM,
                PureToken.TWA,
                PureToken.VAR_P,
                PureToken.VAR_S,
            ]
        ] = None,
        bucketduration: Optional[int] = None,
        align: Optional[Union[int, StringT]] = None,
        buckettimestamp: Optional[StringT] = None,
        empty: Optional[bool] = None,
    ) -> Union[Tuple[Tuple[int, float], ...], Tuple[()]]:
        """
        Query a range in reverse direction
        """
        pieces: CommandArgList = [
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            pieces.append(b"LATEST")
        if filter_by_ts:
            _ts: List[int] = list(filter_by_ts)
            pieces.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            pieces.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])
        if aggregator and bucketduration is not None:
            if align is not None:
                pieces.extend([PrefixToken.ALIGN, align])
            pieces.extend([PrefixToken.AGGREGATION, aggregator, bucketduration])
            if buckettimestamp is not None:
                pieces.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
                if empty is not None:
                    pieces.append(PureToken.EMPTY)

        return await self.execute_module_command(
            CommandName.TS_REVRANGE, *pieces, callback=SamplesCallback()
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @mutually_inclusive_parameters("groupby", "reducer")
    @module_command(
        CommandName.TS_MRANGE,
        group=CommandGroup.TIMESERIES,
        module="timeseries",
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeTimeSeries(),
        ),
    )
    async def mrange(
        self,
        fromtimestamp: Union[int, datetime, StringT],
        totimestamp: Union[int, datetime, StringT],
        *,
        latest: Optional[StringT] = None,
        filters: Optional[Parameters[StringT]] = None,
        filter_by_ts: Optional[Parameters[int]] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        withlabels: Optional[bool] = None,
        selected_labels: Optional[Parameters[StringT]] = None,
        count: Optional[int] = None,
        align: Optional[Union[int, StringT]] = None,
        aggregator: Optional[
            Literal[
                PureToken.AVG,
                PureToken.COUNT,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.RANGE,
                PureToken.STD_P,
                PureToken.STD_S,
                PureToken.SUM,
                PureToken.TWA,
                PureToken.VAR_P,
                PureToken.VAR_S,
            ]
        ] = None,
        bucketduration: Optional[int] = None,
        buckettimestamp: Optional[StringT] = None,
        groupby: Optional[StringT] = None,
        reducer: Optional[
            Literal[
                PureToken.AVG,
                PureToken.COUNT,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.RANGE,
                PureToken.STD_P,
                PureToken.STD_S,
                PureToken.SUM,
                PureToken.VAR_P,
                PureToken.VAR_S,
            ]
        ] = None,
        empty: Optional[bool] = None,
    ) -> Dict[
        AnyStr,
        Tuple[Dict[AnyStr, AnyStr], Union[Tuple[Tuple[int, float], ...], Tuple[()]]],
    ]:
        """
        Query a range across multiple time series by filters in forward direction
        """
        pieces: CommandArgList = [
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            pieces.append(b"LATEST")
        if filter_by_ts:
            _ts: List[int] = list(filter_by_ts)
            pieces.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            pieces.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if withlabels:
            pieces.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: List[StringT] = list(selected_labels)
            pieces.extend([PureToken.SELECTED_LABELS, *_labels])
        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])
        if aggregator or buckettimestamp is not None:
            if align is not None:
                pieces.extend([PrefixToken.ALIGN, align])
            if aggregator and bucketduration is not None:
                pieces.extend([PrefixToken.AGGREGATION, aggregator, bucketduration])
            if buckettimestamp is not None:
                pieces.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty:
                pieces.append(PureToken.EMPTY)
        if filters:
            _filters: List[StringT] = list(filters)
            pieces.extend([PrefixToken.FILTER, *_filters])
        if groupby and reducer:
            pieces.extend([PureToken.GROUPBY, groupby, b"REDUCE", reducer])
        return await self.execute_module_command(
            CommandName.TS_MRANGE,
            *pieces,
            callback=TimeSeriesMultiCallback[AnyStr](),
            grouped=groupby is not None,
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @mutually_inclusive_parameters("groupby", "reducer")
    @module_command(
        CommandName.TS_MREVRANGE,
        group=CommandGroup.TIMESERIES,
        module="timeseries",
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES, combine=ClusterMergeTimeSeries()
        ),
    )
    async def mrevrange(
        self,
        fromtimestamp: Union[int, datetime, StringT],
        totimestamp: Union[int, datetime, StringT],
        filters: Optional[Parameters[StringT]] = None,
        filter_by_ts: Optional[Parameters[int]] = None,
        latest: Optional[StringT] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        withlabels: Optional[bool] = None,
        selected_labels: Optional[Parameters[StringT]] = None,
        count: Optional[int] = None,
        aggregator: Optional[
            Literal[
                PureToken.AVG,
                PureToken.COUNT,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.RANGE,
                PureToken.STD_P,
                PureToken.STD_S,
                PureToken.SUM,
                PureToken.TWA,
                PureToken.VAR_P,
                PureToken.VAR_S,
            ]
        ] = None,
        bucketduration: Optional[int] = None,
        align: Optional[Union[int, StringT]] = None,
        buckettimestamp: Optional[StringT] = None,
        empty: Optional[bool] = None,
        groupby: Optional[StringT] = None,
        reducer: Optional[StringT] = None,
    ) -> Dict[
        AnyStr,
        Tuple[Dict[AnyStr, AnyStr], Union[Tuple[Tuple[int, float], ...], Tuple[()]]],
    ]:
        """
        Query a range across multiple time-series by filters in reverse direction
        """
        pieces: CommandArgList = [
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            pieces.append(b"LATEST")
        if filter_by_ts:
            _ts: List[int] = list(filter_by_ts)
            pieces.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            pieces.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if withlabels:
            pieces.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: List[StringT] = list(selected_labels)
            pieces.extend([PureToken.SELECTED_LABELS, *_labels])
        if count is not None:
            pieces.extend([PrefixToken.COUNT, count])
        if aggregator or buckettimestamp is not None:
            if align is not None:
                pieces.extend([PrefixToken.ALIGN, align])
            if aggregator and bucketduration is not None:
                pieces.extend([PrefixToken.AGGREGATION, aggregator, bucketduration])
            if buckettimestamp is not None:
                pieces.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty:
                pieces.append(PureToken.EMPTY)
        if filters:
            _filters: List[StringT] = list(filters)
            pieces.extend([PrefixToken.FILTER, *_filters])
        if groupby and reducer and reducer:
            pieces.extend([PureToken.GROUPBY, groupby, b"REDUCE", reducer])

        return await self.execute_module_command(
            CommandName.TS_MREVRANGE,
            *pieces,
            callback=TimeSeriesMultiCallback[AnyStr](),
            grouped=groupby is not None,
        )

    @module_command(
        CommandName.TS_GET, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def get(
        self, key: KeyT, latest: Optional[bool] = None
    ) -> Union[Tuple[int, float], Tuple[()]]:
        """
        Get the sample with the highest timestamp from a given time series
        """
        pieces: CommandArgList = [key]
        if latest:
            pieces.append(b"LATEST")
        return await self.execute_module_command(
            CommandName.TS_GET, *pieces, callback=SampleCallback()
        )

    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @module_command(
        CommandName.TS_MGET,
        group=CommandGroup.TIMESERIES,
        module="timeseries",
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeTimeSeries(),
        ),
    )
    async def mget(
        self,
        filters: Parameters[StringT],
        latest: Optional[bool] = None,
        withlabels: Optional[bool] = None,
        selected_labels: Optional[Parameters[StringT]] = None,
    ) -> Dict[AnyStr, Tuple[Dict[AnyStr, AnyStr], Tuple[int, float]]]:
        """
        Get the sample with the highest timestamp from each time series matching a specific filter


        """
        pieces: CommandArgList = []
        if latest:
            pieces.append(b"LATEST")
        if withlabels:
            pieces.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: List[StringT] = list(selected_labels)
            pieces.extend([b"SELECTED_LABELS", *_labels])
        pieces.extend([PrefixToken.FILTER, *filters])
        return await self.execute_module_command(
            CommandName.TS_MGET, *pieces, callback=TimeSeriesCallback[AnyStr]()
        )

    @module_command(
        CommandName.TS_INFO, group=CommandGroup.TIMESERIES, module="timeseries"
    )
    async def info(
        self, key: KeyT, debug: Optional[bool] = None
    ) -> Dict[AnyStr, ResponseType]:
        """
        Returns information and statistics for a time series
        """
        pieces: CommandArgList = [key]
        if debug:
            pieces.append(b"DEBUG")
        return await self.execute_module_command(
            CommandName.TS_INFO, *pieces, callback=TimeSeriesInfoCallback[AnyStr]()
        )

    @module_command(
        CommandName.TS_QUERYINDEX,
        group=CommandGroup.TIMESERIES,
        module="timeseries",
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets(),
        ),
    )
    async def queryindex(self, filters: Parameters[StringT]) -> Set[AnyStr]:
        """
        Get all time series keys matching a filter list
        """
        pieces: CommandArgList = [*filters]

        return await self.execute_module_command(
            CommandName.TS_QUERYINDEX, *pieces, callback=SetCallback[AnyStr]()
        )
