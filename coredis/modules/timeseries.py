from __future__ import annotations

import itertools
from datetime import datetime, timedelta

from deprecated.sphinx import versionadded

from coredis.typing import (
    AnyStr,
    CommandArgList,
    KeyT,
    Literal,
    Mapping,
    Parameters,
    RedisValueT,
    ResponseType,
    StringT,
    ValueT,
)

from .._utils import dict_to_flat_list
from ..commands._utils import normalized_milliseconds, normalized_time_milliseconds
from ..commands._validators import (
    mutually_exclusive_parameters,
    mutually_inclusive_parameters,
)
from ..commands._wrappers import ClusterCommandConfig
from ..commands.constants import CommandFlag, CommandGroup, CommandName, NodeFlag
from ..commands.request import CommandRequest
from ..response._callbacks import (
    ClusterMergeSets,
    IntCallback,
    SetCallback,
    SimpleStringCallback,
    TupleCallback,
)
from ..tokens import PrefixToken, PureToken
from .base import Module, ModuleGroup, module_command
from .response._callbacks.timeseries import (
    ClusterMergeTimeSeries,
    SampleCallback,
    SamplesCallback,
    TimeSeriesCallback,
    TimeSeriesInfoCallback,
    TimeSeriesMultiCallback,
)


def normalized_timestamp(ts: int | datetime | StringT) -> StringT | int:
    if isinstance(ts, (bytes, str)):
        return ts
    return normalized_time_milliseconds(ts)


class RedisTimeSeries(Module[AnyStr]):
    NAME = "timeseries"
    FULL_NAME = "RedisTimeSeries"
    DESCRIPTION = """RedisTimeSeries is a Redis module that implements a time series
data structure. It is designed to be used as a database for time series data,
and is optimized for fast insertion and retrieval of time series data.
    """
    DOCUMENTATION_URL = "https://redis.io/docs/develop/data-types/timeseries/"


@versionadded(version="4.12")
class TimeSeries(ModuleGroup[AnyStr]):
    MODULE = RedisTimeSeries
    COMMAND_GROUP = CommandGroup.TIMESERIES

    @module_command(
        CommandName.TS_CREATE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def create(
        self,
        key: KeyT,
        retention: int | timedelta | None = None,
        encoding: Literal[PureToken.COMPRESSED, PureToken.UNCOMPRESSED] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: None
        | (
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ) = None,
        labels: Mapping[StringT, ValueT] | None = None,
    ) -> CommandRequest[bool]:
        """
        Create a new time series with the given key.

        :param key: The key name for the time series.
        :param retention: Maximum age for samples compared to the highest reported timestamp,
         in milliseconds.
        :param encoding: Specifies the series samples encoding format as ``COMPRESSED`` or
         ``UNCOMPRESSED``.
        :param chunk_size: Initial allocation size, in bytes, for the data part of each new chunk.
        :param duplicate_policy: Policy for handling insertion of multiple samples with identical
         timestamps.
        :param labels: A dictionary of labels to be associated with the time series.
        :return: True if the time series was created successfully, False otherwise.
        """
        command_arguments: CommandArgList = [key]
        if retention is not None:
            command_arguments.extend([PrefixToken.RETENTION, normalized_milliseconds(retention)])
        if encoding:
            command_arguments.extend([PrefixToken.ENCODING, encoding])
        if chunk_size is not None:
            command_arguments.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy is not None:
            command_arguments.extend([PrefixToken.DUPLICATE_POLICY, duplicate_policy])
        if labels:
            command_arguments.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        return self.client.create_request(
            CommandName.TS_CREATE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TS_DEL,
        group=COMMAND_GROUP,
        version_introduced="1.6.0",
        module=MODULE,
    )
    def delete(
        self,
        key: KeyT,
        fromtimestamp: int | datetime | StringT,
        totimestamp: int | datetime | StringT,
    ) -> CommandRequest[int]:
        """
        Delete all samples between two timestamps for a given time series.

        :param key: Key name for the time series.
        :param fromtimestamp: Start timestamp for the range deletion.
        :param totimestamp: End timestamp for the range deletion.
        :return: The number of samples that were deleted, or an error reply.
        """
        return self.client.create_request(
            CommandName.TS_DEL,
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
            callback=IntCallback(),
        )

    @module_command(
        CommandName.TS_ALTER,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def alter(
        self,
        key: KeyT,
        labels: Mapping[StringT, StringT] | None = None,
        retention: int | None = None,
        chunk_size: int | None = None,
        duplicate_policy: None
        | (
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ) = None,
    ) -> CommandRequest[bool]:
        """
        Update the retention, chunk size, duplicate policy, and labels of an existing time series.

        :param key: Key name for the time series.
        :param labels: Dictionary mapping labels to values that represent metadata labels of the
         key and serve as a secondary index.
        :param retention: Maximum retention period, compared to the maximum existing timestamp, in
         milliseconds.
        :param chunk_size: Initial allocation size, in bytes, for the data part of each new chunk.
        :param duplicate_policy: Policy for handling multiple samples with identical timestamps.
        :return: True if executed correctly, False otherwise.
        """
        command_arguments: CommandArgList = [key]
        if labels:
            command_arguments.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        if retention is not None:
            command_arguments.extend([PrefixToken.RETENTION, retention])
        if chunk_size is not None:
            command_arguments.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy:
            command_arguments.extend([PrefixToken.DUPLICATE_POLICY, duplicate_policy])
        return self.client.create_request(
            CommandName.TS_ALTER, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TS_ADD,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def add(
        self,
        key: KeyT,
        timestamp: int | datetime | StringT,
        value: int | float,
        retention: int | None = None,
        encoding: Literal[PureToken.COMPRESSED, PureToken.UNCOMPRESSED] | None = None,
        chunk_size: int | None = None,
        duplicate_policy: None
        | (
            Literal[
                PureToken.BLOCK,
                PureToken.FIRST,
                PureToken.LAST,
                PureToken.MAX,
                PureToken.MIN,
                PureToken.SUM,
            ]
        ) = None,
        labels: Mapping[StringT, ValueT] | None = None,
    ) -> CommandRequest[int]:
        """
        Add a sample to a time series.

        :param key: Name of the time series.
        :param timestamp: UNIX sample timestamp in milliseconds or `*` to set the timestamp
         according to the server clock.
        :param value: Numeric data value of the sample.
        :param retention: Maximum retention period, compared to the maximum existing timestamp, in
         milliseconds.
        :param encoding: Encoding format for the series sample.
        :param chunk_size: Memory size, in bytes, allocated for each data chunk.
        :param duplicate_policy: Policy for handling samples with identical timestamps.
        :param labels: Dictionary of labels associated with the sample.
        :return: Number of samples added to the time series.
        """
        command_arguments: CommandArgList = [
            key,
            normalized_timestamp(timestamp),
            value,
        ]
        if retention is not None:
            command_arguments.extend([PrefixToken.RETENTION, retention])
        if encoding:
            command_arguments.extend([PrefixToken.ENCODING, encoding])
        if chunk_size is not None:
            command_arguments.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if duplicate_policy:
            command_arguments.extend([PrefixToken.ON_DUPLICATE, duplicate_policy])
        if labels:
            command_arguments.extend(
                [
                    PrefixToken.LABELS,
                    *dict_to_flat_list(labels),  # type: ignore
                ]
            )
        return self.client.create_request(
            CommandName.TS_ADD, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_MADD,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def madd(
        self, ktvs: Parameters[tuple[AnyStr, int, int | float]]
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Append new samples to one or more time series.

        :param ktvs: A list of tuples, where each tuple contains the key name for the time series,
         an integer UNIX sample timestamp in milliseconds or `*` to set the timestamp according
         to the server clock, and a numeric data value of the sample.
        :return: A tuple of integers representing the timestamp of each added sample
        """
        command_arguments: CommandArgList = list(itertools.chain(*ktvs))

        return self.client.create_request(
            CommandName.TS_MADD, *command_arguments, callback=TupleCallback[int]()
        )

    @module_command(
        CommandName.TS_INCRBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def incrby(
        self,
        key: KeyT,
        value: int | float,
        labels: Mapping[StringT, RedisValueT] | None = None,
        timestamp: datetime | int | StringT | None = None,
        retention: int | timedelta | None = None,
        uncompressed: bool | None = None,
        chunk_size: int | None = None,
    ) -> CommandRequest[int]:
        """
        Increments the value of the sample with the maximum existing timestamp, or creates
        a new sample with a value equal to the value of the sample with the maximum existing
        timestamp with a given increment.

        :param key: Name of the time series.
        :param value: Numeric data value of the sample.
        :param labels: Set of label-value pairs that represent metadata labels of the key and serve
         as a secondary index. Use it only if you are creating a new time series.
        :param timestamp: UNIX sample timestamp in milliseconds or `*` to set the timestamp
         according to the server clock. `timestamp` must be equal to or higher than the maximum
         existing timestamp. When not specified, the timestamp is set according to the server clock.
        :param retention: Maximum retention period, compared to the maximum existing timestamp,
         in milliseconds. Use it only if you are creating a new time series.
        :param uncompressed: Changes data storage from compressed (default) to uncompressed.
         Use it only if you are creating a new time series.
        :param chunk_size: Memory size, in bytes, allocated for each data chunk.
         Use it only if you are creating a new time series.
        :return: The timestamp of the upserted sample, or an error.
        """
        command_arguments: CommandArgList = [key, value]
        if timestamp:
            command_arguments.extend([PrefixToken.TIMESTAMP, normalized_timestamp(timestamp)])
        if retention:
            command_arguments.extend([PrefixToken.RETENTION, normalized_milliseconds(retention)])
        if uncompressed:
            command_arguments.append(PureToken.UNCOMPRESSED)
        if chunk_size:
            command_arguments.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if labels:
            command_arguments.extend(
                [PrefixToken.LABELS, *dict_to_flat_list(labels)]  # type: ignore
            )

        return self.client.create_request(
            CommandName.TS_INCRBY, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_DECRBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def decrby(
        self,
        key: KeyT,
        value: int | float,
        labels: Mapping[StringT, RedisValueT] | None = None,
        timestamp: datetime | int | StringT | None = None,
        retention: int | timedelta | None = None,
        uncompressed: bool | None = None,
        chunk_size: int | None = None,
    ) -> CommandRequest[int]:
        """
        Decrease the value of the sample with the maximum existing timestamp, or create a new
        sample with a value equal to the value of the sample with the maximum existing timestamp
        with a given decrement.

        :param key: Key name for the time series.
        :param value: Numeric data value of the sample.
        :param labels: Mapping of labels to values that represent metadata labels of the key
         and serve as a secondary index. Use it only if you are creating a new time series.
        :param timestamp: UNIX sample timestamp in milliseconds or `*` to set the timestamp
         according to the server clock. When not specified, the timestamp is set according
         to the server clock.
        :param retention: Maximum retention period, compared to the maximum existing timestamp,
         in milliseconds. Use it only if you are creating a new time series. It is ignored if
         you are adding samples to an existing time series.
        :param uncompressed: Changes data storage from compressed (default) to uncompressed.
         Use it only if you are creating a new time series. It is ignored if you are adding samples
         to an existing time series.
        :param chunk_size: Memory size, in bytes, allocated for each data chunk. Use it only if
         you are creating a new time series. It is ignored if you are adding samples to an existing
         time series.
        :return: The timestamp of the upserted sample, or an error if the operation failed.
        """
        command_arguments: CommandArgList = [key, value]

        if timestamp:
            command_arguments.extend([PrefixToken.TIMESTAMP, normalized_timestamp(timestamp)])
        if retention:
            command_arguments.extend([PrefixToken.RETENTION, normalized_milliseconds(retention)])
        if uncompressed:
            command_arguments.append(PureToken.UNCOMPRESSED)
        if chunk_size:
            command_arguments.extend([PrefixToken.CHUNK_SIZE, chunk_size])
        if labels:
            command_arguments.extend(
                [PrefixToken.LABELS, *dict_to_flat_list(labels)]  # type: ignore
            )
        return self.client.create_request(
            CommandName.TS_DECRBY, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.TS_CREATERULE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        arguments={"aligntimestamp": {"version_introduced": "1.8.0"}},
        module=MODULE,
    )
    def createrule(
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
        bucketduration: int | timedelta,
        aligntimestamp: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Create a compaction rule

        :param source: Key name for the source time series.
        :param destination: Key name for the destination (compacted) time series.
        :param aggregation: Aggregates results into time buckets by the given aggregation type
        :param bucketduration: Duration of each bucket, in milliseconds.
        :param aligntimestamp: Ensures that there is a bucket that starts exactly at
         ``aligntimestamp`` and aligns all other buckets accordingly. It is expressed
         in milliseconds. The default value is 0 aligned with the epoch.
        :return: True if executed correctly, False otherwise.
        """
        command_arguments: CommandArgList = [source, destination]
        command_arguments.extend(
            [
                PrefixToken.AGGREGATION,
                aggregation,
                normalized_milliseconds(bucketduration),
            ]
        )
        if aligntimestamp is not None:
            command_arguments.append(aligntimestamp)
        return self.client.create_request(
            CommandName.TS_CREATERULE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.TS_DELETERULE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def deleterule(self, source: KeyT, destination: KeyT) -> CommandRequest[bool]:
        """
        Delete a compaction rule from a RedisTimeSeries sourceKey to a destinationKey.

        :param source: Key name for the source time series.
        :param destination: Key name for the destination (compacted) time series.
        :return: True if the command executed correctly, False otherwise.

        .. warning:: This command does not delete the compacted series.
        """
        command_arguments: CommandArgList = [source, destination]

        return self.client.create_request(
            CommandName.TS_DELETERULE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @module_command(
        CommandName.TS_RANGE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        arguments={
            "latest": {"version_introduced": "1.8.0"},
            "empty": {"version_introduced": "1.8.0"},
        },
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def range(
        self,
        key: KeyT,
        fromtimestamp: datetime | int | StringT,
        totimestamp: datetime | int | StringT,
        *,
        filter_by_ts: Parameters[int] | None = None,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        count: int | None = None,
        aggregator: None
        | (
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
        ) = None,
        bucketduration: int | timedelta | None = None,
        align: int | StringT | None = None,
        buckettimestamp: StringT | None = None,
        empty: bool | None = None,
        latest: bool | None = None,
    ) -> CommandRequest[tuple[tuple[int, float], ...] | tuple[()]]:
        """
        Query a range in forward direction.

        :param key: The key name for the time series.
        :param fromtimestamp: Start timestamp for the range query (integer UNIX timestamp in
         milliseconds) or `-` to denote the timestamp of the earliest sample in the time series.
        :param totimestamp: End timestamp for the range query (integer UNIX timestamp in
         milliseconds) or `+` to denote the timestamp of the latest sample in the time series.
        :param filter_by_ts: List of specific timestamps to filter samples by.
        :param min_value: Minimum value to filter samples by.
        :param max_value: Maximum value to filter samples by.
        :param count: Limits the number of returned samples.
        :param aggregator: Aggregates samples into time buckets by the provided aggregation type.
        :param bucketduration: Duration of each bucket in milliseconds.
        :param align: Time bucket alignment control for :paramref:`aggregator`.
        :param buckettimestamp: Timestamp of the first bucket.
        :param empty: If True, returns an empty list instead of raising an error when no data
         is available.
        :param latest: Used when a time series is a compaction. When ``True``, the command also
         reports the compacted value of the latest, possibly partial, bucket, given that
         this bucket's start time falls within ``[fromtimestamp, totimestamp]``.

        :return: A tuple of samples, where each sample is a tuple of timestamp and value.
        """
        command_arguments: CommandArgList = [
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            command_arguments.append(b"LATEST")
        if filter_by_ts:
            _ts: list[int] = list(filter_by_ts)
            command_arguments.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            command_arguments.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if aggregator and bucketduration is not None:
            if align is not None:
                command_arguments.extend([PrefixToken.ALIGN, align])
            command_arguments.extend(
                [
                    PrefixToken.AGGREGATION,
                    aggregator,
                    normalized_milliseconds(bucketduration),
                ]
            )
            if buckettimestamp is not None:
                command_arguments.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty is not None:
                command_arguments.append(PureToken.EMPTY)

        return self.client.create_request(
            CommandName.TS_RANGE, *command_arguments, callback=SamplesCallback()
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @module_command(
        CommandName.TS_REVRANGE,
        group=COMMAND_GROUP,
        version_introduced="1.4.0",
        arguments={
            "latest": {"version_introduced": "1.8.0"},
            "empty": {"version_introduced": "1.8.0"},
        },
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def revrange(
        self,
        key: KeyT,
        fromtimestamp: int | datetime | StringT,
        totimestamp: int | datetime | StringT,
        *,
        filter_by_ts: Parameters[int] | None = None,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        count: int | None = None,
        aggregator: None
        | (
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
        ) = None,
        bucketduration: int | timedelta | None = None,
        align: int | StringT | None = None,
        buckettimestamp: StringT | None = None,
        empty: bool | None = None,
        latest: bool | None = None,
    ) -> CommandRequest[tuple[tuple[int, float], ...] | tuple[()]]:
        """
        Query a range in reverse direction from a RedisTimeSeries key.

        :param key: The key name for the time series.
        :param fromtimestamp: Start timestamp for the range query (integer UNIX timestamp
         in milliseconds) or `-` to denote the timestamp of the earliest sample in the time series.
        :param totimestamp: End timestamp for the range query (integer UNIX timestamp in
         milliseconds) or `+` to denote the timestamp of the latest sample in the time series.
        :param filter_by_ts: List of specific timestamps to filter samples by.
        :param min_value: Minimum value to filter samples by.
        :param max_value: Maximum value to filter samples by.
        :param count: Limit the number of returned samples.
        :param aggregator: Aggregates samples into time buckets by the provided aggregation type.
        :param bucketduration: Duration of each bucket in milliseconds.
        :param align: Time bucket alignment control for :paramref:`aggregator`.
        :param buckettimestamp: Timestamp for the first bucket.
        :param empty: Return an empty list if no samples are found.
        :param latest: Report the compacted value of the latest, possibly partial, bucket.

        :return: A tuple of timestamp-value pairs in reverse order.
        """
        command_arguments: CommandArgList = [
            key,
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            command_arguments.append(b"LATEST")
        if filter_by_ts:
            _ts: list[int] = list(filter_by_ts)
            command_arguments.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            command_arguments.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if aggregator and bucketduration is not None:
            if align is not None:
                command_arguments.extend([PrefixToken.ALIGN, align])
            command_arguments.extend(
                [
                    PrefixToken.AGGREGATION,
                    aggregator,
                    normalized_milliseconds(bucketduration),
                ]
            )
            if buckettimestamp is not None:
                command_arguments.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty is not None:
                command_arguments.append(PureToken.EMPTY)

        return self.client.create_request(
            CommandName.TS_REVRANGE, *command_arguments, callback=SamplesCallback()
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @mutually_inclusive_parameters("groupby", "reducer")
    @module_command(
        CommandName.TS_MRANGE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        arguments={
            "latest": {"version_introduced": "1.8.0"},
            "empty": {"version_introduced": "1.8.0"},
        },
        module=MODULE,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeTimeSeries(),
        ),
        flags={CommandFlag.READONLY},
    )
    def mrange(
        self,
        fromtimestamp: int | datetime | StringT,
        totimestamp: int | datetime | StringT,
        filters: Parameters[StringT] | None = None,
        *,
        filter_by_ts: Parameters[int] | None = None,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        withlabels: bool | None = None,
        selected_labels: Parameters[StringT] | None = None,
        count: int | None = None,
        align: int | StringT | None = None,
        aggregator: None
        | (
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
        ) = None,
        bucketduration: int | timedelta | None = None,
        buckettimestamp: StringT | None = None,
        groupby: StringT | None = None,
        reducer: None
        | (
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
        ) = None,
        empty: bool | None = None,
        latest: bool | None = None,
    ) -> CommandRequest[
        dict[
            AnyStr,
            tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
        ]
    ]:
        """
        Query a range across multiple time series by filters in forward direction.

        :param fromtimestamp: Start timestamp for the range query (integer UNIX timestamp
         in milliseconds) or `-` to denote the timestamp of the earliest sample amongst
         all time series that passes the filters.
        :param totimestamp: End timestamp for the range query (integer UNIX timestamp in
         milliseconds) or `+` to denote the timestamp of the latest sample amongst all
         time series that passes the filters
        :param filters: Filter expressions to apply to the time series.
        :param filter_by_ts: Timestamps to filter the time series by.
        :param min_value: Minimum value to filter the time series by.
        :param max_value: Maximum value to filter the time series by.
        :param withlabels: Whether to include labels in the response.
        :param selected_labels: Returns a subset of the label-value pairs that represent metadata
         labels of the time series. Use when a large number of labels exists per series, but only
         the values of some of the labels are required. If :paramref:`withlabels` or
         :paramref:`selected_labels` are not specified, by default, an empty mapping is reported
         as label-value pairs.
        :param count: Limit the number of samples returned.
        :param align: Time bucket alignment control for :paramref:`aggregator`.
        :param aggregator: Aggregates samples into time buckets by the provided aggregation type.
        :param bucketduration: Duration of each bucket, in milliseconds.
        :param buckettimestamp: Timestamp of the first bucket.
        :param groupby: Label to group the samples by
        :param reducer: Aggregation type to aggregate the results in each group
        :param empty: Optional boolean to include empty time series in the response.
        :param latest: Report the compacted value of the latest, possibly partial, bucket.

        :return: A dictionary containing the time series data.
        """
        command_arguments: CommandArgList = [
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            command_arguments.append(b"LATEST")
        if filter_by_ts:
            _ts: list[int] = list(filter_by_ts)
            command_arguments.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            command_arguments.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if withlabels:
            command_arguments.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: list[StringT] = list(selected_labels)
            command_arguments.extend([PureToken.SELECTED_LABELS, *_labels])
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if aggregator or buckettimestamp is not None:
            if align is not None:
                command_arguments.extend([PrefixToken.ALIGN, align])
            if aggregator and bucketduration is not None:
                command_arguments.extend(
                    [
                        PrefixToken.AGGREGATION,
                        aggregator,
                        normalized_milliseconds(bucketduration),
                    ]
                )
            if buckettimestamp is not None:
                command_arguments.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty:
                command_arguments.append(PureToken.EMPTY)
        if filters:
            _filters: list[StringT] = list(filters)
            command_arguments.extend([PrefixToken.FILTER, *_filters])
        if groupby and reducer:
            command_arguments.extend([PureToken.GROUPBY, groupby, b"REDUCE", reducer])
        return self.client.create_request(
            CommandName.TS_MRANGE,
            *command_arguments,
            callback=TimeSeriesMultiCallback[AnyStr](grouped=groupby is not None),
        )

    @mutually_inclusive_parameters("min_value", "max_value")
    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @mutually_inclusive_parameters("aggregator", "bucketduration")
    @mutually_inclusive_parameters("groupby", "reducer")
    @module_command(
        CommandName.TS_MREVRANGE,
        group=COMMAND_GROUP,
        version_introduced="1.4.0",
        arguments={
            "latest": {"version_introduced": "1.8.0"},
            "empty": {"version_introduced": "1.8.0"},
        },
        module=MODULE,
        cluster=ClusterCommandConfig(route=NodeFlag.PRIMARIES, combine=ClusterMergeTimeSeries()),
        flags={CommandFlag.READONLY},
    )
    def mrevrange(
        self,
        fromtimestamp: int | datetime | StringT,
        totimestamp: int | datetime | StringT,
        filters: Parameters[StringT] | None = None,
        *,
        filter_by_ts: Parameters[int] | None = None,
        min_value: int | float | None = None,
        max_value: int | float | None = None,
        withlabels: bool | None = None,
        selected_labels: Parameters[StringT] | None = None,
        count: int | None = None,
        align: int | StringT | None = None,
        aggregator: None
        | (
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
        ) = None,
        bucketduration: int | timedelta | None = None,
        buckettimestamp: StringT | None = None,
        groupby: StringT | None = None,
        reducer: StringT | None = None,
        empty: bool | None = None,
        latest: bool | None = None,
    ) -> CommandRequest[
        dict[
            AnyStr,
            tuple[dict[AnyStr, AnyStr], tuple[tuple[int, float], ...] | tuple[()]],
        ]
    ]:
        """
        Query a range across multiple time series by filters in reverse direction.

        :param fromtimestamp: Start timestamp for the range query (integer UNIX timestamp
         in milliseconds) or `-` to denote the timestamp of the earliest sample amongst
         all time series that passes the filters.
        :param totimestamp: End timestamp for the range query (integer UNIX timestamp in
         milliseconds) or `+` to denote the timestamp of the latest sample amongst all
         time series that passes the filters
        :param filters: Filter expressions to apply to the time series.
        :param filter_by_ts: Timestamps to filter the time series by.
        :param min_value: Minimum value to filter the time series by.
        :param max_value: Maximum value to filter the time series by.
        :param withlabels: Whether to include labels in the response.
        :param selected_labels: Returns a subset of the label-value pairs that represent metadata
         labels of the time series. Use when a large number of labels exists per series, but only
         the values of some of the labels are required. If :paramref:`withlabels` or
         :paramref:`selected_labels` are not specified, by default, an empty mapping is reported
         as label-value pairs.
        :param count: Limit the number of samples returned.
        :param align: Time bucket alignment control for :paramref:`aggregator`.
        :param aggregator: Aggregates samples into time buckets by the provided aggregation type.
        :param bucketduration: Duration of each bucket, in milliseconds.
        :param buckettimestamp: Timestamp of the first bucket.
        :param groupby: Label to group the samples by
        :param reducer: Aggregation type to aggregate the results in each group
        :param empty: Optional boolean to include empty time series in the response.
        :param latest: Report the compacted value of the latest, possibly partial, bucket.

        :return: A dictionary containing the result of the query.
        """
        command_arguments: CommandArgList = [
            normalized_timestamp(fromtimestamp),
            normalized_timestamp(totimestamp),
        ]
        if latest:
            command_arguments.append(b"LATEST")
        if filter_by_ts:
            _ts: list[int] = list(filter_by_ts)
            command_arguments.extend([PrefixToken.FILTER_BY_TS, *_ts])
        if min_value is not None and max_value is not None:
            command_arguments.extend([PureToken.FILTER_BY_VALUE, min_value, max_value])
        if withlabels:
            command_arguments.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: list[StringT] = list(selected_labels)
            command_arguments.extend([PureToken.SELECTED_LABELS, *_labels])
        if count is not None:
            command_arguments.extend([PrefixToken.COUNT, count])
        if aggregator or buckettimestamp is not None:
            if align is not None:
                command_arguments.extend([PrefixToken.ALIGN, align])
            if aggregator and bucketduration is not None:
                command_arguments.extend(
                    [
                        PrefixToken.AGGREGATION,
                        aggregator,
                        normalized_milliseconds(bucketduration),
                    ]
                )
            if buckettimestamp is not None:
                command_arguments.extend([PureToken.BUCKETTIMESTAMP, buckettimestamp])
            if empty:
                command_arguments.append(PureToken.EMPTY)
        if filters:
            _filters: list[StringT] = list(filters)
            command_arguments.extend([PrefixToken.FILTER, *_filters])
        if groupby and reducer and reducer:
            command_arguments.extend([PureToken.GROUPBY, groupby, b"REDUCE", reducer])

        return self.client.create_request(
            CommandName.TS_MREVRANGE,
            *command_arguments,
            callback=TimeSeriesMultiCallback[AnyStr](grouped=groupby is not None),
        )

    @module_command(
        CommandName.TS_GET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        arguments={"latest": {"version_introduced": "1.8.0"}},
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def get(
        self, key: KeyT, latest: bool | None = None
    ) -> CommandRequest[tuple[int, float] | tuple[()]]:
        """
        Get the sample with the highest timestamp from a given time series.

        :param key: The key name for the time series.
        :param latest: If the time series is a compaction, if ``True``, reports
         the compacted value of the latest, possibly partial, bucket. When ``False``,
         does not report the latest, possibly partial, bucket. When a time series is not a
         compaction, the parameter is ignored.
        :return: A tuple of (timestamp, value) of the sample with the highest timestamp,
         or an empty tuple if the time series is empty.
        """
        command_arguments: CommandArgList = [key]
        if latest:
            command_arguments.append(b"LATEST")
        return self.client.create_request(
            CommandName.TS_GET, *command_arguments, callback=SampleCallback()
        )

    @mutually_exclusive_parameters("withlabels", "selected_labels")
    @module_command(
        CommandName.TS_MGET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        arguments={"latest": {"version_introduced": "1.8.0"}},
        module=MODULE,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeTimeSeries(),
        ),
        flags={CommandFlag.READONLY},
    )
    def mget(
        self,
        filters: Parameters[StringT],
        withlabels: bool | None = None,
        selected_labels: Parameters[StringT] | None = None,
        latest: bool | None = None,
    ) -> CommandRequest[dict[AnyStr, tuple[dict[AnyStr, AnyStr], tuple[int, float] | tuple[()]]]]:
        """
        Get the sample with the highest timestamp from each time series matching a specific filter.

        :param filters: Filters time series based on their labels and label values. At least one
         `label=value` filter is required.
        :param withlabels: Includes in the reply all label-value pairs representing metadata labels
         of the time series. If :paramref:`withlabels` or :paramref:`selected_labels` are not
         specified, by default, an empty dictionary is reported as label-value pairs.
        :param selected_labels: Returns a subset of the label-value pairs that represent metadata
         labels of the time series. Use when a large number of labels exists per series, but only
         the values of some of the labels are required. If :paramref:`withlabels` or
         :paramref:`selected_labels` are not specified, by default, an empty mapping is reported
         as label-value pairs.
        :param latest: Used when a time series is a compaction. If ``True``, the command also
         reports the compacted value of the latest possibly partial bucket, given that this
         bucket's start time falls within `[fromTimestamp, toTimestamp]`. If ``False``,
         the command does not report the latest possibly partial bucket. When a time series is
         not a compaction, the argument is ignored
        :return: For each time series matching the specified filters, a dictionary is returned with
         the time series key name as the key and a tuple containing the label-value pairs and a
         single timestamp-value pair as the value.
        """
        command_arguments: CommandArgList = []
        if latest:
            command_arguments.append(b"LATEST")
        if withlabels:
            command_arguments.append(PureToken.WITHLABELS)
        if selected_labels:
            _labels: list[StringT] = list(selected_labels)
            command_arguments.extend([b"SELECTED_LABELS", *_labels])
        command_arguments.extend([PrefixToken.FILTER, *filters])
        return self.client.create_request(
            CommandName.TS_MGET,
            *command_arguments,
            callback=TimeSeriesCallback[AnyStr](),
        )

    @module_command(
        CommandName.TS_INFO,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def info(
        self, key: KeyT, debug: bool | None = None
    ) -> CommandRequest[dict[AnyStr, ResponseType]]:
        """
        Return information and statistics for a time series.

        :param key: Key name of the time series.
        :param debug: Optional flag to get a more detailed information about the chunks.
        :return: Dictionary with information about the time series (name-value pairs).
        """
        command_arguments: CommandArgList = [key]
        if debug:
            command_arguments.append(b"DEBUG")
        return self.client.create_request(
            CommandName.TS_INFO,
            *command_arguments,
            callback=TimeSeriesInfoCallback[AnyStr](),
        )

    @module_command(
        CommandName.TS_QUERYINDEX,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        cluster=ClusterCommandConfig(
            route=NodeFlag.PRIMARIES,
            combine=ClusterMergeSets(),
        ),
        flags={CommandFlag.READONLY},
    )
    def queryindex(self, filters: Parameters[StringT]) -> CommandRequest[set[AnyStr]]:
        """
        Get all time series keys matching a filter list.

        :param filters: A list of filter expressions to match time series based on their labels
         and label values. Each filter expression has one of the following syntaxes:

          - ``label=value``, where ``label`` equals ``value``
          - ``label!=value``, where ``label`` does not equal ``value``
          - ``label=``, where ``key`` does not have label ``label``
          - ``label!=``, where ``key`` has label ``label``
          - ``label=(value1,value2,...)``, where ``key`` with label ``label`` equals one of
            the values in the list
          - ``label!=(value1,value2,...)``, where key with label ``label`` does not equal
            any of the values in the list

         At least one ``label=value`` filter is required. Filters are conjunctive. For example, the
         filter ``type=temperature room=study`` means the a time series is a temperature time series
         of a study room. Don't use whitespaces in the filter expression.
        :return: A set of time series keys matching the filter list. The set is empty if no time
         series matches the filter. An error is returned on invalid filter expression.

        """
        command_arguments: CommandArgList = [*filters]

        return self.client.create_request(
            CommandName.TS_QUERYINDEX,
            *command_arguments,
            callback=SetCallback[AnyStr](),
        )
