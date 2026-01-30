from __future__ import annotations

from deprecated.sphinx import versionadded

from .._utils import dict_to_flat_list
from ..commands._validators import mutually_inclusive_parameters
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..commands.request import CommandRequest
from ..response._callbacks import (
    BoolCallback,
    BoolsCallback,
    DictCallback,
    FirstValueCallback,
    FloatCallback,
    FloatsCallback,
    IntCallback,
    MixedTupleCallback,
    SimpleStringCallback,
    TupleCallback,
)
from ..tokens import PrefixToken, PureToken
from ..typing import (
    AnyStr,
    CommandArgList,
    KeyT,
    Literal,
    Mapping,
    Parameters,
    ResponsePrimitive,
    StringT,
    ValueT,
)
from .base import Module, ModuleGroup, module_command


class RedisBloom(Module[AnyStr]):
    NAME = "bf"
    FULL_NAME = "RedisBloom"
    DESCRIPTION = """RedisBloom is a Redis module that implements various probabilistic
data structures such as BloomFilter.
    """
    DOCUMENTATION_URL = "https://redis.io/docs/develop/data-types/probabilistic"


@versionadded(version="4.12")
class BloomFilter(ModuleGroup[AnyStr]):
    MODULE = RedisBloom
    COMMAND_GROUP = CommandGroup.BF

    @module_command(
        CommandName.BF_RESERVE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def reserve(
        self,
        key: KeyT,
        error_rate: int | float,
        capacity: int,
        expansion: int | None = None,
        nonscaling: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Creates a new Bloom Filter

        :param key: The key under which the filter is found.
        :param error_rate: The desired probability for false positives.
        :param capacity: The number of entries intended to be added to the filter.
        :param expansion: The size of the new sub-filter when `capacity` is reached.
        :param nonscaling: Prevents the filter from creating additional sub-filters.
        """
        command_arguments: CommandArgList = [key, error_rate, capacity]
        if expansion is not None:
            command_arguments.extend([PrefixToken.EXPANSION, expansion])
        if nonscaling:
            command_arguments.append(PureToken.NONSCALING)

        return self.client.create_request(
            CommandName.BF_RESERVE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.BF_ADD,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def add(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Adds an item to a Bloom Filter

        :param key: The key under which the filter is found.
        :param item: The item to add to the filter.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.BF_ADD, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.BF_MADD,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def madd(self, key: KeyT, items: Parameters[ValueT]) -> CommandRequest[tuple[bool, ...]]:
        """
        Adds one or more items to a Bloom Filter. A filter will be created if it does not exist

        :param key: The key under which the filter is found.
        :param items: One or more items to add.
        """
        command_arguments: CommandArgList = [key, *items]

        return self.client.create_request(
            CommandName.BF_MADD, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.BF_INSERT,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def insert(
        self,
        key: KeyT,
        items: Parameters[ValueT],
        capacity: int | None = None,
        error: int | float | None = None,
        expansion: int | None = None,
        nocreate: bool | None = None,
        nonscaling: bool | None = None,
    ) -> CommandRequest[tuple[bool, ...]]:
        """
        Adds one or more items to a Bloom Filter. A filter will be created if it
        does not exist

        :param key: The key under which the filter is found.
        :param items: One or more items to add.
        :param capacity: The desired capacity for the filter to be created.
        :param error: The error ratio of the newly created filter if it does not yet exist.
        :param expansion: The expansion factor for the filter when capacity is reached.
        :param nocreate: Indicates that the filter should not be created if it does not
         already exist.
        :param nonscaling: Prevents the filter from creating additional sub-filters
         if initial capacity is reached.
        """
        command_arguments: CommandArgList = [key]
        if capacity is not None:
            command_arguments.extend([PrefixToken.CAPACITY, capacity])
        if error is not None:
            command_arguments.extend([PrefixToken.ERROR, error])
        if expansion is not None:
            command_arguments.extend([PrefixToken.EXPANSION, expansion])
        if nocreate:
            command_arguments.append(PureToken.NOCREATE)
        if nonscaling:
            command_arguments.append(PureToken.NONSCALING)
        command_arguments.append(PureToken.ITEMS)
        command_arguments.extend(items)
        return self.client.create_request(
            CommandName.BF_INSERT, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.BF_EXISTS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def exists(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Checks whether an item exists in a Bloom Filter

        :param key: The key under which the filter is found.
        :param item: The item to check for existence.
        """
        return self.client.create_request(CommandName.BF_EXISTS, key, item, callback=BoolCallback())

    @module_command(
        CommandName.BF_MEXISTS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def mexists(self, key: KeyT, items: Parameters[ValueT]) -> CommandRequest[tuple[bool, ...]]:
        """
        Checks whether one or more items exist in a Bloom Filter

        :param key: The key under which the filter is found.
        :param items: One or more items to check.
        """
        return self.client.create_request(
            CommandName.BF_MEXISTS, key, *items, callback=BoolsCallback()
        )

    @module_command(
        CommandName.BF_SCANDUMP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def scandump(self, key: KeyT, iterator: int) -> CommandRequest[tuple[int, bytes | None]]:
        """
        Begins an incremental save of the bloom filter

        :param key: The key under which the filter is found.
        :param iterator: Iterator value; either 0 to start a dump or the iterator
         from a previous invocation of this command.

        :return: A tuple of iterator value and data. If iterator is 0, iteration has
         completed. The iterator-data pair should be passed to :meth:`loadchunk` when
         restoring the filter.
        """

        return self.client.create_request(
            CommandName.BF_SCANDUMP,
            key,
            iterator,
            callback=MixedTupleCallback[int, bytes | None](),
            execution_parameters={"decode": False},
        )

    @module_command(
        CommandName.BF_LOADCHUNK,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def loadchunk(self, key: KeyT, iterator: int, data: bytes) -> CommandRequest[bool]:
        """
        Restores a filter previously saved using :meth:`scandump`

        :param key: Name of the key to restore.
        :param iterator: Iterator value associated with the data chunk.
        :param data: Current data chunk.
        """
        command_arguments: CommandArgList = [key, iterator, data]

        return self.client.create_request(
            CommandName.BF_LOADCHUNK,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.BF_INFO,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def info(
        self,
        key: KeyT,
        single_value: None
        | (
            Literal[
                PureToken.CAPACITY,
                PureToken.EXPANSION,
                PureToken.FILTERS,
                PureToken.ITEMS,
                PureToken.SIZE,
            ]
        ) = None,
    ) -> CommandRequest[int] | CommandRequest[dict[AnyStr, int]]:
        """
        Returns information about a Bloom Filter

        :param key: The key name for an existing Bloom filter.
        :param single_value: Optional parameter to get a specific information field.

        :return: A dictionary with all information fields if :paramref:`single_value`
         is not specified, or an integer representing the value of the specified field.
        """
        if single_value:
            return self.client.create_request(
                CommandName.BF_INFO,
                key,
                single_value,
                callback=FirstValueCallback[int](),
            )
        else:
            return self.client.create_request(
                CommandName.BF_INFO, key, callback=DictCallback[AnyStr, int]()
            )

    @module_command(
        CommandName.BF_CARD,
        group=COMMAND_GROUP,
        version_introduced="2.4.4",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def card(self, key: KeyT) -> CommandRequest[int]:
        """
        Returns the cardinality of a Bloom filter

        :param key: The key name for an existing Bloom filter.
        """
        return self.client.create_request(CommandName.BF_CARD, key, callback=IntCallback())


@versionadded(version="4.12")
class CuckooFilter(ModuleGroup[AnyStr]):
    MODULE = RedisBloom
    COMMAND_GROUP = CommandGroup.CF

    @module_command(
        CommandName.CF_RESERVE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def reserve(
        self,
        key: KeyT,
        capacity: int,
        bucketsize: int | None = None,
        maxiterations: int | None = None,
        expansion: int | None = None,
    ) -> CommandRequest[bool]:
        """
        Creates a new Cuckoo Filter

        :param key: The name of the filter.
        :param capacity: Estimated capacity for the filter.
        :param bucketsize: Number of items in each bucket.
        :param maxiterations: Number of attempts to swap items between buckets
         before declaring filter as full and creating an additional filter.
        :param expansion: When a new filter is created, its size is the size of the
         current filter multiplied by ``expansion``.
        """
        command_arguments: CommandArgList = [key, capacity]
        if bucketsize is not None:
            command_arguments.extend([PrefixToken.BUCKETSIZE, bucketsize])
        if maxiterations is not None:
            command_arguments.extend([PrefixToken.MAXITERATIONS, maxiterations])
        if expansion is not None:
            command_arguments.extend([PrefixToken.EXPANSION, expansion])
        return self.client.create_request(
            CommandName.CF_RESERVE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.CF_ADD,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def add(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Adds an item to a Cuckoo Filter

        :param key: The name of the filter.
        :param item: The item to add.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.CF_ADD, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.CF_ADDNX,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def addnx(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Adds an item to a Cuckoo Filter if the item did not exist previously.

        :param key: The name of the filter.
        :param item: The item to add.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.CF_ADDNX, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.CF_INSERT,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def insert(
        self,
        key: KeyT,
        items: Parameters[ValueT],
        capacity: int | None = None,
        nocreate: bool | None = None,
    ) -> CommandRequest[tuple[bool, ...]]:
        """
        Adds one or more items to a Cuckoo Filter. A filter will be created if it does not exist

        :param key: The name of the filter.
        :param items: One or more items to add.
        :param capacity: Specifies the desired capacity of the new filter, if this filter
         does not exist yet.
        :param nocreate: If specified, prevents automatic filter creation if the filter
         does not exist.
        :return: A tuple of boolean values indicating if the command was executed correctly.
        """
        command_arguments: CommandArgList = [key]
        if capacity is not None:
            command_arguments.extend([PrefixToken.CAPACITY, capacity])
        if nocreate is not None:
            command_arguments.append(PureToken.NOCREATE)
        command_arguments.append(PureToken.ITEMS)
        command_arguments.extend(items)

        return self.client.create_request(
            CommandName.CF_INSERT, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.CF_INSERTNX,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def insertnx(
        self,
        key: KeyT,
        items: Parameters[ValueT],
        capacity: int | None = None,
        nocreate: bool | None = None,
    ) -> CommandRequest[tuple[bool, ...]]:
        """
        Adds one or more items to a Cuckoo Filter if the items did not exist previously.
        A filter will be created if it does not exist

        :param key: The name of the filter.
        :param items: One or more items to add.
        :param capacity: Specifies the desired capacity of the new filter,
         if this filter does not exist yet.
        :param nocreate: If specified, prevents automatic filter creation
         if the filter does not exist.
        """
        command_arguments: CommandArgList = [key]
        if capacity is not None:
            command_arguments.extend([PrefixToken.CAPACITY, capacity])
        if nocreate is not None:
            command_arguments.append(PureToken.NOCREATE)
        command_arguments.append(PureToken.ITEMS)
        command_arguments.extend(items)

        return self.client.create_request(
            CommandName.CF_INSERTNX, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.CF_EXISTS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def exists(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Checks whether an item exist in a Cuckoo Filter

        :param key: The name of the filter.
        :param item: The item to check for.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.CF_EXISTS, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.CF_MEXISTS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def mexists(self, key: KeyT, items: Parameters[ValueT]) -> CommandRequest[tuple[bool, ...]]:
        """
        Checks whether one or more items exist in a Cuckoo Filter

        :param key: The name of the filter.
        :param items: The item(s) to check for.
        """
        command_arguments: CommandArgList = [key, *items]

        return self.client.create_request(
            CommandName.CF_MEXISTS, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.CF_DEL,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def delete(self, key: KeyT, item: ValueT) -> CommandRequest[bool]:
        """
        Deletes an item from a Cuckoo Filter

        :param key: The name of the filter.
        :param item: The item to delete from the filter.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.CF_DEL, *command_arguments, callback=BoolCallback()
        )

    @module_command(
        CommandName.CF_COUNT,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def count(self, key: KeyT, item: ValueT) -> CommandRequest[int]:
        """
        Return the number of times an item might be in a Cuckoo Filter

        :param key: The name of the filter.
        :param item: The item to count.
        """
        command_arguments: CommandArgList = [key, item]

        return self.client.create_request(
            CommandName.CF_COUNT, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.CF_SCANDUMP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def scandump(self, key: KeyT, iterator: int) -> CommandRequest[tuple[int, bytes | None]]:
        """
        Begins an incremental save of the bloom filter

        :param key: Name of the filter.
        :param iterator: Iterator value. This is either 0, or the iterator from a
         previous invocation of this command.
        """
        command_arguments: CommandArgList = [key, iterator]

        return self.client.create_request(
            CommandName.CF_SCANDUMP,
            *command_arguments,
            execution_parameters={"decode": False},
            callback=MixedTupleCallback[int, bytes | None](),
        )

    @module_command(
        CommandName.CF_LOADCHUNK,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def loadchunk(self, key: KeyT, iterator: int, data: StringT) -> CommandRequest[bool]:
        """
        Restores a filter previously saved using SCANDUMP

        :param key: Name of the key to restore.
        :param iter: Iterator value associated with :paramref:`data` (returned by :meth:`scandump`).
        :param data: Current data chunk (returned by :meth:`scandump`).

        """
        command_arguments: CommandArgList = [key, iterator, data]

        return self.client.create_request(
            CommandName.CF_LOADCHUNK,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.CF_INFO,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    def info(self, key: KeyT) -> CommandRequest[dict[AnyStr, ResponsePrimitive]]:
        """
        Returns information about a Cuckoo Filter

        :param key: The name of the filter.
        """

        return self.client.create_request(
            CommandName.CF_INFO,
            key,
            callback=DictCallback[AnyStr, ResponsePrimitive](),
        )


@versionadded(version="4.12")
class CountMinSketch(ModuleGroup[AnyStr]):
    MODULE = RedisBloom
    COMMAND_GROUP = CommandGroup.CMS

    @module_command(
        CommandName.CMS_INITBYDIM,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def initbydim(self, key: KeyT, width: int, depth: int) -> CommandRequest[bool]:
        """
        Initializes a Count-Min Sketch to dimensions specified by user

        :param key: Name of the sketch.
        :param width: Number of counters in each array. Reduces error size.
        :param depth: Number of counter-arrays. Reduces error probability.
        """
        return self.client.create_request(
            CommandName.CMS_INITBYDIM,
            key,
            width,
            depth,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.CMS_INITBYPROB,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def initbyprob(
        self, key: KeyT, error: int | float, probability: int | float
    ) -> CommandRequest[bool]:
        """
        Initializes a Count-Min Sketch to accommodate requested tolerances.

        :param key: Name of the sketch.
        :param error: Estimate size of error as a percent of total counted items.
        :param probability: Desired probability for inflated count as a decimal value
         between 0 and 1.
        """
        return self.client.create_request(
            CommandName.CMS_INITBYPROB,
            key,
            error,
            probability,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.CMS_INCRBY,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def incrby(self, key: KeyT, items: Mapping[AnyStr, int]) -> CommandRequest[tuple[int, ...]]:
        """
        Increases the count of one or more items by increment

        :param key: The name of the HyperLogLog sketch.
        :param items: A dictionary containing the items to increment and
         their respective increments.
        """

        return self.client.create_request(
            CommandName.CMS_INCRBY,
            key,
            *dict_to_flat_list(items),
            callback=TupleCallback[int](),
        )

    @module_command(
        CommandName.CMS_QUERY,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def query(
        self,
        key: KeyT,
        items: Parameters[StringT],
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Returns the count for one or more items in a sketch

        :param key: The name of the Count-Min Sketch.
        :param items: One or more items for which to return the count.
        """
        command_arguments: CommandArgList = [key, *items]

        return self.client.create_request(
            CommandName.CMS_QUERY, *command_arguments, callback=TupleCallback[int]()
        )

    @module_command(
        CommandName.CMS_MERGE,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def merge(
        self,
        destination: KeyT,
        sources: Parameters[KeyT],
        weights: Parameters[int | float] | None = None,
    ) -> CommandRequest[bool]:
        """
        Merges several sketches into one sketch

        :param destination: The name of the destination sketch. Must be initialized.
        :param sources: Names of the source sketches to be merged.
        :param weights: Multiples of each sketch. Default is 1.
        """
        _sources: list[KeyT] = list(sources)
        command_arguments: CommandArgList = [destination, len(_sources), *_sources]
        if weights:
            command_arguments.append(PrefixToken.WEIGHTS)
            command_arguments.extend(weights)

        return self.client.create_request(
            CommandName.CMS_MERGE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.CMS_INFO,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def info(self, key: KeyT) -> CommandRequest[dict[AnyStr, int]]:
        """
        Returns information about a sketch

        :param key: The name of the sketch.
        :return: A dictionary containing the width, depth, and total count of the sketch.
        """

        return self.client.create_request(
            CommandName.CMS_INFO,
            key,
            callback=DictCallback[AnyStr, int](),
        )


@versionadded(version="4.12")
class TopK(ModuleGroup[AnyStr]):
    MODULE = RedisBloom
    COMMAND_GROUP = CommandGroup.TOPK

    @mutually_inclusive_parameters("width", "depth", "decay")
    @module_command(
        CommandName.TOPK_RESERVE,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def reserve(
        self,
        key: KeyT,
        topk: int,
        width: int | None = None,
        depth: int | None = None,
        decay: int | float | None = None,
    ) -> CommandRequest[bool]:
        """
        Reserve a TopK sketch with specified parameters.

        :param key: Name of the TOP-K sketch.
        :param topk: Number of top occurring items to keep.
        :param width: Number of counters kept in each array.
        :param depth: Number of arrays.
        :param decay: The probability of reducing a counter in an occupied bucket.
         It is raised to power of it's counter (``decay ^ bucket[i].counter``).
         Therefore, as the counter gets higher, the chance of a reduction is being reduced.
        """
        command_arguments: CommandArgList = [key, topk]
        if width is not None and depth is not None and decay is not None:
            command_arguments.extend([width, depth, decay])
        return self.client.create_request(
            CommandName.TOPK_RESERVE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.TOPK_ADD,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def add(
        self, key: KeyT, items: Parameters[AnyStr]
    ) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """
        Increases the count of one or more items by increment

        :param key: Name of the TOP-K sketch.
        :param items: Item(s) to be added.

        """
        return self.client.create_request(
            CommandName.TOPK_ADD,
            key,
            *items,
            callback=TupleCallback[AnyStr | None](),
        )

    @module_command(
        CommandName.TOPK_INCRBY,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def incrby(
        self, key: KeyT, items: Mapping[AnyStr, int]
    ) -> CommandRequest[tuple[AnyStr | None, ...]]:
        """
        Increases the count of one or more items by increment

        :param key: Name of the TOP-K sketch.
        :param items: Dictionary of items and their corresponding increment values.
        """
        return self.client.create_request(
            CommandName.TOPK_INCRBY,
            key,
            *dict_to_flat_list(items),
            callback=TupleCallback[AnyStr | None](),
        )

    @module_command(
        CommandName.TOPK_QUERY,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def query(
        self,
        key: KeyT,
        items: Parameters[StringT],
    ) -> CommandRequest[tuple[bool, ...]]:
        """
        Checks whether an item is one of Top-K items.
        Multiple items can be checked at once.

        :param key: Name of the TOP-K sketch.
        :param items: Item(s) to be queried.
        """
        command_arguments: CommandArgList = [key, *items]

        return self.client.create_request(
            CommandName.TOPK_QUERY, *command_arguments, callback=BoolsCallback()
        )

    @module_command(
        CommandName.TOPK_COUNT,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def count(
        self,
        key: KeyT,
        items: Parameters[StringT],
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Return the count for one or more items are in a sketch

        :param key: The name of the TOP-K sketch.
        :param items: One or more items to count.
        """
        command_arguments: CommandArgList = [key, *items]

        return self.client.create_request(
            CommandName.TOPK_COUNT, *command_arguments, callback=TupleCallback[int]()
        )

    @module_command(
        CommandName.TOPK_LIST,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def list(
        self, key: KeyT, withcount: bool | None = None
    ) -> CommandRequest[dict[AnyStr, int]] | CommandRequest[tuple[AnyStr, ...]]:
        """
        Return full list of items in Top K list

        :param key: Name of the TOP-K sketch.
        :param withcount: Whether to include counts of each element.
        """
        command_arguments: CommandArgList = [key]
        if withcount:
            command_arguments.append(PureToken.WITHCOUNT)
            return self.client.create_request(
                CommandName.TOPK_LIST,
                *command_arguments,
                callback=DictCallback[AnyStr, int](),
            )
        else:
            return self.client.create_request(
                CommandName.TOPK_LIST,
                *command_arguments,
                callback=TupleCallback[AnyStr](),
            )

    @module_command(
        CommandName.TOPK_INFO,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def info(self, key: KeyT) -> CommandRequest[dict[AnyStr, int]]:
        """
        Returns information about a sketch

        :param key: Name of the TOP-K sketch.
        :return: A dictionary containing the following information:
         - ``k``: The number of items tracked by the sketch.
         - ``width``: The width of the sketch.
         - ``depth``: The depth of the sketch.
         - ``decay``: The decay factor used by the sketch.
        """

        return self.client.create_request(
            CommandName.TOPK_INFO,
            key,
            callback=DictCallback[AnyStr, int](),
        )


@versionadded(version="4.12")
class TDigest(ModuleGroup[AnyStr]):
    MODULE = RedisBloom
    COMMAND_GROUP = CommandGroup.TDIGEST

    @module_command(
        CommandName.TDIGEST_CREATE,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
    )
    def create(self, key: KeyT, compression: int | None = None) -> CommandRequest[bool]:
        """
        Allocates memory and initializes a new t-digest sketch

        :param key: The key name for the new t-digest sketch.
        :param compression: A controllable tradeoff between accuracy and memory consumption.
        """
        command_arguments: CommandArgList = [key]
        if compression is not None:
            command_arguments.extend([PrefixToken.COMPRESSION, compression])
        return self.client.create_request(
            CommandName.TDIGEST_CREATE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.TDIGEST_RESET,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
    )
    def reset(self, key: KeyT) -> CommandRequest[bool]:
        """
        Resets a t-digest sketch: empty the sketch and re-initializes it.

        :param key: The key name for an existing t-digest sketch.
        """
        return self.client.create_request(
            CommandName.TDIGEST_RESET, key, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.TDIGEST_ADD,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
    )
    def add(
        self,
        key: KeyT,
        values: Parameters[int | float],
    ) -> CommandRequest[bool]:
        """
        Adds one or more observations to a t-digest sketch

        :param key: Key name for an existing t-digest sketch.
        :param values: value(s) of observation(s)
        """
        command_arguments: CommandArgList = [key, *values]

        return self.client.create_request(
            CommandName.TDIGEST_ADD,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.TDIGEST_MERGE,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
    )
    def merge(
        self,
        destination_key: KeyT,
        source_keys: Parameters[KeyT],
        compression: int | None = None,
        override: bool | None = None,
    ) -> CommandRequest[bool]:
        """
        Merges multiple t-digest sketches into a single sketch

        :param destination_key: Key name for a t-digest sketch to merge observation values to.
         If it does not exist, a new sketch is created.
         If it is an existing sketch, its values are merged with the values of the source keys.
         To override the destination key contents use :paramref:`override`.
        :param source_keys: Key names for t-digest sketches to merge observation values from.
        :param compression: Controllable tradeoff between accuracy and memory consumption.
        :param override: When specified, if :paramref:`destination_key` already exists,
         it is overwritten.

        """
        _source_keys: list[KeyT] = list(source_keys)
        command_arguments: CommandArgList = [
            destination_key,
            len(_source_keys),
            *_source_keys,
        ]
        if compression is not None:
            command_arguments.extend([PrefixToken.COMPRESSION, compression])
        if override is not None:
            command_arguments.append(PureToken.OVERRIDE)
        return self.client.create_request(
            CommandName.TDIGEST_MERGE,
            *command_arguments,
            callback=SimpleStringCallback(),
        )

    @module_command(
        CommandName.TDIGEST_MIN,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def min(self, key: KeyT) -> CommandRequest[float]:
        """
        Returns the minimum observation value from a t-digest sketch

        :param key: The key name for an existing t-digest sketch.
        """

        return self.client.create_request(CommandName.TDIGEST_MIN, key, callback=FloatCallback())

    @module_command(
        CommandName.TDIGEST_MAX,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def max(self, key: KeyT) -> CommandRequest[float]:
        """
        Returns the maximum observation value from a t-digest sketch

        :param key: The key name for an existing t-digest sketch.
        """

        return self.client.create_request(CommandName.TDIGEST_MAX, key, callback=FloatCallback())

    @module_command(
        CommandName.TDIGEST_QUANTILE,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def quantile(
        self,
        key: KeyT,
        quantiles: Parameters[int | float],
    ) -> CommandRequest[tuple[float, ...]]:
        """
        Returns, for each input fraction, an estimation of the value (floating point)
        that is smaller than the given fraction of observations

        :param key: Key name for an existing t-digest sketch.
        :param quantiles: Input fractions (between 0 and 1 inclusively).
        """
        command_arguments: CommandArgList = [key, *quantiles]

        return self.client.create_request(
            CommandName.TDIGEST_QUANTILE, *command_arguments, callback=FloatsCallback()
        )

    @module_command(
        CommandName.TDIGEST_CDF,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def cdf(
        self,
        key: KeyT,
        values: Parameters[int | float],
    ) -> CommandRequest[tuple[float, ...]]:
        """
        Returns, for each input value, an estimation of the fraction (floating-point)
        of (observations smaller than the given value + half the observations equal
        to the given value)

        :param key: The key name for an existing t-digest sketch.
        :param values: The values for which the CDF should be retrieved.
        """
        command_arguments: CommandArgList = [key, *values]

        return self.client.create_request(
            CommandName.TDIGEST_CDF, *command_arguments, callback=FloatsCallback()
        )

    @module_command(
        CommandName.TDIGEST_TRIMMED_MEAN,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def trimmed_mean(
        self,
        key: KeyT,
        low_cut_quantile: int | float,
        high_cut_quantile: int | float,
    ) -> CommandRequest[float]:
        """
        Returns an estimation of the mean value from the sketch,
        excluding observation values outside the low and high cutoff quantiles

        :param key: The key name for an existing t-digest sketch.
        :param low_cut_quantile: A floating-point value in the range [0..1],
         should be lower than :paramref:`high_cut_quantile`.
        :param high_cut_quantile: A floating-point value in the range [0..1],
         should be higher than `low_cut_quantile`.

        """
        command_arguments: CommandArgList = [key, low_cut_quantile, high_cut_quantile]

        return self.client.create_request(
            CommandName.TDIGEST_TRIMMED_MEAN,
            *command_arguments,
            callback=FloatCallback(),
        )

    @module_command(
        CommandName.TDIGEST_RANK,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def rank(
        self,
        key: KeyT,
        values: Parameters[int | float],
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Returns, for each input value (floating-point), the estimated rank of
        the value (the number of observations in the sketch that are smaller
        than the value + half the number of observations that are equal to the value)

        :param key: The key name for an existing t-digest sketch.
        :param values: Input values for which the rank should be estimated.
        """
        command_arguments: CommandArgList = [key, *values]

        return self.client.create_request(
            CommandName.TDIGEST_RANK, *command_arguments, callback=TupleCallback[int]()
        )

    @module_command(
        CommandName.TDIGEST_REVRANK,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def revrank(
        self,
        key: KeyT,
        values: Parameters[int | float],
    ) -> CommandRequest[tuple[int, ...]]:
        """
        Returns, for each input value (floating-point), the estimated reverse rank of
        the value (the number of observations in the sketch that are larger than
        the value + half the number of observations that are equal to the value)

        :param key: The name of an existing t-digest sketch.
        :param values: The input values for which the reverse rank should be estimated.
        """
        command_arguments: CommandArgList = [key, *values]

        return self.client.create_request(
            CommandName.TDIGEST_REVRANK,
            *command_arguments,
            callback=TupleCallback[int](),
        )

    @module_command(
        CommandName.TDIGEST_BYRANK,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def byrank(
        self,
        key: KeyT,
        ranks: Parameters[int | float],
    ) -> CommandRequest[tuple[float, ...]]:
        """
        Returns, for each input rank, an estimation of the value (floating-point) with
        that rank

        :param key: The key name for an existing t-digest sketch.
        :param ranks: The ranks for which the estimated values should be retrieved.
        """
        command_arguments: CommandArgList = [key, *ranks]

        return self.client.create_request(
            CommandName.TDIGEST_BYRANK, *command_arguments, callback=FloatsCallback()
        )

    @module_command(
        CommandName.TDIGEST_BYREVRANK,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def byrevrank(
        self,
        key: KeyT,
        reverse_ranks: Parameters[int | float],
    ) -> CommandRequest[tuple[float, ...]]:
        """
        Returns, for each input reverse rank, an estimation of the value
        (floating-point) with that reverse rank

        :param key: The key name for an existing t-digest sketch.
        :param reverse_ranks: The reverse ranks for which the values should be retrieved.
        """
        command_arguments: CommandArgList = [key, *reverse_ranks]

        return self.client.create_request(
            CommandName.TDIGEST_BYREVRANK,
            *command_arguments,
            callback=FloatsCallback(),
        )

    @module_command(
        CommandName.TDIGEST_INFO,
        group=COMMAND_GROUP,
        version_introduced="2.4.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def info(self, key: KeyT) -> CommandRequest[dict[AnyStr, ResponsePrimitive]]:
        """
        Returns information and statistics about a t-digest sketch

        :param key: The key name for an existing t-digest sketch.
        :return: Dictionary with information about the sketch, including compression,
         capacity, number of merged and unmerged nodes, weight of merged and unmerged nodes,
         number of observations, total compressions, and memory usage.
        """

        return self.client.create_request(
            CommandName.TDIGEST_INFO,
            key,
            callback=DictCallback[AnyStr, ResponsePrimitive](),
        )
