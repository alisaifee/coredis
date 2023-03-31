from __future__ import annotations

from coredis.typing import (
    AnyStr,
    CommandArgList,
    Dict,
    KeyT,
    Literal,
    Optional,
    Parameters,
    Tuple,
    Union,
    ValueT,
)

from ..commands.constants import CommandGroup, CommandName
from ..response._callbacks import (
    BoolCallback,
    BoolsCallback,
    DictCallback,
    FirstValueCallback,
    IntCallback,
    MixedTupleCallback,
    SimpleStringCallback,
)
from ..tokens import PrefixToken, PureToken
from .base import ModuleGroup, module_command


class BloomFilter(ModuleGroup[AnyStr]):
    @module_command(CommandName.BF_RESERVE, group=CommandGroup.BF, module="bf")
    async def reserve(
        self,
        key: KeyT,
        error_rate: Union[int, float],
        capacity: int,
        expansion: Optional[int] = None,
        nonscaling: Optional[bool] = None,
    ) -> bool:
        """
        Creates a new Bloom Filter
        """
        pieces: CommandArgList = [key, error_rate, capacity]
        if expansion is not None:
            pieces.extend([PrefixToken.EXPANSION, expansion])
        if nonscaling:
            pieces.append(PureToken.NONSCALING)

        return await self.execute_module_command(
            CommandName.BF_RESERVE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(CommandName.BF_ADD, group=CommandGroup.BF, module="bf")
    async def add(self, key: KeyT, item: ValueT) -> bool:
        """
        Adds an item to a Bloom Filter
        """
        pieces: CommandArgList = [key, item]

        return await self.execute_module_command(
            CommandName.BF_ADD, *pieces, callback=BoolCallback()
        )

    @module_command(CommandName.BF_MADD, group=CommandGroup.BF, module="bf")
    async def madd(self, key: KeyT, items: Parameters[ValueT]) -> Tuple[bool, ...]:
        """
        Adds one or more items to a Bloom Filter. A filter will be created if it does not exist
        """
        pieces: CommandArgList = [key, *items]

        return await self.execute_module_command(
            CommandName.BF_MADD, *pieces, callback=BoolsCallback()
        )

    @module_command(CommandName.BF_INSERT, group=CommandGroup.BF, module="bf")
    async def insert(
        self,
        key: KeyT,
        items: Parameters[ValueT],
        capacity: Optional[int] = None,
        error: Optional[Union[int, float]] = None,
        expansion: Optional[int] = None,
        nocreate: Optional[bool] = None,
        nonscaling: Optional[bool] = None,
    ) -> Tuple[bool, ...]:
        """
        Adds one or more items to a Bloom Filter. A filter will be created if it
        does not exist
        """
        pieces: CommandArgList = [key]
        if capacity is not None:
            pieces.extend([PrefixToken.CAPACITY, capacity])
        if error is not None:
            pieces.extend([PrefixToken.ERROR, error])
        if expansion is not None:
            pieces.extend([PrefixToken.EXPANSION, expansion])
        if nocreate:
            pieces.append(PureToken.NOCREATE)
        if nonscaling:
            pieces.append(PureToken.NONSCALING)
        pieces.append(PureToken.ITEMS)
        pieces.extend(items)
        return await self.execute_module_command(
            CommandName.BF_INSERT, *pieces, callback=BoolsCallback()
        )

    @module_command(CommandName.BF_EXISTS, group=CommandGroup.BF, module="bf")
    async def exists(self, key: KeyT, item: ValueT) -> bool:
        """
        Checks whether an item exists in a Bloom Filter
        """
        return await self.execute_module_command(
            CommandName.BF_EXISTS, key, item, callback=BoolCallback()
        )

    @module_command(CommandName.BF_MEXISTS, group=CommandGroup.BF, module="bf")
    async def mexists(self, key: KeyT, items: Parameters[ValueT]) -> Tuple[bool, ...]:
        """
        Checks whether one or more items exist in a Bloom Filter
        """
        return await self.execute_module_command(
            CommandName.BF_MEXISTS, key, *items, callback=BoolsCallback()
        )

    @module_command(CommandName.BF_SCANDUMP, group=CommandGroup.BF, module="bf")
    async def scandump(self, key: KeyT, iterator: int) -> Tuple[int, bytes]:
        """
        Begins an incremental save of the bloom filter
        :return: a tuple containing (next iter, data)
        """

        return await self.execute_module_command(
            CommandName.BF_SCANDUMP,
            key,
            iterator,
            callback=MixedTupleCallback[int, bytes](),
            decode=False,
        )

    @module_command(CommandName.BF_LOADCHUNK, group=CommandGroup.BF, module="bf")
    async def loadchunk(self, key: KeyT, iterator: int, data: bytes) -> bool:
        """
        Restores a filter previously saved using :meth:`scandump`
        """
        pieces: CommandArgList = [key, iterator, data]

        return await self.execute_module_command(
            CommandName.BF_LOADCHUNK, *pieces, callback=SimpleStringCallback()
        )

    @module_command(CommandName.BF_INFO, group=CommandGroup.BF, module="bf")
    async def info(
        self,
        key: KeyT,
        single_value: Optional[
            Literal[
                PureToken.CAPACITY,
                PureToken.EXPANSION,
                PureToken.FILTERS,
                PureToken.ITEMS,
                PureToken.SIZE,
            ]
        ] = None,
    ) -> Union[Dict[AnyStr, int], int]:
        """
        Returns information about a Bloom Filter
        """
        if single_value:
            return await self.execute_module_command(
                CommandName.BF_INFO,
                key,
                single_value,
                callback=FirstValueCallback[int](),
            )
        else:
            return await self.execute_module_command(
                CommandName.BF_INFO, key, callback=DictCallback[AnyStr, int]()
            )

    @module_command(CommandName.BF_CARD, group=CommandGroup.BF, module="bf")
    async def card(self, key: KeyT) -> int:
        """
        Returns the cardinality of a Bloom filter
        """
        return await self.execute_module_command(
            CommandName.BF_CARD, key, callback=IntCallback()
        )
