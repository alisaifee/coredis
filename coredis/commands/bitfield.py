from __future__ import annotations

import enum

from coredis._protocols import AbstractExecutor
from coredis.commands.constants import CommandName
from coredis.exceptions import ReadOnlyError
from coredis.tokens import PrefixToken, PureToken
from coredis.typing import (
    AnyStr,
    CommandArgList,
    Generic,
    KeyT,
    Literal,
    ResponseType,
    Union,
)


class BitFieldSubCommand(bytes, enum.Enum):
    SET = PrefixToken.SET
    GET = PrefixToken.GET
    INCRBY = PrefixToken.INCRBY
    OVERFLOW = PrefixToken.OVERFLOW


class BitFieldOperation(Generic[AnyStr]):
    """
    The command treats a Redis string as a array of bits,
    and is capable of addressing specific integer fields
    of varying bit widths and arbitrary non (necessary) aligned offset.

    The supported types are up to 64 bits for signed integers,
    and up to 63 bits for unsigned integers.

    Offset can be num prefixed with `#` character or num directly.

    Redis command documentation: `BITFIELD <https://redios.io/commands/bitfield>`__
    """

    def __init__(
        self, redis_client: AbstractExecutor, key: KeyT, readonly: bool = False
    ) -> None:
        self._command = (
            CommandName.BITFIELD if not readonly else CommandName.BITFIELD_RO
        )
        self._command_stack: CommandArgList = [key]
        self.redis = redis_client
        self.readonly = readonly

    def __del__(self) -> None:
        self._command_stack.clear()

    def set(
        self, encoding: str, offset: Union[int, str], value: int
    ) -> BitFieldOperation[AnyStr]:
        """
        Set the specified bit field and returns its old value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend([BitFieldSubCommand.SET, encoding, offset, value])

        return self

    def get(self, encoding: str, offset: Union[int, str]) -> BitFieldOperation[AnyStr]:
        """
        Returns the specified bit field.
        """

        self._command_stack.extend([BitFieldSubCommand.GET, encoding, offset])

        return self

    def incrby(
        self, encoding: str, offset: Union[int, str], increment: int
    ) -> BitFieldOperation[AnyStr]:
        """
        Increments or decrements (if a negative increment is given)
        the specified bit field and returns the new value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend(
            [BitFieldSubCommand.INCRBY, encoding, offset, increment]
        )

        return self

    def overflow(
        self,
        behavior: Literal[
            PureToken.SAT, PureToken.WRAP, PureToken.FAIL
        ] = PureToken.SAT,
    ) -> BitFieldOperation[AnyStr]:
        """
        fine-tune the behavior of the increment or decrement overflow,
        have no effect unless used before :meth:`incrby`
        three :paramref:`behavior` types are available: ``WRAP|SAT|FAIL``
        """

        if self.readonly:
            raise ReadOnlyError()
        self._command_stack.extend([BitFieldSubCommand.OVERFLOW, behavior])

        return self

    async def exc(self) -> ResponseType:
        """execute commands in command stack"""

        return await self.redis.execute_command(
            self._command, *self._command_stack, decode=False
        )
