from __future__ import annotations

import enum

from coredis.commands import CommandName
from coredis.exceptions import ReadOnlyError
from coredis.tokens import PrefixToken, PureToken
from coredis.typing import Literal


class BitFieldSubCommand(bytes, enum.Enum):
    SET = PrefixToken.SET
    GET = PrefixToken.GET
    INCRBY = PrefixToken.INCRBY
    OVERFLOW = PrefixToken.OVERFLOW


class BitFieldOperation:
    """
    The command treats a Redis string as a array of bits,
    and is capable of addressing specific integer fields
    of varying bit widths and arbitrary non (necessary) aligned offset.

    The supported types are up to 64 bits for signed integers,
    and up to 63 bits for unsigned integers.

    Offset can be num prefixed with `#` character or num directly.

    Redis command documentation: `BITFIELD <https://redios.io/commands/bitfield>`__
    """

    def __init__(self, redis_client, key, readonly=False):
        self._command_stack = [
            CommandName.BITFIELD if not readonly else CommandName.BITFIELD_RO,
            key,
        ]
        self.redis = redis_client
        self.readonly = readonly

    def __del__(self):
        self._command_stack.clear()

    def set(self, type_, offset, value):
        """
        Set the specified bit field and returns its old value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend([BitFieldSubCommand.SET, type_, offset, value])

        return self

    def get(self, type_, offset):
        """
        Returns the specified bit field.
        """

        self._command_stack.extend([BitFieldSubCommand.GET, type_, offset])

        return self

    def incrby(self, type_, offset, increment):
        """
        Increments or decrements (if a negative increment is given)
        the specified bit field and returns the new value.
        """

        if self.readonly:
            raise ReadOnlyError()

        self._command_stack.extend(
            [BitFieldSubCommand.INCRBY, type_, offset, increment]
        )

        return self

    def overflow(
        self,
        type_: Literal[PureToken.SAT, PureToken.WRAP, PureToken.FAIL] = PureToken.SAT,
    ):
        """
        fine-tune the behavior of the increment or decrement overflow,
        have no effect unless used before `incrby`
        three types are available: WRAP|SAT|FAIL
        """

        if self.readonly:
            raise ReadOnlyError()
        self._command_stack.extend([BitFieldSubCommand.OVERFLOW, type_])

        return self

    async def exc(self):
        """execute commands in command stack"""

        return await self.redis.execute_command(*self._command_stack, decode=False)
