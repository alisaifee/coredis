from __future__ import annotations

from typing import Any, cast

from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response.types import Command
from coredis.typing import (
    AnyStr,
    Hashable,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


class CommandCallback(
    ResponseCallback[
        list[list[ResponsePrimitive | dict[Hashable, ResponseType] | set[Hashable]]],
        dict[str, Command],
    ]
):
    def transform(
        self,
        response: list[list[ResponsePrimitive | dict[Hashable, ResponseType] | set[Hashable]]],
    ) -> dict[str, Command]:
        commands: dict[str, Command] = {}

        for command in response:
            if command:
                # FIXME: giving up for now.
                command = cast(list[Any], command)
                name = nativestr(command[0])

                if len(command) >= 6:
                    commands[name] = {
                        "name": command[0],
                        "arity": command[1],
                        "flags": command[2],
                        "first-key": command[3],
                        "last-key": command[4],
                        "step": command[5],
                        "acl-categories": None,
                        "tips": None,
                        "key-specifications": None,
                        "sub-commands": None,
                    }

                if len(command) >= 7:
                    commands[name]["acl-categories"] = command[6]

                if len(command) >= 8:
                    commands[name]["tips"] = command[7]
                    commands[name]["key-specifications"] = command[8]
                    commands[name]["sub-commands"] = command[9]

        return commands


class CommandKeyFlagCallback(
    ResponseCallback[list[list[StringT | set[StringT]]], dict[AnyStr, set[AnyStr]]]
):
    def transform(
        self,
        response: list[list[StringT | set[StringT]]],
    ) -> dict[AnyStr, set[AnyStr]]:
        return {cast(AnyStr, k[0]): set(cast(set[AnyStr], k[1])) for k in response}
