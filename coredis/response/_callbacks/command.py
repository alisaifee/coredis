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
                detail = cast(list[Any], command)
                name = nativestr(detail[0])

                if len(command) >= 6:
                    commands[name] = {
                        "name": detail[0],
                        "arity": detail[1],
                        "flags": detail[2],
                        "first-key": detail[3],
                        "last-key": detail[4],
                        "step": detail[5],
                        "acl-categories": None,
                        "tips": None,
                        "key-specifications": None,
                        "sub-commands": None,
                    }

                if len(command) >= 7:
                    commands[name]["acl-categories"] = detail[6]

                if len(command) >= 8:
                    commands[name]["tips"] = detail[7]
                    commands[name]["key-specifications"] = detail[8]
                    commands[name]["sub-commands"] = detail[9]

        return commands


class CommandKeyFlagCallback(
    ResponseCallback[list[list[StringT | set[StringT]]], dict[AnyStr, set[AnyStr]]]
):
    def transform(
        self,
        response: list[list[StringT | set[StringT]]],
    ) -> dict[AnyStr, set[AnyStr]]:
        return {cast(AnyStr, k[0]): set(cast(set[AnyStr], k[1])) for k in response}
