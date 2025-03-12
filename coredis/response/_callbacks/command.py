from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import Command
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
    ValueT,
)


class CommandCallback(ResponseCallback[list[ResponseType], list[ResponseType], dict[str, Command]]):
    def transform(
        self, response: list[ResponseType], **options: ValueT | None
    ) -> dict[str, Command]:
        commands: dict[str, Command] = {}

        for command in response:
            assert isinstance(command, list)

            if command:
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
    ResponseCallback[list[ResponseType], list[ResponseType], dict[AnyStr, set[AnyStr]]]
):
    def transform(
        self, response: list[ResponseType], **options: ValueT | None
    ) -> dict[AnyStr, set[AnyStr]]:
        return {k[0]: set(k[1]) for k in response}


class CommandDocCallback(
    ResponseCallback[
        list[ResponseType],
        dict[ResponsePrimitive, ResponseType],
        dict[AnyStr, dict[AnyStr, ResponseType]],
    ]
):
    def transform(
        self, response: list[ResponseType], **options: ValueT | None
    ) -> dict[AnyStr, dict[AnyStr, ResponseType]]:
        cmd_mapping = flat_pairs_to_dict(response)
        for cmd, doc in cmd_mapping.items():
            cmd_mapping[cmd] = EncodingInsensitiveDict(flat_pairs_to_dict(doc))
            cmd_mapping[cmd]["arguments"] = [
                flat_pairs_to_dict(arg) for arg in cmd_mapping[cmd].get("arguments", [])
            ]
        return cmd_mapping

    def transform_3(
        self,
        response: dict[ResponsePrimitive, ResponseType],
        **options: ValueT | None,
    ) -> dict[AnyStr, dict[AnyStr, ResponseType]]:
        return response  # noqa
