from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.response.types import Command
from coredis.response.utils import flat_pairs_to_dict, pairs_to_dict
from coredis.typing import AbstractSet, Any, AnyStr, Mapping, MutableMapping
from coredis.utils import EncodingInsensitiveDict, nativestr


class CommandCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Mapping[str, Command]:
        commands: MutableMapping[str, Command] = {}

        for command in response:
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


class CommandKeyFlagCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Mapping[AnyStr, AbstractSet[AnyStr]]:
        return {k[0]: set(k[1]) for k in response}

    def transform_3(
        self, response: Any, **options: Any
    ) -> Mapping[AnyStr, AbstractSet[AnyStr]]:
        return pairs_to_dict(response)


class CommandDocCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Mapping[AnyStr, Mapping]:
        cmd_mapping = flat_pairs_to_dict(response)
        for cmd, doc in cmd_mapping.items():
            cmd_mapping[cmd] = EncodingInsensitiveDict(flat_pairs_to_dict(doc))
            cmd_mapping[cmd]["arguments"] = [
                flat_pairs_to_dict(arg) for arg in cmd_mapping[cmd]["arguments"]
            ]
        return cmd_mapping

    def transform_3(self, response: Any, **options: Any) -> Mapping[AnyStr, Mapping]:
        return dict(response)
