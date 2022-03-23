from coredis.response.callbacks import SimpleCallback
from coredis.response.types import Command
from coredis.typing import Any, AnyStr, Dict, Set
from coredis.utils import flat_pairs_to_dict, pairs_to_dict


class CommandCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[str, Command]:
        commands: Dict[str, Command] = {}

        for command in response:
            if command:
                name = command[0]

                if len(command) >= 6:
                    commands[name] = {
                        "name": name,
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


class CommandKeyFlagCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[AnyStr, Set[AnyStr]]:
        return {k[0]: set(k[1]) for k in response}

    def transform_3(self, response: Any) -> Dict[AnyStr, Set[AnyStr]]:
        return pairs_to_dict(response)


class CommandDocCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[AnyStr, Any]:
        cmd = response[0]
        docs = {cmd: flat_pairs_to_dict(response[1])}
        docs[cmd]["arguments"] = [
            flat_pairs_to_dict(arg) for arg in docs[cmd]["arguments"]
        ]
        return docs

    def transform_3(self, response: Any) -> Dict[AnyStr, Dict]:
        return dict(response)
