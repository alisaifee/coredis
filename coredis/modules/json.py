from __future__ import annotations

from deprecated.sphinx import versionadded

from .._json import json
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..commands.request import CommandRequest
from ..response._callbacks import (
    IntCallback,
    NoopCallback,
    OneOrManyCallback,
    SimpleStringCallback,
)
from ..tokens import PureToken
from ..typing import (
    AnyStr,
    CommandArgList,
    JsonType,
    KeyT,
    Literal,
    Parameters,
    RedisValueT,
    ResponseType,
    StringT,
)
from .base import Module, ModuleGroup, module_command
from .response._callbacks.json import JsonCallback


class RedisJSON(Module[AnyStr]):
    NAME = "ReJSON"
    FULL_NAME = "RedisJSON"
    DESCRIPTION = """RedisJSON is a Redis module that implements a JSON data type
and a set of commands to operate on it."""
    DOCUMENTATION_URL = "https://redis.io/docs/develop/data-types/json/"


@versionadded(version="4.12")
class Json(ModuleGroup[AnyStr]):
    MODULE = RedisJSON
    COMMAND_GROUP = CommandGroup.JSON

    @module_command(
        CommandName.JSON_DEL,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def delete(self, key: KeyT, path: StringT | None = None) -> CommandRequest[int]:
        """
        Delete a value from a JSON document.

        :param key: The key of the JSON document.
        :param path: The JSONPath to specify.
        :return: The number of paths deleted
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_DEL, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_GET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        cacheable=True,
        flags={CommandFlag.READONLY},
    )
    def get(
        self,
        key: KeyT,
        *paths: StringT,
    ) -> CommandRequest[JsonType]:
        """
        Gets the value at one or more paths

        :param key: The key of the JSON document.
        :param paths: JSONPath(s) to get values from.
        :return: The value at :paramref:`path`
        """
        command_arguments: CommandArgList = [key]
        if paths:
            command_arguments.extend(paths)

        return self.client.create_request(
            CommandName.JSON_GET, *command_arguments, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_FORGET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def forget(self, key: KeyT, path: RedisValueT | None = None) -> CommandRequest[int]:
        """
        Deletes an element from a path from a json object

        :param key: The key of the JSON document.
        :param path: The path(s) to delete from the JSON object.

        :return: The number of deleted elements.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)
        return self.client.create_request(
            CommandName.JSON_FORGET, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_TOGGLE,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def toggle(self, key: KeyT, path: RedisValueT) -> CommandRequest[JsonType]:
        """
        Toggles a boolean value

        :param key: Redis key to modify.
        :param path: JSONPath to specify.
        :return: A list of integer replies for each path, the new value
         (`0` if `false` or `1` if `true`), or ``None`` for JSON values matching
         the path that are not Boolean.
        """
        command_arguments: CommandArgList = [key, path]
        return self.client.create_request(
            CommandName.JSON_TOGGLE,
            *command_arguments,
            callback=JsonCallback(),
        )

    @module_command(
        CommandName.JSON_CLEAR,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    def clear(self, key: KeyT, path: RedisValueT | None = None) -> CommandRequest[int]:
        """
        Clears all values from an array or an object and sets numeric values to `0`

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        :return: The number of values cleared.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_CLEAR, *command_arguments, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_SET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def set(
        self,
        key: KeyT,
        path: RedisValueT,
        value: JsonType,
        condition: Literal[PureToken.NX, PureToken.XX] | None = None,
    ) -> CommandRequest[bool]:
        """
        Sets or updates the JSON value at a path

        :param key: The key to parse.
        :param path: JSONPath to specify. For new Redis keys the ``path`` must be the root.
         For existing keys, when the entire `path` exists, the value that it contains is replaced
         with :paramref:`value`. For existing keys, when the ``path`` exists, except for the
         last element, a new child is added with :paramref:`value`.
         Adds a key (with its respective value) to a JSON Object (in a json data type key)
         only if it is the last child in the ``path``, or it is the parent of a new child being
         added in the ``path``. Optional argument :paramref:`condition` modifies this behavior
         for both new json data type keys as well as the JSON Object keys in them.
        :param value: Value to set at the specified :paramref:`path`.
        :param condition: Optional argument to modify the behavior of adding a key to a JSON Object.
         If ``NX``, the key is set only if it does not already exist. If ``XX``, the key is set only
         if it already exists.
        :return: `True` if the value was set successfully, `False` otherwise.
        """
        command_arguments: CommandArgList = [key, path, json.dumps(value)]
        if condition:
            command_arguments.append(condition)
        return self.client.create_request(
            CommandName.JSON_SET, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_MGET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    def mget(self, keys: Parameters[KeyT], path: StringT) -> CommandRequest[JsonType]:
        """
        Returns the values at a path from one or more keys

        :param keys: one or more keys to retrieve values from.
        :param path: JSONPath to specify.
        :return: The values at :paramref:`path` for each of the keys in :paramref:`keys`.
        """
        command_arguments: CommandArgList = [*keys, path]
        return self.client.create_request(
            CommandName.JSON_MGET,
            *command_arguments,
            callback=JsonCallback(),
        )

    @module_command(
        CommandName.JSON_MSET,
        group=COMMAND_GROUP,
        version_introduced="2.6.0",
        module=MODULE,
    )
    def mset(self, triplets: Parameters[tuple[KeyT, StringT, JsonType]]) -> CommandRequest[bool]:
        """
        Sets or updates the JSON value of one or more keys

        :param triplets: Collection of triplets containing (``key``, ``path``, ``value``)
         to set.

        :return: `True` if all the values were set successfully
        """
        command_arguments: CommandArgList = []
        for key, path, value in triplets:
            command_arguments.extend([key, path, json.dumps(value)])

        return self.client.create_request(
            CommandName.JSON_MSET, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_MERGE,
        group=COMMAND_GROUP,
        version_introduced="2.6.0",
        module=MODULE,
    )
    def merge(self, key: KeyT, path: StringT, value: JsonType) -> CommandRequest[bool]:
        """
        Merge a JSON object into an existing Redis key at a specified path.

        :param key: The Redis key to merge the JSON object into.
        :param path: The JSONPath within the Redis key to merge the JSON object into.
        :param value: The JSON object to merge into the Redis key.
        :return: True if the merge was successful, False otherwise.
        """
        command_arguments: CommandArgList = [key, path, json.dumps(value)]

        return self.client.create_request(
            CommandName.JSON_MERGE, *command_arguments, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_NUMINCRBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def numincrby(
        self, key: KeyT, path: RedisValueT, value: int | float
    ) -> CommandRequest[JsonType]:
        """
        Increments the numeric value at path by a value

        :param key: The key to modify.
        :param path: The JSONPath to specify.
        :param value: The number value to increment.
        """
        command_arguments: CommandArgList = [key, path, value]

        return self.client.create_request(
            CommandName.JSON_NUMINCRBY, *command_arguments, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_NUMMULTBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def nummultby(
        self, key: KeyT, path: RedisValueT, value: int | float
    ) -> CommandRequest[JsonType]:
        """
        Multiplies the numeric value at path by a value

        :param key: Key to modify.
        :param path: JSONPath to specify.
        :param value: Number value to multiply.
        """
        command_arguments: CommandArgList = [key, path, value]

        return self.client.create_request(
            CommandName.JSON_NUMMULTBY, *command_arguments, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_STRAPPEND,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def strappend(
        self,
        key: KeyT,
        value: str | bytes | int | float | None,
        path: KeyT | None = None,
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Appends a string to a JSON string value at path

        :param key: The key to modify
        :param value: The value to append to the string(s) at the specified :paramref:`path`.
        :param path: The JSONPath to specify the location of the string(s) to modify.
        :return: A list of integer replies for each path, the string's new length,
         or ``None`` if the matching JSON value is not a string.
        """
        command_arguments: CommandArgList = [key]
        if path is not None:
            command_arguments.append(path)
        command_arguments.append(json.dumps(value))
        return self.client.create_request(
            CommandName.JSON_STRAPPEND,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_STRLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def strlen(
        self, key: KeyT, path: KeyT | None = None
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Returns the length of the JSON String at path in key

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.
        :return: An array of integer replies for each path, the array's length,
         or ``None``, if the matching JSON value is not a string.


        """
        command_arguments: CommandArgList = [key]
        if path is not None:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_STRLEN,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRAPPEND,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def arrappend(
        self,
        key: KeyT,
        values: Parameters[JsonType],
        path: KeyT | None = None,
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Append one or more json values into the array at path after the last element in it.

        :param key: The key to modify.
        :param values: One or more values to append to one or more arrays.
        :param path: JSONPath to specify.
        :return: An array of integer replies for each path, the array's new size,
         or `None` if the matching JSON value is not an array.

        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)
        command_arguments.extend([json.dumps(value) for value in values])
        return self.client.create_request(
            CommandName.JSON_ARRAPPEND,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRINDEX,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def arrindex(
        self,
        key: KeyT,
        path: RedisValueT,
        value: str | bytes | int | float,
        start: int | None = None,
        stop: int | None = None,
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Returns the index of the first occurrence of a JSON scalar value in the array at path

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        :param value: The value to find its index in one or more arrays.
        :param start: Inclusive start value to specify in a slice of the array to search.
        :param stop: Exclusive stop value to specify in a slice of the array to search,
         including the last element. Negative values are interpreted as starting from the end.
        :return: The index of the first occurrence of the value in the array,
         or a list of indices if the value is found in multiple arrays.
        """
        command_arguments: CommandArgList = [key, path, json.dumps(value)]
        if start is not None:
            command_arguments.append(start)
        if stop is not None:
            command_arguments.append(stop)

        return self.client.create_request(
            CommandName.JSON_ARRINDEX,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRINSERT,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def arrinsert(
        self,
        key: KeyT,
        path: RedisValueT,
        index: int,
        values: Parameters[JsonType],
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Inserts the JSON scalar(s) value at the specified index in the array at path

        :param key: Key to modify.
        :param path: JSONPath to specify.
        :param index: Position in the array where you want to insert a value.
         The index must be in the array's range. Inserting at `index` 0 prepends to the array.
         Negative index values start from the end of the array.
        :param values: One or more values to insert in one or more arrays.
        :returns: The length of the array after the insert operation or a list of lengths of
         the arrays after the insert operation if the path matches multiple arrays
        """
        command_arguments: CommandArgList = [key, path, index]
        command_arguments.extend([json.dumps(value) for value in values])

        return self.client.create_request(
            CommandName.JSON_ARRINSERT,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def arrlen(
        self, key: KeyT, path: KeyT | None = None
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Returns the length of the array at path

        :param key: The key to parse.
        :param path: The JSONPath to specify.

        :return: An integer if the matching value is an array, or a list of integers if
         multiple matching values are arrays. Returns ``None`` if the :paramref:`key` or
         :paramref:`path` do not exist.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_ARRLEN,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRPOP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def arrpop(
        self, key: KeyT, path: KeyT | None = None, index: int | None = None
    ) -> CommandRequest[JsonType]:
        """
        Removes and returns the element at the specified index in the array at path

        :param key: Key to modify.
        :param path: JSONPath to specify.
        :param index: Position in the array to start popping from. Out-of-range indexes
         round to their respective array ends.
        :return: The popped value, or ``None`` if the matching JSON value is not an array.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)
        if index is not None:
            command_arguments.append(index)

        return self.client.create_request(
            CommandName.JSON_ARRPOP, *command_arguments, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_ARRTRIM,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def arrtrim(
        self, key: KeyT, path: RedisValueT, start: int, stop: int
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Trims the array at path to contain only the specified inclusive range of indices
        from start to stop

        :param key: Key to modify.
        :param path: The JSONPath to specify.
        :param start: The index of the first element to keep (previous elements are trimmed).
        :param stop: The index of the last element to keep (following elements are trimmed),
         including the last element. Negative values are interpreted as starting from the end.
        :return: The number of elements removed or a list if multiple matching values are arrays.
        """
        command_arguments: CommandArgList = [key, path, start, stop]

        return self.client.create_request(
            CommandName.JSON_ARRTRIM,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_OBJKEYS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def objkeys(self, key: KeyT, path: StringT | None = None) -> CommandRequest[ResponseType]:
        """
        Returns the JSON keys of the object at path

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.

        :return: A list of the key names in the object or a list of lists if multiple objects
         match the :paramref:`path`, or `None` if the matching value is not an object.

        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_OBJKEYS,
            *command_arguments,
            callback=NoopCallback[ResponseType](),
        )

    @module_command(
        CommandName.JSON_OBJLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    def objlen(
        self, key: KeyT, path: KeyT | None = None
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Returns the number of keys of the object at path

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.
        :return: An integer if the path matches exactly one object or a list of integer
         replies for each path specified as the number of keys in the object or ``None``,
         if the matching JSON value is not an object.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_OBJLEN,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_TYPE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cacheable=True,
    )
    def type(
        self, key: KeyT, path: KeyT | None = None
    ) -> CommandRequest[AnyStr | list[AnyStr | None] | None]:
        """
        Returns the type of the JSON value at path

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        """
        command_arguments: CommandArgList = [key]
        if path is not None:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_TYPE,
            *command_arguments,
            callback=OneOrManyCallback[AnyStr](),
        )

    @module_command(
        CommandName.JSON_RESP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        version_deprecated="2.6.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    def resp(self, key: KeyT, path: KeyT | None = None) -> CommandRequest[ResponseType]:
        """
        Returns the JSON value at path in Redis Serialization Protocol (RESP)

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_RESP,
            *command_arguments,
            callback=NoopCallback[ResponseType](),
        )

    @module_command(
        CommandName.JSON_DEBUG_MEMORY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    def debug_memory(
        self, key: KeyT, path: KeyT | None = None
    ) -> CommandRequest[int | list[int | None] | None]:
        """
        Reports the size in bytes of a key
        """
        command_arguments: CommandArgList = [key]
        if path:
            command_arguments.append(path)

        return self.client.create_request(
            CommandName.JSON_DEBUG_MEMORY,
            *command_arguments,
            callback=OneOrManyCallback[int](),
        )
