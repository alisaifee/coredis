from __future__ import annotations

from deprecated.sphinx import versionadded

from .._json import json
from ..commands._wrappers import CacheConfig
from ..commands.constants import CommandFlag, CommandGroup, CommandName
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
    KeyT,
    List,
    Literal,
    Optional,
    Parameters,
    ResponseType,
    StringT,
    Tuple,
    Union,
    ValueT,
)
from .base import Module, ModuleGroup, module_command
from .response._callbacks.json import JsonCallback
from .response.types import JsonType


class RedisJSON(Module[AnyStr]):
    NAME = "ReJSON"
    FULL_NAME = "RedisJSON"
    DESCRIPTION = """RedisJSON is a Redis module that implements a JSON data type 
and a set of commands to operate on it."""
    DOCUMENTATION_URL = "https://redis.io/docs/stack/json/"


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
    async def delete(self, key: KeyT, path: Optional[StringT] = None) -> int:
        """
        Delete a value from a JSON document.

        :param key: The key of the JSON document.
        :param path: The JSONPath to specify.
        :return: The number of paths deleted
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_DEL, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_GET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        cache_config=CacheConfig(lambda *a, **_: a[0]),
        flags={CommandFlag.READONLY},
    )
    async def get(
        self,
        key: KeyT,
        *paths: StringT,
    ) -> JsonType:
        """
        Gets the value at one or more paths

        :param key: The key of the JSON document.
        :param paths: JSONPath(s) to get values from.
        :return: The value at :paramref:`path`
        """
        pieces: CommandArgList = [key]
        if paths:
            pieces.extend(paths)

        return await self.execute_module_command(
            CommandName.JSON_GET, *pieces, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_FORGET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def forget(self, key: KeyT, path: Optional[ValueT] = None) -> int:
        """
        Deletes an element from a path from a json object

        :param key: The key of the JSON document.
        :param path: The path(s) to delete from the JSON object.

        :return: The number of deleted elements.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)
        return await self.execute_module_command(
            CommandName.JSON_FORGET, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_TOGGLE,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    async def toggle(self, key: KeyT, path: ValueT) -> JsonType:
        """
        Toggles a boolean value

        :param key: Redis key to modify.
        :param path: JSONPath to specify.
        :return: A list of integer replies for each path, the new value
         (`0` if `false` or `1` if `true`), or ``None`` for JSON values matching
         the path that are not Boolean.
        """
        pieces: CommandArgList = [key, path]
        return await self.execute_module_command(
            CommandName.JSON_TOGGLE,
            *pieces,
            callback=JsonCallback(),
        )

    @module_command(
        CommandName.JSON_CLEAR,
        group=COMMAND_GROUP,
        version_introduced="2.0.0",
        module=MODULE,
    )
    async def clear(self, key: KeyT, path: Optional[ValueT] = None) -> int:
        """
        Clears all values from an array or an object and sets numeric values to `0`

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        :return: The number of values cleared.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_CLEAR, *pieces, callback=IntCallback()
        )

    @module_command(
        CommandName.JSON_SET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def set(
        self,
        key: KeyT,
        path: ValueT,
        value: JsonType,
        condition: Optional[Literal[PureToken.NX, PureToken.XX]] = None,
    ) -> bool:
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
        pieces: CommandArgList = [key, path, json.dumps(value)]
        if condition:
            pieces.append(condition)
        return await self.execute_module_command(
            CommandName.JSON_SET, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_MGET,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    async def mget(self, keys: Parameters[KeyT], path: StringT) -> JsonType:
        """
        Returns the values at a path from one or more keys

        :param keys: one or more keys to retrieve values from.
        :param path: JSONPath to specify.
        :return: The values at :paramref:`path` for each of the keys in :paramref:`keys`.
        """
        pieces: CommandArgList = [*keys, path]
        return await self.execute_module_command(
            CommandName.JSON_MGET,
            *pieces,
            callback=JsonCallback(),
            keys=keys,  # type: ignore
        )

    @module_command(
        CommandName.JSON_MSET,
        group=COMMAND_GROUP,
        version_introduced="2.6.0",
        module=MODULE,
    )
    async def mset(self, triplets: Parameters[Tuple[KeyT, StringT, JsonType]]) -> bool:
        """
        Sets or updates the JSON value of one or more keys

        :param triplets: Collection of triplets containing (``key``, ``path``, ``value``)
         to set.

        :return: `True` if all the values were set successfully
        """
        pieces: CommandArgList = []
        for key, path, value in triplets:
            pieces.extend([key, path, json.dumps(value)])

        return await self.execute_module_command(
            CommandName.JSON_MSET, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_MERGE,
        group=COMMAND_GROUP,
        version_introduced="2.6.0",
        module=MODULE,
    )
    async def merge(self, key: KeyT, path: StringT, value: JsonType) -> bool:
        """
        Merge a JSON object into an existing Redis key at a specified path.

        :param key: The Redis key to merge the JSON object into.
        :param path: The JSONPath within the Redis key to merge the JSON object into.
        :param value: The JSON object to merge into the Redis key.
        :return: True if the merge was successful, False otherwise.
        """
        pieces: CommandArgList = [key, path, json.dumps(value)]

        return await self.execute_module_command(
            CommandName.JSON_MERGE, *pieces, callback=SimpleStringCallback()
        )

    @module_command(
        CommandName.JSON_NUMINCRBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def numincrby(
        self, key: KeyT, path: ValueT, value: Union[int, float]
    ) -> JsonType:
        """
        Increments the numeric value at path by a value

        :param key: The key to modify.
        :param path: The JSONPath to specify.
        :param value: The number value to increment.
        """
        pieces: CommandArgList = [key, path, value]

        return await self.execute_module_command(
            CommandName.JSON_NUMINCRBY, *pieces, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_NUMMULTBY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def nummultby(
        self, key: KeyT, path: ValueT, value: Union[int, float]
    ) -> JsonType:
        """
        Multiplies the numeric value at path by a value

        :param key: Key to modify.
        :param path: JSONPath to specify.
        :param value: Number value to multiply.
        """
        pieces: CommandArgList = [key, path, value]

        return await self.execute_module_command(
            CommandName.JSON_NUMMULTBY, *pieces, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_STRAPPEND,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def strappend(
        self,
        key: KeyT,
        value: Optional[Union[str, bytes, int, float]],
        path: Optional[KeyT] = None,
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Appends a string to a JSON string value at path

        :param key: The key to modify
        :param value: The value to append to the string(s) at the specified :paramref:`path`.
        :param path: The JSONPath to specify the location of the string(s) to modify.
        :return: A list of integer replies for each path, the string's new length,
         or ``None`` if the matching JSON value is not a string.
        """
        pieces: CommandArgList = [key]
        if path is not None:
            pieces.append(path)
        pieces.append(json.dumps(value))
        return await self.execute_module_command(
            CommandName.JSON_STRAPPEND, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_STRLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def strlen(
        self, key: KeyT, path: Optional[KeyT] = None
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Returns the length of the JSON String at path in key

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.
        :return: An array of integer replies for each path, the array's length,
         or ``None``, if the matching JSON value is not a string.


        """
        pieces: CommandArgList = [key]
        if path is not None:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_STRLEN, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_ARRAPPEND,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def arrappend(
        self,
        key: KeyT,
        values: Parameters[JsonType],
        path: Optional[KeyT] = None,
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Append one or more json values into the array at path after the last element in it.

        :param key: The key to modify.
        :param values: One or more values to append to one or more arrays.
        :param path: JSONPath to specify.
        :return: An array of integer replies for each path, the array's new size,
         or `None` if the matching JSON value is not an array.

        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)
        pieces.extend([json.dumps(value) for value in values])
        return await self.execute_module_command(
            CommandName.JSON_ARRAPPEND,
            *pieces,
            callback=OneOrManyCallback[int](),
        )

    @module_command(
        CommandName.JSON_ARRINDEX,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def arrindex(
        self,
        key: KeyT,
        path: ValueT,
        value: Union[str, bytes, int, float],
        start: Optional[int] = None,
        stop: Optional[int] = None,
    ) -> Optional[Union[int, List[Optional[int]]]]:
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
        pieces: CommandArgList = [key, path, json.dumps(value)]
        if start is not None:
            pieces.append(start)
        if stop is not None:
            pieces.append(stop)

        return await self.execute_module_command(
            CommandName.JSON_ARRINDEX, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_ARRINSERT,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def arrinsert(
        self,
        key: KeyT,
        path: ValueT,
        index: int,
        values: Parameters[JsonType],
    ) -> Optional[Union[int, List[Optional[int]]]]:
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
        pieces: CommandArgList = [key, path, index]
        pieces.extend([json.dumps(value) for value in values])

        return await self.execute_module_command(
            CommandName.JSON_ARRINSERT, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_ARRLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def arrlen(
        self, key: KeyT, path: Optional[KeyT] = None
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Returns the length of the array at path

        :param key: The key to parse.
        :param path: The JSONPath to specify.

        :return: An integer if the matching value is an array, or a list of integers if
         multiple matching values are arrays. Returns ``None`` if the :paramref:`key` or
         :paramref:`path` do not exist.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_ARRLEN, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_ARRPOP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def arrpop(
        self, key: KeyT, path: Optional[KeyT] = None, index: Optional[int] = None
    ) -> JsonType:
        """
        Removes and returns the element at the specified index in the array at path

        :param key: Key to modify.
        :param path: JSONPath to specify.
        :param index: Position in the array to start popping from. Out-of-range indexes
         round to their respective array ends.
        :return: The popped value, or ``None`` if the matching JSON value is not an array.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)
        if index is not None:
            pieces.append(index)

        return await self.execute_module_command(
            CommandName.JSON_ARRPOP, *pieces, callback=JsonCallback()
        )

    @module_command(
        CommandName.JSON_ARRTRIM,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def arrtrim(
        self, key: KeyT, path: ValueT, start: int, stop: int
    ) -> Optional[Union[int, List[Optional[int]]]]:
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
        pieces: CommandArgList = [key, path, start, stop]

        return await self.execute_module_command(
            CommandName.JSON_ARRTRIM, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_OBJKEYS,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def objkeys(self, key: KeyT, path: Optional[StringT] = None) -> ResponseType:
        """
        Returns the JSON keys of the object at path

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.

        :return: A list of the key names in the object or a list of lists if multiple objects
         match the :paramref:`path`, or `None` if the matching value is not an object.

        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_OBJKEYS, *pieces, callback=NoopCallback[ResponseType]()
        )

    @module_command(
        CommandName.JSON_OBJLEN,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
    )
    async def objlen(
        self, key: KeyT, path: Optional[KeyT] = None
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Returns the number of keys of the object at path

        :param key: The key of the JSON document.
        :param path: JSONPath to specify.
        :return: An integer if the path matches exactly one object or a list of integer
         replies for each path specified as the number of keys in the object or ``None``,
         if the matching JSON value is not an object.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_OBJLEN, *pieces, callback=OneOrManyCallback[int]()
        )

    @module_command(
        CommandName.JSON_TYPE,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
        cache_config=CacheConfig(lambda *a, **_: a[0]),
    )
    async def type(
        self, key: KeyT, path: Optional[KeyT] = None
    ) -> Optional[Union[AnyStr, List[Optional[AnyStr]]]]:
        """
        Returns the type of the JSON value at path

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        """
        pieces: CommandArgList = [key]
        if path is not None:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_TYPE, *pieces, callback=OneOrManyCallback[AnyStr]()
        )

    @module_command(
        CommandName.JSON_RESP,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        version_deprecated="2.6.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    async def resp(self, key: KeyT, path: Optional[KeyT] = None) -> ResponseType:
        """
        Returns the JSON value at path in Redis Serialization Protocol (RESP)

        :param key: The key to parse.
        :param path: The JSONPath to specify.
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_RESP, *pieces, callback=NoopCallback[ResponseType]()
        )

    @module_command(
        CommandName.JSON_DEBUG_MEMORY,
        group=COMMAND_GROUP,
        version_introduced="1.0.0",
        module=MODULE,
        flags={CommandFlag.READONLY},
    )
    async def debug_memory(
        self, key: KeyT, path: Optional[KeyT] = None
    ) -> Optional[Union[int, List[Optional[int]]]]:
        """
        Reports the size in bytes of a key
        """
        pieces: CommandArgList = [key]
        if path:
            pieces.append(path)

        return await self.execute_module_command(
            CommandName.JSON_DEBUG_MEMORY, *pieces, callback=OneOrManyCallback[int]()
        )
