from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import LibraryDefinition
from coredis.typing import (
    AnyStr,
    Mapping,
    RedisValueT,
    ResponsePrimitive,
    ResponseType,
)


class FunctionListCallback(
    ResponseCallback[list[ResponseType], Mapping[AnyStr, LibraryDefinition]]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> Mapping[AnyStr, LibraryDefinition]:
        libraries = [
            EncodingInsensitiveDict(flat_pairs_to_dict(cast(list[RedisValueT], library)))
            for library in response
        ]
        transformed = EncodingInsensitiveDict()
        for library in libraries:
            lib_name = library["library_name"]
            functions = EncodingInsensitiveDict({})
            for function in library.get("functions", []):
                function_definition = EncodingInsensitiveDict(flat_pairs_to_dict(function))
                functions[function_definition["name"]] = function_definition
                functions[function_definition["name"]]["flags"] = set(function_definition["flags"])
            library["functions"] = functions
            transformed[lib_name] = EncodingInsensitiveDict(
                LibraryDefinition(
                    name=library["library_name"],
                    engine=library["engine"],
                    functions=library["functions"],
                    library_code=library.get("library_code", None),
                )
            )
        return transformed


class FunctionStatsCallback(
    ResponseCallback[
        dict[
            AnyStr,
            AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None,
        ],
        dict[
            AnyStr,
            AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None,
        ],
    ]
):
    def transform(
        self,
        response: dict[
            AnyStr,
            AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None,
        ],
    ) -> dict[AnyStr, AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None]:
        return response
