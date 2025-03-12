from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import LibraryDefinition
from coredis.typing import (
    AnyStr,
    Mapping,
    ResponsePrimitive,
    ResponseType,
    ValueT,
)


class FunctionListCallback(
    ResponseCallback[list[ResponseType], list[ResponseType], Mapping[AnyStr, LibraryDefinition]]
):
    def transform(
        self, response: list[ResponseType], **options: ValueT | None
    ) -> Mapping[AnyStr, LibraryDefinition]:
        libraries = [
            EncodingInsensitiveDict(flat_pairs_to_dict(cast(list[ValueT], library)))
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
                    name=library["name"],
                    engine=library["engine"],
                    description=library["description"],
                    functions=library["functions"],
                    library_code=library["library_code"],
                )
            )
        return transformed


class FunctionStatsCallback(
    ResponseCallback[
        list[ResponseType],
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
        response: list[ResponseType],
        **options: ValueT | None,
    ) -> dict[AnyStr, AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None]:
        transformed = flat_pairs_to_dict(response)
        key = cast(AnyStr, b"engines" if b"engines" in transformed else "engines")
        engines = flat_pairs_to_dict(cast(list[AnyStr], transformed.pop(key)))
        engines_transformed = {}
        for engine, stats in engines.items():
            engines_transformed[engine] = flat_pairs_to_dict(cast(list[AnyStr], stats))
        transformed[key] = engines_transformed  # type: ignore
        return cast(
            dict[AnyStr, AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]]],
            transformed,
        )

    def transform_3(
        self,
        response: dict[
            AnyStr,
            AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None,
        ],
        **options: ValueT | None,
    ) -> dict[AnyStr, AnyStr | dict[AnyStr, dict[AnyStr, ResponsePrimitive]] | None]:
        return response
