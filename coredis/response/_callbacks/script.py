from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import LibraryDefinition
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Mapping,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Union,
    ValueT,
)


class FunctionListCallback(
    ResponseCallback[
        List[ResponseType], List[ResponseType], Mapping[str, LibraryDefinition]
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Mapping[str, LibraryDefinition]:
        libraries = [
            EncodingInsensitiveDict(flat_pairs_to_dict(cast(List[ValueT], library)))
            for library in response
        ]
        transformed = EncodingInsensitiveDict()
        for library in libraries:
            lib_name = library["library_name"]
            functions = EncodingInsensitiveDict({})
            for function in library.get("functions", []):
                function_definition = EncodingInsensitiveDict(
                    flat_pairs_to_dict(function)
                )
                functions[function_definition["name"]] = function_definition
                functions[function_definition["name"]]["flags"] = set(
                    function_definition["flags"]
                )
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
        List[ResponseType],
        Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]],
        Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]],
    ]
):
    def transform(
        self,
        response: List[ResponseType],
        **options: Optional[ValueT],
    ) -> Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]]:
        transformed = flat_pairs_to_dict(response)
        key = cast(AnyStr, b"engines" if b"engines" in transformed else "engines")
        engines = flat_pairs_to_dict(cast(List[AnyStr], transformed.pop(key)))
        engines_transformed = {}
        for engine, stats in engines.items():
            engines_transformed[engine] = flat_pairs_to_dict(cast(List[AnyStr], stats))
        transformed[key] = engines_transformed  # type: ignore
        return cast(
            Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]],
            transformed,
        )

    def transform_3(
        self,
        response: Dict[
            AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]
        ],
        **options: Optional[ValueT],
    ) -> Dict[AnyStr, Union[AnyStr, Dict[AnyStr, Dict[AnyStr, ResponsePrimitive]]]]:
        return response
