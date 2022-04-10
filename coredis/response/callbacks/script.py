from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.response.types import LibraryDefinition
from coredis.response.utils import flat_pairs_to_dict
from coredis.typing import Any, AnyStr, Mapping, Union
from coredis.utils import EncodingInsensitiveDict


class FunctionListCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Mapping[str, LibraryDefinition]:
        libraries = [
            EncodingInsensitiveDict(flat_pairs_to_dict(library)) for library in response
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
            transformed[lib_name] = EncodingInsensitiveDict(  # type: ignore
                LibraryDefinition(
                    name=library["name"],
                    engine=library["engine"],
                    description=library["description"],
                    functions=library["functions"],
                    library_code=library["library_code"],
                )
            )
        return transformed


class FunctionStatsCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Mapping[AnyStr, Union[AnyStr, Mapping]]:
        transformed = flat_pairs_to_dict(response)
        key = b"engines" if b"engines" in transformed else "engines"
        engines = flat_pairs_to_dict(transformed.pop(key))
        for engine, stats in engines.items():
            transformed.setdefault(key, {})[engine] = flat_pairs_to_dict(stats)
        return transformed
