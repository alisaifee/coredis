from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.response.types import LibraryDefinition
from coredis.typing import Any, AnyStr, Dict, Union
from coredis.utils import AnyDict, flat_pairs_to_dict


class FunctionListCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Dict[str, LibraryDefinition]:
        libraries = [AnyDict(flat_pairs_to_dict(library)) for library in response]
        transformed = AnyDict()
        for library in libraries:
            lib_name = library["library_name"]
            functions = AnyDict({})
            for function in library.get("functions", []):
                function_definition = AnyDict(flat_pairs_to_dict(function))
                functions[function_definition["name"]] = function_definition
                functions[function_definition["name"]]["flags"] = set(
                    function_definition["flags"]
                )
            library["functions"] = functions
            transformed[lib_name] = AnyDict(  # type: ignore
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
    ) -> Dict[AnyStr, Union[AnyStr, Dict]]:
        transformed = flat_pairs_to_dict(response)
        key = b"engines" if b"engines" in transformed else "engines"
        engines = flat_pairs_to_dict(transformed.pop(key))
        for engine, stats in engines.items():
            transformed.setdefault(key, {})[engine] = flat_pairs_to_dict(stats)
        return transformed
