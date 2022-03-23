from coredis.response.callbacks import SimpleCallback
from coredis.response.types import FunctionDefinition, LibraryDefinition
from coredis.typing import Any, AnyStr, Dict, Union
from coredis.utils import AnyDict, flat_pairs_to_dict, nativestr


class FunctionListCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[str, LibraryDefinition]:
        libraries = [AnyDict(flat_pairs_to_dict(library)) for library in response]
        transformed = {}
        for library in libraries:
            lib_name = library["library_name"]
            functions = {}
            for function in AnyDict(library).get("functions", []):
                function_definition = AnyDict(flat_pairs_to_dict(function))
                functions[function_definition["name"]] = FunctionDefinition(
                    name=function_definition["name"],
                    description=function_definition["description"],
                    flags=set(function_definition["flags"]),
                )
            library["functions"] = functions
            transformed[nativestr(lib_name)] = LibraryDefinition(
                name=library["name"],
                engine=library["engine"],
                description=library["description"],
                functions=library["functions"],
                library_code=library["library_code"],
            )
        return transformed


class FunctionStatsCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[AnyStr, Union[AnyStr, Dict]]:
        transformed = flat_pairs_to_dict(response)
        key = b"engines" if b"engines" in transformed else "engines"
        engines = flat_pairs_to_dict(transformed.pop(key))
        for engine, stats in engines.items():
            transformed.setdefault(key, {})[engine] = flat_pairs_to_dict(stats)
        return transformed
