from frozendict import frozendict

from coredis.commands import ParametrizedCallback, SimpleCallback
from coredis.typing import Any, AnyStr, Dict, Tuple, Union
from coredis.utils import flat_pairs_to_dict


class HScanCallback(SimpleCallback):
    def transform(self, response: Any) -> Tuple[int, Dict[AnyStr, AnyStr]]:
        cursor, r = response

        return int(cursor), r and flat_pairs_to_dict(r) or {}


class HRandFieldCallback(ParametrizedCallback):
    def transform_3(
        self, response: Any, **options: Any
    ) -> Union[str, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]:
        if options.get("count"):
            if options.get("withvalues"):
                return dict(response)
            return tuple(response)
        return response

    def transform(
        self, response: Any, **options: Any
    ) -> Union[str, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]:
        if options.get("count"):
            if options.get("withvalues"):
                return flat_pairs_to_dict(response)
            else:
                return tuple(response)
        else:
            return response


class HGetAllCallback(SimpleCallback):
    def transform_3(self, response: Any) -> Dict[AnyStr, AnyStr]:
        if isinstance(response, frozendict):
            return dict(response)
        return response

    def transform(self, response: Any) -> Dict[AnyStr, AnyStr]:
        return flat_pairs_to_dict(response) if response else {}
