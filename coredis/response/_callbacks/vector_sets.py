from __future__ import annotations

from coredis._json import json
from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import VectorData
from coredis.typing import AnyStr, JsonType, ResponsePrimitive, StringT


class VSimCallback(
    ResponseCallback[
        list[AnyStr],
        list[AnyStr] | dict[AnyStr, float | list[float | JsonType]],
        tuple[AnyStr, ...]
        | dict[AnyStr, float]
        | dict[AnyStr, JsonType]
        | dict[AnyStr, tuple[float, JsonType]],
    ],
):
    def transform(
        self,
        response: list[AnyStr],
    ) -> (
        tuple[AnyStr, ...]
        | dict[AnyStr, float]
        | dict[AnyStr, JsonType]
        | dict[AnyStr, tuple[float, JsonType]]
    ):
        withscores, withattribs = self.options.get("withscores"), self.options.get("withattribs")
        if withscores or withattribs:
            it = iter(response)
            match withscores, withattribs:
                case True, None | False:
                    return dict(list(zip(it, map(float, it))))
                case None | False, True:
                    return dict(list(zip(it, map(json.loads, it))))
                case True, True:
                    return dict(
                        list(zip(it, map(lambda x: (float(x[0]), json.loads(x[1])), zip(it, it))))
                    )
        else:
            return self.transform_3(response)

    def transform_3(
        self,
        response: list[AnyStr]
        | dict[AnyStr, float]
        | dict[AnyStr, AnyStr]
        | dict[AnyStr, list[float | AnyStr]],
    ) -> (
        tuple[AnyStr, ...]
        | dict[AnyStr, float]
        | dict[AnyStr, JsonType]
        | dict[AnyStr, tuple[float, JsonType]]
    ):
        withscores, withattribs = self.options.get("withscores"), self.options.get("withattribs")
        if withscores or withattribs:
            assert isinstance(response, dict)
            match withscores, withattribs:
                case None | False, True:
                    return {k: json.loads(v) for k, v in response.items()}
                case True, True:
                    return {k: (v[0], json.loads(v[1])) for k, v in response.items()}
                case _:
                    return response
        else:
            return tuple(response)


class VLinksCallback(
    ResponseCallback[
        list[list[AnyStr]] | None,
        list[list[AnyStr] | dict[AnyStr, float]] | None,
        tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None,
    ],
):
    def transform(
        self,
        response: list[list[AnyStr]] | None,
    ) -> tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None:
        if response:
            if self.options.get("withscores"):
                return tuple(dict(zip(it := iter(layer), map(float, it))) for layer in response)
            else:
                return tuple(tuple(layer) for layer in response)
        return None

    def transform_3(
        self,
        response: list[list[AnyStr] | dict[AnyStr, float]] | None,
    ) -> tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None:
        if response:
            if self.options.get("withscores"):
                assert isinstance(response[0], dict)
                return tuple(response)
            else:
                return tuple(tuple(layer) for layer in response)
        return None


class VEmbCallback(
    ResponseCallback[
        list[StringT] | list[ResponsePrimitive],
        list[float] | list[ResponsePrimitive],
        tuple[float, ...] | VectorData | None,
    ]
):
    def transform(
        self,
        response: list[StringT] | list[ResponsePrimitive] | None,
    ) -> tuple[float, ...] | VectorData | None:
        if response:
            if self.options.get("raw"):
                return VectorData(
                    quantization=nativestr(response[0]),
                    blob=response[1],
                    l2_norm=float(response[2]),
                    quantization_range=float(response[3]) if len(response) == 4 else None,
                )
            else:
                return tuple(map(float, response))
        return None

    def transform_3(
        self,
        response: list[float] | list[ResponsePrimitive] | None,
    ) -> tuple[float, ...] | VectorData | None:
        if response:
            if self.options.get("raw"):
                return VectorData(
                    quantization=nativestr(response[0]),
                    blob=response[1],
                    l2_norm=response[2],
                    quantization_range=response[3] if len(response) == 4 else None,
                )
            else:
                return tuple(response)
        return None


class VInfoCallback(
    ResponseCallback[
        list[AnyStr | int] | None,
        dict[AnyStr, AnyStr | int] | None,
        dict[AnyStr, AnyStr | int] | None,
    ]
):
    def transform(
        self,
        response: list[AnyStr | int] | None,
    ) -> dict[AnyStr, AnyStr | int] | None:
        return flat_pairs_to_dict(response) if response else None

    def transform_3(
        self,
        response: dict[AnyStr, AnyStr | int] | None,
    ) -> dict[AnyStr, AnyStr | int] | None:
        return response
