from __future__ import annotations

from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import VectorData
from coredis.typing import AnyStr, ResponsePrimitive, StringT


class VSimCallback(
    ResponseCallback[
        list[AnyStr],
        list[AnyStr] | dict[AnyStr, float],
        tuple[AnyStr, ...] | dict[AnyStr, float],
    ],
):
    def transform(
        self,
        response: list[AnyStr],
    ) -> tuple[AnyStr, ...] | dict[AnyStr, float]:
        if self.options.get("withscores"):
            it = iter(response)
            return dict(list(zip(it, map(float, it))))
        else:
            return tuple(response)

    def transform_3(
        self,
        response: list[AnyStr] | dict[AnyStr, float],
    ) -> tuple[AnyStr, ...] | dict[AnyStr, float]:
        if self.options.get("withscores"):
            assert isinstance(response, dict)
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
