from __future__ import annotations

from typing import Any, cast

from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import VectorData
from coredis.typing import AnyStr, ResponsePrimitive


class VSimCallback(
    ResponseCallback[
        list[AnyStr | None],
        list[AnyStr | None] | dict[AnyStr, float],
        tuple[AnyStr, ...] | dict[AnyStr, float],
    ],
):
    def transform(
        self,
        response: list[AnyStr | None],
        **options: Any,
    ) -> tuple[AnyStr, ...] | dict[AnyStr, float]:
        if options.get("withscores"):
            it = iter(cast(list[AnyStr], response))
            return dict(list(zip(it, map(float, it))))
        else:
            return tuple(response)

    def transform_3(
        self, response: list[AnyStr] | dict[AnyStr, float], **options: Any
    ) -> tuple[AnyStr, ...] | dict[AnyStr, float]:
        if options.get("withscores"):
            return response
        else:
            return tuple(response)


class VLinksCallback(
    ResponseCallback[
        list[list[ResponsePrimitive]] | None,
        list[list[ResponsePrimitive] | dict[AnyStr, float]] | None,
        tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None,
    ],
):
    def transform(
        self,
        response: list[list[ResponsePrimitive]] | None,
        **options: Any,
    ) -> tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None:
        if response:
            if options.get("withscores"):
                return tuple(
                    dict(zip(it := iter(cast(list[AnyStr], layer)), map(float, it)))
                    for layer in response
                )
            else:
                return tuple(tuple(layer) for layer in response)

    def transform_3(
        self,
        response: list[list[ResponsePrimitive] | dict[AnyStr, float]] | None,
        **options: Any,
    ) -> tuple[tuple[AnyStr, ...] | dict[AnyStr, float], ...] | None:
        if response:
            if options.get("withscores"):
                return tuple(response)
            else:
                return tuple(tuple(layer) for layer in response)


class VEmbCallback(
    ResponseCallback[
        list[AnyStr] | list[ResponsePrimitive],
        list[float] | list[ResponsePrimitive],
        tuple[float, ...] | VectorData,
    ]
):
    def transform(
        self, response: list[AnyStr] | list[ResponsePrimitive] | None, **options: Any
    ) -> tuple[float, ...] | VectorData | None:
        if response:
            if options.get("raw"):
                return VectorData(
                    quantization=nativestr(response[0]),
                    blob=response[1],
                    l2_norm=float(response[2]),
                    quantization_range=float(response[3]) if len(response) == 4 else None,
                )
            else:
                return tuple(map(float, response))

    def transform_3(
        self, response: list[float] | list[ResponsePrimitive] | None, **options: Any
    ) -> tuple[float, ...] | VectorData | None:
        if response:
            if options.get("raw"):
                return VectorData(
                    quantization=nativestr(response[0]),
                    blob=response[1],
                    l2_norm=response[2],
                    quantization_range=response[3] if len(response) == 4 else None,
                )
            else:
                return tuple(response)


class VInfoCallback(
    ResponseCallback[
        list[ResponsePrimitive] | None,
        dict[AnyStr, AnyStr | int] | None,
        dict[AnyStr, AnyStr | int] | None,
    ]
):
    def transform(
        self, response: list[ResponsePrimitive] | None, **options: Any
    ) -> dict[AnyStr, AnyStr | int] | None:
        return flat_pairs_to_dict(response) if response else None

    def transform_3(
        self, response: dict[AnyStr, AnyStr | int] | None, **options: Any
    ) -> dict[AnyStr, AnyStr | int] | None:
        return response
