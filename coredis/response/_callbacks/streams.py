from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict, flat_pairs_to_ordered_dict
from coredis.response.types import (
    StreamEntry,
    StreamInfo,
    StreamPending,
    StreamPendingExt,
)
from coredis.typing import (
    AnyStr,
    OrderedDict,
    ResponseType,
    StringT,
)


class StreamRangeCallback(
    ResponseCallback[list[list[StringT | list[StringT]]], tuple[StreamEntry, ...]]
):
    def transform(
        self,
        response: list[list[StringT | list[StringT]]],
    ) -> tuple[StreamEntry, ...]:
        return tuple(
            StreamEntry(cast(StringT, r[0]), flat_pairs_to_ordered_dict(cast(list[StringT], r[1])))
            for r in response
        )


class ClaimCallback(
    ResponseCallback[
        list[StringT | list[StringT | list[StringT]]], tuple[AnyStr, ...] | tuple[StreamEntry, ...]
    ]
):
    def transform(
        self,
        response: list[StringT | list[StringT | list[StringT]]],
    ) -> tuple[AnyStr, ...] | tuple[StreamEntry, ...]:
        if self.options.get("justid") is not None:
            return tuple(cast(list[AnyStr], response))
        else:
            return StreamRangeCallback()(cast(list[list[StringT | list[StringT]]], response))


class AutoClaimCallback(
    ResponseCallback[
        list[StringT | list[StringT | list[StringT | list[StringT]]]],
        tuple[AnyStr, tuple[AnyStr, ...]]
        | tuple[AnyStr, tuple[StreamEntry, ...], tuple[AnyStr, ...]],
    ]
):
    def transform(
        self, response: list[StringT | list[StringT | list[StringT | list[StringT]]]]
    ) -> (
        tuple[AnyStr, tuple[AnyStr, ...]]
        | tuple[AnyStr, tuple[StreamEntry, ...], tuple[AnyStr, ...]]
    ):
        if self.options.get("justid") is not None:
            return cast(AnyStr, response[0]), tuple(cast(list[AnyStr], response[1]))
        else:
            return (
                cast(AnyStr, response[0]),
                StreamRangeCallback()(cast(list[list[StringT | list[StringT]]], response[1])),
                tuple(cast(list[AnyStr], response[2])) if len(response) > 2 else (),
            )


class MultiStreamRangeCallback(
    ResponseCallback[
        dict[StringT, list[list[list[StringT] | StringT]]] | None,
        dict[AnyStr, tuple[StreamEntry, ...]] | None,
    ]
):
    def transform(
        self,
        response: dict[StringT, list[list[list[StringT] | StringT]]] | None,
    ) -> dict[AnyStr, tuple[StreamEntry, ...]] | None:
        if response:
            mapping: dict[AnyStr, tuple[StreamEntry, ...]] = {}

            for stream_id, entries in response.items():
                mapping[cast(AnyStr, stream_id)] = tuple(
                    StreamEntry(
                        cast(StringT, r[0]), flat_pairs_to_ordered_dict(cast(list[StringT], r[1]))
                    )
                    for r in entries
                )

            return mapping
        return None


class PendingCallback(
    ResponseCallback[
        list[StringT | int | list[StringT | int | list[StringT]] | None],
        StreamPending | tuple[StreamPendingExt, ...],
    ]
):
    def transform(
        self,
        response: list[StringT | int | list[StringT | int | list[StringT]] | None],
    ) -> StreamPending | tuple[StreamPendingExt, ...]:
        if not self.options.get("count"):
            consumers = [(r[0], int(r[1])) for r in cast(list[list[StringT]], response[3] or [])]
            return StreamPending(
                cast(int, response[0]),
                cast(StringT | None, response[1]),
                cast(StringT | None, response[2]),
                OrderedDict(consumers),
            )
        else:
            return tuple(
                StreamPendingExt(*(cast(tuple[StringT, StringT, int, int], tuple(sub[:4]))))
                for sub in cast(list[list[StringT | int]], response)
            )


class StreamInfoCallback(ResponseCallback[dict[StringT, ResponseType], StreamInfo]):
    def transform(
        self,
        response: dict[StringT, ResponseType],
    ) -> StreamInfo:
        res = EncodingInsensitiveDict(response)
        if not self.options.get("full"):
            k1 = "first-entry"
            kn = "last-entry"
            e1: StreamEntry | None = None
            en: StreamEntry | None = None

            if len(res.get(k1, []) or []) > 0:
                v = res.get(k1)
                e1 = StreamEntry(v[0], flat_pairs_to_ordered_dict(v[1]))
                res.pop(k1)

            if len(res.get(kn, []) or []) > 0:
                v = res.get(kn)
                en = StreamEntry(v[0], flat_pairs_to_ordered_dict(v[1]))
                res.pop(kn)
            res.update({"first-entry": e1, "last-entry": en})
        else:
            groups = res.get("groups")
            if groups:
                normalized_groups = []
                for group in groups:
                    g = EncodingInsensitiveDict(flat_pairs_to_dict(group))
                    consumers = g["consumers"]
                    normalized_consumers = []
                    for consumer in consumers:
                        normalized_consumers.append(flat_pairs_to_dict(consumer))
                    g["consumers"] = normalized_consumers
                    normalized_groups.append(g)
                res["groups"] = normalized_groups
            res.update(
                {
                    "entries": tuple(
                        StreamEntry(k[0], flat_pairs_to_ordered_dict(k[1]))
                        for k in res.get("entries", [])
                    )
                }
            )
        stream_info: StreamInfo = {
            "first-entry": res.get("first-entry"),
            "last-entry": res.get("last-entry"),
            "length": res["length"],
            "radix-tree-keys": res["radix-tree-keys"],
            "radix-tree-nodes": res["radix-tree-nodes"],
            "groups": res["groups"],
            "last-generated-id": res["last-generated-id"],
            "max-deleted-entry-id": str(res.get("max-deleted-entry-id")),
            "entries-added": int(res.get("entries-added", 0)),
            "recorded-first-entry-id": str(res.get("recorded-first-entry-id")),
            "entries-read": int(res.get("entries-read", 0)),
            "entries": res.get("entries"),
        }
        return stream_info
