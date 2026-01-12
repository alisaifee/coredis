from __future__ import annotations

from typing import Any

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


class StreamRangeCallback(ResponseCallback[ResponseType, tuple[StreamEntry, ...]]):
    def transform(
        self,
        response: ResponseType,
    ) -> tuple[StreamEntry, ...]:
        return tuple(StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in response)


class ClaimCallback(ResponseCallback[ResponseType, tuple[AnyStr, ...] | tuple[StreamEntry, ...]]):
    def transform(
        self,
        response: ResponseType,
    ) -> tuple[AnyStr, ...] | tuple[StreamEntry, ...]:
        if self.options.get("justid") is not None:
            return tuple(response)
        else:
            return StreamRangeCallback()(response)


class AutoClaimCallback(
    ResponseCallback[
        ResponseType,
        tuple[AnyStr, tuple[AnyStr, ...]]
        | tuple[AnyStr, tuple[StreamEntry, ...], tuple[AnyStr, ...]],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> (
        tuple[AnyStr, tuple[AnyStr, ...]]
        | tuple[AnyStr, tuple[StreamEntry, ...], tuple[AnyStr, ...]]
    ):
        if self.options.get("justid") is not None:
            return response[0], tuple(response[1])
        else:
            return (
                response[0],
                StreamRangeCallback()(response[1]),
                tuple(response[2]) if len(response) > 2 else (),
            )


class MultiStreamRangeCallback(
    ResponseCallback[ResponseType, dict[AnyStr, tuple[StreamEntry, ...]] | None]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[AnyStr, tuple[StreamEntry, ...]] | None:
        if response:
            mapping: dict[AnyStr, tuple[StreamEntry, ...]] = {}

            for stream_id, entries in response.items():
                mapping[stream_id] = tuple(
                    StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in entries
                )

            return mapping
        return None


class PendingCallback(ResponseCallback[ResponseType, StreamPending | tuple[StreamPendingExt, ...]]):
    def transform(
        self,
        response: ResponseType,
    ) -> StreamPending | tuple[StreamPendingExt, ...]:
        if not self.options.get("count"):
            return StreamPending(
                response[0],
                response[1],
                response[2],
                OrderedDict((r[0], int(r[1])) for r in response[3] or []),
            )
        else:
            return tuple(StreamPendingExt(sub[0], sub[1], sub[2], sub[3]) for sub in response)


class XInfoCallback(ResponseCallback[ResponseType, tuple[dict[AnyStr, AnyStr], ...]]):
    def transform(
        self,
        response: ResponseType,
    ) -> tuple[dict[AnyStr, AnyStr], ...]:
        return tuple(flat_pairs_to_dict(row) for row in response)


class StreamInfoCallback(ResponseCallback[ResponseType, StreamInfo]):
    def transform(
        self,
        response: ResponseType,
    ) -> StreamInfo:
        res: dict[StringT, Any] = EncodingInsensitiveDict(flat_pairs_to_dict(response))
        if not self.options.get("full"):
            k1 = "first-entry"
            kn = "last-entry"
            e1: StreamEntry | None = None
            en: StreamEntry | None = None

            if len(res.get(k1, [])) > 0:
                v = res.get(k1)
                e1 = StreamEntry(v[0], flat_pairs_to_ordered_dict(v[1]))
                res.pop(k1)

            if len(res.get(kn, [])) > 0:
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
