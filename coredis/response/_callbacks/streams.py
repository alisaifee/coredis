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
    Dict,
    Optional,
    OrderedDict,
    ResponseType,
    StringT,
    Tuple,
    Union,
    ValueT,
)


class StreamRangeCallback(
    ResponseCallback[ResponseType, ResponseType, Tuple[StreamEntry, ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[StreamEntry, ...]:

        return tuple(
            StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in response
        )


class ClaimCallback(
    ResponseCallback[
        ResponseType, ResponseType, Union[Tuple[AnyStr, ...], Tuple[StreamEntry, ...]]
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Union[Tuple[AnyStr, ...], Tuple[StreamEntry, ...]]:

        if options.get("justid") is not None:
            return tuple(response)
        else:
            return StreamRangeCallback()(response)


class AutoClaimCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        Union[
            Tuple[AnyStr, Tuple[AnyStr, ...]],
            Tuple[AnyStr, Tuple[StreamEntry, ...], Tuple[AnyStr, ...]],
        ],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Union[
        Tuple[AnyStr, Tuple[AnyStr, ...]],
        Tuple[AnyStr, Tuple[StreamEntry, ...], Tuple[AnyStr, ...]],
    ]:

        if options.get("justid") is not None:
            return response[0], tuple(response[1])
        else:
            return (
                response[0],
                StreamRangeCallback()(response[1]),
                tuple(response[2]) if len(response) > 2 else (),
            )


class MultiStreamRangeCallback(
    ResponseCallback[
        ResponseType, ResponseType, Optional[Dict[AnyStr, Tuple[StreamEntry, ...]]]
    ]
):
    def transform_3(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Optional[Dict[AnyStr, Tuple[StreamEntry, ...]]]:

        if response:
            mapping: Dict[AnyStr, Tuple[StreamEntry, ...]] = {}

            for stream_id, entries in response.items():
                mapping[stream_id] = tuple(
                    StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in entries
                )

            return mapping
        return None

    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Optional[Dict[AnyStr, Tuple[StreamEntry, ...]]]:
        if response:

            mapping: Dict[AnyStr, Tuple[StreamEntry, ...]] = {}

            for stream_id, entries in response:
                mapping[stream_id] = tuple(
                    StreamEntry(r[0], flat_pairs_to_ordered_dict(r[1])) for r in entries
                )

            return mapping
        return None


class PendingCallback(
    ResponseCallback[
        ResponseType, ResponseType, Union[StreamPending, Tuple[StreamPendingExt, ...]]
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Union[StreamPending, Tuple[StreamPendingExt, ...]]:

        if not options.get("count"):
            return StreamPending(
                response[0],
                response[1],
                response[2],
                OrderedDict((r[0], int(r[1])) for r in response[3] or []),
            )
        else:
            return tuple(
                StreamPendingExt(sub[0], sub[1], sub[2], sub[3]) for sub in response
            )


class XInfoCallback(
    ResponseCallback[ResponseType, ResponseType, Tuple[Dict[AnyStr, AnyStr], ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[Dict[AnyStr, AnyStr], ...]:

        return tuple(flat_pairs_to_dict(row) for row in response)


class StreamInfoCallback(ResponseCallback[ResponseType, ResponseType, StreamInfo]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> StreamInfo:

        res: Dict[StringT, Any] = EncodingInsensitiveDict(flat_pairs_to_dict(response))
        if not options.get("full"):

            k1 = "first-entry"
            kn = "last-entry"
            e1: Optional[StreamEntry] = None
            en: Optional[StreamEntry] = None

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
                res.update({"groups": flat_pairs_to_dict(groups)})
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
