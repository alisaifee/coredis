from __future__ import annotations

import datetime
from typing import Any

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.response.types import ClientInfo, RoleInfo, SlowLogInfo
from coredis.typing import (
    AnyStr,
    ClassVar,
    Dict,
    Optional,
    ResponsePrimitive,
    ResponseType,
    StringT,
    Tuple,
    Union,
    ValueT,
)


class TimeCallback(ResponseCallback[ResponseType, ResponseType, datetime.datetime]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> datetime.datetime:

        return datetime.datetime.fromtimestamp(int(response[0])) + datetime.timedelta(
            microseconds=int(response[1]) / 1000.0
        )


class SlowlogCallback(
    ResponseCallback[ResponseType, ResponseType, Tuple[SlowLogInfo, ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[SlowLogInfo, ...]:

        return tuple(
            SlowLogInfo(
                id=item[0],
                start_time=int(item[1]),
                duration=int(item[2]),
                command=item[3],
                client_addr=item[4],
                client_name=item[5],
            )
            for item in response
        )


class ClientInfoCallback(ResponseCallback[ResponseType, ResponseType, ClientInfo]):
    INT_FIELDS: ClassVar = {
        "id",
        "fd",
        "age",
        "idle",
        "db",
        "sub",
        "psub",
        "multi",
        "qbuf-free",
        "argv-mem",
        "multi-mem",
        "obl",
        "oll",
        "omem",
        "tot-mem",
        "redir",
    }

    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> ClientInfo:

        decoded_response = nativestr(response)
        pairs = [pair.split("=", 1) for pair in decoded_response.strip().split(" ")]

        info: ClientInfo = {}  # type: ignore
        for k, v in pairs:
            if k in ClientInfoCallback.INT_FIELDS:
                info[k] = int(v)  # type: ignore
            else:
                info[k] = v  # type: ignore
        return info


class ClientListCallback(
    ResponseCallback[ResponseType, ResponseType, Tuple[ClientInfo, ...]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[ClientInfo, ...]:

        return tuple(ClientInfoCallback()(c) for c in response.splitlines())


class DebugCallback(
    ResponseCallback[ResponseType, ResponseType, Dict[str, Union[str, int]]]
):
    INT_FIELDS: ClassVar = {"refcount", "serializedlength", "lru", "lru_seconds_idle"}

    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[str, Union[str, int]]:
        # The 'type' of the object is the first item in the response, but isn't
        # prefixed with a name

        response = nativestr(response)
        response = "type:" + response
        parsed: Dict[str, Union[str, int]] = {}
        parsed.update(dict([kv.split(":") for kv in response.split()]))

        # parse some expected int values from the string response
        # note: this cmd isn't spec'd so these may not appear in all redis versions

        for field in DebugCallback.INT_FIELDS:
            if field in parsed:
                parsed[field] = int(parsed[field])

        return parsed


class InfoCallback(
    ResponseCallback[
        StringT,
        StringT,
        Dict[str, ResponseType],
    ]
):
    def transform(
        self, response: StringT, **options: Optional[ValueT]
    ) -> Dict[str, ResponseType]:
        """Parses the result of Redis's INFO command into a Python dict"""

        info: Dict[str, Any] = {}
        response = nativestr(response)

        def get_value(value: str) -> ResponseType:
            if "," not in value or "=" not in value:
                try:
                    if "." in value:
                        return float(value)
                    else:
                        return int(value)
                except ValueError:
                    return value
            else:
                sub_dict: Dict[ResponsePrimitive, ResponseType] = {}

                for item in value.split(","):
                    k, v = item.rsplit("=", 1)
                    sub_dict[k] = get_value(v)

                return sub_dict

        for line in response.splitlines():
            if line and not line.startswith("#"):
                if line.find(":") != -1:
                    key, value = line.split(":", 1)
                    info[key] = get_value(value)
                else:
                    # if the line isn't splittable, append it to the "__raw__" key
                    info.setdefault("__raw__", []).append(line)

        return info


class RoleCallback(ResponseCallback[ResponseType, ResponseType, RoleInfo]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> RoleInfo:

        role = nativestr(response[0])

        def _parse_master(response: Any) -> Any:
            offset, replicas = response[1:]
            res: Dict[str, Any] = {"role": role, "offset": offset, "slaves": []}

            for replica in replicas:
                host, port, offset = replica
                res["slaves"].append(
                    {"host": host, "port": int(port), "offset": int(offset)}
                )

            return res

        def _parse_replica(response: Any) -> Any:
            host, port, status, offset = response[1:]

            return dict(
                role=role,
                status=status,
                offset=offset,
            )

        def _parse_sentinel(response: Any) -> Any:
            return {"role": role, "masters": response[1]}

        parser = {
            "master": _parse_master,
            "slave": _parse_replica,
            "sentinel": _parse_sentinel,
        }[role]
        return RoleInfo(**parser(response))  # type: ignore


class LatencyHistogramCallback(
    ResponseCallback[ResponseType, ResponseType, Dict[AnyStr, Dict[AnyStr, ValueT]]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, Dict[AnyStr, ValueT]]:

        histogram = flat_pairs_to_dict(response)
        for key, value in histogram.items():
            histogram[key] = EncodingInsensitiveDict(flat_pairs_to_dict(value))
            histogram[key]["histogram_usec"] = flat_pairs_to_dict(
                histogram[key]["histogram_usec"]
            )
        return histogram


class LatencyCallback(
    ResponseCallback[ResponseType, ResponseType, Dict[AnyStr, Tuple[int, int, int]]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, Tuple[int, int, int]]:

        return {k[0]: (k[1], k[2], k[3]) for k in response}
