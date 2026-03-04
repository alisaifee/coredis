from __future__ import annotations

import datetime
from typing import Any, cast

from coredis._utils import nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response.types import ClientInfo, RoleInfo, SlowLogInfo
from coredis.typing import (
    AnyStr,
    ClassVar,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


class TimeCallback(ResponseCallback[list[AnyStr], datetime.datetime]):
    def transform(
        self,
        response: list[AnyStr],
    ) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(response[0])) + datetime.timedelta(
            microseconds=int(response[1]) / 1000.0
        )


class SlowlogCallback(
    ResponseCallback[list[list[int | StringT | list[StringT]]], tuple[SlowLogInfo, ...]]
):
    def transform(
        self,
        response: list[list[int | StringT | list[StringT]]],
    ) -> tuple[SlowLogInfo, ...]:
        return tuple(
            SlowLogInfo(
                id=cast(int, item[0]),
                start_time=cast(int, item[1]),
                duration=cast(int, item[2]),
                command=cast(list[StringT], item[3]),
                client_addr=cast(StringT, item[4]),
                client_name=cast(StringT, item[5]),
            )
            for item in response
        )


class ClientInfoCallback(ResponseCallback[ResponseType, ClientInfo]):
    INT_FIELDS: ClassVar[set[str]] = {
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
        self,
        response: ResponseType,
    ) -> ClientInfo:
        decoded_response = nativestr(response)
        pairs = [pair.split("=", 1) for pair in decoded_response.strip().split(" ")]

        return cast(
            ClientInfo, {k: int(v) if k in ClientInfoCallback.INT_FIELDS else v for k, v in pairs}
        )


class ClientListCallback(ResponseCallback[StringT, tuple[ClientInfo, ...]]):
    def transform(
        self,
        response: StringT,
    ) -> tuple[ClientInfo, ...]:
        return tuple(ClientInfoCallback()(c) for c in response.splitlines())


class DebugCallback(ResponseCallback[ResponseType, dict[str, str | int]]):
    INT_FIELDS: ClassVar[set[str]] = {"refcount", "serializedlength", "lru", "lru_seconds_idle"}

    def transform(
        self,
        response: ResponseType,
    ) -> dict[str, str | int]:
        # The 'type' of the object is the first item in the response, but isn't
        # prefixed with a name

        response = nativestr(response)
        response = "type:" + response
        parsed: dict[str, str | int] = {}
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
        dict[str, ResponseType],
    ]
):
    def transform(
        self,
        response: StringT,
    ) -> dict[str, ResponseType]:
        """Parses the result of Redis's INFO command into a Python dict"""

        info: dict[str, Any] = {}
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
                sub_dict: dict[str, ResponseType] = {}

                for item in value.split(","):
                    k, v = item.rsplit("=", 1)
                    sub_dict[k] = get_value(v)

                return cast(ResponseType, sub_dict)

        cur_info: dict[str, Any] = {}
        header = None
        for line in response.splitlines():
            if line and not line.startswith("#"):
                if line.find(":") != -1:
                    key, value = line.split(":", 1)
                    if key in cur_info:
                        if not isinstance(cur_info[key], list):
                            cur_info[key] = [cur_info[key]]
                        cur_info[key].append(get_value(value))
                    else:
                        cur_info[key] = get_value(value)
                else:
                    # if the line isn't splittable, append it to the "__raw__" key
                    cur_info.setdefault("__raw__", []).append(line)
            elif line:
                if cur_info and header:
                    if self.options.get("nested"):
                        info[header] = cur_info
                    else:
                        info.update(cur_info)
                    cur_info = {}
                header = line.lstrip("#").strip().lower()
        if header and header not in info:
            if self.options.get("nested"):
                info[header] = cur_info
            else:
                info.update(cur_info)
        return info


class RoleCallback(
    ResponseCallback[
        list[ResponsePrimitive | list[ResponsePrimitive] | list[list[ResponsePrimitive]]], RoleInfo
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive | list[ResponsePrimitive] | list[list[ResponsePrimitive]]],
    ) -> RoleInfo:
        role = nativestr(cast(StringT, response[0]))

        def _parse_master(response: Any) -> Any:
            offset, replicas = response[1:]
            res: dict[str, Any] = {"role": role, "offset": offset, "slaves": []}

            for replica in replicas:
                host, port, offset = replica
                res["slaves"].append({"host": host, "port": int(port), "offset": int(offset)})

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
        return RoleInfo(**parser(response))


class LatencyCallback(
    ResponseCallback[list[list[StringT | int]], dict[AnyStr, tuple[int, int, int]]]
):
    def transform(
        self,
        response: list[list[StringT | int]],
    ) -> dict[AnyStr, tuple[int, int, int]]:
        return {cast(AnyStr, k[0]): (int(k[1]), int(k[2]), int(k[3])) for k in response}
