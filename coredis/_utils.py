from __future__ import annotations

import logging
from collections import UserDict
from typing import Any, Awaitable, overload

from anyio import create_task_group

from coredis.typing import (
    Hashable,
    Iterable,
    Mapping,
    MutableMapping,
    ResponseType,
    StringT,
    TypeVar,
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

T = TypeVar("T")
U = TypeVar("U")


class EncodingInsensitiveDict(UserDict[Any, Any]):
    def __init__(
        self,
        initial: MutableMapping[Any, Any] | None = None,
        encoding: str = "utf-8",
    ):
        self._encoding = encoding
        super().__init__(initial or {})

    def _alt_key(self, key: StringT) -> StringT:
        if isinstance(key, str):
            try:
                byte_alt = key.encode(self._encoding)
                if byte_alt in self.data:
                    return byte_alt
            except UnicodeEncodeError:
                pass
        elif isinstance(key, bytes):
            try:
                str_alt = key.decode(self._encoding)
                if str_alt in self.data:
                    return str_alt
            except UnicodeDecodeError:
                pass
        return key

    def __getitem__(self, key: StringT) -> Any:
        if key in self.data:
            return self.data[key]
        alt = self._alt_key(key)
        if alt in self.data:
            return self.data[alt]
        raise KeyError(key)

    def __setitem__(self, key: StringT, value: Any) -> None:
        alt = self._alt_key(key)
        self.data[alt] = value

    def __delitem__(self, key: StringT) -> None:
        alt = self._alt_key(key)
        del self.data[alt]

    def __contains__(self, key: Any) -> bool:
        return key in self.data or self._alt_key(key) in self.data

    def get(self, key: StringT, default: Any = None) -> Any:
        return self.data.get(key, self.data.get(self._alt_key(key), default))

    def pop(self, key: StringT, default: Any = None) -> Any:
        if key in self.data:
            return self.data.pop(key)
        alt = self._alt_key(key)
        return self.data.pop(alt, default)

    def stringify_keys(self) -> dict[str, Any]:
        d = {}
        for key, value in self.items():
            d[key.decode(self._encoding) if isinstance(key, bytes) else key] = (
                value.stringify_keys() if isinstance(value, EncodingInsensitiveDict) else value
            )
        return d


def b(x: ResponseType, encoding: str | None = None) -> bytes:
    if isinstance(x, bytes):
        return x
    if not isinstance(x, str):
        _v = str(x)
    else:
        _v = x
    return _v.encode(encoding) if encoding else _v.encode()


def nativestr(x: ResponseType, encoding: str = "utf-8") -> str:
    if isinstance(x, (str, bytes)):
        return x if isinstance(x, str) else x.decode(encoding, "replace")
    elif isinstance(x, (int, float, bool)):
        return str(x)
    raise ValueError(f"Unable to cast {x} to string")


def tuples_to_flat_list(nested_list: Iterable[tuple[T, ...]]) -> list[T]:
    return [item for sublist in nested_list for item in sublist]


def dict_to_flat_list(mapping: Mapping[T, U], reverse: bool = False) -> list[T | U]:
    e1: list[T | U] = list(mapping.keys())
    e2: list[T | U] = list(mapping.values())

    ret: list[T | U] = []

    if reverse:
        e1, e2 = e2, e1

    for idx, e in enumerate(e1):
        ret.append(e)
        ret.append(e2[idx])

    return ret


def make_hashable(*args: Any) -> tuple[Hashable, ...]:
    return tuple(
        (
            tuple(make_hashable(v)[0] for v in a)
            if isinstance(a, (tuple, list))
            else (
                frozenset(make_hashable(v)[0] for v in a)
                if isinstance(a, set)
                else (
                    tuple((k, make_hashable(v)[0]) for k, v in a.items())
                    if isinstance(a, dict)
                    # this will fail downstream if `a` is not hashable
                    else a
                )
            )
        )
        for a in args
    )


def query_param_to_bool(value: Any | None) -> bool | None:
    if value is None or value in ("", b""):
        return None
    if isinstance(value, (int, float, bool, str, bytes)):
        value = nativestr(value)
        if value.upper() in ("0", "F", "FALSE", "N", "NO"):
            return False
        elif value.upper() in ("1", "T", "TRUE", "Y", "YES"):
            return True

    return bool(value)


# ++++++++++ cluster utils ++++++++++++++

x_mode_m_crc16_lookup = [
    0x0000,
    0x1021,
    0x2042,
    0x3063,
    0x4084,
    0x50A5,
    0x60C6,
    0x70E7,
    0x8108,
    0x9129,
    0xA14A,
    0xB16B,
    0xC18C,
    0xD1AD,
    0xE1CE,
    0xF1EF,
    0x1231,
    0x0210,
    0x3273,
    0x2252,
    0x52B5,
    0x4294,
    0x72F7,
    0x62D6,
    0x9339,
    0x8318,
    0xB37B,
    0xA35A,
    0xD3BD,
    0xC39C,
    0xF3FF,
    0xE3DE,
    0x2462,
    0x3443,
    0x0420,
    0x1401,
    0x64E6,
    0x74C7,
    0x44A4,
    0x5485,
    0xA56A,
    0xB54B,
    0x8528,
    0x9509,
    0xE5EE,
    0xF5CF,
    0xC5AC,
    0xD58D,
    0x3653,
    0x2672,
    0x1611,
    0x0630,
    0x76D7,
    0x66F6,
    0x5695,
    0x46B4,
    0xB75B,
    0xA77A,
    0x9719,
    0x8738,
    0xF7DF,
    0xE7FE,
    0xD79D,
    0xC7BC,
    0x48C4,
    0x58E5,
    0x6886,
    0x78A7,
    0x0840,
    0x1861,
    0x2802,
    0x3823,
    0xC9CC,
    0xD9ED,
    0xE98E,
    0xF9AF,
    0x8948,
    0x9969,
    0xA90A,
    0xB92B,
    0x5AF5,
    0x4AD4,
    0x7AB7,
    0x6A96,
    0x1A71,
    0x0A50,
    0x3A33,
    0x2A12,
    0xDBFD,
    0xCBDC,
    0xFBBF,
    0xEB9E,
    0x9B79,
    0x8B58,
    0xBB3B,
    0xAB1A,
    0x6CA6,
    0x7C87,
    0x4CE4,
    0x5CC5,
    0x2C22,
    0x3C03,
    0x0C60,
    0x1C41,
    0xEDAE,
    0xFD8F,
    0xCDEC,
    0xDDCD,
    0xAD2A,
    0xBD0B,
    0x8D68,
    0x9D49,
    0x7E97,
    0x6EB6,
    0x5ED5,
    0x4EF4,
    0x3E13,
    0x2E32,
    0x1E51,
    0x0E70,
    0xFF9F,
    0xEFBE,
    0xDFDD,
    0xCFFC,
    0xBF1B,
    0xAF3A,
    0x9F59,
    0x8F78,
    0x9188,
    0x81A9,
    0xB1CA,
    0xA1EB,
    0xD10C,
    0xC12D,
    0xF14E,
    0xE16F,
    0x1080,
    0x00A1,
    0x30C2,
    0x20E3,
    0x5004,
    0x4025,
    0x7046,
    0x6067,
    0x83B9,
    0x9398,
    0xA3FB,
    0xB3DA,
    0xC33D,
    0xD31C,
    0xE37F,
    0xF35E,
    0x02B1,
    0x1290,
    0x22F3,
    0x32D2,
    0x4235,
    0x5214,
    0x6277,
    0x7256,
    0xB5EA,
    0xA5CB,
    0x95A8,
    0x8589,
    0xF56E,
    0xE54F,
    0xD52C,
    0xC50D,
    0x34E2,
    0x24C3,
    0x14A0,
    0x0481,
    0x7466,
    0x6447,
    0x5424,
    0x4405,
    0xA7DB,
    0xB7FA,
    0x8799,
    0x97B8,
    0xE75F,
    0xF77E,
    0xC71D,
    0xD73C,
    0x26D3,
    0x36F2,
    0x0691,
    0x16B0,
    0x6657,
    0x7676,
    0x4615,
    0x5634,
    0xD94C,
    0xC96D,
    0xF90E,
    0xE92F,
    0x99C8,
    0x89E9,
    0xB98A,
    0xA9AB,
    0x5844,
    0x4865,
    0x7806,
    0x6827,
    0x18C0,
    0x08E1,
    0x3882,
    0x28A3,
    0xCB7D,
    0xDB5C,
    0xEB3F,
    0xFB1E,
    0x8BF9,
    0x9BD8,
    0xABBB,
    0xBB9A,
    0x4A75,
    0x5A54,
    0x6A37,
    0x7A16,
    0x0AF1,
    0x1AD0,
    0x2AB3,
    0x3A92,
    0xFD2E,
    0xED0F,
    0xDD6C,
    0xCD4D,
    0xBDAA,
    0xAD8B,
    0x9DE8,
    0x8DC9,
    0x7C26,
    0x6C07,
    0x5C64,
    0x4C45,
    0x3CA2,
    0x2C83,
    0x1CE0,
    0x0CC1,
    0xEF1F,
    0xFF3E,
    0xCF5D,
    0xDF7C,
    0xAF9B,
    0xBFBA,
    0x8FD9,
    0x9FF8,
    0x6E17,
    0x7E36,
    0x4E55,
    0x5E74,
    0x2E93,
    0x3EB2,
    0x0ED1,
    0x1EF0,
]


def crc16(data: bytes) -> int:
    crc = 0

    for byte in data:
        crc = ((crc << 8) & 0xFF00) ^ x_mode_m_crc16_lookup[((crc >> 8) & 0xFF) ^ byte]

    return crc & 0xFFFF


def hash_slot(key: bytes) -> int:
    start = key.find(b"{")

    if start > -1:
        end = key.find(b"}", start + 1)

        if end > -1 and end != start + 1:
            key = key[start + 1 : end]

    return crc16(key) % 16384


__all__ = [
    "hash_slot",
    "EncodingInsensitiveDict",
]

T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    /,
) -> tuple[T1, T2]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    /,
) -> tuple[T1, T2, T3]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    /,
) -> tuple[T1, T2, T3, T4]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    awaitable5: Awaitable[T5],
    /,
) -> tuple[T1, T2, T3, T4, T5]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    awaitable5: Awaitable[T5],
    awaitable6: Awaitable[T6],
    /,
) -> tuple[T1, T2, T3, T4, T5, T6]: ...


@overload
async def gather(*awaitables: Awaitable[T1]) -> tuple[T1, ...]: ...


async def gather(*awaitables: Awaitable[Any]) -> tuple[Any, ...]:
    if len(awaitables) == 1:
        return (await awaitables[0],)
    results: list[Any] = [None] * len(awaitables)

    async def runner(awaitable: Awaitable[Any], i: int) -> None:
        results[i] = await awaitable

    async with create_task_group() as tg:
        for i, awaitable in enumerate(awaitables):
            tg.start_soon(runner, awaitable, i)
    return tuple(results)
