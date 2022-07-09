from __future__ import annotations

from coredis._utils import b
from coredis.typing import Callable, ClassVar, Dict, Tuple, ValueT


class KeySpec:
    READONLY: ClassVar[
        Dict[bytes, Callable[[Tuple[ValueT, ...]], Tuple[ValueT, ...]]]
    ] = {
        b"BITCOUNT": lambda args: ((args[1],)),
        b"BITFIELD_RO": lambda args: ((args[1],)),
        b"BITOP": lambda args: (args[3 : (len(args))]),
        b"BITPOS": lambda args: ((args[1],)),
        b"COPY": lambda args: ((args[1],)),
        b"DUMP": lambda args: ((args[1],)),
        b"EVALSHA_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"EVAL_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"EXISTS": lambda args: (args[1 : (len(args))]),
        b"EXPIRETIME": lambda args: ((args[1],)),
        b"FCALL_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"GEODIST": lambda args: ((args[1],)),
        b"GEOHASH": lambda args: ((args[1],)),
        b"GEOPOS": lambda args: ((args[1],)),
        b"GEORADIUS": lambda args: ((args[1],)),
        b"GEORADIUSBYMEMBER": lambda args: ((args[1],)),
        b"GEORADIUSBYMEMBER_RO": lambda args: ((args[1],)),
        b"GEORADIUS_RO": lambda args: ((args[1],)),
        b"GEOSEARCH": lambda args: ((args[1],)),
        b"GEOSEARCHSTORE": lambda args: ((args[2],)),
        b"GET": lambda args: ((args[1],)),
        b"GETBIT": lambda args: ((args[1],)),
        b"GETRANGE": lambda args: ((args[1],)),
        b"HEXISTS": lambda args: ((args[1],)),
        b"HGET": lambda args: ((args[1],)),
        b"HGETALL": lambda args: ((args[1],)),
        b"HKEYS": lambda args: ((args[1],)),
        b"HLEN": lambda args: ((args[1],)),
        b"HMGET": lambda args: ((args[1],)),
        b"HRANDFIELD": lambda args: ((args[1],)),
        b"HSCAN": lambda args: ((args[1],)),
        b"HSTRLEN": lambda args: ((args[1],)),
        b"HVALS": lambda args: ((args[1],)),
        b"LCS": lambda args: (args[1:2]),
        b"LINDEX": lambda args: ((args[1],)),
        b"LLEN": lambda args: ((args[1],)),
        b"LPOS": lambda args: ((args[1],)),
        b"LRANGE": lambda args: ((args[1],)),
        b"MEMORY USAGE": lambda args: ((args[1],)),
        b"MGET": lambda args: (args[1 : (len(args))]),
        b"OBJECT ENCODING": lambda args: ((args[1],)),
        b"OBJECT FREQ": lambda args: ((args[1],)),
        b"OBJECT IDLETIME": lambda args: ((args[1],)),
        b"OBJECT REFCOUNT": lambda args: ((args[1],)),
        b"PEXPIRETIME": lambda args: ((args[1],)),
        b"PFMERGE": lambda args: (args[2 : (len(args))]),
        b"PTTL": lambda args: ((args[1],)),
        b"SCARD": lambda args: ((args[1],)),
        b"SDIFF": lambda args: (args[1 : (len(args))]),
        b"SDIFFSTORE": lambda args: (args[2 : (len(args))]),
        b"SINTER": lambda args: (args[1 : (len(args))]),
        b"SINTERCARD": lambda args: (args[2 : 2 + int(args[1])]),
        b"SINTERSTORE": lambda args: (args[2 : (len(args))]),
        b"SISMEMBER": lambda args: ((args[1],)),
        b"SMEMBERS": lambda args: ((args[1],)),
        b"SMISMEMBER": lambda args: ((args[1],)),
        b"SORT": lambda args: ((args[1],)),
        b"SORT_RO": lambda args: ((args[1],)),
        b"SRANDMEMBER": lambda args: ((args[1],)),
        b"SSCAN": lambda args: ((args[1],)),
        b"STRLEN": lambda args: ((args[1],)),
        b"SUBSTR": lambda args: ((args[1],)),
        b"SUNION": lambda args: (args[1 : (len(args))]),
        b"SUNIONSTORE": lambda args: (args[2 : (len(args))]),
        b"TOUCH": lambda args: (args[1 : (len(args))]),
        b"TTL": lambda args: ((args[1],)),
        b"TYPE": lambda args: ((args[1],)),
        b"WATCH": lambda args: (args[1 : (len(args))]),
        b"XINFO CONSUMERS": lambda args: ((args[1],)),
        b"XINFO GROUPS": lambda args: ((args[1],)),
        b"XINFO STREAM": lambda args: ((args[1],)),
        b"XLEN": lambda args: ((args[1],)),
        b"XPENDING": lambda args: ((args[1],)),
        b"XRANGE": lambda args: ((args[1],)),
        b"XREAD": lambda args: (
            (
                (
                    lambda kwpos: tuple(
                        args[1 + kwpos : len(args) - (len(args) - (kwpos + 1)) // 2]
                    )
                )(args.index(b"STREAMS", 1))
                if b"STREAMS" in args
                else ()
            )
        ),
        b"XREADGROUP": lambda args: (
            (
                (
                    lambda kwpos: tuple(
                        args[1 + kwpos : len(args) - (len(args) - (kwpos + 1)) // 2]
                    )
                )(args.index(b"STREAMS", 4))
                if b"STREAMS" in args
                else ()
            )
        ),
        b"XREVRANGE": lambda args: ((args[1],)),
        b"ZCARD": lambda args: ((args[1],)),
        b"ZCOUNT": lambda args: ((args[1],)),
        b"ZDIFF": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZDIFFSTORE": lambda args: (args[3 : 3 + int(args[2])]),
        b"ZINTER": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZINTERCARD": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZINTERSTORE": lambda args: (args[3 : 3 + int(args[2])]),
        b"ZLEXCOUNT": lambda args: ((args[1],)),
        b"ZMSCORE": lambda args: ((args[1],)),
        b"ZRANDMEMBER": lambda args: ((args[1],)),
        b"ZRANGE": lambda args: ((args[1],)),
        b"ZRANGEBYLEX": lambda args: ((args[1],)),
        b"ZRANGEBYSCORE": lambda args: ((args[1],)),
        b"ZRANGESTORE": lambda args: ((args[2],)),
        b"ZRANK": lambda args: ((args[1],)),
        b"ZREVRANGE": lambda args: ((args[1],)),
        b"ZREVRANGEBYLEX": lambda args: ((args[1],)),
        b"ZREVRANGEBYSCORE": lambda args: ((args[1],)),
        b"ZREVRANK": lambda args: ((args[1],)),
        b"ZSCAN": lambda args: ((args[1],)),
        b"ZSCORE": lambda args: ((args[1],)),
        b"ZUNION": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZUNIONSTORE": lambda args: (args[3 : 3 + int(args[2])]),
    }
    ALL: ClassVar[Dict[bytes, Callable[[Tuple[ValueT, ...]], Tuple[ValueT, ...]]]] = {
        b"OBJECT": lambda args: ((args[2],)),
        b"DEBUG OBJECT": lambda args: ((args[1],)),
        b"APPEND": lambda args: ((args[1],)),
        b"BITFIELD": lambda args: ((args[1],)),
        b"BLMOVE": lambda args: ((args[1],) + (args[2],)),
        b"BLMPOP": lambda args: (args[3 : 3 + int(args[2])]),
        b"BLPOP": lambda args: (args[1 : (len(args) - 1)]),
        b"BRPOP": lambda args: (args[1 : (len(args) - 1)]),
        b"BRPOPLPUSH": lambda args: ((args[1],) + (args[2],)),
        b"BZMPOP": lambda args: (args[3 : 3 + int(args[2])]),
        b"BZPOPMAX": lambda args: (args[1 : (len(args) - 1)]),
        b"BZPOPMIN": lambda args: (args[1 : (len(args) - 1)]),
        b"DECR": lambda args: ((args[1],)),
        b"DECRBY": lambda args: ((args[1],)),
        b"EVAL": lambda args: (args[3 : 3 + int(args[2])]),
        b"EVALSHA": lambda args: (args[3 : 3 + int(args[2])]),
        b"EXPIRE": lambda args: ((args[1],)),
        b"EXPIREAT": lambda args: ((args[1],)),
        b"FCALL": lambda args: (args[3 : 3 + int(args[2])]),
        b"GEOADD": lambda args: ((args[1],)),
        b"GETDEL": lambda args: ((args[1],)),
        b"GETEX": lambda args: ((args[1],)),
        b"GETSET": lambda args: ((args[1],)),
        b"HDEL": lambda args: ((args[1],)),
        b"HINCRBY": lambda args: ((args[1],)),
        b"HINCRBYFLOAT": lambda args: ((args[1],)),
        b"HMSET": lambda args: ((args[1],)),
        b"HSET": lambda args: ((args[1],)),
        b"HSETNX": lambda args: ((args[1],)),
        b"INCR": lambda args: ((args[1],)),
        b"INCRBY": lambda args: ((args[1],)),
        b"INCRBYFLOAT": lambda args: ((args[1],)),
        b"LINSERT": lambda args: ((args[1],)),
        b"LMOVE": lambda args: ((args[1],) + (args[2],)),
        b"LMPOP": lambda args: (args[2 : 2 + int(args[1])]),
        b"LPOP": lambda args: ((args[1],)),
        b"LPUSH": lambda args: ((args[1],)),
        b"LPUSHX": lambda args: ((args[1],)),
        b"LREM": lambda args: ((args[1],)),
        b"LSET": lambda args: ((args[1],)),
        b"LTRIM": lambda args: ((args[1],)),
        b"MIGRATE": lambda args: (
            (args[3],)
            + (
                (lambda kwpos: tuple(args[1 + kwpos : len(args)]))(
                    len(args) - list(reversed(args)).index(b"KEYS", 1) - 1
                )
                if b"KEYS" in args
                else ()
            )
        ),
        b"MOVE": lambda args: ((args[1],)),
        b"PERSIST": lambda args: ((args[1],)),
        b"PEXPIRE": lambda args: ((args[1],)),
        b"PEXPIREAT": lambda args: ((args[1],)),
        b"PFADD": lambda args: ((args[1],)),
        b"PFCOUNT": lambda args: (args[1 : (len(args))]),
        b"PFDEBUG": lambda args: ((args[2],)),
        b"PFMERGE": lambda args: ((args[1],) + args[2 : (len(args))]),
        b"RENAME": lambda args: ((args[1],) + (args[2],)),
        b"RENAMENX": lambda args: ((args[1],) + (args[2],)),
        b"RPOP": lambda args: ((args[1],)),
        b"RPOPLPUSH": lambda args: ((args[1],) + (args[2],)),
        b"RPUSH": lambda args: ((args[1],)),
        b"RPUSHX": lambda args: ((args[1],)),
        b"SADD": lambda args: ((args[1],)),
        b"SET": lambda args: ((args[1],)),
        b"SETBIT": lambda args: ((args[1],)),
        b"SETRANGE": lambda args: ((args[1],)),
        b"SINTERSTORE": lambda args: ((args[1],) + args[2 : (len(args))]),
        b"SMOVE": lambda args: ((args[1],) + (args[2],)),
        b"SPOP": lambda args: ((args[1],)),
        b"SREM": lambda args: ((args[1],)),
        b"XACK": lambda args: ((args[1],)),
        b"XADD": lambda args: ((args[1],)),
        b"XAUTOCLAIM": lambda args: ((args[1],)),
        b"XCLAIM": lambda args: ((args[1],)),
        b"XDEL": lambda args: ((args[1],)),
        b"XGROUP CREATE": lambda args: ((args[1],)),
        b"XGROUP CREATECONSUMER": lambda args: ((args[1],)),
        b"XGROUP DELCONSUMER": lambda args: ((args[1],)),
        b"XGROUP DESTROY": lambda args: ((args[1],)),
        b"XGROUP SETID": lambda args: ((args[1],)),
        b"XSETID": lambda args: ((args[1],)),
        b"XTRIM": lambda args: ((args[1],)),
        b"ZADD": lambda args: ((args[1],)),
        b"ZINCRBY": lambda args: ((args[1],)),
        b"ZMPOP": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZPOPMAX": lambda args: ((args[1],)),
        b"ZPOPMIN": lambda args: ((args[1],)),
        b"ZREM": lambda args: ((args[1],)),
        b"ZREMRANGEBYLEX": lambda args: ((args[1],)),
        b"ZREMRANGEBYRANK": lambda args: ((args[1],)),
        b"ZREMRANGEBYSCORE": lambda args: ((args[1],)),
        b"BITCOUNT": lambda args: ((args[1],)),
        b"BITFIELD_RO": lambda args: ((args[1],)),
        b"BITOP": lambda args: (args[3 : (len(args))] + (args[2],)),
        b"BITPOS": lambda args: ((args[1],)),
        b"COPY": lambda args: ((args[1],) + (args[2],)),
        b"DUMP": lambda args: ((args[1],)),
        b"EVALSHA_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"EVAL_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"EXISTS": lambda args: (args[1 : (len(args))]),
        b"EXPIRETIME": lambda args: ((args[1],)),
        b"FCALL_RO": lambda args: (args[3 : 3 + int(args[2])]),
        b"GEODIST": lambda args: ((args[1],)),
        b"GEOHASH": lambda args: ((args[1],)),
        b"GEOPOS": lambda args: ((args[1],)),
        b"GEORADIUS": lambda args: (
            (args[1],)
            + (
                (lambda kwpos: tuple((args[kwpos + 1],)))(args.index(b"STORE", 6))
                if b"STORE" in args
                else ()
            )
            + (
                (lambda kwpos: tuple((args[kwpos + 1],)))(args.index(b"STOREDIST", 6))
                if b"STOREDIST" in args
                else ()
            )
        ),
        b"GEORADIUSBYMEMBER": lambda args: (
            (args[1],)
            + (
                (lambda kwpos: tuple((args[kwpos + 1],)))(args.index(b"STORE", 5))
                if b"STORE" in args
                else ()
            )
            + (
                (lambda kwpos: tuple((args[kwpos + 1],)))(args.index(b"STOREDIST", 5))
                if b"STOREDIST" in args
                else ()
            )
        ),
        b"GEORADIUSBYMEMBER_RO": lambda args: ((args[1],)),
        b"GEORADIUS_RO": lambda args: ((args[1],)),
        b"GEOSEARCH": lambda args: ((args[1],)),
        b"GEOSEARCHSTORE": lambda args: ((args[2],) + (args[1],)),
        b"GET": lambda args: ((args[1],)),
        b"GETBIT": lambda args: ((args[1],)),
        b"GETRANGE": lambda args: ((args[1],)),
        b"HEXISTS": lambda args: ((args[1],)),
        b"HGET": lambda args: ((args[1],)),
        b"HGETALL": lambda args: ((args[1],)),
        b"HKEYS": lambda args: ((args[1],)),
        b"HLEN": lambda args: ((args[1],)),
        b"HMGET": lambda args: ((args[1],)),
        b"HRANDFIELD": lambda args: ((args[1],)),
        b"HSCAN": lambda args: ((args[1],)),
        b"HSTRLEN": lambda args: ((args[1],)),
        b"HVALS": lambda args: ((args[1],)),
        b"LCS": lambda args: (args[1:2]),
        b"LINDEX": lambda args: ((args[1],)),
        b"LLEN": lambda args: ((args[1],)),
        b"LPOS": lambda args: ((args[1],)),
        b"LRANGE": lambda args: ((args[1],)),
        b"MEMORY USAGE": lambda args: ((args[1],)),
        b"MGET": lambda args: (args[1 : (len(args))]),
        b"OBJECT ENCODING": lambda args: ((args[1],)),
        b"OBJECT FREQ": lambda args: ((args[1],)),
        b"OBJECT IDLETIME": lambda args: ((args[1],)),
        b"OBJECT REFCOUNT": lambda args: ((args[1],)),
        b"PEXPIRETIME": lambda args: ((args[1],)),
        b"PTTL": lambda args: ((args[1],)),
        b"SCARD": lambda args: ((args[1],)),
        b"SDIFF": lambda args: (args[1 : (len(args))]),
        b"SDIFFSTORE": lambda args: (args[2 : (len(args))] + (args[1],)),
        b"SINTER": lambda args: (args[1 : (len(args))]),
        b"SINTERCARD": lambda args: (args[2 : 2 + int(args[1])]),
        b"SISMEMBER": lambda args: ((args[1],)),
        b"SMEMBERS": lambda args: ((args[1],)),
        b"SMISMEMBER": lambda args: ((args[1],)),
        b"SORT": lambda args: ((args[1],)),
        b"SORT_RO": lambda args: ((args[1],)),
        b"SRANDMEMBER": lambda args: ((args[1],)),
        b"SSCAN": lambda args: ((args[1],)),
        b"STRLEN": lambda args: ((args[1],)),
        b"SUBSTR": lambda args: ((args[1],)),
        b"SUNION": lambda args: (args[1 : (len(args))]),
        b"SUNIONSTORE": lambda args: (args[2 : (len(args))] + (args[1],)),
        b"TOUCH": lambda args: (args[1 : (len(args))]),
        b"TTL": lambda args: ((args[1],)),
        b"TYPE": lambda args: ((args[1],)),
        b"WATCH": lambda args: (args[1 : (len(args))]),
        b"XINFO CONSUMERS": lambda args: ((args[1],)),
        b"XINFO GROUPS": lambda args: ((args[1],)),
        b"XINFO STREAM": lambda args: ((args[1],)),
        b"XLEN": lambda args: ((args[1],)),
        b"XPENDING": lambda args: ((args[1],)),
        b"XRANGE": lambda args: ((args[1],)),
        b"XREAD": lambda args: (
            (
                (
                    lambda kwpos: tuple(
                        args[1 + kwpos : len(args) - (len(args) - (kwpos + 1)) // 2]
                    )
                )(args.index(b"STREAMS", 1))
                if b"STREAMS" in args
                else ()
            )
        ),
        b"XREADGROUP": lambda args: (
            (
                (
                    lambda kwpos: tuple(
                        args[1 + kwpos : len(args) - (len(args) - (kwpos + 1)) // 2]
                    )
                )(args.index(b"STREAMS", 4))
                if b"STREAMS" in args
                else ()
            )
        ),
        b"XREVRANGE": lambda args: ((args[1],)),
        b"ZCARD": lambda args: ((args[1],)),
        b"ZCOUNT": lambda args: ((args[1],)),
        b"ZDIFF": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZDIFFSTORE": lambda args: (args[3 : 3 + int(args[2])] + (args[1],)),
        b"ZINTER": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZINTERCARD": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZINTERSTORE": lambda args: (args[3 : 3 + int(args[2])] + (args[1],)),
        b"ZLEXCOUNT": lambda args: ((args[1],)),
        b"ZMSCORE": lambda args: ((args[1],)),
        b"ZRANDMEMBER": lambda args: ((args[1],)),
        b"ZRANGE": lambda args: ((args[1],)),
        b"ZRANGEBYLEX": lambda args: ((args[1],)),
        b"ZRANGEBYSCORE": lambda args: ((args[1],)),
        b"ZRANGESTORE": lambda args: ((args[2],) + (args[1],)),
        b"ZRANK": lambda args: ((args[1],)),
        b"ZREVRANGE": lambda args: ((args[1],)),
        b"ZREVRANGEBYLEX": lambda args: ((args[1],)),
        b"ZREVRANGEBYSCORE": lambda args: ((args[1],)),
        b"ZREVRANK": lambda args: ((args[1],)),
        b"ZSCAN": lambda args: ((args[1],)),
        b"ZSCORE": lambda args: ((args[1],)),
        b"ZUNION": lambda args: (args[2 : 2 + int(args[1])]),
        b"ZUNIONSTORE": lambda args: (args[3 : 3 + int(args[2])] + (args[1],)),
        b"MSET": lambda args: (args[1 : (len(args)) : 2]),
        b"MSETNX": lambda args: (args[1 : (len(args)) : 2]),
        b"PSETEX": lambda args: ((args[1],)),
        b"RESTORE": lambda args: ((args[1],)),
        b"RESTORE-ASKING": lambda args: ((args[1],)),
        b"SETEX": lambda args: ((args[1],)),
        b"SETNX": lambda args: ((args[1],)),
        b"DEL": lambda args: (args[1 : (len(args))]),
        b"SPUBLISH": lambda args: ((args[1],)),
        b"SSUBSCRIBE": lambda args: (args[1 : (len(args))]),
        b"SUNSUBSCRIBE": lambda args: (args[1 : (len(args))]),
        b"UNLINK": lambda args: (args[1 : (len(args))]),
        b"PUBLISH": lambda args: ((args[1],)),
        b"EXPIREMEMBER": lambda args: ((args[1],)),
        b"EXPIREMEMBERAT": lambda args: ((args[1],)),
        b"PEXPIREMEMBERAT": lambda args: ((args[1],)),
        b"KEYDB.HRENAME": lambda args: ((args[1],)),
        b"KEYDB.MEXISTS": lambda args: (args[1:]),
        b"OBJECT LASTMODIFIED": lambda args: ((args[1],)),
    }

    @classmethod
    def extract_keys(
        cls, arguments: Tuple[ValueT, ...], readonly_command: bool = False
    ) -> Tuple[ValueT, ...]:
        if len(arguments) <= 1:
            return ()

        command = b(arguments[0])

        try:
            if readonly_command and command in cls.READONLY:
                return cls.READONLY[command](arguments)
            else:
                return cls.ALL[command](arguments)
        except KeyError:
            return ()
