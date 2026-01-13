from __future__ import annotations

from coredis._enum import CaseAndEncodingInsensitiveEnum


class PureToken(CaseAndEncodingInsensitiveEnum):
    """
    Enum for using pure-tokens with the redis api.
    """

    #: Used by:
    #:
    #:  - ``ACL LOG``
    RESET = b"RESET"

    #: Used by:
    #:
    #:  - ``BGSAVE``
    SCHEDULE = b"SCHEDULE"

    #: Used by:
    #:
    #:  - ``BITCOUNT``
    #:  - ``BITPOS``
    BIT = b"BIT"

    #: Used by:
    #:
    #:  - ``BITCOUNT``
    #:  - ``BITPOS``
    BYTE = b"BYTE"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    FAIL = b"FAIL"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    SAT = b"SAT"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    WRAP = b"WRAP"

    #: Used by:
    #:
    #:  - ``BITOP``
    AND = b"AND"

    #: Used by:
    #:
    #:  - ``BITOP``
    ANDOR = b"ANDOR"

    #: Used by:
    #:
    #:  - ``BITOP``
    DIFF = b"DIFF"

    #: Used by:
    #:
    #:  - ``BITOP``
    DIFF1 = b"DIFF1"

    #: Used by:
    #:
    #:  - ``BITOP``
    NOT = b"NOT"

    #: Used by:
    #:
    #:  - ``BITOP``
    #:  - ``REPLICAOF``
    #:  - ``SLAVEOF``
    ONE = b"ONE"

    #: Used by:
    #:
    #:  - ``BITOP``
    OR = b"OR"

    #: Used by:
    #:
    #:  - ``BITOP``
    XOR = b"XOR"

    #: Used by:
    #:
    #:  - ``BLMOVE``
    #:  - ``BLMPOP``
    #:  - ``LMOVE``
    #:  - ``LMPOP``
    LEFT = b"LEFT"

    #: Used by:
    #:
    #:  - ``BLMOVE``
    #:  - ``BLMPOP``
    #:  - ``LMOVE``
    #:  - ``LMPOP``
    RIGHT = b"RIGHT"

    #: Used by:
    #:
    #:  - ``BZMPOP``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    #:  - ``ZINTER``
    #:  - ``ZINTERSTORE``
    #:  - ``ZMPOP``
    #:  - ``ZUNION``
    #:  - ``ZUNIONSTORE``
    MAX = b"MAX"

    #: Used by:
    #:
    #:  - ``BZMPOP``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    #:  - ``ZINTER``
    #:  - ``ZINTERSTORE``
    #:  - ``ZMPOP``
    #:  - ``ZUNION``
    #:  - ``ZUNIONSTORE``
    MIN = b"MIN"

    #: Used by:
    #:
    #:  - ``CLIENT CACHING``
    #:  - ``CLIENT KILL``
    #:  - ``REPLICAOF``
    #:  - ``SCRIPT DEBUG``
    #:  - ``SLAVEOF``
    NO = b"NO"

    #: Used by:
    #:
    #:  - ``CLIENT CACHING``
    #:  - ``CLIENT KILL``
    #:  - ``SCRIPT DEBUG``
    YES = b"YES"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    MASTER = b"MASTER"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    NORMAL = b"NORMAL"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    PUBSUB = b"PUBSUB"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    REPLICA = b"REPLICA"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    SLAVE = b"SLAVE"

    #: Used by:
    #:
    #:  - ``CLIENT NO-EVICT``
    #:  - ``CLIENT NO-TOUCH``
    #:  - ``CLIENT REPLY``
    #:  - ``CLIENT TRACKING``
    OFF = b"OFF"

    #: Used by:
    #:
    #:  - ``CLIENT NO-EVICT``
    #:  - ``CLIENT NO-TOUCH``
    #:  - ``CLIENT REPLY``
    #:  - ``CLIENT TRACKING``
    ON = b"ON"

    #: Used by:
    #:
    #:  - ``CLIENT PAUSE``
    ALL = b"ALL"

    #: Used by:
    #:
    #:  - ``CLIENT PAUSE``
    WRITE = b"WRITE"

    #: Used by:
    #:
    #:  - ``CLIENT REPLY``
    SKIP = b"SKIP"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    BCAST = b"BCAST"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    NOLOOP = b"NOLOOP"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    OPTIN = b"OPTIN"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    OPTOUT = b"OPTOUT"

    #: Used by:
    #:
    #:  - ``CLIENT UNBLOCK``
    ERROR = b"ERROR"

    #: Used by:
    #:
    #:  - ``CLIENT UNBLOCK``
    TIMEOUT = b"TIMEOUT"

    #: Used by:
    #:
    #:  - ``CLUSTER FAILOVER``
    #:  - ``FAILOVER``
    #:  - ``SHUTDOWN``
    #:  - ``XCLAIM``
    FORCE = b"FORCE"

    #: Used by:
    #:
    #:  - ``CLUSTER FAILOVER``
    TAKEOVER = b"TAKEOVER"

    #: Used by:
    #:
    #:  - ``CLUSTER RESET``
    HARD = b"HARD"

    #: Used by:
    #:
    #:  - ``CLUSTER RESET``
    SOFT = b"SOFT"

    #: Used by:
    #:
    #:  - ``CLUSTER SETSLOT``
    STABLE = b"STABLE"

    #: Used by:
    #:
    #:  - ``CLUSTER SLOT-STATS``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    #:  - ``SORT``
    #:  - ``SORT_RO``
    ASC = b"ASC"

    #: Used by:
    #:
    #:  - ``CLUSTER SLOT-STATS``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    #:  - ``SORT``
    #:  - ``SORT_RO``
    DESC = b"DESC"

    #: Used by:
    #:
    #:  - ``COPY``
    #:  - ``FUNCTION LOAD``
    #:  - ``FUNCTION RESTORE``
    #:  - ``MIGRATE``
    #:  - ``RESTORE``
    #:  - ``RESTORE-ASKING``
    REPLACE = b"REPLACE"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:  - ``EXPIREAT``
    #:  - ``HEXPIRE``
    #:  - ``HEXPIREAT``
    #:  - ``HPEXPIRE``
    #:  - ``HPEXPIREAT``
    #:  - ``PEXPIRE``
    #:  - ``PEXPIREAT``
    #:  - ``ZADD``
    GT = b"GT"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:  - ``EXPIREAT``
    #:  - ``HEXPIRE``
    #:  - ``HEXPIREAT``
    #:  - ``HPEXPIRE``
    #:  - ``HPEXPIREAT``
    #:  - ``PEXPIRE``
    #:  - ``PEXPIREAT``
    #:  - ``ZADD``
    LT = b"LT"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:  - ``EXPIREAT``
    #:  - ``GEOADD``
    #:  - ``HEXPIRE``
    #:  - ``HEXPIREAT``
    #:  - ``HPEXPIRE``
    #:  - ``HPEXPIREAT``
    #:  - ``JSON.SET``
    #:  - ``PEXPIRE``
    #:  - ``PEXPIREAT``
    #:  - ``SET``
    #:  - ``ZADD``
    NX = b"NX"

    #: Used by:
    #:
    #:  - ``EXPIRE``
    #:  - ``EXPIREAT``
    #:  - ``GEOADD``
    #:  - ``HEXPIRE``
    #:  - ``HEXPIREAT``
    #:  - ``HPEXPIRE``
    #:  - ``HPEXPIREAT``
    #:  - ``JSON.SET``
    #:  - ``PEXPIRE``
    #:  - ``PEXPIREAT``
    #:  - ``SET``
    #:  - ``ZADD``
    XX = b"XX"

    #: Used by:
    #:
    #:  - ``FAILOVER``
    #:  - ``SHUTDOWN``
    ABORT = b"ABORT"

    #: Used by:
    #:
    #:  - ``FLUSHALL``
    #:  - ``FLUSHDB``
    #:  - ``FUNCTION FLUSH``
    #:  - ``SCRIPT FLUSH``
    ASYNC = b"ASYNC"

    #: Used by:
    #:
    #:  - ``FLUSHALL``
    #:  - ``FLUSHDB``
    #:  - ``FUNCTION FLUSH``
    #:  - ``SCRIPT DEBUG``
    #:  - ``SCRIPT FLUSH``
    SYNC = b"SYNC"

    #: Used by:
    #:
    #:  - ``FUNCTION LIST``
    WITHCODE = b"WITHCODE"

    #: Used by:
    #:
    #:  - ``FUNCTION RESTORE``
    APPEND = b"APPEND"

    #: Used by:
    #:
    #:  - ``FUNCTION RESTORE``
    FLUSH = b"FLUSH"

    #: Used by:
    #:
    #:  - ``GEOADD``
    #:  - ``ZADD``
    CHANGE = b"CH"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``GEODIST``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    FT = b"FT"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``GEODIST``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    KM = b"KM"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``GEODIST``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    M = b"M"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``GEODIST``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    MI = b"MI"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    ANY = b"ANY"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    WITHCOORD = b"WITHCOORD"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    WITHDIST = b"WITHDIST"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    WITHHASH = b"WITHHASH"

    #: Used by:
    #:
    #:  - ``GEOSEARCHSTORE``
    STOREDIST = b"STOREDIST"

    #: Used by:
    #:
    #:  - ``GETEX``
    #:  - ``HGETEX``
    PERSIST = b"PERSIST"

    #: Used by:
    #:
    #:  - ``HRANDFIELD``
    WITHVALUES = b"WITHVALUES"

    #: Used by:
    #:
    #:  - ``HSCAN``
    NOVALUES = b"NOVALUES"

    #: Used by:
    #:
    #:  - ``HSETEX``
    FNX = b"FNX"

    #: Used by:
    #:
    #:  - ``HSETEX``
    FXX = b"FXX"

    #: Used by:
    #:
    #:  - ``HSETEX``
    #:  - ``SET``
    KEEPTTL = b"KEEPTTL"

    #: Used by:
    #:
    #:  - ``LCS``
    IDX = b"IDX"

    #: Used by:
    #:
    #:  - ``LCS``
    LEN = b"LEN"

    #: Used by:
    #:
    #:  - ``LCS``
    WITHMATCHLEN = b"WITHMATCHLEN"

    #: Used by:
    #:
    #:  - ``LINSERT``
    AFTER = b"AFTER"

    #: Used by:
    #:
    #:  - ``LINSERT``
    BEFORE = b"BEFORE"

    #: Used by:
    #:
    #:  - ``MIGRATE``
    COPY = b"COPY"

    #: Used by:
    #:
    #:  - ``MIGRATE``
    EMPTY_STRING = b""

    #: Used by:
    #:
    #:  - ``RESTORE``
    #:  - ``RESTORE-ASKING``
    ABSTTL = b"ABSTTL"

    #: Used by:
    #:
    #:  - ``SET``
    GET = b"GET"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    NOSAVE = b"NOSAVE"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    NOW = b"NOW"

    #: Used by:
    #:
    #:  - ``SHUTDOWN``
    SAVE = b"SAVE"

    #: Used by:
    #:
    #:  - ``SORT``
    #:  - ``SORT_RO``
    SORTING = b"ALPHA"

    #: Used by:
    #:
    #:  - ``XACKDEL``
    #:  - ``XADD``
    #:  - ``XDELEX``
    #:  - ``XTRIM``
    ACKED = b"ACKED"

    #: Used by:
    #:
    #:  - ``XACKDEL``
    #:  - ``XADD``
    #:  - ``XDELEX``
    #:  - ``XTRIM``
    DELREF = b"DELREF"

    #: Used by:
    #:
    #:  - ``XACKDEL``
    #:  - ``XADD``
    #:  - ``XDELEX``
    #:  - ``XTRIM``
    KEEPREF = b"KEEPREF"

    #: Used by:
    #:
    #:  - ``XADD``
    #:  - ``XTRIM``
    APPROXIMATELY = b"~"

    #: Used by:
    #:
    #:  - ``XADD``
    AUTO_ID = b"*"

    #: Used by:
    #:
    #:  - ``XADD``
    #:  - ``XTRIM``
    EQUAL = b"="

    #: Used by:
    #:
    #:  - ``XADD``
    #:  - ``XTRIM``
    MAXLEN = b"MAXLEN"

    #: Used by:
    #:
    #:  - ``XADD``
    #:  - ``XTRIM``
    MINID = b"MINID"

    #: Used by:
    #:
    #:  - ``XADD``
    NOMKSTREAM = b"NOMKSTREAM"

    #: Used by:
    #:
    #:  - ``XAUTOCLAIM``
    #:  - ``XCLAIM``
    JUSTID = b"JUSTID"

    #: Used by:
    #:
    #:  - ``XGROUP CREATE``
    MKSTREAM = b"MKSTREAM"

    #: Used by:
    #:
    #:  - ``XGROUP CREATE``
    #:  - ``XGROUP SETID``
    NEW_ID = b"$"

    #: Used by:
    #:
    #:  - ``XINFO STREAM``
    FULL = b"FULL"

    #: Used by:
    #:
    #:  - ``XREADGROUP``
    NOACK = b"NOACK"

    #: Used by:
    #:
    #:  - ``ZADD``
    INCREMENT = b"INCR"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``FT.SUGGET``
    #:  - ``VLINKS``
    #:  - ``VSIM``
    #:  - ``ZDIFF``
    #:  - ``ZINTER``
    #:  - ``ZRANDMEMBER``
    #:  - ``ZRANGE``
    #:  - ``ZRANGEBYSCORE``
    #:  - ``ZREVRANGE``
    #:  - ``ZREVRANGEBYSCORE``
    #:  - ``ZUNION``
    WITHSCORES = b"WITHSCORES"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    #:  - ``ZINTER``
    #:  - ``ZINTERSTORE``
    #:  - ``ZUNION``
    #:  - ``ZUNIONSTORE``
    SUM = b"SUM"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:  - ``ZRANGESTORE``
    BYLEX = b"BYLEX"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:  - ``ZRANGESTORE``
    BYSCORE = b"BYSCORE"

    #: Used by:
    #:
    #:  - ``ZRANGE``
    #:  - ``ZRANGESTORE``
    REV = b"REV"

    #: Used by:
    #:
    #:  - ``ZRANK``
    #:  - ``ZREVRANK``
    WITHSCORE = b"WITHSCORE"

    #: Used by:
    #:
    #:  - ``VADD``
    BIN = b"BIN"

    #: Used by:
    #:
    #:  - ``VADD``
    CAS = b"CAS"

    #: Used by:
    #:
    #:  - ``VADD``
    #:  - ``VSIM``
    FP32 = b"FP32"

    #: Used by:
    #:
    #:  - ``VADD``
    NOQUANT = b"NOQUANT"

    #: Used by:
    #:
    #:  - ``VADD``
    Q8 = b"Q8"

    #: Used by:
    #:
    #:  - ``VADD``
    #:  - ``VSIM``
    VALUES = b"VALUES"

    #: Used by:
    #:
    #:  - ``VSIM``
    ELE = b"ELE"

    #: Used by:
    #:
    #:  - ``VSIM``
    NOTHREAD = b"NOTHREAD"

    #: Used by:
    #:
    #:  - ``VSIM``
    TRUTH = b"TRUTH"

    #: Used by:
    #:
    #:  - ``VSIM``
    WITHATTRIBS = b"WITHATTRIBS"

    #: Used by:
    #:
    #:  - ``VEMB``
    RAW = b"RAW"

    #: Used by:
    #:
    #:  - ``BF.INSERT``
    #:  - ``BF.RESERVE``
    NONSCALING = b"NONSCALING"

    #: Used by:
    #:
    #:  - ``BF.INFO``
    #:  - ``BF.INSERT``
    #:  - ``CF.INSERT``
    #:  - ``CF.INSERTNX``
    ITEMS = b"ITEMS"

    #: Used by:
    #:
    #:  - ``BF.INSERT``
    #:  - ``CF.INSERT``
    #:  - ``CF.INSERTNX``
    NOCREATE = b"NOCREATE"

    #: Used by:
    #:
    #:  - ``BF.INFO``
    CAPACITY = b"CAPACITY"

    #: Used by:
    #:
    #:  - ``BF.INFO``
    EXPANSION = b"EXPANSION"

    #: Used by:
    #:
    #:  - ``BF.INFO``
    FILTERS = b"FILTERS"

    #: Used by:
    #:
    #:  - ``BF.INFO``
    SIZE = b"SIZE"

    #: Used by:
    #:
    #:  - ``CMS.MERGE``
    WEIGHTS = b"WEIGHTS"

    #: Used by:
    #:
    #:  - ``TOPK.LIST``
    WITHCOUNT = b"WITHCOUNT"

    #: Used by:
    #:
    #:  - ``TDIGEST.MERGE``
    COMPRESSION = b"COMPRESSION"

    #: Used by:
    #:
    #:  - ``TDIGEST.MERGE``
    OVERRIDE = b"OVERRIDE"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    BLOCK = b"BLOCK"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.CREATE``
    COMPRESSED = b"COMPRESSED"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    FIRST = b"FIRST"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    LAST = b"LAST"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.CREATE``
    #:  - ``TS.DECRBY``
    #:  - ``TS.INCRBY``
    UNCOMPRESSED = b"UNCOMPRESSED"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    AVG = b"AVG"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    COUNT = b"COUNT"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    RANGE = b"RANGE"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    STD_P = b"STD.P"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    STD_S = b"STD.S"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    TWA = b"TWA"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    VAR_P = b"VAR.P"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    VAR_S = b"VAR.S"

    #: Used by:
    #:
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    BUCKETTIMESTAMP = b"BUCKETTIMESTAMP"

    #: Used by:
    #:
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    EMPTY = b"EMPTY"

    #: Used by:
    #:
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    FILTER_BY_VALUE = b"FILTER_BY_VALUE"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    END = b"END"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    HYPHEN_MINUS = b"-"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    MID = b"MID"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    PLUS_SIGN = b"+"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    START = b"START"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    TILDE = b"~"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    GROUPBY = b"GROUPBY"

    #: Used by:
    #:
    #:  - ``TS.MGET``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    SELECTED_LABELS = b"SELECTED_LABELS"

    #: Used by:
    #:
    #:  - ``TS.MGET``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    WITHLABELS = b"WITHLABELS"

    #: Used by:
    #:
    #:  - ``TS.MREVRANGE``
    LATEST = b"LATEST"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    GEO = b"GEO"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    HASH = b"HASH"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    INDEXEMPTY = b"INDEXEMPTY"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    INDEXMISSING = b"INDEXMISSING"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    JSON = b"JSON"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    MAXTEXTFIELDS = b"MAXTEXTFIELDS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NOFIELDS = b"NOFIELDS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NOFREQS = b"NOFREQS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NOHL = b"NOHL"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NOINDEX = b"NOINDEX"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NOOFFSETS = b"NOOFFSETS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    NUMERIC = b"NUMERIC"

    #: Used by:
    #:
    #:  - ``FT.ALTER``
    #:  - ``FT.CREATE``
    SCHEMA = b"SCHEMA"

    #: Used by:
    #:
    #:  - ``FT.ALTER``
    #:  - ``FT.CREATE``
    #:  - ``FT.SYNUPDATE``
    SKIPINITIALSCAN = b"SKIPINITIALSCAN"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    SORTABLE = b"SORTABLE"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    TAG = b"TAG"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    TEXT = b"TEXT"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    UNF = b"UNF"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    VECTOR = b"VECTOR"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    WITHSUFFIXTRIE = b"WITHSUFFIXTRIE"

    #: Used by:
    #:
    #:  - ``FT.ALTER``
    ADD = b"ADD"

    #: Used by:
    #:
    #:  - ``FT.DROPINDEX``
    DELETE_DOCS = b"DD"

    #: Used by:
    #:
    #:  - ``FT.SPELLCHECK``
    EXCLUDE = b"EXCLUDE"

    #: Used by:
    #:
    #:  - ``FT.SPELLCHECK``
    INCLUDE = b"INCLUDE"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    EXPLAINSCORE = b"EXPLAINSCORE"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    HIGHLIGHT = b"HIGHLIGHT"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    INORDER = b"INORDER"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    LIMIT = b"LIMIT"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    NOCONTENT = b"NOCONTENT"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    NOSTOPWORDS = b"NOSTOPWORDS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    PARAMS = b"PARAMS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    SUMMARIZE = b"SUMMARIZE"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    TAGS = b"TAGS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.SEARCH``
    VERBATIM = b"VERBATIM"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``FT.SUGGET``
    WITHPAYLOADS = b"WITHPAYLOADS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    WITHSORTKEYS = b"WITHSORTKEYS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    COUNT_DISTINCT = b"COUNT_DISTINCT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    COUNT_DISTINCTISH = b"COUNT_DISTINCTISH"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    FIRST_VALUE = b"FIRST_VALUE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    LOADALL = b"LOAD *"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    QUANTILE = b"QUANTILE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    RANDOM_SAMPLE = b"RANDOM_SAMPLE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    REDUCE = b"REDUCE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    STDDEV = b"STDDEV"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    TOLIST = b"TOLIST"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    WITHCURSOR = b"WITHCURSOR"

    #: Used by:
    #:
    #:  - ``FT.PROFILE``
    AGGREGATE = b"AGGREGATE"

    #: Used by:
    #:
    #:  - ``FT.PROFILE``
    LIMITED = b"LIMITED"

    #: Used by:
    #:
    #:  - ``FT.PROFILE``
    QUERYWORD = b"QUERY"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    #:  - ``FT.PROFILE``
    SEARCH = b"SEARCH"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    ADHOC = b"ADHOC"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    BATCHES = b"BATCHES"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    COMBINE = b"COMBINE"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    KNN = b"KNN"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    LINEAR = b"LINEAR"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    NOSORT = b"NOSORT"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    RRF = b"RRF"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    VSIM = b"VSIM"

    #: Used by:
    #:
    #:  - ``FT.SUGADD``
    INCR = b"INCR"

    #: Used by:
    #:
    #:  - ``FT.SUGGET``
    FUZZY = b"FUZZY"


class PrefixToken(CaseAndEncodingInsensitiveEnum):
    """
    Enum for internal use when adding prefixes to arguments
    """

    #: Used by:
    #:
    #:  - ``BITFIELD``
    #:  - ``BITFIELD_RO``
    #:  - ``SORT``
    #:  - ``SORT_RO``
    GET = b"GET"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    INCRBY = b"INCRBY"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    OVERFLOW = b"OVERFLOW"

    #: Used by:
    #:
    #:  - ``BITFIELD``
    SET = b"SET"

    #: Used by:
    #:
    #:  - ``BLMPOP``
    #:  - ``BZMPOP``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.CURSOR READ``
    #:  - ``FT.HYBRID``
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``GEORADIUSBYMEMBER_RO``
    #:  - ``GEORADIUS_RO``
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    #:  - ``HSCAN``
    #:  - ``LMPOP``
    #:  - ``LPOS``
    #:  - ``SCAN``
    #:  - ``SSCAN``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    #:  - ``VSIM``
    #:  - ``XAUTOCLAIM``
    #:  - ``XINFO STREAM``
    #:  - ``XRANGE``
    #:  - ``XREAD``
    #:  - ``XREADGROUP``
    #:  - ``XREVRANGE``
    #:  - ``ZMPOP``
    #:  - ``ZSCAN``
    COUNT = b"COUNT"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    ADDR = b"ADDR"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    IDENTIFIER = b"ID"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    LADDR = b"LADDR"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    MAXAGE = b"MAXAGE"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    SKIPME = b"SKIPME"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    #:  - ``CLIENT LIST``
    #:  - ``SCAN``
    TYPE = b"TYPE"

    #: Used by:
    #:
    #:  - ``CLIENT KILL``
    USER = b"USER"

    #: Used by:
    #:
    #:  - ``CLIENT SETINFO``
    LIB_NAME = b"LIB-NAME"

    #: Used by:
    #:
    #:  - ``CLIENT SETINFO``
    LIB_VER = b"LIB-VER"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    #:  - ``FT.CREATE``
    PREFIX = b"PREFIX"

    #: Used by:
    #:
    #:  - ``CLIENT TRACKING``
    REDIRECT = b"REDIRECT"

    #: Used by:
    #:
    #:  - ``CLUSTER SETSLOT``
    IMPORTING = b"IMPORTING"

    #: Used by:
    #:
    #:  - ``CLUSTER SETSLOT``
    MIGRATING = b"MIGRATING"

    #: Used by:
    #:
    #:  - ``CLUSTER SETSLOT``
    NODE = b"NODE"

    #: Used by:
    #:
    #:  - ``CLUSTER SLOT-STATS``
    #:  - ``SINTERCARD``
    #:  - ``SORT``
    #:  - ``SORT_RO``
    #:  - ``XADD``
    #:  - ``XTRIM``
    #:  - ``ZINTERCARD``
    #:  - ``ZRANGE``
    #:  - ``ZRANGEBYLEX``
    #:  - ``ZRANGEBYSCORE``
    #:  - ``ZRANGESTORE``
    #:  - ``ZREVRANGEBYLEX``
    #:  - ``ZREVRANGEBYSCORE``
    LIMIT = b"LIMIT"

    #: Used by:
    #:
    #:  - ``CLUSTER SLOT-STATS``
    ORDERBY = b"ORDERBY"

    #: Used by:
    #:
    #:  - ``CLUSTER SLOT-STATS``
    SLOTSRANGE = b"SLOTSRANGE"

    #: Used by:
    #:
    #:  - ``COMMAND LIST``
    ACLCAT = b"ACLCAT"

    #: Used by:
    #:
    #:  - ``COMMAND LIST``
    FILTERBY = b"FILTERBY"

    #: Used by:
    #:
    #:  - ``COMMAND LIST``
    MODULE = b"MODULE"

    #: Used by:
    #:
    #:  - ``COMMAND LIST``
    PATTERN = b"PATTERN"

    #: Used by:
    #:
    #:  - ``COPY``
    DB = b"DB"

    #: Used by:
    #:
    #:  - ``FAILOVER``
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    TIMEOUT = b"TIMEOUT"

    #: Used by:
    #:
    #:  - ``FAILOVER``
    TO = b"TO"

    #: Used by:
    #:
    #:  - ``FUNCTION LIST``
    LIBRARYNAME = b"LIBRARYNAME"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    #:  - ``SORT``
    STORE = b"STORE"

    #: Used by:
    #:
    #:  - ``GEORADIUS``
    #:  - ``GEORADIUSBYMEMBER``
    STOREDIST = b"STOREDIST"

    #: Used by:
    #:
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    BYBOX = b"BYBOX"

    #: Used by:
    #:
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    BYRADIUS = b"BYRADIUS"

    #: Used by:
    #:
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    FROMLONLAT = b"FROMLONLAT"

    #: Used by:
    #:
    #:  - ``GEOSEARCH``
    #:  - ``GEOSEARCHSTORE``
    FROMMEMBER = b"FROMMEMBER"

    #: Used by:
    #:
    #:  - ``GETEX``
    #:  - ``HGETEX``
    #:  - ``HSETEX``
    #:  - ``SET``
    EX = b"EX"

    #: Used by:
    #:
    #:  - ``GETEX``
    #:  - ``HGETEX``
    #:  - ``HSETEX``
    #:  - ``SET``
    EXAT = b"EXAT"

    #: Used by:
    #:
    #:  - ``GETEX``
    #:  - ``HGETEX``
    #:  - ``HSETEX``
    #:  - ``SET``
    PX = b"PX"

    #: Used by:
    #:
    #:  - ``GETEX``
    #:  - ``HGETEX``
    #:  - ``HSETEX``
    #:  - ``SET``
    PXAT = b"PXAT"

    #: Used by:
    #:
    #:  - ``HELLO``
    #:  - ``MIGRATE``
    AUTH = b"AUTH"

    #: Used by:
    #:
    #:  - ``HELLO``
    SETNAME = b"SETNAME"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``HEXPIRE``
    #:  - ``HEXPIREAT``
    #:  - ``HEXPIRETIME``
    #:  - ``HGETDEL``
    #:  - ``HGETEX``
    #:  - ``HPERSIST``
    #:  - ``HPEXPIRE``
    #:  - ``HPEXPIREAT``
    #:  - ``HPEXPIRETIME``
    #:  - ``HPTTL``
    #:  - ``HSETEX``
    #:  - ``HTTL``
    FIELDS = b"FIELDS"

    #: Used by:
    #:
    #:  - ``HSCAN``
    #:  - ``SCAN``
    #:  - ``SSCAN``
    #:  - ``ZSCAN``
    MATCH = b"MATCH"

    #: Used by:
    #:
    #:  - ``LCS``
    MINMATCHLEN = b"MINMATCHLEN"

    #: Used by:
    #:
    #:  - ``LOLWUT``
    VERSION = b"VERSION"

    #: Used by:
    #:
    #:  - ``LPOS``
    MAXLEN = b"MAXLEN"

    #: Used by:
    #:
    #:  - ``LPOS``
    RANK = b"RANK"

    #: Used by:
    #:
    #:  - ``MEMORY USAGE``
    SAMPLES = b"SAMPLES"

    #: Used by:
    #:
    #:  - ``MIGRATE``
    AUTH2 = b"AUTH2"

    #: Used by:
    #:
    #:  - ``MIGRATE``
    KEYS = b"KEYS"

    #: Used by:
    #:
    #:  - ``MODULE LOADEX``
    ARGS = b"ARGS"

    #: Used by:
    #:
    #:  - ``MODULE LOADEX``
    CONFIG = b"CONFIG"

    #: Used by:
    #:
    #:  - ``RESTORE``
    #:  - ``RESTORE-ASKING``
    FREQ = b"FREQ"

    #: Used by:
    #:
    #:  - ``RESTORE``
    #:  - ``RESTORE-ASKING``
    IDLETIME = b"IDLETIME"

    #: Used by:
    #:
    #:  - ``SORT``
    #:  - ``SORT_RO``
    BY = b"BY"

    #: Used by:
    #:
    #:  - ``XACKDEL``
    #:  - ``XDELEX``
    IDS = b"IDS"

    #: Used by:
    #:
    #:  - ``XCLAIM``
    #:  - ``XPENDING``
    IDLE = b"IDLE"

    #: Used by:
    #:
    #:  - ``XCLAIM``
    LASTID = b"LASTID"

    #: Used by:
    #:
    #:  - ``XCLAIM``
    RETRYCOUNT = b"RETRYCOUNT"

    #: Used by:
    #:
    #:  - ``XCLAIM``
    TIME = b"TIME"

    #: Used by:
    #:
    #:  - ``XGROUP CREATE``
    #:  - ``XGROUP SETID``
    ENTRIESREAD = b"ENTRIESREAD"

    #: Used by:
    #:
    #:  - ``XREAD``
    #:  - ``XREADGROUP``
    BLOCK = b"BLOCK"

    #: Used by:
    #:
    #:  - ``XREAD``
    #:  - ``XREADGROUP``
    STREAMS = b"STREAMS"

    #: Used by:
    #:
    #:  - ``XREADGROUP``
    GROUP = b"GROUP"

    #: Used by:
    #:
    #:  - ``XSETID``
    ENTRIESADDED = b"ENTRIESADDED"

    #: Used by:
    #:
    #:  - ``XSETID``
    MAXDELETEDID = b"MAXDELETEDID"

    #: Used by:
    #:
    #:  - ``ZINTER``
    #:  - ``ZINTERSTORE``
    #:  - ``ZUNION``
    #:  - ``ZUNIONSTORE``
    AGGREGATE = b"AGGREGATE"

    #: Used by:
    #:
    #:  - ``ZINTER``
    #:  - ``ZINTERSTORE``
    #:  - ``ZUNION``
    #:  - ``ZUNIONSTORE``
    WEIGHTS = b"WEIGHTS"

    #: Used by:
    #:
    #:  - ``VADD``
    #:  - ``VSIM``
    EF = b"EF"

    #: Used by:
    #:
    #:  - ``VADD``
    M = b"M"

    #: Used by:
    #:
    #:  - ``VADD``
    REDUCE = b"REDUCE"

    #: Used by:
    #:
    #:  - ``VADD``
    SETATTR = b"SETATTR"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    #:  - ``VSIM``
    EPSILON = b"EPSILON"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.CREATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    #:  - ``TS.MGET``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``VSIM``
    FILTER = b"FILTER"

    #: Used by:
    #:
    #:  - ``VSIM``
    FILTER_EF = b"FILTER-EF"

    #: Used by:
    #:
    #:  - ``JSON.GET``
    INDENT = b"INDENT"

    #: Used by:
    #:
    #:  - ``JSON.GET``
    NEWLINE = b"NEWLINE"

    #: Used by:
    #:
    #:  - ``JSON.GET``
    SPACE = b"SPACE"

    #: Used by:
    #:
    #:  - ``BF.INSERT``
    #:  - ``BF.RESERVE``
    #:  - ``CF.RESERVE``
    EXPANSION = b"EXPANSION"

    #: Used by:
    #:
    #:  - ``BF.INSERT``
    #:  - ``CF.INSERT``
    #:  - ``CF.INSERTNX``
    CAPACITY = b"CAPACITY"

    #: Used by:
    #:
    #:  - ``BF.INSERT``
    ERROR = b"ERROR"

    #: Used by:
    #:
    #:  - ``CF.RESERVE``
    BUCKETSIZE = b"BUCKETSIZE"

    #: Used by:
    #:
    #:  - ``CF.RESERVE``
    MAXITERATIONS = b"MAXITERATIONS"

    #: Used by:
    #:
    #:  - ``TDIGEST.CREATE``
    COMPRESSION = b"COMPRESSION"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.DECRBY``
    #:  - ``TS.INCRBY``
    CHUNK_SIZE = b"CHUNK_SIZE"

    #: Used by:
    #:
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    DUPLICATE_POLICY = b"DUPLICATE_POLICY"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.CREATE``
    ENCODING = b"ENCODING"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.DECRBY``
    #:  - ``TS.INCRBY``
    LABELS = b"LABELS"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    #:  - ``TS.ALTER``
    #:  - ``TS.CREATE``
    #:  - ``TS.DECRBY``
    #:  - ``TS.INCRBY``
    RETENTION = b"RETENTION"

    #: Used by:
    #:
    #:  - ``TS.ADD``
    ON_DUPLICATE = b"ON_DUPLICATE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``TS.DECRBY``
    #:  - ``TS.INCRBY``
    TIMESTAMP = b"TIMESTAMP"

    #: Used by:
    #:
    #:  - ``TS.CREATERULE``
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    AGGREGATION = b"AGGREGATION"

    #: Used by:
    #:
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    ALIGN = b"ALIGN"

    #: Used by:
    #:
    #:  - ``TS.MRANGE``
    #:  - ``TS.MREVRANGE``
    #:  - ``TS.RANGE``
    #:  - ``TS.REVRANGE``
    FILTER_BY_TS = b"FILTER_BY_TS"

    #: Used by:
    #:
    #:  - ``TS.REVRANGE``
    BUCKETTIMESTAMP = b"BUCKETTIMESTAMP"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.CREATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    AS = b"AS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    #:  - ``FT.SEARCH``
    LANGUAGE = b"LANGUAGE"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    LANGUAGE_FIELD = b"LANGUAGE_FIELD"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    ON = b"ON"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    PAYLOAD_FIELD = b"PAYLOAD_FIELD"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    SCORE = b"SCORE"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    SCORE_FIELD = b"SCORE_FIELD"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    STOPWORDS = b"STOPWORDS"

    #: Used by:
    #:
    #:  - ``FT.CREATE``
    TEMPORARY = b"TEMPORARY"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.EXPLAIN``
    #:  - ``FT.EXPLAINCLI``
    #:  - ``FT.SEARCH``
    #:  - ``FT.SPELLCHECK``
    DIALECT = b"DIALECT"

    #: Used by:
    #:
    #:  - ``FT.SPELLCHECK``
    DISTANCE = b"DISTANCE"

    #: Used by:
    #:
    #:  - ``FT.SPELLCHECK``
    TERMS = b"TERMS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    EXPANDER = b"EXPANDER"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    FRAGS = b"FRAGS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    GEOFILTER = b"GEOFILTER"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    INFIELDS = b"INFIELDS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    INKEYS = b"INKEYS"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    LEN = b"LEN"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    #:  - ``FT.SUGADD``
    PAYLOAD = b"PAYLOAD"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    RETURN = b"RETURN"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    SCORER = b"SCORER"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    SEPARATOR = b"SEPARATOR"

    #: Used by:
    #:
    #:  - ``FT.SEARCH``
    SLOP = b"SLOP"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    #:  - ``FT.SEARCH``
    SORTBY = b"SORTBY"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    ABS = b"ABS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    APPLY = b"APPLY"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    CEIL = b"CEIL"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    CONTAINS = b"CONTAINS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    DAY = b"DAY"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    DAYOFMONTH = b"DAYOFMONTH"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    DAYOFWEEK = b"DAYOFWEEK"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    DAYOFYEAR = b"DAYOFYEAR"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    EXISTS = b"EXISTS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    EXP = b"EXP"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    FLOOR = b"FLOOR"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    FMT = b"FMT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    FORMAT = b"FORMAT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    GEODISTANCE = b"GEODISTANCE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    GROUPBY = b"GROUPBY"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    HOUR = b"HOUR"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    LOAD = b"LOAD"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    LOG = b"LOG"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    LOG2 = b"LOG2"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    LOWER = b"LOWER"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    MATCHED_TERMS = b"MATCHED_TERMS"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.SUGGET``
    MAX = b"MAX"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    MAX_TERMS_100 = b"MAX_TERMS=100"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    MAXIDLE = b"MAXIDLE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    MINUTE = b"MINUTE"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    MONTH = b"MONTH"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    MONTHOFYEAR = b"MONTHOFYEAR"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    OFFSET = b"OFFSET"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    PARSETIME = b"PARSETIME"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    S = b"S"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    S1 = b"S1"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    S2 = b"S2"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    SPLIT = b"SPLIT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    SQRT = b"SQRT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    STARTSWITH = b"STARTSWITH"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    STRLEN = b"STRLEN"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    SUBSTR = b"SUBSTR"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    TIMEFMT = b"TIMEFMT"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    TIMESHARING = b"TIMESHARING"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    UPPER = b"UPPER"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    X = b"X"

    #: Used by:
    #:
    #:  - ``FT.AGGREGATE``
    #:  - ``FT.HYBRID``
    YEAR = b"YEAR"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    ALPHA = b"ALPHA"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    BATCH_SIZE = b"BATCH_SIZE"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    BETA = b"BETA"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    CONSTANT = b"CONSTANT"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    EF_RUNTIME = b"EF_RUNTIME"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    K = b"K"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    POLICY = b"POLICY"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    RADIUS = b"RADIUS"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    WINDOW = b"WINDOW"

    #: Used by:
    #:
    #:  - ``FT.HYBRID``
    YIELD_SCORE_AS = b"YIELD_SCORE_AS"
