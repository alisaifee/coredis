"""
coredis.commands.constants
--------------------------
Constants relating to redis command names and groups
"""

from __future__ import annotations

import enum

from coredis._utils import CaseAndEncodingInsensitiveEnum


class CommandName(CaseAndEncodingInsensitiveEnum):
    """
    Enum for listing all redis commands
    """

    #: Commands for server
    BGREWRITEAOF = b"BGREWRITEAOF"  # Since redis: 1.0.0
    BGSAVE = b"BGSAVE"  # Since redis: 1.0.0
    DBSIZE = b"DBSIZE"  # Since redis: 1.0.0
    DEBUG = b"DEBUG"  # Since redis: 1.0.0
    FLUSHALL = b"FLUSHALL"  # Since redis: 1.0.0
    FLUSHDB = b"FLUSHDB"  # Since redis: 1.0.0
    INFO = b"INFO"  # Since redis: 1.0.0
    LASTSAVE = b"LASTSAVE"  # Since redis: 1.0.0
    MONITOR = b"MONITOR"  # Since redis: 1.0.0
    SAVE = b"SAVE"  # Since redis: 1.0.0
    SHUTDOWN = b"SHUTDOWN"  # Since redis: 1.0.0
    SYNC = b"SYNC"  # Since redis: 1.0.0
    CONFIG = b"CONFIG"  # Since redis: 2.0.0
    CONFIG_GET = b"CONFIG GET"  # Since redis: 2.0.0
    CONFIG_RESETSTAT = b"CONFIG RESETSTAT"  # Since redis: 2.0.0
    CONFIG_SET = b"CONFIG SET"  # Since redis: 2.0.0
    SLOWLOG = b"SLOWLOG"  # Since redis: 2.2.12
    SLOWLOG_GET = b"SLOWLOG GET"  # Since redis: 2.2.12
    SLOWLOG_LEN = b"SLOWLOG LEN"  # Since redis: 2.2.12
    SLOWLOG_RESET = b"SLOWLOG RESET"  # Since redis: 2.2.12
    TIME = b"TIME"  # Since redis: 2.6.0
    CONFIG_REWRITE = b"CONFIG REWRITE"  # Since redis: 2.8.0
    PSYNC = b"PSYNC"  # Since redis: 2.8.0
    ROLE = b"ROLE"  # Since redis: 2.8.12
    COMMAND = b"COMMAND"  # Since redis: 2.8.13
    COMMAND_COUNT = b"COMMAND COUNT"  # Since redis: 2.8.13
    COMMAND_GETKEYS = b"COMMAND GETKEYS"  # Since redis: 2.8.13
    COMMAND_INFO = b"COMMAND INFO"  # Since redis: 2.8.13
    LATENCY = b"LATENCY"  # Since redis: 2.8.13
    LATENCY_DOCTOR = b"LATENCY DOCTOR"  # Since redis: 2.8.13
    LATENCY_GRAPH = b"LATENCY GRAPH"  # Since redis: 2.8.13
    LATENCY_HELP = b"LATENCY HELP"  # Since redis: 2.8.13
    LATENCY_HISTORY = b"LATENCY HISTORY"  # Since redis: 2.8.13
    LATENCY_LATEST = b"LATENCY LATEST"  # Since redis: 2.8.13
    LATENCY_RESET = b"LATENCY RESET"  # Since redis: 2.8.13
    REPLCONF = b"REPLCONF"  # Since redis: 3.0.0
    RESTORE_ASKING = b"RESTORE-ASKING"  # Since redis: 3.0.0
    MEMORY = b"MEMORY"  # Since redis: 4.0.0
    MEMORY_DOCTOR = b"MEMORY DOCTOR"  # Since redis: 4.0.0
    MEMORY_HELP = b"MEMORY HELP"  # Since redis: 4.0.0
    MEMORY_MALLOC_STATS = b"MEMORY MALLOC-STATS"  # Since redis: 4.0.0
    MEMORY_PURGE = b"MEMORY PURGE"  # Since redis: 4.0.0
    MEMORY_STATS = b"MEMORY STATS"  # Since redis: 4.0.0
    MEMORY_USAGE = b"MEMORY USAGE"  # Since redis: 4.0.0
    MODULE = b"MODULE"  # Since redis: 4.0.0
    MODULE_LIST = b"MODULE LIST"  # Since redis: 4.0.0
    MODULE_LOAD = b"MODULE LOAD"  # Since redis: 4.0.0
    MODULE_UNLOAD = b"MODULE UNLOAD"  # Since redis: 4.0.0
    SWAPDB = b"SWAPDB"  # Since redis: 4.0.0
    COMMAND_HELP = b"COMMAND HELP"  # Since redis: 5.0.0
    CONFIG_HELP = b"CONFIG HELP"  # Since redis: 5.0.0
    LOLWUT = b"LOLWUT"  # Since redis: 5.0.0
    MODULE_HELP = b"MODULE HELP"  # Since redis: 5.0.0
    REPLICAOF = b"REPLICAOF"  # Since redis: 5.0.0
    ACL = b"ACL"  # Since redis: 6.0.0
    ACL_CAT = b"ACL CAT"  # Since redis: 6.0.0
    ACL_DELUSER = b"ACL DELUSER"  # Since redis: 6.0.0
    ACL_GENPASS = b"ACL GENPASS"  # Since redis: 6.0.0
    ACL_GETUSER = b"ACL GETUSER"  # Since redis: 6.0.0
    ACL_HELP = b"ACL HELP"  # Since redis: 6.0.0
    ACL_LIST = b"ACL LIST"  # Since redis: 6.0.0
    ACL_LOAD = b"ACL LOAD"  # Since redis: 6.0.0
    ACL_LOG = b"ACL LOG"  # Since redis: 6.0.0
    ACL_SAVE = b"ACL SAVE"  # Since redis: 6.0.0
    ACL_SETUSER = b"ACL SETUSER"  # Since redis: 6.0.0
    ACL_USERS = b"ACL USERS"  # Since redis: 6.0.0
    ACL_WHOAMI = b"ACL WHOAMI"  # Since redis: 6.0.0
    FAILOVER = b"FAILOVER"  # Since redis: 6.2.0
    SLOWLOG_HELP = b"SLOWLOG HELP"  # Since redis: 6.2.0
    ACL_DRYRUN = b"ACL DRYRUN"  # Since redis: 7.0.0
    COMMAND_DOCS = b"COMMAND DOCS"  # Since redis: 7.0.0
    COMMAND_GETKEYSANDFLAGS = b"COMMAND GETKEYSANDFLAGS"  # Since redis: 7.0.0
    COMMAND_LIST = b"COMMAND LIST"  # Since redis: 7.0.0
    LATENCY_HISTOGRAM = b"LATENCY HISTOGRAM"  # Since redis: 7.0.0
    MODULE_LOADEX = b"MODULE LOADEX"  # Since redis: 7.0.0
    SLAVEOF = b"SLAVEOF"  # Deprecated in redis: 5.0.0

    #: Commands for string
    DECR = b"DECR"  # Since redis: 1.0.0
    DECRBY = b"DECRBY"  # Since redis: 1.0.0
    GET = b"GET"  # Since redis: 1.0.0
    INCR = b"INCR"  # Since redis: 1.0.0
    INCRBY = b"INCRBY"  # Since redis: 1.0.0
    MGET = b"MGET"  # Since redis: 1.0.0
    SET = b"SET"  # Since redis: 1.0.0
    SETNX = b"SETNX"  # Since redis: 1.0.0
    MSET = b"MSET"  # Since redis: 1.0.1
    MSETNX = b"MSETNX"  # Since redis: 1.0.1
    APPEND = b"APPEND"  # Since redis: 2.0.0
    SETEX = b"SETEX"  # Since redis: 2.0.0
    SETRANGE = b"SETRANGE"  # Since redis: 2.2.0
    STRLEN = b"STRLEN"  # Since redis: 2.2.0
    GETRANGE = b"GETRANGE"  # Since redis: 2.4.0
    INCRBYFLOAT = b"INCRBYFLOAT"  # Since redis: 2.6.0
    PSETEX = b"PSETEX"  # Since redis: 2.6.0
    GETDEL = b"GETDEL"  # Since redis: 6.2.0
    GETEX = b"GETEX"  # Since redis: 6.2.0
    LCS = b"LCS"  # Since redis: 7.0.0
    GETSET = b"GETSET"  # Deprecated in redis: 6.2.0
    SUBSTR = b"SUBSTR"  # Deprecated in redis: 2.0.0

    #: Commands for cluster
    ASKING = b"ASKING"  # Since redis: 3.0.0
    CLUSTER = b"CLUSTER"  # Since redis: 3.0.0
    CLUSTER_ADDSLOTS = b"CLUSTER ADDSLOTS"  # Since redis: 3.0.0
    CLUSTER_BUMPEPOCH = b"CLUSTER BUMPEPOCH"  # Since redis: 3.0.0
    CLUSTER_COUNT_FAILURE_REPORTS = (
        b"CLUSTER COUNT-FAILURE-REPORTS"  # Since redis: 3.0.0
    )
    CLUSTER_COUNTKEYSINSLOT = b"CLUSTER COUNTKEYSINSLOT"  # Since redis: 3.0.0
    CLUSTER_DELSLOTS = b"CLUSTER DELSLOTS"  # Since redis: 3.0.0
    CLUSTER_FAILOVER = b"CLUSTER FAILOVER"  # Since redis: 3.0.0
    CLUSTER_FLUSHSLOTS = b"CLUSTER FLUSHSLOTS"  # Since redis: 3.0.0
    CLUSTER_FORGET = b"CLUSTER FORGET"  # Since redis: 3.0.0
    CLUSTER_GETKEYSINSLOT = b"CLUSTER GETKEYSINSLOT"  # Since redis: 3.0.0
    CLUSTER_INFO = b"CLUSTER INFO"  # Since redis: 3.0.0
    CLUSTER_KEYSLOT = b"CLUSTER KEYSLOT"  # Since redis: 3.0.0
    CLUSTER_MEET = b"CLUSTER MEET"  # Since redis: 3.0.0
    CLUSTER_MYID = b"CLUSTER MYID"  # Since redis: 3.0.0
    CLUSTER_NODES = b"CLUSTER NODES"  # Since redis: 3.0.0
    CLUSTER_REPLICATE = b"CLUSTER REPLICATE"  # Since redis: 3.0.0
    CLUSTER_RESET = b"CLUSTER RESET"  # Since redis: 3.0.0
    CLUSTER_SAVECONFIG = b"CLUSTER SAVECONFIG"  # Since redis: 3.0.0
    CLUSTER_SET_CONFIG_EPOCH = b"CLUSTER SET-CONFIG-EPOCH"  # Since redis: 3.0.0
    CLUSTER_SETSLOT = b"CLUSTER SETSLOT"  # Since redis: 3.0.0
    READONLY = b"READONLY"  # Since redis: 3.0.0
    READWRITE = b"READWRITE"  # Since redis: 3.0.0
    CLUSTER_HELP = b"CLUSTER HELP"  # Since redis: 5.0.0
    CLUSTER_REPLICAS = b"CLUSTER REPLICAS"  # Since redis: 5.0.0
    CLUSTER_ADDSLOTSRANGE = b"CLUSTER ADDSLOTSRANGE"  # Since redis: 7.0.0
    CLUSTER_DELSLOTSRANGE = b"CLUSTER DELSLOTSRANGE"  # Since redis: 7.0.0
    CLUSTER_LINKS = b"CLUSTER LINKS"  # Since redis: 7.0.0
    CLUSTER_SHARDS = b"CLUSTER SHARDS"  # Since redis: 7.0.0
    CLUSTER_SLAVES = b"CLUSTER SLAVES"  # Deprecated in redis: 5.0.0
    CLUSTER_SLOTS = b"CLUSTER SLOTS"  # Deprecated in redis: 7.0.0

    #: Commands for connection
    AUTH = b"AUTH"  # Since redis: 1.0.0
    ECHO = b"ECHO"  # Since redis: 1.0.0
    PING = b"PING"  # Since redis: 1.0.0
    QUIT = b"QUIT"  # Since redis: 1.0.0
    SELECT = b"SELECT"  # Since redis: 1.0.0
    CLIENT = b"CLIENT"  # Since redis: 2.4.0
    CLIENT_KILL = b"CLIENT KILL"  # Since redis: 2.4.0
    CLIENT_LIST = b"CLIENT LIST"  # Since redis: 2.4.0
    CLIENT_GETNAME = b"CLIENT GETNAME"  # Since redis: 2.6.9
    CLIENT_SETNAME = b"CLIENT SETNAME"  # Since redis: 2.6.9
    CLIENT_PAUSE = b"CLIENT PAUSE"  # Since redis: 2.9.50
    CLIENT_REPLY = b"CLIENT REPLY"  # Since redis: 3.2.0
    CLIENT_HELP = b"CLIENT HELP"  # Since redis: 5.0.0
    CLIENT_ID = b"CLIENT ID"  # Since redis: 5.0.0
    CLIENT_UNBLOCK = b"CLIENT UNBLOCK"  # Since redis: 5.0.0
    CLIENT_CACHING = b"CLIENT CACHING"  # Since redis: 6.0.0
    CLIENT_GETREDIR = b"CLIENT GETREDIR"  # Since redis: 6.0.0
    CLIENT_TRACKING = b"CLIENT TRACKING"  # Since redis: 6.0.0
    HELLO = b"HELLO"  # Since redis: 6.0.0
    CLIENT_INFO = b"CLIENT INFO"  # Since redis: 6.2.0
    CLIENT_TRACKINGINFO = b"CLIENT TRACKINGINFO"  # Since redis: 6.2.0
    CLIENT_UNPAUSE = b"CLIENT UNPAUSE"  # Since redis: 6.2.0
    RESET = b"RESET"  # Since redis: 6.2.0
    CLIENT_NO_EVICT = b"CLIENT NO-EVICT"  # Since redis: 7.0.0

    #: Commands for bitmap
    GETBIT = b"GETBIT"  # Since redis: 2.2.0
    SETBIT = b"SETBIT"  # Since redis: 2.2.0
    BITCOUNT = b"BITCOUNT"  # Since redis: 2.6.0
    BITOP = b"BITOP"  # Since redis: 2.6.0
    BITPOS = b"BITPOS"  # Since redis: 2.8.7
    BITFIELD = b"BITFIELD"  # Since redis: 3.2.0
    BITFIELD_RO = b"BITFIELD_RO"  # Since redis: 6.0.0

    #: Commands for list
    LINDEX = b"LINDEX"  # Since redis: 1.0.0
    LLEN = b"LLEN"  # Since redis: 1.0.0
    LPOP = b"LPOP"  # Since redis: 1.0.0
    LPUSH = b"LPUSH"  # Since redis: 1.0.0
    LRANGE = b"LRANGE"  # Since redis: 1.0.0
    LREM = b"LREM"  # Since redis: 1.0.0
    LSET = b"LSET"  # Since redis: 1.0.0
    LTRIM = b"LTRIM"  # Since redis: 1.0.0
    RPOP = b"RPOP"  # Since redis: 1.0.0
    RPUSH = b"RPUSH"  # Since redis: 1.0.0
    BLPOP = b"BLPOP"  # Since redis: 2.0.0
    BRPOP = b"BRPOP"  # Since redis: 2.0.0
    LINSERT = b"LINSERT"  # Since redis: 2.2.0
    LPUSHX = b"LPUSHX"  # Since redis: 2.2.0
    RPUSHX = b"RPUSHX"  # Since redis: 2.2.0
    LPOS = b"LPOS"  # Since redis: 6.0.6
    BLMOVE = b"BLMOVE"  # Since redis: 6.2.0
    LMOVE = b"LMOVE"  # Since redis: 6.2.0
    BLMPOP = b"BLMPOP"  # Since redis: 7.0.0
    LMPOP = b"LMPOP"  # Since redis: 7.0.0
    RPOPLPUSH = b"RPOPLPUSH"  # Deprecated in redis: 6.2.0
    BRPOPLPUSH = b"BRPOPLPUSH"  # Deprecated in redis: 6.2.0

    #: Commands for sorted-set
    ZADD = b"ZADD"  # Since redis: 1.2.0
    ZCARD = b"ZCARD"  # Since redis: 1.2.0
    ZINCRBY = b"ZINCRBY"  # Since redis: 1.2.0
    ZRANGE = b"ZRANGE"  # Since redis: 1.2.0
    ZREM = b"ZREM"  # Since redis: 1.2.0
    ZREMRANGEBYSCORE = b"ZREMRANGEBYSCORE"  # Since redis: 1.2.0
    ZSCORE = b"ZSCORE"  # Since redis: 1.2.0
    ZCOUNT = b"ZCOUNT"  # Since redis: 2.0.0
    ZINTERSTORE = b"ZINTERSTORE"  # Since redis: 2.0.0
    ZRANK = b"ZRANK"  # Since redis: 2.0.0
    ZREMRANGEBYRANK = b"ZREMRANGEBYRANK"  # Since redis: 2.0.0
    ZREVRANK = b"ZREVRANK"  # Since redis: 2.0.0
    ZUNIONSTORE = b"ZUNIONSTORE"  # Since redis: 2.0.0
    ZSCAN = b"ZSCAN"  # Since redis: 2.8.0
    ZLEXCOUNT = b"ZLEXCOUNT"  # Since redis: 2.8.9
    ZREMRANGEBYLEX = b"ZREMRANGEBYLEX"  # Since redis: 2.8.9
    BZPOPMAX = b"BZPOPMAX"  # Since redis: 5.0.0
    BZPOPMIN = b"BZPOPMIN"  # Since redis: 5.0.0
    ZPOPMAX = b"ZPOPMAX"  # Since redis: 5.0.0
    ZPOPMIN = b"ZPOPMIN"  # Since redis: 5.0.0
    ZDIFF = b"ZDIFF"  # Since redis: 6.2.0
    ZDIFFSTORE = b"ZDIFFSTORE"  # Since redis: 6.2.0
    ZINTER = b"ZINTER"  # Since redis: 6.2.0
    ZMSCORE = b"ZMSCORE"  # Since redis: 6.2.0
    ZRANDMEMBER = b"ZRANDMEMBER"  # Since redis: 6.2.0
    ZRANGESTORE = b"ZRANGESTORE"  # Since redis: 6.2.0
    ZUNION = b"ZUNION"  # Since redis: 6.2.0
    BZMPOP = b"BZMPOP"  # Since redis: 7.0.0
    ZINTERCARD = b"ZINTERCARD"  # Since redis: 7.0.0
    ZMPOP = b"ZMPOP"  # Since redis: 7.0.0
    ZRANGEBYSCORE = b"ZRANGEBYSCORE"  # Deprecated in redis: 6.2.0
    ZREVRANGE = b"ZREVRANGE"  # Deprecated in redis: 6.2.0
    ZREVRANGEBYSCORE = b"ZREVRANGEBYSCORE"  # Deprecated in redis: 6.2.0
    ZRANGEBYLEX = b"ZRANGEBYLEX"  # Deprecated in redis: 6.2.0
    ZREVRANGEBYLEX = b"ZREVRANGEBYLEX"  # Deprecated in redis: 6.2.0

    #: Commands for generic
    DEL = b"DEL"  # Since redis: 1.0.0
    EXISTS = b"EXISTS"  # Since redis: 1.0.0
    EXPIRE = b"EXPIRE"  # Since redis: 1.0.0
    KEYS = b"KEYS"  # Since redis: 1.0.0
    MOVE = b"MOVE"  # Since redis: 1.0.0
    RANDOMKEY = b"RANDOMKEY"  # Since redis: 1.0.0
    RENAME = b"RENAME"  # Since redis: 1.0.0
    RENAMENX = b"RENAMENX"  # Since redis: 1.0.0
    SORT = b"SORT"  # Since redis: 1.0.0
    TTL = b"TTL"  # Since redis: 1.0.0
    TYPE = b"TYPE"  # Since redis: 1.0.0
    EXPIREAT = b"EXPIREAT"  # Since redis: 1.2.0
    PERSIST = b"PERSIST"  # Since redis: 2.2.0
    OBJECT = b"OBJECT"  # Since redis: 2.2.3
    OBJECT_ENCODING = b"OBJECT ENCODING"  # Since redis: 2.2.3
    OBJECT_IDLETIME = b"OBJECT IDLETIME"  # Since redis: 2.2.3
    OBJECT_REFCOUNT = b"OBJECT REFCOUNT"  # Since redis: 2.2.3
    DUMP = b"DUMP"  # Since redis: 2.6.0
    MIGRATE = b"MIGRATE"  # Since redis: 2.6.0
    PEXPIRE = b"PEXPIRE"  # Since redis: 2.6.0
    PEXPIREAT = b"PEXPIREAT"  # Since redis: 2.6.0
    PTTL = b"PTTL"  # Since redis: 2.6.0
    RESTORE = b"RESTORE"  # Since redis: 2.6.0
    SCAN = b"SCAN"  # Since redis: 2.8.0
    WAIT = b"WAIT"  # Since redis: 3.0.0
    TOUCH = b"TOUCH"  # Since redis: 3.2.1
    OBJECT_FREQ = b"OBJECT FREQ"  # Since redis: 4.0.0
    UNLINK = b"UNLINK"  # Since redis: 4.0.0
    COPY = b"COPY"  # Since redis: 6.2.0
    OBJECT_HELP = b"OBJECT HELP"  # Since redis: 6.2.0
    EXPIRETIME = b"EXPIRETIME"  # Since redis: 7.0.0
    PEXPIRETIME = b"PEXPIRETIME"  # Since redis: 7.0.0
    SORT_RO = b"SORT_RO"  # Since redis: 7.0.0

    #: Commands for transactions
    EXEC = b"EXEC"  # Since redis: 1.2.0
    MULTI = b"MULTI"  # Since redis: 1.2.0
    DISCARD = b"DISCARD"  # Since redis: 2.0.0
    UNWATCH = b"UNWATCH"  # Since redis: 2.2.0
    WATCH = b"WATCH"  # Since redis: 2.2.0

    #: Commands for scripting
    EVAL = b"EVAL"  # Since redis: 2.6.0
    EVALSHA = b"EVALSHA"  # Since redis: 2.6.0
    SCRIPT = b"SCRIPT"  # Since redis: 2.6.0
    SCRIPT_EXISTS = b"SCRIPT EXISTS"  # Since redis: 2.6.0
    SCRIPT_FLUSH = b"SCRIPT FLUSH"  # Since redis: 2.6.0
    SCRIPT_KILL = b"SCRIPT KILL"  # Since redis: 2.6.0
    SCRIPT_LOAD = b"SCRIPT LOAD"  # Since redis: 2.6.0
    SCRIPT_DEBUG = b"SCRIPT DEBUG"  # Since redis: 3.2.0
    SCRIPT_HELP = b"SCRIPT HELP"  # Since redis: 5.0.0
    EVALSHA_RO = b"EVALSHA_RO"  # Since redis: 7.0.0
    EVAL_RO = b"EVAL_RO"  # Since redis: 7.0.0
    FCALL = b"FCALL"  # Since redis: 7.0.0
    FCALL_RO = b"FCALL_RO"  # Since redis: 7.0.0
    FUNCTION = b"FUNCTION"  # Since redis: 7.0.0
    FUNCTION_DELETE = b"FUNCTION DELETE"  # Since redis: 7.0.0
    FUNCTION_DUMP = b"FUNCTION DUMP"  # Since redis: 7.0.0
    FUNCTION_FLUSH = b"FUNCTION FLUSH"  # Since redis: 7.0.0
    FUNCTION_HELP = b"FUNCTION HELP"  # Since redis: 7.0.0
    FUNCTION_KILL = b"FUNCTION KILL"  # Since redis: 7.0.0
    FUNCTION_LIST = b"FUNCTION LIST"  # Since redis: 7.0.0
    FUNCTION_LOAD = b"FUNCTION LOAD"  # Since redis: 7.0.0
    FUNCTION_RESTORE = b"FUNCTION RESTORE"  # Since redis: 7.0.0
    FUNCTION_STATS = b"FUNCTION STATS"  # Since redis: 7.0.0

    #: Commands for geo
    GEOADD = b"GEOADD"  # Since redis: 3.2.0
    GEODIST = b"GEODIST"  # Since redis: 3.2.0
    GEOHASH = b"GEOHASH"  # Since redis: 3.2.0
    GEOPOS = b"GEOPOS"  # Since redis: 3.2.0
    GEOSEARCH = b"GEOSEARCH"  # Since redis: 6.2.0
    GEOSEARCHSTORE = b"GEOSEARCHSTORE"  # Since redis: 6.2.0
    GEORADIUS = b"GEORADIUS"  # Deprecated in redis: 6.2.0
    GEORADIUSBYMEMBER = b"GEORADIUSBYMEMBER"  # Deprecated in redis: 6.2.0
    GEORADIUSBYMEMBER_RO = b"GEORADIUSBYMEMBER_RO"  # Deprecated in redis: 6.2.0
    GEORADIUS_RO = b"GEORADIUS_RO"  # Deprecated in redis: 6.2.0

    #: Commands for hash
    HDEL = b"HDEL"  # Since redis: 2.0.0
    HEXISTS = b"HEXISTS"  # Since redis: 2.0.0
    HGET = b"HGET"  # Since redis: 2.0.0
    HGETALL = b"HGETALL"  # Since redis: 2.0.0
    HINCRBY = b"HINCRBY"  # Since redis: 2.0.0
    HKEYS = b"HKEYS"  # Since redis: 2.0.0
    HLEN = b"HLEN"  # Since redis: 2.0.0
    HMGET = b"HMGET"  # Since redis: 2.0.0
    HSET = b"HSET"  # Since redis: 2.0.0
    HSETNX = b"HSETNX"  # Since redis: 2.0.0
    HVALS = b"HVALS"  # Since redis: 2.0.0
    HINCRBYFLOAT = b"HINCRBYFLOAT"  # Since redis: 2.6.0
    HSCAN = b"HSCAN"  # Since redis: 2.8.0
    HSTRLEN = b"HSTRLEN"  # Since redis: 3.2.0
    HRANDFIELD = b"HRANDFIELD"  # Since redis: 6.2.0
    HMSET = b"HMSET"  # Deprecated in redis: 4.0.0

    #: Commands for hyperloglog
    PFADD = b"PFADD"  # Since redis: 2.8.9
    PFCOUNT = b"PFCOUNT"  # Since redis: 2.8.9
    PFDEBUG = b"PFDEBUG"  # Since redis: 2.8.9
    PFMERGE = b"PFMERGE"  # Since redis: 2.8.9
    PFSELFTEST = b"PFSELFTEST"  # Since redis: 2.8.9

    #: Commands for pubsub
    PSUBSCRIBE = b"PSUBSCRIBE"  # Since redis: 2.0.0
    PUBLISH = b"PUBLISH"  # Since redis: 2.0.0
    PUNSUBSCRIBE = b"PUNSUBSCRIBE"  # Since redis: 2.0.0
    SUBSCRIBE = b"SUBSCRIBE"  # Since redis: 2.0.0
    UNSUBSCRIBE = b"UNSUBSCRIBE"  # Since redis: 2.0.0
    PUBSUB = b"PUBSUB"  # Since redis: 2.8.0
    PUBSUB_CHANNELS = b"PUBSUB CHANNELS"  # Since redis: 2.8.0
    PUBSUB_NUMPAT = b"PUBSUB NUMPAT"  # Since redis: 2.8.0
    PUBSUB_NUMSUB = b"PUBSUB NUMSUB"  # Since redis: 2.8.0
    PUBSUB_HELP = b"PUBSUB HELP"  # Since redis: 6.2.0
    PUBSUB_SHARDCHANNELS = b"PUBSUB SHARDCHANNELS"  # Since redis: 7.0.0
    PUBSUB_SHARDNUMSUB = b"PUBSUB SHARDNUMSUB"  # Since redis: 7.0.0
    SPUBLISH = b"SPUBLISH"  # Since redis: 7.0.0
    SSUBSCRIBE = b"SSUBSCRIBE"  # Since redis: 7.0.0
    SUNSUBSCRIBE = b"SUNSUBSCRIBE"  # Since redis: 7.0.0

    #: Commands for set
    SADD = b"SADD"  # Since redis: 1.0.0
    SCARD = b"SCARD"  # Since redis: 1.0.0
    SDIFF = b"SDIFF"  # Since redis: 1.0.0
    SDIFFSTORE = b"SDIFFSTORE"  # Since redis: 1.0.0
    SINTER = b"SINTER"  # Since redis: 1.0.0
    SINTERSTORE = b"SINTERSTORE"  # Since redis: 1.0.0
    SISMEMBER = b"SISMEMBER"  # Since redis: 1.0.0
    SMEMBERS = b"SMEMBERS"  # Since redis: 1.0.0
    SMOVE = b"SMOVE"  # Since redis: 1.0.0
    SPOP = b"SPOP"  # Since redis: 1.0.0
    SRANDMEMBER = b"SRANDMEMBER"  # Since redis: 1.0.0
    SREM = b"SREM"  # Since redis: 1.0.0
    SUNION = b"SUNION"  # Since redis: 1.0.0
    SUNIONSTORE = b"SUNIONSTORE"  # Since redis: 1.0.0
    SSCAN = b"SSCAN"  # Since redis: 2.8.0
    SMISMEMBER = b"SMISMEMBER"  # Since redis: 6.2.0
    SINTERCARD = b"SINTERCARD"  # Since redis: 7.0.0

    #: Commands for stream
    XACK = b"XACK"  # Since redis: 5.0.0
    XADD = b"XADD"  # Since redis: 5.0.0
    XCLAIM = b"XCLAIM"  # Since redis: 5.0.0
    XDEL = b"XDEL"  # Since redis: 5.0.0
    XGROUP = b"XGROUP"  # Since redis: 5.0.0
    XGROUP_CREATE = b"XGROUP CREATE"  # Since redis: 5.0.0
    XGROUP_DELCONSUMER = b"XGROUP DELCONSUMER"  # Since redis: 5.0.0
    XGROUP_DESTROY = b"XGROUP DESTROY"  # Since redis: 5.0.0
    XGROUP_HELP = b"XGROUP HELP"  # Since redis: 5.0.0
    XGROUP_SETID = b"XGROUP SETID"  # Since redis: 5.0.0
    XINFO = b"XINFO"  # Since redis: 5.0.0
    XINFO_CONSUMERS = b"XINFO CONSUMERS"  # Since redis: 5.0.0
    XINFO_GROUPS = b"XINFO GROUPS"  # Since redis: 5.0.0
    XINFO_HELP = b"XINFO HELP"  # Since redis: 5.0.0
    XINFO_STREAM = b"XINFO STREAM"  # Since redis: 5.0.0
    XLEN = b"XLEN"  # Since redis: 5.0.0
    XPENDING = b"XPENDING"  # Since redis: 5.0.0
    XRANGE = b"XRANGE"  # Since redis: 5.0.0
    XREAD = b"XREAD"  # Since redis: 5.0.0
    XREADGROUP = b"XREADGROUP"  # Since redis: 5.0.0
    XREVRANGE = b"XREVRANGE"  # Since redis: 5.0.0
    XSETID = b"XSETID"  # Since redis: 5.0.0
    XTRIM = b"XTRIM"  # Since redis: 5.0.0
    XAUTOCLAIM = b"XAUTOCLAIM"  # Since redis: 6.2.0
    XGROUP_CREATECONSUMER = b"XGROUP CREATECONSUMER"  # Since redis: 6.2.0

    #: Oddball command
    DEBUG_OBJECT = b"DEBUG OBJECT"

    #: Sentinel commands
    SENTINEL_CKQUORUM = b"SENTINEL CKQUORUM"
    SENTINEL_CONFIG_GET = b"SENTINEL CONFIG GET"
    SENTINEL_CONFIG_SET = b"SENTINEL CONFIG SET"
    SENTINEL_GET_MASTER_ADDR_BY_NAME = b"SENTINEL GET-MASTER-ADDR-BY-NAME"
    SENTINEL_FAILOVER = b"SENTINEL FAILOVER"
    SENTINEL_FLUSHCONFIG = b"SENTINEL FLUSHCONFIG"
    SENTINEL_INFO_CACHE = b"SENTINEL INFO-CACHE"
    SENTINEL_IS_MASTER_DOWN_BY_ADDR = b"SENTINEL IS-MASTER-DOWN-BY-ADDR"
    SENTINEL_MASTER = b"SENTINEL MASTER"
    SENTINEL_MASTERS = b"SENTINEL MASTERS"
    SENTINEL_MONITOR = b"SENTINEL MONITOR"
    SENTINEL_MYID = b"SENTINEL MYID"
    SENTINEL_PENDING_SCRIPTS = b"SENTINEL PENDING-SCRIPTS"
    SENTINEL_REMOVE = b"SENTINEL REMOVE"
    SENTINEL_SLAVES = b"SENTINEL SLAVES"  # Deprecated
    SENTINEL_REPLICAS = b"SENTINEL REPLICAS"
    SENTINEL_RESET = b"SENTINEL RESET"
    SENTINEL_SENTINELS = b"SENTINEL SENTINELS"
    SENTINEL_SET = b"SENTINEL SET"


class CommandGroup(enum.Enum):
    BITMAP = "bitmap"
    CLUSTER = "cluster"
    CONNECTION = "connection"
    GENERIC = "generic"
    GEO = "geo"
    HASH = "hash"
    HYPERLOGLOG = "hyperloglog"
    LIST = "list"
    PUBSUB = "pubsub"
    SCRIPTING = "scripting"
    SERVER = "server"
    SET = "set"
    SORTED_SET = "sorted-set"
    STREAM = "stream"
    STRING = "string"
    TRANSACTIONS = "transactions"


class NodeFlag(enum.Enum):
    ALL = "all"
    PRIMARIES = "primaries"
    REPLICAS = "replicas"
    RANDOM = "random"
    SLOT_ID = "slot-id"
