[![docs](https://readthedocs.org/projects/coredis/badge/?version=stable)](https://coredis.readthedocs.org)
[![codecov](https://codecov.io/gh/alisaifee/coredis/branch/master/graph/badge.svg)](https://codecov.io/gh/alisaifee/coredis)
[![Latest Version in PyPI](https://img.shields.io/pypi/v/coredis.svg)](https://pypi.python.org/pypi/coredis/)
[![ci](https://github.com/alisaifee/coredis/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/alisaifee/coredis/actions?query=branch%3Amaster+workflow%3ACI)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/coredis.svg)](https://pypi.python.org/pypi/coredis/)

> [!IMPORTANT]
> The `master` branch contains the **coredis 6.x** codebase which is **not backward 
> compatible** with 5.x. If you are looking for the **5.x** implementation, please 
> refer to the [5.x branch](https://github.com/alisaifee/coredis/tree/5.x).

# coredis

Fast, async, fully-typed Redis client with support for cluster and sentinel

## Features

- Fully typed, even when using pipelines, Lua scripts, and libraries
- Redis [Cluster](https://coredis.readthedocs.org/en/latest/handbook/cluster.html#redis-cluster) and [Sentinel](https://coredis.readthedocs.org/en/latest/api/clients.html#sentinel) support
- Built with structured concurrency on `anyio`, supports both `asyncio` and `trio`
- Server-assisted [client-side caching](https://coredis.readthedocs.org/en/latest/handbook/caching.html) implementation
- [Redis Stack modules](https://coredis.readthedocs.org/en/latest/handbook/modules.html) support
- [Redis PubSub](https://coredis.readthedocs.org/en/latest/handbook/pubsub.html)
- [Pipelining](https://coredis.readthedocs.org/en/latest/handbook/pipelines.html)
- [Lua scripts](https://coredis.readthedocs.org/en/latest/handbook/scripting.html#lua_scripting) and [Redis functions](https://coredis.readthedocs.org/en/latest/handbook/scripting.html#library-functions) \[`>= Redis 7.0`\] support, with optional types
- Convenient [Stream Consumers](https://coredis.readthedocs.org/en/latest/handbook/streams.html) implementation
- Comprehensive documentation
- Optional [runtime type validation](https://coredis.readthedocs.org/en/latest/handbook/typing.html#runtime-type-checking) (via [beartype](https://github.com/beartype/beartype))

## Installation

```console
$ pip install coredis
```

## Getting started

To start, you'll need to connect to your `Redis` instance:

```python
import trio
from coredis import Redis

client = Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
async with client:
    await client.flushdb()
    await client.set('foo', 1)
    assert await client.exists(['foo']) == 1
    assert await client.incr('foo') == 2
    assert await client.incrby('foo', increment=100) == 102
    assert int(await client.get('foo') or 0) == 102

    assert await client.expire('foo', 1)
    await trio.sleep(0.1)
    assert await client.ttl('foo') == 1
    assert await client.pttl('foo') < 1000
    await trio.sleep(1)
    assert not await client.exists(['foo'])
```

Sentinel is also supported:

```python
from coredis.sentinel import Sentinel

sentinel = Sentinel(sentinels=[("localhost", 26379)])
async with sentinel:
    primary = sentinel.primary_for("myservice")
    replica = sentinel.replica_for("myservice")

    async with primary, replica:
        assert await primary.set("fubar", 1)
        assert int(await replica.get("fubar")) == 1
```

## Compatibility

To see a full list of supported Redis commands refer to the [Command
compatibility](https://coredis.readthedocs.io/en/latest/compatibility.html)
documentation. Details about supported Redis modules and their commands can be found
[here](https://coredis.readthedocs.io/en/latest/handbook/modules.html).

coredis is tested against redis versions >= `7.0`
The test matrix status can be reviewed
[here](https://github.com/alisaifee/coredis/actions/workflows/main.yml)

coredis is additionally tested against:

- `uvloop >= 0.15.0`
- `trio`

### Supported python versions

- 3.10
- 3.11
- 3.12
- 3.13
- PyPy 3.10

### Redis API compatible databases

**coredis** is known to work with the following databases that have redis protocol compatibility:

- [Dragonfly](https://dragonflydb.io/)
- [Redict](https://redict.io/)
- [Valkey](https://github.com/valkey-io/valkey)

## References

- [Documentation (Stable)](http://coredis.readthedocs.org/en/stable)
- [Documentation (Latest)](http://coredis.readthedocs.org/en/latest)
- [Changelog](http://coredis.readthedocs.org/en/stable/release_notes.html)
- [Project History](http://coredis.readthedocs.org/en/stable/history.html)
