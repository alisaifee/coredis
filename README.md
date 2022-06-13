# coredis

[![docs](https://readthedocs.org/projects/coredis/badge/?version=stable)](https://coredis.readthedocs.org)
[![codecov](https://codecov.io/gh/alisaifee/coredis/branch/master/graph/badge.svg)](https://codecov.io/gh/alisaifee/coredis)
[![Latest Version in PyPI](https://img.shields.io/pypi/v/coredis.svg)](https://pypi.python.org/pypi/coredis/)
[![ci](https://github.com/alisaifee/coredis/workflows/CI/badge.svg?branch=master)](https://github.com/alisaifee/coredis/actions?query=branch%3Amaster+workflow%3ACI)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/coredis.svg)](https://pypi.python.org/pypi/coredis/)

coredis is an async redis client with support for redis server, cluster
& sentinel.

<!-- TOC depthFrom:2 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Installation](#installation)
- [Feature Summary](#feature-summary)
	- [Deployment topologies](#deployment-topologies)
	- [Application patterns](#application-patterns)
	- [Server side scripting](#server-side-scripting)
	- [Miscellaneous](#miscellaneous)
- [Quick start](#quick-start)
	- [Single Node client](#single-node-client)
	- [Cluster client](#cluster-client)
- [Compatibility](#compatibility)
	- [Supported python versions](#supported-python-versions)
- [Experimental Backends](#experimental-backends)
- [References](#references)

<!-- /TOC -->

## Installation

To install coredis:

```bash
$ pip install coredis
```

## Feature Summary

### Deployment topologies
- [Redis Cluster](https://coredis.readthedocs.org/en/latest/handbook/cluster.html#redis-cluster)
- [Sentinel](https://coredis.readthedocs.org/en/latest/api.html#sentinel)

### Application patterns
- [PubSub](https://coredis.readthedocs.org/en/latest/handbook/pubsub.html)
- [Sharded PubSub](https://coredis.readthedocs.org/en/latest/handbook/pubsub.html#cluster-pub-sub) [`>= Redis 7.0`]
- [Stream Consumers](https://coredis.readthedocs.org/en/latest/handbook/streams.html)
- [Pipelining](https://coredis.readthedocs.org/en/latest/handbook/pipelines.html)
- [Client side caching](https://coredis.readthedocs.org/en/latest/handbook/caching.html)

### Server side scripting
- [LUA Scripting](https://coredis.readthedocs.org/en/latest/handbook/scripting.html#lua_scripting)
- [Redis Libraries and functions](https://coredis.readthedocs.org/en/latest/handbook/scripting.html#library-functions) [`>= Redis 7.0`]

### Miscellaneous
- [RESP3](https://coredis.readthedocs.org/en/latest/handbook/response.html#resp3) Protocol Support
- Public API annotated with type annotations
- Optional [Runtime Type Validation](https://coredis.readthedocs.org/en/latest/handbook/typing.html#runtime-type-checking) (via [beartype](https://github.com/beartype/beartype))

## Quick start

### Single Node client

```python
import asyncio
from coredis import Redis

async def example():
    client = Redis(host='127.0.0.1', port=6379, db=0)
    await client.flushdb()
    await client.set('foo', 1)
    assert await client.exists(['foo']) == 1
    await client.incr('foo')
    await client.incrby('foo', increment=100)

    assert int(await client.get('foo')) == 102
    await client.expire('foo', 1)
    await asyncio.sleep(0.1)
    await client.ttl('foo')
    await asyncio.sleep(1)
    assert not await client.exists(['foo'])

asyncio.run(example())
```

### Cluster client

```python
import asyncio
from coredis import RedisCluster

async def example():
    client = RedisCluster(host='172.17.0.2', port=7001)
    await client.flushdb()
    await client.set('foo', 1)
    await client.lpush('a', [1])
    print(await client.cluster_slots())

    await client.rpoplpush('a', 'b')
    assert await client.rpop('b') == b'1'

asyncio.run(example())
# {(10923, 16383): [{'host': b'172.17.0.2', 'node_id': b'332f41962b33fa44bbc5e88f205e71276a9d64f4', 'server_type': 'master', 'port': 7002},
# {'host': b'172.17.0.2', 'node_id': b'c02deb8726cdd412d956f0b9464a88812ef34f03', 'server_type': 'slave', 'port': 7005}],
# (5461, 10922): [{'host': b'172.17.0.2', 'node_id': b'3d1b020fc46bf7cb2ffc36e10e7d7befca7c5533', 'server_type': 'master', 'port': 7001},
# {'host': b'172.17.0.2', 'node_id': b'aac4799b65ff35d8dd2ad152a5515d15c0dc8ab7', 'server_type': 'slave', 'port': 7004}],
# (0, 5460): [{'host': b'172.17.0.2', 'node_id': b'0932215036dc0d908cf662fdfca4d3614f221b01', 'server_type': 'master', 'port': 7000},
# {'host': b'172.17.0.2', 'node_id': b'f6603ab4cb77e672de23a6361ec165f3a1a2bb42', 'server_type': 'slave', 'port': 7003}]}
```

To see a full list of supported redis commands refer to the [Command
compatibility](https://coredis.readthedocs.io/en/stable/compatibility.html)
documentation

## Compatibility

coredis is tested against redis versions `6.0.x`, `6.2.x` & `7.0.x`. The
test matrix status can be reviewed
[here](https://github.com/alisaifee/coredis/actions/workflows/main.yml)

coredis is additionally tested against:

-   `hiredis >= 2.0.0`
-   ` uvloop >= 0.15.0`

`hiredis` if available will be used by default as the RESP (or RESP3) parser
as it provides significant performance gains in response parsing. For more
details refer to the [the documentation section on Parsers](https://coredis.readthedocs.org/en/stable/handbook/response.html#parsers)

### Supported python versions

-   3.8
-   3.9
-   3.10


## Experimental Backends

**coredis** has experimental support for the following redis compatible backends:

- [KeyDB](https://docs.keydb.dev/)
- [Dragonfly](https://dragonflydb.io/)

## References

- [Documentation (Stable)](http://coredis.readthedocs.org/en/stable)
- [Documentation (Latest)](http://coredis.readthedocs.org/en/latest)
- [Changelog](http://coredis.readthedocs.org/en/stable/release_notes.html)
- [Project History](http://coredis.readthedocs.org/en/stable/history.html)
