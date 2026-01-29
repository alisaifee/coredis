from __future__ import annotations

import asyncio
import contextlib
import os
import platform
import socket
import time
from functools import total_ordering
from typing import Any, Generator

import pytest
import redis
from packaging import version
from pytest_lazy_fixtures import lf

import coredis
import coredis.sentinel
from coredis._utils import EncodingInsensitiveDict, b, hash_slot, nativestr
from coredis.cache import LRUCache
from coredis.client.basic import Redis
from coredis.credentials import UserPassCredentialProvider
from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    RUNTIME_TYPECHECKS,
    Callable,
    ExecutionParameters,
    R,
    RedisCommandP,
    Unpack,
)

REDIS_VERSIONS = {}
SERVER_TYPES = {}
MODULE_VERSIONS = {}
PY_IMPLEMENTATION = platform.python_implementation()
PY_VERSION = version.Version(platform.python_version())
DOCKER_TAG_MAPPING = {
    "7.0": {
        "default": "7",
        "sentinel": "7.0.15",
        "stack": "7.0.6-RC9",
        "redict": "7-alpine",
    },
    "7.2": {"default": "7.2", "stack": "7.2.0-v15"},
    "7.4": {"default": "7.4", "stack": "7.4.0-v3"},
    "8.0": {"default": "8.0", "stack": "latest", "valkey": "8"},
    "8.2": {"default": "8.2", "stack": "latest", "sentinel": "latest"},
    "latest": {"default": "latest", "stack": "latest"},
    "next": {"default": "latest"},
}

SERVER_DEFAULT_ARGS = {
    "7.0": None,
    "7.2": None,
}


def get_backends():
    backend = os.environ.get("COREDIS_ANYIO_BACKEND", None) or "asyncio"
    if backend == "all":
        return "asyncio", "trio"
    elif backend == "asyncio":
        return (("asyncio", {"use_uvloop": os.environ.get("COREDIS_UVLOOP", None) == "True"}),)
    return (backend,)


@pytest.fixture(scope="module", params=get_backends())
def anyio_backend(request: Any) -> Any:
    return request.param


@total_ordering
class UnparseableVersion:
    def __init__(self, version: str):
        self.version = version

    def __str__(self):
        return self.version

    def __eq__(self, other):
        return str(other) == self.version

    def __lt__(self, other):
        return True


async def get_module_versions(client: Redis):
    if str(client) not in MODULE_VERSIONS:
        MODULE_VERSIONS[str(client)] = {}
        try:
            module_list = await client.module_list()

            for module in module_list:
                mod = EncodingInsensitiveDict(module)
                name = nativestr(mod["name"])
                ver = mod["ver"]
                ver, patch = divmod(ver, 100)
                ver, minor = divmod(ver, 100)
                ver, major = divmod(ver, 100)
                MODULE_VERSIONS.setdefault(str(client), {})[name] = version.Version(
                    f"{major}.{minor}.{patch}"
                )
        except Exception:
            pass

    return MODULE_VERSIONS[str(client)]


async def get_version(client):
    if str(client) not in REDIS_VERSIONS:
        try:
            if isinstance(client, coredis.RedisCluster):
                node = list(client.primaries).pop()
                async with node:
                    version_string = (await node.info())["redis_version"]
                    REDIS_VERSIONS[str(client)] = version.parse(version_string)
            elif isinstance(client, coredis.sentinel.Sentinel):
                version_string = (await client.sentinels[0].info())["redis_version"]
                REDIS_VERSIONS[str(client)] = version.parse(version_string)
            else:
                client_info = await client.info()

                if "dragonfly_version" in client_info:
                    SERVER_TYPES[str(client)] = "dragonfly"
                if "redict_version" in client_info:
                    SERVER_TYPES[str(client)] = "redict"
                if "valkey" == client_info.get("server_name"):
                    SERVER_TYPES[str(client)] = "valkey"

                version_string = client_info["redis_version"]
                REDIS_VERSIONS[str(client)] = version.parse(version_string)
        except version.InvalidVersion:
            REDIS_VERSIONS[str(client)] = UnparseableVersion(version_string)

    return REDIS_VERSIONS[str(client)]


async def check_test_constraints(request, client):
    async with client:
        await get_version(client)
        await get_module_versions(client)
    client_version = REDIS_VERSIONS[str(client)]
    for marker in request.node.iter_markers():
        if marker.name == "min_python" and marker.args:
            if PY_VERSION < version.parse(marker.args[0]):
                return pytest.skip(f"Skipped for python versions < {marker.args[0]}")

        if marker.name == "min_server_version" and marker.args:
            if client_version < version.parse(marker.args[0]):
                return pytest.skip(f"Skipped for versions < {marker.args[0]}")

        if marker.name == "max_server_version" and marker.args:
            if client_version > version.parse(marker.args[0]):
                return pytest.skip(f"Skipped for versions > {marker.args[0]}")

        if marker.name == "min_module_version" and marker.args:
            name, ver = marker.args[0], marker.args[1]
            cur_ver = MODULE_VERSIONS.get(str(client), {}).get(name)

            if not cur_ver or cur_ver < version.parse(ver):
                return pytest.skip(f"Skipped for module {name} versions < {ver}")

        if marker.name == "max_module_version" and marker.args:
            name, ver = marker.args[0], marker.args[1]
            cur_ver = MODULE_VERSIONS.get(str(client), {}).get(name)

            if not cur_ver or cur_ver > version.parse(ver):
                return pytest.skip(f"Skipped for module {name} versions > {ver}")

        if marker.name == "nocluster" and isinstance(client, coredis.RedisCluster):
            return pytest.skip("Skipped for redis cluster")

        if marker.name == "replicated_clusteronly":
            is_cluster = isinstance(client, coredis.RedisCluster)

            if not is_cluster or not any(
                node.server_type == "replica"
                for _, node in client.connection_pool.nodes.nodes.items()
            ):
                return pytest.skip("Skipped for non replicated cluster")

        if marker.name == "runtimechecks" and not RUNTIME_TYPECHECKS:
            return pytest.skip("Skipped with runtime checks disabled")
        if marker.name == "noruntimechecks" and RUNTIME_TYPECHECKS:
            return pytest.skip("Skipped with runtime checks enabled")

        if marker.name == "clusteronly" and not isinstance(client, coredis.RedisCluster):
            return pytest.skip("Skipped for non redis cluster")

        if marker.name == "os" and not marker.args[0].lower() == platform.system().lower():
            return pytest.skip(f"Skipped for {platform.system()}")

        if marker.name == "nodragonfly" and SERVER_TYPES.get(str(client)) == "dragonfly":
            return pytest.skip("Skipped for Dragonfly")

        if marker.name == "novalkey" and SERVER_TYPES.get(str(client)) == "valkey":
            return pytest.skip("Skipped for Valkey")

        if marker.name == "noredict" and SERVER_TYPES.get(str(client)) == "redict":
            return pytest.skip("Skipped for redict")

        if marker.name == "nopypy" and PY_IMPLEMENTATION == "PyPy":
            return pytest.skip("Skipped for PyPy")

        if marker.name == "pypyonly" and PY_IMPLEMENTATION != "PyPy":
            return pytest.skip("Skipped for !PyPy")


async def set_default_test_config(client, variant=None):
    await get_version(client)

    if isinstance(client, coredis.sentinel.Sentinel):
        if REDIS_VERSIONS[str(client)] >= version.parse("6.2.0"):
            await client.sentinels[0].sentinel_config_set("resolve-hostnames", "yes")
    else:
        if not variant:
            await client.config_set({"maxmemory-policy": "noeviction"})
            await client.config_set({"latency-monitor-threshold": 10})

            if REDIS_VERSIONS[str(client)] >= version.parse("6.0.0"):
                await client.acl_log(reset=True)


def get_client_test_args(request) -> dict[str, int]:
    if "client_arguments" in request.fixturenames:
        return request.getfixturevalue("client_arguments")

    return {"stream_timeout": 5, "connect_timeout": 1}


def get_remapped_slots(request):
    if "cluster_remap_keyslots" in request.fixturenames:
        return request.getfixturevalue("cluster_remap_keyslots")

    return []


@contextlib.asynccontextmanager
async def remapped_slots(client, request):
    keys = get_remapped_slots(request)
    slots = {hash_slot(b(key)) for key in keys}
    sources = {}
    destinations = {}
    originals = {}
    moves = {}

    for slot in slots:
        sources[slot] = client.connection_pool.nodes.node_from_slot(slot)
        destinations[slot] = [
            k for k in client.connection_pool.nodes.all_primaries() if k != sources[slot]
        ][0]
        originals[slot] = sources[slot].node_id
        moves[slot] = destinations[slot].node_id
    try:
        for slot in moves.keys():
            for p in client.primaries:
                async with p:
                    await p.cluster_setslot(slot, node=moves[slot])
        yield
    finally:
        if originals:
            await client.flushall()

            for slot in originals.keys():
                for p in client.primaries:
                    async with p:
                        await p.cluster_setslot(slot, node=originals[slot])


def check_redis_cluster_ready(host, port):
    try:
        return redis.Redis(host, port).cluster("info")["cluster_state"] == "ok"
    except Exception:
        return False


def check_sentinel_ready(host, port):
    try:
        info = redis.Redis(host, port).sentinel_slaves("mymaster")
        return info[0]["flags"] == "slave" and info[0]["master-link-status"] == "ok"
    except Exception:
        return False


def check_sentinel_auth_ready(host, port):
    return ping_socket(host, 36379)


def ping_socket(host, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))

        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def host_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()

    return ip


@pytest.fixture(scope="session")
def host_ip_env(host_ip):
    os.environ["HOST_IP"] = str(host_ip)


@pytest.fixture(scope="session")
def docker_tags():
    redis_server_version = os.environ.get("COREDIS_REDIS_VERSION", "latest")
    mapping = DOCKER_TAG_MAPPING.get(redis_server_version, {"default": "latest"})

    for env, key in {
        "REDIS_VERSION": "standalone",
        "REDIS_SENTINEL_VERSION": "sentinel",
        "REDIS_SSL_VERSION": "ssl",
        "REDIS_STACK_VERSION": "stack",
        "DRAGONFLY_VERSION": "dragonfly",
        "VALKEY_VERSION": "valkey",
        "REDICT_VERSION": "redict",
    }.items():
        os.environ.setdefault(env, mapping.get(key, mapping.get("default")))

    args = SERVER_DEFAULT_ARGS.get(redis_server_version, None)

    if args is not None:
        os.environ.setdefault("DEFAULT_ARGS", args)


@pytest.fixture(scope="session")
def docker_services(host_ip_env, docker_tags, docker_services):
    return docker_services


@pytest.fixture(scope="session")
def redis_basic_server(docker_services):
    docker_services.start("redis-basic")
    docker_services.wait_for_service("redis-basic", 6379, ping_socket)
    yield ["localhost", 6379]


@pytest.fixture(scope="session")
def redis_uds_server(docker_services):
    if platform.system().lower() == "darwin":
        pytest.skip("Fixture not supported on OSX")
    docker_services.start("redis-uds")
    yield "/tmp/coredis.redis.sock"


@pytest.fixture(scope="session")
def redis_auth_server(docker_services):
    docker_services.start("redis-auth")
    docker_services.wait_for_service("redis-auth", 6389, ping_socket)
    yield ["localhost", 6389]


@pytest.fixture(scope="session")
def redis_ssl_server(docker_services):
    docker_services.start("redis-ssl")
    docker_services.wait_for_service("redis-ssl", 8379, ping_socket)
    yield ["localhost", 8379]


@pytest.fixture(scope="session")
def redis_ssl_server_no_client_auth(docker_services):
    docker_services.start("redis-ssl-no-client-auth")
    docker_services.wait_for_service("redis-ssl-no-client-auth", 7379, ping_socket)
    yield ["localhost", 7379]


@pytest.fixture(scope="session")
def redis_cluster_server(docker_services):
    docker_services.start("redis-cluster-init")
    docker_services.wait_for_service("redis-cluster-6", 7005, check_redis_cluster_ready)

    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield ["localhost", 7000]


@pytest.fixture(scope="session")
def redis_cluster_auth_server(docker_services):
    docker_services.start("redis-cluster-auth-init")
    docker_services.wait_for_service(
        "redis-cluster-auth-1", 8500, lambda h, p: ping_socket("localhost", 8500)
    )

    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield ["localhost", 8500]


@pytest.fixture(scope="session")
def redis_cluster_noreplica_server(docker_services):
    docker_services.start("redis-cluster-noreplica-init")
    docker_services.wait_for_service("redis-cluster-noreplica-3", 8402, check_redis_cluster_ready)

    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield ["localhost", 8400]


@pytest.fixture(scope="session")
def redis_ssl_cluster_server(docker_services):
    docker_services.start("redis-ssl-cluster-init")
    docker_services.wait_for_service(
        "redis-ssl-cluster-6", 8306, lambda h, p: ping_socket("localhost", 8306)
    )

    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield ["localhost", 8301]


@pytest.fixture(scope="session")
def redis_stack_cluster_server(docker_services):
    docker_services.start("redis-stack-cluster-init")
    docker_services.wait_for_service("redis-stack-cluster-6", 9005, check_redis_cluster_ready)

    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield ["localhost", 9005]


@pytest.fixture(scope="session")
def redis_sentinel_server(docker_services) -> Generator[tuple[str, int], Any, None]:
    docker_services.start("redis-sentinel")
    docker_services.wait_for_service("redis-sentinel", 26379, check_sentinel_ready)
    yield "localhost", 26379


@pytest.fixture(scope="session")
def redis_sentinel_auth_server(docker_services):
    docker_services.start("redis-sentinel-auth")
    docker_services.wait_for_service("redis-sentinel-auth", 26379, check_sentinel_auth_ready)
    yield ["localhost", 36379]


@pytest.fixture(scope="session")
def redis_stack_server(docker_services):
    if os.environ.get("CI") == "True" and not os.environ.get("REDIS_STACK_VERSION"):
        pytest.skip("Redis stack tests skipped")

    if os.environ.get("CI") == "True":
        time.sleep(10)
    docker_services.start("redis-stack")
    docker_services.wait_for_service("redis-stack", 6379, ping_socket)
    yield ["localhost", 9379]


@pytest.fixture(scope="session")
def dragonfly_server(docker_services):
    docker_services.start("dragonfly")
    docker_services.wait_for_service("dragonfly", 6379, ping_socket)
    yield ["localhost", 11379]


@pytest.fixture(scope="session")
def valkey_server(docker_services):
    docker_services.start("valkey")
    docker_services.wait_for_service("valkey", 6379, ping_socket)
    yield ["localhost", 12379]


@pytest.fixture(scope="session")
def redict_server(docker_services):
    docker_services.start("redict")
    docker_services.wait_for_service("redict", 6379, ping_socket)
    yield ["localhost", 13379]


@pytest.fixture
async def redis_basic(redis_basic_server, request):
    client = coredis.Redis(
        "localhost",
        6379,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_stack(redis_stack_server, request):
    client = coredis.Redis(
        *redis_stack_server, decode_responses=True, **get_client_test_args(request)
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_stack_raw(redis_stack_server, request):
    client = coredis.Redis(*redis_stack_server, **get_client_test_args(request))
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_stack_cached(redis_stack_server, request):
    cache = LRUCache()
    client = coredis.Redis(
        *redis_stack_server,
        decode_responses=True,
        cache=cache,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_basic_raw(redis_basic_server, request):
    client = coredis.Redis(
        "localhost", 6379, decode_responses=False, **get_client_test_args(request)
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_ssl(redis_ssl_server, request):
    storage_url = (
        "rediss://localhost:8379/?ssl_cert_reqs=required"
        "&ssl_keyfile=./tests/tls/client.key"
        "&ssl_certfile=./tests/tls/client.crt"
        "&ssl_ca_certs=./tests/tls/ca.crt"
    )
    client = coredis.Redis.from_url(
        storage_url, decode_responses=True, **get_client_test_args(request)
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_ssl_no_client_auth(redis_ssl_server_no_client_auth, request):
    storage_url = "rediss://localhost:7379/?ssl_cert_reqs=none"
    client = coredis.Redis.from_url(
        storage_url, decode_responses=True, **get_client_test_args(request)
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_auth(redis_auth_server, request):
    client = coredis.Redis.from_url(
        f"redis://:sekret@{redis_auth_server[0]}:{redis_auth_server[1]}",
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_auth_cred_provider(redis_auth_server, request):
    client = coredis.Redis(
        host=redis_auth_server[0],
        port=redis_auth_server[1],
        credential_provider=UserPassCredentialProvider(password="sekret"),
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_uds(redis_uds_server, request):
    client = coredis.Redis(
        unix_socket_path=redis_uds_server,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_cached(redis_basic_server, request):
    cache = LRUCache()
    client = coredis.Redis(
        "localhost",
        6379,
        decode_responses=True,
        cache=cache,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client)
        yield client


@pytest.fixture
async def redis_cluster(redis_cluster_server, request):
    cluster = coredis.RedisCluster(
        "localhost",
        7000,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)

        async with remapped_slots(cluster, request):
            yield cluster


@pytest.fixture
async def redis_cluster_auth(redis_cluster_auth_server, request):
    cluster = coredis.RedisCluster(
        "localhost",
        8500,
        decode_responses=True,
        password="sekret",
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)

        async with remapped_slots(cluster, request):
            yield cluster


@pytest.fixture
async def redis_cluster_auth_cred_provider(redis_cluster_auth_server, request):
    cluster = coredis.RedisCluster(
        "localhost",
        8500,
        decode_responses=True,
        credential_provider=UserPassCredentialProvider(password="sekret"),
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)

        async with remapped_slots(cluster, request):
            yield cluster


@pytest.fixture
async def redis_cluster_noreplica(redis_cluster_noreplica_server, request):
    cluster = coredis.RedisCluster(
        "localhost",
        8400,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)

        async with remapped_slots(cluster, request):
            yield cluster


@pytest.fixture
async def redis_cluster_ssl(redis_ssl_cluster_server, request):
    storage_url = (
        "rediss://localhost:8301/?ssl_cert_reqs=required"
        "&ssl_keyfile=./tests/tls/client.key"
        "&ssl_certfile=./tests/tls/client.crt"
        "&ssl_ca_certs=./tests/tls/ca.crt"
    )
    cluster = coredis.RedisCluster.from_url(
        storage_url, decode_responses=True, **get_client_test_args(request)
    )

    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)
        yield cluster


@pytest.fixture
async def redis_cluster_cached(redis_cluster_server, request):
    cache = LRUCache()
    cluster = coredis.RedisCluster(
        "localhost",
        7000,
        decode_responses=True,
        cache=cache,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)
        yield cluster


@pytest.fixture
async def redis_cluster_raw(redis_cluster_server, request):
    cluster = coredis.RedisCluster(
        "localhost",
        7000,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)
        yield cluster


@pytest.fixture
async def redis_stack_cluster(redis_stack_cluster_server, request):
    cluster = coredis.RedisCluster(
        *redis_stack_cluster_server,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, cluster)
    async with cluster:
        await cluster.flushall()
        await cluster.flushdb()

        for primary in cluster.primaries:
            async with primary:
                await set_default_test_config(primary)

        async with remapped_slots(cluster, request):
            yield cluster


@pytest.fixture
async def redis_sentinel(redis_sentinel_server: tuple[str, int], request):
    sentinel = coredis.Sentinel(
        sentinels=[redis_sentinel_server],
        sentinel_kwargs={"connect_timeout": 1},
        decode_responses=True,
        **get_client_test_args(request),
    )
    async with sentinel:
        yield sentinel


@pytest.fixture
async def redis_sentinel_raw(redis_sentinel_server, request):
    sentinel = coredis.sentinel.Sentinel(
        [redis_sentinel_server],
        sentinel_kwargs={},
        **get_client_test_args(request),
    )
    async with sentinel:
        master = sentinel.primary_for("mymaster")
        await check_test_constraints(request, master)
        async with master:
            await set_default_test_config(sentinel)
            await master.flushall()
            yield sentinel


@pytest.fixture
async def redis_sentinel_auth(redis_sentinel_auth_server, request):
    sentinel = coredis.sentinel.Sentinel(
        [redis_sentinel_auth_server],
        sentinel_kwargs={"password": "sekret"},
        password="sekret",
        decode_responses=True,
        **get_client_test_args(request),
    )
    async with sentinel:
        master = sentinel.primary_for("mymaster")
        await check_test_constraints(request, master)
        async with master:
            await set_default_test_config(sentinel)
            await master.flushall()
            await asyncio.sleep(0.1)

            yield sentinel


@pytest.fixture
async def redis_sentinel_auth_cred_provider(redis_sentinel_auth_server, request):
    sentinel = coredis.sentinel.Sentinel(
        [redis_sentinel_auth_server],
        sentinel_kwargs={"credential_provider": UserPassCredentialProvider(password="sekret")},
        credential_provider=UserPassCredentialProvider(password="sekret"),
        decode_responses=True,
        **get_client_test_args(request),
    )
    async with sentinel:
        master = sentinel.primary_for("mymaster")
        await check_test_constraints(request, master)
        async with master:
            await set_default_test_config(sentinel)
            await master.flushall()
            await asyncio.sleep(0.1)

            yield sentinel


@pytest.fixture
def fake_redis():
    class _(coredis.client.Redis):
        responses = {}

        def __init__(self):
            self.cache = None

        async def initialize(self):
            pass

        async def execute_command(
            self,
            command: RedisCommandP,
            callback: Callable[..., R] = NoopCallback(),
            **options: Unpack[ExecutionParameters],
        ) -> R:
            resp = self.responses.get(command.name, {}).get(command.arguments)

            if isinstance(resp, Exception):
                raise resp

            return callback(resp)

    return _()


@pytest.fixture
def fake_redis_cluster():
    class _(coredis.client.RedisCluster):
        responses = {}

        def __init__(self):
            self.cache = None

        async def initialize(self):
            pass

        async def execute_command(
            self,
            command: RedisCommandP,
            callback: Callable[..., R] = NoopCallback(),
            **options: Unpack[ExecutionParameters],
        ) -> R:
            resp = self.responses.get(command.name, {}).get(command.arguments)

            if isinstance(resp, Exception):
                raise resp

            return callback(resp)

    return _()


@pytest.fixture
async def dragonfly(dragonfly_server, request):
    client = coredis.Redis(
        "localhost",
        11379,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client, variant="dragonfly")
        yield client


@pytest.fixture
async def valkey(valkey_server, request):
    client = coredis.Redis(
        "localhost",
        12379,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client, variant="valkey")
        yield client


@pytest.fixture
async def redict(redict_server, request):
    client = coredis.Redis(
        "localhost",
        13379,
        decode_responses=True,
        **get_client_test_args(request),
    )
    await check_test_constraints(request, client)
    async with client:
        await client.flushall()
        await set_default_test_config(client, variant="redict")
        yield client


@pytest.fixture(scope="session")
def docker_services_project_name():
    return "coredis"


@pytest.fixture(scope="session")
def docker_compose_files(pytestconfig):
    """Get the docker-compose.yml absolute path.
    Override this fixture in your tests if you need a custom location.
    """

    return ["docker-compose.yml"]


def targets(*targets):
    return pytest.mark.parametrize(
        "client",
        [pytest.param(lf(target)) for target in targets],
    )


def module_targets():
    redis_server_version = os.environ.get("COREDIS_REDIS_VERSION", "latest")
    if redis_server_version in ["latest", "next"] or version.parse(
        redis_server_version
    ) >= version.parse("8.0.0"):
        targets = [
            "redis_basic",
            "redis_basic_raw",
            "redis_cached",
            "redis_cluster",
        ]
    else:
        targets = ["redis_stack", "redis_stack_raw", "redis_stack_cached", "redis_stack_cluster"]

    return pytest.mark.parametrize(
        "client",
        [pytest.param(lf(target)) for target in targets],
    )


@pytest.fixture
def redis_server_time():
    async def _get_server_time(client):
        if isinstance(client, coredis.RedisCluster):
            node = list(client.primaries).pop()

            async with node:
                return await node.time()
        elif isinstance(client, coredis.Redis):
            return await client.time()

    return _get_server_time


@pytest.fixture
def _s(client):
    def str_or_bytes(value):
        if isinstance(client, coredis.client.Client):
            if client.decode_responses:
                return str(value)
            else:
                value = str(value)

                return value.encode(client.encoding)
        elif isinstance(client, coredis.sentinel.Sentinel):
            if client.connection_kwargs.get("decode_responses"):
                return str(value)
            else:
                value = str(value)

            return value.encode(client.connection_kwargs.get("encoding", "utf-8"))

    return str_or_bytes


@pytest.fixture
def cloner():
    async def _cloner(client, connection_kwargs={}, **kwargs):
        cache = kwargs.pop("cache", None)
        pool = kwargs.pop("connection_pool", None)
        if isinstance(client, coredis.client.Redis):
            c_kwargs = client.connection_pool.connection_kwargs
            c_kwargs.update(connection_kwargs)
            c = client.__class__(
                decode_responses=client.decode_responses,
                encoding=client.encoding,
                connection_pool=pool or client.connection_pool.__class__(_cache=cache, **c_kwargs),
                **kwargs,
            )
        else:
            c = client.__class__(
                client.connection_pool.nodes.startup_nodes[0].host,
                client.connection_pool.nodes.startup_nodes[0].port,
                decode_responses=client.decode_responses,
                encoding=client.encoding,
                cache=cache,
                connection_pool=pool,
                **kwargs,
            )
        return c

    return _cloner


@pytest.fixture
async def user_client(client, cloner):
    users = set()

    async def _user(name, *permissions):
        users.add(name)
        await client.acl_setuser(name, "nopass", *permissions)

        return await cloner(client, connection_kwargs={"username": name}, username=name)

    yield _user
    assert len(users) == await client.acl_deluser(users)


@contextlib.contextmanager
def server_deprecation_warning(message: str, client, since: str = "1.0"):
    if version.parse(since) <= REDIS_VERSIONS[str(client)]:
        with pytest.warns(DeprecationWarning, match=message):
            yield
    else:
        yield


def pytest_collection_modifyitems(items):
    for item in items:
        if hasattr(item, "callspec") and "client" in item.callspec.params:
            client_name = item.callspec.params["client"].name

            if client_name.startswith("redis_"):
                tokens = client_name.replace("redis_", "").split("_")

                for token in tokens:
                    if token in item.config.getini("markers"):
                        item.add_marker(getattr(pytest.mark, token))
            elif client_name.startswith("dragonfly"):
                item.add_marker(getattr(pytest.mark, "dragonfly"))
                tokens = client_name.replace("dragonfly_", "").split("_")

                for token in tokens:
                    item.add_marker(getattr(pytest.mark, token))
            elif client_name.startswith("valkey"):
                item.add_marker(getattr(pytest.mark, "valkey"))
                tokens = client_name.replace("valkey_", "").split("_")

                for token in tokens:
                    item.add_marker(getattr(pytest.mark, token))
            elif client_name.startswith("redict"):
                item.add_marker(getattr(pytest.mark, "redict"))
                tokens = client_name.replace("redict_", "").split("_")

                for token in tokens:
                    item.add_marker(getattr(pytest.mark, token))
