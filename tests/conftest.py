from __future__ import annotations

import asyncio
import os
import platform
import socket
import time

import pytest
import redis
from packaging import version

import coredis
import coredis.connection
import coredis.parsers
import coredis.sentinel

REDIS_VERSIONS = {}


@pytest.fixture(scope="session", autouse=True)
def uvloop():
    if os.environ.get("COREDIS_UVLOOP") == "True":
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def get_version(client):
    if client not in REDIS_VERSIONS:
        if isinstance(client, coredis.RedisCluster):
            await client
            node = list(client.primaries).pop()
            REDIS_VERSIONS[client] = version.parse((await node.info())["redis_version"])
        else:
            REDIS_VERSIONS[client] = version.parse(
                (await client.info())["redis_version"]
            )
    return REDIS_VERSIONS[client]


async def check_test_constraints(request, client, protocol=2):
    await get_version(client)
    for marker in request.node.iter_markers():
        if marker.name == "min_server_version" and marker.args:
            if REDIS_VERSIONS[client] < version.parse(marker.args[0]):
                return pytest.skip(f"Skipped for versions < {marker.args[0]}")

        if marker.name == "max_server_version" and marker.args:
            if REDIS_VERSIONS[client] > version.parse(marker.args[0]):
                return pytest.skip(f"Skipped for versions > {marker.args[0]}")

        if marker.name == "nocluster" and isinstance(client, coredis.RedisCluster):
            return pytest.skip("Skipped for redis cluster")

        if marker.name == "clusteronly" and not isinstance(
            client, coredis.RedisCluster
        ):
            return pytest.skip("Skipped for non redis cluster")
        if (
            marker.name == "os"
            and not marker.args[0].lower() == platform.system().lower()
        ):
            return pytest.skip(f"Skipped for {platform.system()}")

        if protocol == 3 and REDIS_VERSIONS[client] < version.parse("6.0.0"):
            return pytest.skip(f"Skipped RESP3 for {REDIS_VERSIONS[client]}")

        if marker.name == "nohiredis" and coredis.parsers.HIREDIS_AVAILABLE:
            return pytest.skip("Skipped for hiredis")

        if marker.name == "noresp3" and protocol == 3:
            return pytest.skip("Skipped for RESP3")


async def set_default_test_config(client):
    await get_version(client)
    await client.config_set({"maxmemory-policy": "noeviction"})
    await client.config_set({"latency-monitor-threshold": 10})
    if REDIS_VERSIONS[client] >= version.parse("6.0.0"):
        await client.acl_log(reset=True)


def check_redis_cluster_ready(host, port):
    try:
        return redis.Redis(host, port).cluster("info")["cluster_state"] == "ok"
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
def docker_services(host_ip_env, docker_services):
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
    yield ["localhost", 6389]


@pytest.fixture(scope="session")
def redis_ssl_server(docker_services):
    docker_services.start("redis-ssl")
    yield


@pytest.fixture(scope="session")
def redis_cluster_server(docker_services):
    docker_services.start("redis-cluster-init")
    docker_services.wait_for_service("redis-cluster-6", 7005, check_redis_cluster_ready)
    if os.environ.get("CI") == "True":
        time.sleep(10)
    yield


@pytest.fixture(scope="session")
def redis_sentinel_server(docker_services):
    docker_services.start("redis-sentinel")
    docker_services.wait_for_service("redis-sentinel", 26379, ping_socket)

    yield


@pytest.fixture(scope="session")
def redis_sentinel_auth_server(docker_services):
    docker_services.start("redis-sentinel-auth")
    docker_services.wait_for_service(
        "redis-sentinel-auth", 26379, check_sentinel_auth_ready
    )
    yield


@pytest.fixture(scope="session")
def redis_stack_server(docker_services):
    if os.environ.get("CI") == "True" and not os.environ.get("REDIS_STACK_VERSION"):
        pytest.skip("Redis stack tests skipped")

    docker_services.start("redis-stack")
    docker_services.wait_for_service("redis-stack", 6379, ping_socket)
    yield ["localhost", 9379]


@pytest.fixture
async def redis_basic(redis_basic_server, request):
    client = coredis.Redis(
        "localhost", 6379, decode_responses=True, verify_version=True
    )
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_basic_resp3(redis_basic_server, request):
    client = coredis.Redis(
        "localhost", 6379, decode_responses=True, verify_version=True
    )
    await check_test_constraints(request, client, protocol=3)
    client = coredis.Redis(
        "localhost",
        6379,
        decode_responses=True,
        protocol_version=3,
        verify_version=True,
    )
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_stack(redis_stack_server, request):
    client = coredis.Redis(
        "localhost", 9379, decode_responses=True, verify_version=True
    )
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_stack_raw(redis_stack_server, request):
    client = coredis.Redis("localhost", 9379, verify_version=True)
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_stack_resp3(redis_stack_server, request):
    client = coredis.Redis(
        "localhost", 9379, decode_responses=True, verify_version=True
    )
    await check_test_constraints(request, client, protocol=3)
    client = coredis.Redis(
        "localhost",
        9379,
        decode_responses=True,
        protocol_version=3,
        verify_version=True,
    )
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_stack_raw_resp3(redis_stack_server, request):
    client = coredis.Redis("localhost", 9379, verify_version=True)
    await check_test_constraints(request, client, protocol=3)
    client = coredis.Redis(
        "localhost",
        9379,
        decode_responses=True,
        protocol_version=3,
        verify_version=True,
    )
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_basic_raw(redis_basic_server, request):
    client = coredis.Redis(
        "localhost", 6379, decode_responses=False, verify_version=True
    )
    await check_test_constraints(request, client, protocol=2)
    client = coredis.Redis(
        "localhost", 6379, decode_responses=False, verify_version=True
    )
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_basic_raw_resp3(redis_basic_server, request):
    client = coredis.Redis(
        "localhost", 6379, decode_responses=False, verify_version=True
    )
    await check_test_constraints(request, client, protocol=3)
    client = coredis.Redis(
        "localhost",
        6379,
        decode_responses=False,
        protocol_version=3,
        verify_version=True,
    )
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_ssl(redis_ssl_server, request):
    storage_url = (
        "rediss://localhost:8379/0?ssl_cert_reqs=required"
        "&ssl_keyfile=./tests/tls/client.key"
        "&ssl_certfile=./tests/tls/client.crt"
        "&ssl_ca_certs=./tests/tls/ca.crt"
    )
    client = coredis.Redis.from_url(storage_url, decode_responses=True)
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)
    return client


@pytest.fixture
async def redis_auth(redis_auth_server, request):
    client = coredis.Redis.from_url(
        f"redis://:sekret@{redis_auth_server[0]}:{redis_auth_server[1]}",
        decode_responses=True,
    )
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)
    return client


@pytest.fixture
async def redis_uds(redis_uds_server, request):
    client = coredis.Redis.from_url(f"unix://{redis_uds_server}", decode_responses=True)
    await check_test_constraints(request, client)
    await client.flushall()
    await set_default_test_config(client)

    return client


@pytest.fixture
async def redis_cluster(redis_cluster_server, request):
    cluster = coredis.RedisCluster(
        "localhost", 7000, stream_timeout=10, decode_responses=True
    )
    await check_test_constraints(request, cluster)
    await cluster
    await cluster.flushall()
    await cluster.flushdb()

    for primary in cluster.primaries:
        await set_default_test_config(primary)
    yield cluster

    cluster.connection_pool.disconnect()


@pytest.fixture
async def redis_cluster_resp3(redis_cluster_server, request):
    cluster = coredis.RedisCluster(
        "localhost", 7000, stream_timeout=10, decode_responses=True, protocol_version=3
    )
    await check_test_constraints(request, cluster, protocol=3)
    await cluster
    await cluster.flushall()
    await cluster.flushdb()

    for primary in cluster.primaries:
        await set_default_test_config(primary)
    yield cluster

    cluster.connection_pool.disconnect()


@pytest.fixture
async def redis_sentinel(redis_sentinel_server, request):
    sentinel = coredis.sentinel.Sentinel(
        [("localhost", 26379)],
        sentinel_kwargs={},
        decode_responses=True,
    )
    master = sentinel.master_for("localhost-redis-sentinel")
    await check_test_constraints(request, master)
    await master.flushall()

    return sentinel


@pytest.fixture
async def redis_sentinel_auth(redis_sentinel_auth_server, request):
    sentinel = coredis.sentinel.Sentinel(
        [("localhost", 36379)],
        sentinel_kwargs={"password": "sekret"},
        password="sekret",
        decode_responses=True,
    )
    master = sentinel.master_for("localhost-redis-sentinel")
    await check_test_constraints(request, master)
    await master.flushall()

    return sentinel


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
        [pytest.param(pytest.lazy_fixture(target)) for target in targets],
    )


@pytest.fixture
def redis_server_time():
    async def _get_server_time(client):
        if isinstance(client, coredis.RedisCluster):
            await client
            node = list(client.primaries).pop()
            return await node.time()
        elif isinstance(client, coredis.Redis):
            return await client.time()

    return _get_server_time


@pytest.fixture
def _s(client):
    def str_or_bytes(value):
        if client.decode_responses:
            return str(value)
        else:
            value = str(value)
            return value.encode(client.encoding)

    return str_or_bytes


def pytest_collection_modifyitems(items):
    for item in items:
        if hasattr(item, "callspec") and "client" in item.callspec.params:
            client_name = item.callspec.params["client"].name
            if client_name.startswith("redis_"):
                tokens = client_name.replace("redis_", "").split("_")
                for token in tokens:
                    item.add_marker(getattr(pytest.mark, token))
