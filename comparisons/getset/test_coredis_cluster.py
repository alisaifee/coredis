import asyncio
import os
import time
import coredis

url = os.environ.get("REDIS_URL", "redis://localhost:7000")
max_conn = int(os.environ.get("MAX_CONNECTIONS", 64))


async def task(i, redis):
    key = f"key:{i}"
    v = await redis.get(key)
    new_v = 1 if v is None else int(v) + 1
    await redis.set(key, new_v, ex=600)


async def run(n=15000):
    pool = coredis.BlockingClusterConnectionPool.from_url(url=url, max_connections=max_conn, queue_class=asyncio.LifoQueue)
    redis = await coredis.RedisCluster(connection_pool=pool)
    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.time()
    await asyncio.gather(*tasks)
    t = time.time() - start
    print(f"coredis: {n} tasks with blocking pool with {max_conn} connections: {t}s")


if __name__ == "__main__":
    asyncio.run(run(n=15000))
