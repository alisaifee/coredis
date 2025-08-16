from anyio import run

from coredis import Redis

redis = Redis.from_url("redis://localhost:6379", decode_responses=True)


async def main():
    print(await redis.ping())


run(main)
