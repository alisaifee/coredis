from trio import run

from coredis import Redis

redis = Redis.from_url("redis://localhost:6379", decode_responses=True)


async def main():
    async with redis:
        print(await redis.ping())
        async with redis.pubsub(channels=["mychannel"]) as ps:
            await redis.publish("mychannel", "test message!")
            async for msg in ps:
                print(msg)
                if msg["type"] == "message":
                    break


run(main)
