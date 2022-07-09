import asyncio

class Proto(asyncio.Protocol):
    def connection_made(self, transport):
        print("yay", dir(transport))
        print(transport.write(b"PING\r\n"))

    def data_received(self, data):
        print(data)
    def connection_lost(self, exc):
        print("bye", exc)
async def test():
    s, t = await asyncio.get_running_loop().create_connection(Proto, host="localhost", port=6379)
    print(s)
    await asyncio.sleep(0.1)
    s.write(b"QUIT\r\n")
    await asyncio.sleep(0.1)


asyncio.run(test())
