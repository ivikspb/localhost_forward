import asyncio

LISTEN = ("127.0.0.1", 8069)
TARGET = ("192.168.1.2", 8069)


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def handle(client_r, client_w):
    try:
        server_r, server_w = await asyncio.open_connection(*TARGET)
    except OSError:
        client_w.close()
        return
    await asyncio.gather(pipe(client_r, server_w), pipe(server_r, client_w))


async def main():
    server = await asyncio.start_server(handle, *LISTEN)
    print(f"{LISTEN} -> {TARGET}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
