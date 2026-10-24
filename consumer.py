import asyncio

from producer import broker, queue

@broker.subscriber(queue)
async def process_message(body: str):
    print(f"Message received: {body}")
    try:
        1 / 0
    except ZeroDivisionError:
        pass


async def main():
    async with broker:
        print("Message wait")
        await broker.start()


if __name__ == "__main__":
    asyncio.run(main())