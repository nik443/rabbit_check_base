import asyncio
from faststream.rabbit import RabbitBroker, RabbitQueue

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
queue = RabbitQueue("messages", durable=False)


async def main():
    async with broker:
        await broker.publish(
            message="Hello RabbitMQ!",
            queue=queue,
        )
        print("Message sent")


if __name__ == "__main__":
    asyncio.run(main())