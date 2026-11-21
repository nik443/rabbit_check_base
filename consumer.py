import asyncio

from faststream.rabbit import RabbitMessage

from producer import broker, work_queue, work_exchange, retry_queue, dlx_exchange, dead_letter_queue


@broker.subscriber(work_queue, exchange=work_exchange)
async def process_work_message(body: str, msg: RabbitMessage):
    print(f"Message received: {body}")
    retry_count = msg.headers.get("x-retry-count", 0)
    try:
        1 / 0
    except ZeroDivisionError:
        if retry_count < 3:
            await broker.publish(
                message=body,
                exchange="",
                routing_key=retry_queue.name,
                headers={"x-retry-count": retry_count + 1}
            )
        else:
            await broker.publish(
                message=body,
                exchange=dlx_exchange,
                routing_key="dead-letter",
                headers={"x-retry-count": retry_count}
            )
        await msg.ack()


@broker.subscriber(dead_letter_queue, exchange=dlx_exchange)
async def handle_dead_letter(body: str, msg: RabbitMessage):
    print(f"Dead letter: {body}")


async def main():
    async with broker:
        print("Message wait")
        await broker.declare_queue(retry_queue)
        await broker.start()


if __name__ == "__main__":
    asyncio.run(main())