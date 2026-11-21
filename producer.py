import asyncio
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitExchange, ExchangeType
from faststream.rabbit.schemas.queue import ClassicQueueArgs


broker = RabbitBroker("amqp://guest:guest@localhost:5672/")

work_exchange = RabbitExchange("payments.work", type=ExchangeType.DIRECT)
dlx_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT)

work_queue = RabbitQueue(
    name="work_queue",
    durable=False,
    routing_key="work-letter",
    arguments=ClassicQueueArgs(
        **{
            "x-dead-letter-exchange": "payments.dlx",
            "x-dead-letter-routing-key": "dead-letter",
        }
    ),
)
retry_queue_1 = RabbitQueue(
    name="retry_queue_1",
    durable=False,
    arguments=ClassicQueueArgs(
        **{
            "x-dead-letter-exchange": "payments.work",
            "x-dead-letter-routing-key": "work-letter",
            "x-message-ttl": 5000,
        }
    ),
)
retry_queue_2 = RabbitQueue(
    name="retry_queue_2",
    durable=False,
    arguments=ClassicQueueArgs(
        **{
            "x-dead-letter-exchange": "payments.work",
            "x-dead-letter-routing-key": "work-letter",
            "x-message-ttl": 10000,
        }
    )
)
retry_queue_3 = RabbitQueue(
    name="retry_queue_3",
    durable=False,
    arguments=ClassicQueueArgs(
        **{
            "x-dead-letter-exchange": "payments.work",
            "x-dead-letter-routing-key": "work-letter",
            "x-message-ttl": 20000,
        }
    )
)
dead_letter_queue = RabbitQueue(
    name="dead_letter_queue",
    durable=False,
    routing_key="dead-letter",
)


async def main():
    async with broker as broker_connection:
        await declare_exchanges_and_queues(broker_connection)
        await broker_connection.publish(
            message="New message!",
            exchange=work_exchange,
            routing_key="work-letter",
        )
        print("Message sent")


async def declare_exchanges_and_queues(broker_connection):
    await broker_connection.declare_exchange(work_exchange)
    await broker_connection.declare_queue(work_queue)

    await broker_connection.declare_queue(retry_queue_1)
    await broker_connection.declare_queue(retry_queue_2)
    await broker_connection.declare_queue(retry_queue_3)

    await broker_connection.declare_exchange(dlx_exchange)
    await broker_connection.declare_queue(dead_letter_queue)


if __name__ == "__main__":
    asyncio.run(main())
