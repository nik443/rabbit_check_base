from pika import ConnectionParameters, BlockingConnection

connection_params = ConnectionParameters(
    host="localhost",
    port=5672,
)


def main():
    with BlockingConnection(connection_params) as conn:
        with conn.channel() as channel:
            channel.queue_declare(queue="messages")
            channel.basic_publish(
                exchange="",
                routing_key="messages",
                body="Hello RabbinMQ!",
            )
            print("Message sent")


if __name__ == "__main__":
    main()