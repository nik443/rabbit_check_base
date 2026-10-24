from pika import ConnectionParameters, BlockingConnection

connection_params = ConnectionParameters(
    host="localhost",
    port=5672,
)


def process_message(channel, method, properties, body):
    print(f"Message received: {body.decode('utf-8')}")
    try:
        1/0
    except ZeroDivisionError:
        pass
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    with BlockingConnection(connection_params) as conn:
        with conn.channel() as channel:
            channel.queue_declare(queue="messages")
            channel.basic_consume(
                queue="messages",
                on_message_callback=process_message,
            )
            print("Message wait")
            channel.start_consuming()


if __name__ == "__main__":
    main()