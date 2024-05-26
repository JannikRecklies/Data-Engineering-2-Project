import pulsar

def process_message(message):
    # Process your message here
    print("Processing message:", message)

def update_database(data):
    # Code to update your database
    print("Updating database with:", data)

def main():
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('topic-input', subscription_name='my-subscription', consumer_type=pulsar.ConsumerType.Shared)

    try:
        while True:
            msg = consumer.receive()
            try:
                process_message(msg.data())
                consumer.acknowledge(msg)
            except Exception as e:
                print("Failed to process message:", e)
                consumer.negative_acknowledge(msg)
    finally:
        client.close()

if __name__ == "__main__":
    main()

