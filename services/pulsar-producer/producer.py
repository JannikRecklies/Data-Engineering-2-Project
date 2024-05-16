import pulsar
import os
import time

broker_url = os.getenv('PULSAR_BROKER_URL')  

def pulsar_producer():
    client = pulsar.Client(broker_url)
    producer = client.create_producer('my-topic')

    for i in range(10):
        message = f'Hello Pulsar! {i}'
        producer.send(message.encode('utf-8'))
        print(f'Sent message: {message}')

    while True:
        time.sleep(100)
    
    client.close()

if __name__ == '__main__':
    pulsar_producer()
