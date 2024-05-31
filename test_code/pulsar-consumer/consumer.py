import pulsar
import os

broker_url = os.getenv('PULSAR_BROKER_URL') 
subscription_name = 'shared-sub'

def pulsar_consumer():    
    client = pulsar.Client(broker_url)
    consumer = client.subscribe(topic='my-topic', subscription_name=subscription_name, consumer_type=pulsar.ConsumerType.Shared)

    while True:
        msg = consumer.receive()
        print("Received message: '%s'" % msg.data())
        consumer.acknowledge(msg)

    client.close()

if __name__ == '__main__':
    pulsar_consumer()