import pulsar, logging

class PulsarClient:

    def __init__(self, pulsar_ep):
        self.client = pulsar.Client(pulsar_ep)


    def __del__(self):
        self.client.close()



class PulsarConsumer(PulsarClient):
    def __init__(self, pulsar_ep, topic, sub_name):
        PulsarClient.__init__(self, pulsar_ep)
        self.Subscribe(topic, sub_name)


    def Consume(self):
        msg = self.subscription.receive()
        try:
            self.subscription.acknowledge(msg)
            return msg.data()
        except:
            self.subscription.negative_acknowledge(msg)
            logging.exception("Error: Ack-ing message failed")


    def Subscribe(self, topic, sub_name):
        self.subscription = self.client.subscribe(topic, subscription_name=sub_name)
    



class PulsarProducer(PulsarClient):
    def __init__(self, pulsar_ep, topic):
        PulsarClient.__init__(self, pulsar_ep)
        self.Producer(topic)
        logging.info("connected to pulsar at {} and ready to publish to topic: {}".format(pulsar_ep, topic))

    def Producer(self, topic):
        self.producer = self.client.create_producer(topic)


    def Produce(self, msg, encoding='utf-8'):
        self.producer.send((msg).encode(encoding))
        logging.info("msg: {}\nsent!".format(msg))



