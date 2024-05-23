import requests, json, pulsar
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer, PulsarConsumer

# recvd_obj = {
#     "languages": ["test", "python",...]
# }


def CalcSum(myConsumer, myProducer):
    langs_obj = json.loads(myConsumer.Consume())
    print("recieved info: {}".format(langs_obj))
    langs_count = {}
    for lang in langs_obj["languages"]:
        if not langs_count[lang]:
            langs_count[lang] = 0
        langs_count[lang] += 1
    # store somewhere


def main():
    pulsar_ep = os.getenv("PULSAR_EP")
    if not pulsar_ep:
        print("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return
    pulsar_pull_topic = os.getenv("PULSAR_PULL_TOPIC")
    if not pulsar_pull_topic:
        print("Error: Pulsar pull topic not found in environment variable 'PULSAR__PULL_TOPIC'")
        return
    pulsar_subscription = os.getenv("PULSAR_SUBSCRIPTION")
    if not pulsar_subscription:
        print("Error: Pulsar subscription not found in environment variable 'PULSAR_SUBSCRIPTION'")
        return
    pulsar_push_topic = os.getenv("PULSAR_PUSH_TOPIC")
    if not pulsar_subscription:
        print("Error: Pulsar push topic not found in environment variable 'PULSAR_PUSH_TOPIC'")
        return
    # export PULSAR_PULL_TOPIC="gh_language_topic"
    # export PULSAR_PUSH_TOPIC="gh_language_aggregator_topic"
    # export PULSAR_SUBSCRIPTION="gh_lang_aggregator_subscription"
    # export PULSAR_EP="pulsar://localhost:6650"
    myConsumer = PulsarConsumer(pulsar_ep, pulsar_pull_topic, pulsar_subscription, pulsar.ConsumerType.Shared)
    myProducer = PulsarProducer(pulsar_ep, pulsar_push_topic)
    CalcSum(myConsumer, myProducer)

    

if __name__ == "__main__":
    main()