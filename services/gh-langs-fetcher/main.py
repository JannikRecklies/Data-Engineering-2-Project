import requests, json, pulsar
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer, PulsarConsumer

# initial_obj = {
#     "full_name": "test"
#     "languages_url": "https:/github.api"
#     "commits_url": "https:/github.api"
#     "contents_url": ""
# }


def GetRepoLanguages(token, myConsumer, myProducer):
    gh_obj = json.loads(myConsumer.Consume())
    print("recieved info: {}".format(gh_obj))
    if gh_obj["languages_url"]:
        url = "{}".format(gh_obj["languages_url"])
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "Bearer {}".format(token),
            "X-GitHub-Api-Version": "2022-11-28"
        }
        print("Error: No language ep available for {}".format(gh_obj["full_name"]))

    response = requests.get(url, headers=headers)
    repo_info = {}
    repo_info["languages"] = []
    if response.status_code == 200:
        data = response.json()
        for k in data:
            repo_info["languages"].append(k)
            # print("producing:", json.dumps(repo))
            myProducer.Produce(json.dumps(repo_info))
    else:
        print("Request failed with status code:", response.status_code)
        print("Response body:", response.text)


def main():
    bearer_token = os.getenv("GITHUB_BEARER_TOKEN")
    if not bearer_token:
        print("Error: GitHub bearer token not found in environment variable 'GITHUB_BEARER_TOKEN'")
        return
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
    # export PULSAR_PULL_TOPIC="gh_obj_topic"
    # export PULSAR_PUSH_TOPIC="gh_language_topic"
    # export PULSAR_SUBSCRIPTION="gh_obj_subscription"
    # export PULSAR_EP="pulsar://localhost:6650"
    myConsumer = PulsarConsumer(pulsar_ep, pulsar_pull_topic, pulsar_subscription)
    myProducer = PulsarProducer(pulsar_ep, pulsar_push_topic)
    GetRepoLanguages(bearer_token, myConsumer, myProducer)

    

if __name__ == "__main__":
    main()