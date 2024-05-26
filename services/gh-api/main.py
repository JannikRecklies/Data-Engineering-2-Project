import requests, json, pulsar
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer

# initial_obj = {
#     "full_name": "test"
#     "languages_url": "https:/github.api"
#     "commits_url": "https:/github.api"
#     "contents_url": ""
# }


def GetRepositoriesMetaData(token, myProducer):
    url = "https://api.github.com/repositories?per_page=2"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": "Bearer {}".format(token),
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.get(url, headers=headers)
    repo_info = {}
    if response.status_code == 200:
        data = response.json()
        for repo in data:
            repo_info["full_name"] = repo["full_name"]
            repo_info["languages_url"] = repo["languages_url"]
            repo_info["commits_url"] = repo["commits_url"]
            repo_info["contents_url"] = repo["contents_url"]
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
    pulsar_topic = os.getenv("PULSAR_TOPIC")
    if not pulsar_topic:
        print("Error: Pulsar topic not found in environment variable 'PULSAR_TOPIC'")
        return
    # export PULSAR_TOPIC="gh_obj_topic"
    # export PULSAR_EP="pulsar://localhost:6650"
    myProducer = PulsarProducer(pulsar_ep, pulsar_topic)
    GetRepositoriesMetaData(bearer_token, myProducer)

    

if __name__ == "__main__":
    main()