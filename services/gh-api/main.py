import requests, json, pulsar
import os
import sys
import datetime, time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer

# initial_obj = [{
#     "id": some_num
#     "full_name": "test"
#     "languages_url": "https:/github.api"
#     "commits_url": "https:/github.api"
#     "contents_url": ""
# }, ...]

def MakeRequest(token, producers, gh_url="https://api.github.com/", repos_per_page=50):
    one_year_ago = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    print("starting to fetch repositories from (date): {}\n\n".format(one_year_ago))
    url = gh_url + "repositories?per_page={}&q=created:>{}".format(repos_per_page, one_year_ago)
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": "Bearer {}".format(token),
        "X-GitHub-Api-Version": "2022-11-28"
    }
    next_page_str = 'rel="next"'
    while True:
        response = requests.get(url, headers=headers)
        url = ""
        if response.status_code == 200:
            data = response.json()
            GetRepositoriesMetaData(data, producers)
            for link in response.headers["Link"].split(','):
                if next_page_str in link:
                    link_parts = link.split(';')
                    for part in link_parts:
                        if next_page_str not in part:
                            # time to remove the < and > sign
                            url = part.replace("<", "").replace(">", "")
        elif response.status_code == 422:
            print("endpoint spammed, time to take a break\n\n")
            time.sleep(300) # 5 minutes
            continue
        if url == "":
            break
        print("response status: {}\nnextPage: {}\n\n".format(response.ok, url))


def GetRepositoriesMetaData(data, my_producers):
    repo_info = []
    for repo in data:
        tmp = {}
        tmp["id"] = repo["id"]
        tmp["full_name"] = repo["full_name"]
        tmp["languages_url"] = repo["languages_url"]
        tmp["commits_url"] = repo["commits_url"]
        tmp["contents_url"] = repo["contents_url"]
        repo_info.append(tmp)
        # print("producing:", json.dumps(repo))
    for producer in my_producers:
        producer.Produce(json.dumps(repo_info))

def main():
    bearer_token = os.getenv("GITHUB_BEARER_TOKEN")
    if not bearer_token:
        print("Error: GitHub bearer token not found in environment variable 'GITHUB_BEARER_TOKEN'")
        return
    pulsar_ep = os.getenv("PULSAR_EP")
    if not pulsar_ep:
        print("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return
    pulsar_topic1 = os.getenv("PULSAR_TOPIC1")
    if not pulsar_topic1:
        print("Error: Pulsar topic not found in environment variable 'PULSAR_TOPIC1'")
        return
    pulsar_topic2 = os.getenv("PULSAR_TOPIC2")
    if not pulsar_topic2:
        print("Error: Pulsar topic not found in environment variable 'PULSAR_TOPIC2'")
        return
    # export PULSAR_TOPIC2="gh_obj_topic_for_commits"
    # export PULSAR_TOPIC1="gh_obj_topic"
    # export PULSAR_EP="pulsar://localhost:6650"
    producer1 = PulsarProducer(pulsar_ep, pulsar_topic1)
    producer2 = PulsarProducer(pulsar_ep, pulsar_topic2)
    MakeRequest(token=bearer_token, producers=[producer1, producer2], repos_per_page=10)

if __name__ == "__main__":
    main()
