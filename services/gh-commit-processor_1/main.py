import os
import requests
import json
import time
import pulsar
from datetime import datetime, timedelta
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# initial_obj = [{
#     "id": some_num
#     "full_name": "test"
#     "languages_url": "https:/github.api"
#     "commits_url": "https:/github.api"
#     "contents_url": ""
# }, ...]

# sudo docker build -t jannikrecklies/gh-metadata-fetcher:latest .
# sudo docker push jannikrecklies/gh-metadata-fetcher:latest

PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PUSH_TOPIC1 = "PULSAR_PUSH_TOPIC1"
PULSAR_PULL_TOPIC = "PULSAR_PULL_TOPIC"
PULSAR_PULL_SUBSCRIPTION = "PULSAR_PULL_SUBSCRIPTION"

GITHUB_REPOS_URL = "https://api.github.com/search/repositories" # 30 requests per minute
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"

def get_next_page_url(headers, next_page_str):
    if "Link" in headers:
        for link in headers["Link"].split(','):
            if next_page_str in link:
                return link.split(';')[0].strip().strip('<>')
    return None


def fetch_repo_commits(bearer_token, message, pulsar_producer):
    headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {bearer_token}",
            "X-GitHub-Api-Version": "2022-11-28"
    }
    repo_info = json.loads(message.data())
    commit_url = repo_info["commits_url"].replace("{/sha}", "")
    num_commits = 0
    while True:
        response = requests.get(commit_url, headers=headers)
        if response.status_code == 200:
            resp = response.json()
            num_commits += len(resp)
            commit_url = get_next_page_url(response.headers, 'rel="next"')
            if commit_url is None:
                logging.info("Fetched all commits for {}".format(repo_info["full_name"]))
                repo_info["commits_count"] = num_commits
                pulsar_producer.send(json.dumps(repo_info).encode('utf-8'))
                return
            time.sleep(10)  # Respect rate limits with a delay between requests 
        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            wait_time_sec = max(reset_time - time.time(), 1)
            wait_time_min = wait_time_sec / 60
            logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min} minutes.")
            time.sleep(wait_time_sec + 60)
            logging.info("waking from my slumber\n\n")
        else:
            logging.error(f"Error fetching data: {response.content}")
            raise Exception(f"Error fetching data: {response.json()}")


def env_vars_set_correct():
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PUSH_TOPIC1, PULSAR_PULL_TOPIC, PULSAR_PULL_SUBSCRIPTION]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            logging.error(f"Error: {var} not found in environment variables.")
        return False
    return True

def main():
    bearer_token = os.getenv("GITHUB_BEARER_TOKEN")
    if not bearer_token:
        logging.error("Error: GitHub bearer token not found in environment variable 'GITHUB_BEARER_TOKEN'")
        return
    pulsar_ep = os.getenv("PULSAR_EP")
    if not pulsar_ep:
        logging.error("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return
    pulsar_pull_topic = os.getenv("PULSAR_PULL_TOPIC")
    if not pulsar_pull_topic:
        logging.error("Error: Pulsar pull topic not found in environment variable 'PULSAR__PULL_TOPIC'")
        return
    pulsar_subscription = os.getenv("PULSAR_SUBSCRIPTION")
    if not pulsar_subscription:
        logging.error("Error: Pulsar subscription not found in environment variable 'PULSAR_SUBSCRIPTION'")
        return
    pulsar_push_topic = os.getenv("PULSAR_PUSH_TOPIC")
    if not pulsar_subscription:
        logging.error("Error: Pulsar push topic not found in environment variable 'PULSAR_PUSH_TOPIC'")
        return
    # export PULSAR_PULL_TOPIC="gh_obj_topic"
    # export PULSAR_PUSH_TOPIC="gh_language_topic"
    # export PULSAR_SUBSCRIPTION="gh_obj_subscription"
    # export PULSAR_EP="pulsar://localhost:6650"
    client = pulsar.Client(pulsar_ep)
    consumer = client.subscribe(
        topic=pulsar_pull_topic, 
        subscription_name=pulsar_subscription, 
        consumer_type=pulsar.ConsumerType.Shared
    )
    producer = client.create_producer(pulsar_push_topic)

    try:
        while True:
            msg = consumer.receive()
            try:
                # logging.info("Message received")
                fetch_repo_commits(bearer_token, msg, producer)
                consumer.acknowledge(msg)
                # logging.info("Acked messaged")
            except Exception as e:
                logging.error(f"Failed to process message: {e}")
                consumer.negative_acknowledge(msg)
    finally:
        logging.info("Closing client")
        client.close()

    

if __name__ == "__main__":
    main()
