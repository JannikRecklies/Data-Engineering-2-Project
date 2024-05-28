import os
import requests
import json
import time
import pulsar
from datetime import datetime, timedelta
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer

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
PULSAR_PUSH_TOPIC2 = "PULSAR_PUSH_TOPIC2"

GITHUB_REPOS_URL = "https://api.github.com/search/repositories" # 30 requests per minute
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
GITHUB_REPOS_PER_FETCH = "GITHUB_REPOS_PER_FETCH"

# Helper function to fetch repositories for a specific date
def fetch_repositories_for_date(date, headers, github_repos_per_fetch, pulsar_producers):
    page = 1
    while True:
        #logging.info(f"Fetching {GITHUB_REPOS_PER_PAGE} repositories per page")
        query = f"q=created:{date}&sort=updated&order=desc&per_page={github_repos_per_fetch}&page={page}"
        response = requests.get(f"{GITHUB_REPOS_URL}?{query}", headers=headers)
        if response.status_code == 200:

            data = response.json()
            repsoitories = data["items"]
            logging.info(f"Request url: {response.request.url}")
            #logging.info(f"Received github repos of length: {len(repsoitories)}")
            for repo in repsoitories:
                repo_info = {}
                repo_info["full_name"] = repo["full_name"]
                repo_info["trees_url"] = repo["trees_url"]
                repo_info["languages_url"] = repo["languages_url"]
                repo_info["commits_url"] = repo["commits_url"]
                # logging.info("producing: " + json.dumps(repo_info))
                for producer in pulsar_producers:
                  producer.Produce(json.dumps(repo_info))


            if 'next' not in response.links:
                logging.info(f"Fetched all pages for a day")
                break

            page += 1
            time.sleep(10)  # Respect rate limits with a delay between requests 
        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            wait_time_sec = max(reset_time - time.time(), 1)
            wait_time_min = wait_time_sec / 60
            logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min} minutes.")
            time.sleep(wait_time_sec)
        else:
            logging.error(f"Error fetching data: {response.content}")
            raise Exception(f"Error fetching data: {response.json()}")


# Function to fetch repositories for the past year
def fetch_repositories_for_past_year(bearer_token, github_repos_per_fetch, pulsar_producers):
    headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {bearer_token}",
            "X-GitHub-Api-Version": "2022-11-28"
    }

    today = datetime.now()
    one_year_ago = today - timedelta(days=365)

    current_date = one_year_ago
    i = 0
    while current_date <= today:
        formatted_date = current_date.strftime('%Y-%m-%d')
        logging.info(f"Fetching repositories for date: {formatted_date}; (Progress: {i}/365)")
        fetch_repositories_for_date(formatted_date, headers, github_repos_per_fetch, pulsar_producers)
        current_date += timedelta(days=1)
        i = i + 1
        time.sleep(5)  # Respect rate limits with a delay between requests


def env_vars_set_correct():
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PUSH_TOPIC1, PULSAR_PUSH_TOPIC2, GITHUB_REPOS_PER_FETCH]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            logging.error(f"Error: {var} not found in environment variables.")
        return False
    return True

def main():
    logging.info("Starting application")
    if not env_vars_set_correct():
        logging.error("Environment variables are missing or not set properly")
        return

    pulsar_broker_url = os.getenv(PULSAR_BROKER_URL)
    pulsar_push_topic1 = os.getenv(PULSAR_PUSH_TOPIC1)
    pulsar_push_topic2 = os.getenv(PULSAR_PUSH_TOPIC2)

    bearer_token = os.getenv(GITHUB_BEARER_TOKEN)
    github_repos_per_fetch = os.getenv(GITHUB_REPOS_PER_FETCH)

    producer1 = PulsarProducer(pulsar_broker_url, pulsar_push_topic1)
    producer2 = PulsarProducer(pulsar_broker_url, pulsar_push_topic2)

    fetch_repositories_for_past_year(bearer_token, github_repos_per_fetch, [producer1, producer2])

    logging.info("All repositories for the last year fetched. Closing client.")
    client.close()


if __name__ == "__main__":
    main()
