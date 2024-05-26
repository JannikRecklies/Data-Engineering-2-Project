import os
import requests
import json
import time
import pulsar
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# sudo docker build -t jannikrecklies/gh-metadata-fetcher:latest .
# sudo docker push jannikrecklies/gh-metadata-fetcher:latest

GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PUSH_TOPIC = "PULSAR_PUSH_TOPIC"

GITHUB_REPOS_URL = "https://api.github.com/repositories"
GITHUB_REPOS_PER_PAGE = 1000

# Helper function to fetch repositories for a specific date
def fetch_repositories_for_date(date, headers, pulsar_producer):
    page = 1
    while True:
        query = f"q=created:{date}..{date}&sort=updated&order=desc&per_page={GITHUB_REPOS_PER_PAGE}&page={page}"
        response = requests.get(f"{GITHUB_REPOS_URL}?{query}", headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.json()}")
        
        data = response.json()
        # Process data['items']
        repo_info = {}
        
        for repo in data:
            repo_info["full_name"] = repo["full_name"]
            repo_info["languages_url"] = repo["languages_url"]
            repo_info["commits_url"] = repo["commits_url"]
            repo_info["contents_url"] = repo["contents_url"]
            # print("producing:", json.dumps(repo))
            pulsar_producer.send(json.dumps(repo_info).encode('utf-8'))
        
        if 'next' not in response.links:
            break
        
        page += 1
        # print(f"request url: {response.request.url}")
        time.sleep(10)  # Respect rate limits with a delay between requests
    

# Function to fetch repositories for the past year
def fetch_repositories_for_past_year(bearer_token, pulsar_producer):
    headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {bearer_token}",
            "X-GitHub-Api-Version": "2022-11-28"
    }
    
    today = datetime.utcnow().date()
    one_year_ago = today - timedelta(days=1)
    
    current_date = one_year_ago
    i = 0
    while current_date <= today:
        logging.info(f"Fetching repositories for date: {current_date}; (Progress: {i}/365)")
        fetch_repositories_for_date(current_date, headers, pulsar_producer)
        current_date += timedelta(days=1)
        i = i + 1
        time.sleep(5)  # Respect rate limits with a delay between requests
    

def env_vars_set_correct():
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PUSH_TOPIC]
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

    bearer_token = os.getenv(GITHUB_BEARER_TOKEN)
    pulsar_broker_url = os.getenv(PULSAR_BROKER_URL)
    pulsar_push_topic = os.getenv(PULSAR_PUSH_TOPIC)

    client = pulsar.Client(pulsar_broker_url)
    producer = client.create_producer(pulsar_push_topic)
    
    fetch_repositories_for_past_year(bearer_token, producer)
    
    client.close()

if __name__ == "__main__":
    main()
