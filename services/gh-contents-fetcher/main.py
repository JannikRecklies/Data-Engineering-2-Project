import os
import json
import logging
import requests
import zipfile
import time
import tempfile
import pulsar
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# sudo docker build -t jannikrecklies/gh-tdd-devops-processor:latest .
# sudo docker push jannikrecklies/gh-tdd-devops-processor:latest

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

GITHUB_API_SEARCH_URL = "https://api.github.com/repos"

# Constants for environment variables
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PULL_TOPIC = "PULSAR_PULL_TOPIC"
PULSAR_PUSH_TOPIC = "PULSAR_PUSH_TOPIC"

# Subscription name for Pulsar consumer
PULSAR_SUBSCRIPTION_NAME = "contents-fetcher-sub"

# List of test indicators to check for TDD
TEST_INDICATORS = ['test']

# List of DevOps indicators to check for CI/CD practices
DEVOPS_INDICATORS = [
    '.travis.yml',
    'circle.yml',
    '.github/workflows/',
    '.gitlab-ci.yml',
    'Jenkinsfile',
    'azure-pipelines.yml'
]


##################################### Main Functions #####################################

def process_message(message, bearer_token, producer):
    """Process a received message and determine if the repository follows TDD."""
    data = json.loads(message.data())
    trees_urls = data.get('trees_url', '').replace('{/sha}', f"/{data.get('default_branch')}")

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {bearer_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    try:
        
        # logging.info(f"Data: {data}")
        full_name = data["full_name"]
        # logging.info(f"Name: {full_name}")
        # logging.info(f"trees_urls: {data.get('trees_url')}")
        
        request_url = f"{trees_urls}?recursive=1"
        # logging.info(f"URL: {request_url}")
        response = requests.get(request_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            files = data["tree"]
            # logging.info(f"Length: {len(files)}")
        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            handle_rate_limit(response)
        else:
            logging.error("Error fetching data")
            raise Exception(f"Error fetching data: {response.content}")
    
    except Exception as e:
        logging.error(f"Error processing repository {data.get('full_name')}: {str(e)}")

def env_vars_set_correct():
    """Check if the required environment variables are set."""
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PULL_TOPIC, PULSAR_PUSH_TOPIC]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            logging.error(f"Error: {var} not found in environment variables.")
        return False
    return True

def handle_rate_limit(response):
    """Handle GitHub API rate limiting."""
    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
    wait_time_sec = max(reset_time - time.time(), 1)
    wait_time_min = wait_time_sec / 60
    logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min:.2f} minutes.")
    time.sleep(wait_time_sec)

def main():
    """Main function to run the Pulsar consumer."""
    logging.info("Starting application")
    if not env_vars_set_correct():
        logging.error("Environment variables are missing or not set properly")
        return

    bearer_token = os.getenv(GITHUB_BEARER_TOKEN)
    pulsar_broker_url = os.getenv(PULSAR_BROKER_URL)
    pulsar_pull_topic = os.getenv(PULSAR_PULL_TOPIC)
    pulsar_push_topic = os.getenv(PULSAR_PUSH_TOPIC)

    client = pulsar.Client(pulsar_broker_url)
    consumer = client.subscribe(
        topic=pulsar_pull_topic, 
        subscription_name=PULSAR_SUBSCRIPTION_NAME, 
        consumer_type=pulsar.ConsumerType.Shared
    )
    producer = client.create_producer(pulsar_push_topic)

    try:
        while True:
            msg = consumer.receive()
            try:
                # logging.info("Message received")
                process_message(msg, bearer_token, producer)
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
