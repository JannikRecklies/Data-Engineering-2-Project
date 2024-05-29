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

GITHUB_API_SEARCH_URL = "https://api.github.com/repos" # 5,000 requests per hour

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
    'Dockerfile'
]


def check_for_devops_indicators(files):
    """Check if any of the files matches the indicators."""
    for devops in DEVOPS_INDICATORS:
        if devops in files:
            return True
    return False

def check_for_tdd_indicators(files):
    """Check if any of the files matches the indicators."""
    for test in TEST_INDICATORS:
        if test in files:
            return True
    return False

def process_message(message, bearer_token, producer):
    """Process a received message and determine if the repository follows TDD or has DevOps files."""
    data = json.loads(message.data())
    trees_url = data.get('trees_url', '').replace('{/sha}', f"/{data.get('default_branch')}")
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {bearer_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    try:
        full_name = data["full_name"]
        request_url = f"{trees_url}?recursive=1"

        logging.info(f"Repo to be processed: {full_name}")
        response = requests.get(request_url, headers=headers)
        if response.status_code == 200:
            tree_data = response.json()
            files = tree_data["tree"]
            
            # Check for TDD files
            tdd_files_found = check_for_tdd_indicators(files)
            # Check for DevOps files
            devops_files_found = check_for_devops_indicators(files)
            
            data["tdd_files_found"] = tdd_files_found,
            data["devops_files_found"] = devops_files_found
            
            # Produce result to Pulsar
            producer.send(json.dumps(data).encode('utf-8'))
            
            logging.info(f"Processed repository {full_name}: TDD files found - {tdd_files_found}, DevOps files found - {devops_files_found}")
        
        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            handle_rate_limit(response)
            process_message(message, bearer_token, producer)
        else:
            logging.error("Error fetching additional data")
            raise Exception(f"Error fetching data. Response: {response.content}")
    
    except Exception as e:
        logging.error(f"Error processing message: {data}")
        logging.error(f"Error/Reason: {str(e)}")

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
    time.sleep(wait_time_sec + 60) # extra safegaurd

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
