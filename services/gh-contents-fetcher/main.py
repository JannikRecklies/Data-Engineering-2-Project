import os
import json
import logging
import requests
import zipfile
import time
import tempfile
import pulsar

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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

def process_message(message, bearer_token, producer):
    """Process a received message and determine if the repository follows TDD."""
    data = json.loads(message.data())
    repo_name = data.get('full_name')
    archive_url = data.get('archive_url', '').replace('{archive_format}{/ref}', 'zipball')

    if not archive_url:
        logging.error(f"Error: No archive_url available for {repo_name}")
        return

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    try:
        logging.info(f"Starting archieve_url request: {archive_url}")
        response = requests.get(archive_url, headers=headers)
        logging.info(f"Response for request")
        if response.status_code == 200:
            logging.info(f"Response 200")
            with tempfile.TemporaryDirectory() as tmpdir:
                logging.info(f"Starting to work with downloaded project")
                zip_path = os.path.join(tmpdir, f"{repo_name.replace('/', '_')}.zip")
                with open(zip_path, 'wb') as zip_file:
                    zip_file.write(response.content)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)
                
                logging.info(f"Checking for tdd and devops")
                is_repo_tdd = check_for_tdd(tmpdir)
                is_repo_devops = check_for_devops(tmpdir)
                data["tdd"] = is_repo_tdd
                data["devops"] = is_repo_devops
                producer.send(json.dumps(data).encode('utf-8'))
                logging.info(f"The repository {repo_name} follows TDD ({is_repo_tdd}) and devops ({is_repo_devops})")

        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            handle_rate_limit(response)
        else:
            logging.error("Error fetching data")
            raise Exception(f"Error fetching data: {response.json()}")

    except Exception as e:
        logging.error(f"Error processing repository {repo_name}: {str(e)}")
        return

def check_for_tdd(directory):
    """Check for test files or directories to determine if the repository follows TDD."""
    test_files = 0
    for root, dirs, files in os.walk(directory):
        for name in files:
            if any(indicator in name.lower() for indicator in TEST_INDICATORS):
                test_files += 1
        if test_files >= 3:
            return True
        for name in dirs:
            if any(indicator in name.lower() for indicator in TEST_INDICATORS):
                return True
    return False

def check_for_devops(directory):
    """Check for CI/CD configuration files to determine if the repository follows DevOps practices."""
    for root, dirs, files in os.walk(directory):
        for name in files:
            if any(indicator in os.path.join(root, name).lower() for indicator in DEVOPS_INDICATORS):
                return True
        for name in dirs:
            if any(indicator in os.path.join(root, name).lower() for indicator in DEVOPS_INDICATORS):
                return True
    return False

def handle_rate_limit(response):
    """Handle GitHub API rate limiting."""
    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
    wait_time_sec = max(reset_time - time.time(), 1)
    wait_time_min = wait_time_sec / 60
    logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min:.2f} minutes.")
    time.sleep(wait_time_sec)

def env_vars_set_correct():
    """Check if the required environment variables are set."""
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PULL_TOPIC, PULSAR_PUSH_TOPIC]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            logging.error(f"Error: {var} not found in environment variables.")
        return False
    return True

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
                logging.info("Message received")
                process_message(msg, bearer_token, producer)
                consumer.acknowledge(msg)
            except Exception as e:
                logging.error(f"Failed to process message: {e}")
                consumer.negative_acknowledge(msg)
    finally:
        logging.info("Closing client")
        client.close()

if __name__ == "__main__":
    main()
