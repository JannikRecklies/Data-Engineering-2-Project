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

# sudo docker build -t jannikrecklies/gh-tdd-processor:latest .
# sudo docker push jannikrecklies/gh-tdd-processor:latest

# Constants for environment variables
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PULL_TOPIC = "PULSAR_PULL_TOPIC"
PULSAR_PUSH_TOPIC = "PULSAR_PUSH_TOPIC"

# Subscription name for Pulsar consumer
PULSAR_SUBSCRIPTION_NAME = "contents-fetcher-sub"

# List of test indicators to check for TDD
TEST_INDICATORS = [
    'test'
]

def process_message(message, bearer_token, producer):
    """Process a received message and determine if the repository follows TDD."""
    data = json.loads(message.data())
    repo_name = data.get('full_name')
    archive_url = data.get('archive_url').replace('{archive_format}{/ref}', 'zipball')

    if not archive_url:
        error_msg = f"Error: No archive_url available for {repo_name}"
        logging.error(error_msg)
        return

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {bearer_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    try:
        # Download the zip file
        response = requests.get(archive_url, headers=headers)
        if response.status_code == 200:
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as tmpdir:
                # Define the path to the zip file within the temporary directory
                zip_path = os.path.join(tmpdir, f"{repo_name.replace('/', '_')}.zip")
                
                # Write the zip file to the temporary directory
                with open(zip_path, 'wb') as zip_file:
                    zip_file.write(response.content)
                
                # Extract the zip file
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir)

                # Check for test files or directories
                is_repo_tdd = False
                test_files = 0
                for root, dirs, files in os.walk(tmpdir):
                    for name in files:
                        if any(indicator in name.lower() for indicator in TEST_INDICATORS):
                            test_files += 1
                        is_repo_tdd = test_files >= 3
                    for name in dirs:
                        if any(indicator in name.lower() for indicator in TEST_INDICATORS):
                            is_repo_tdd = True
                            break
                    if is_repo_tdd:
                        break
        elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            wait_time_sec = max(reset_time - time.time(), 1)
            wait_time_min = wait_time_sec / 60
            logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min} minutes.")
            time.sleep(wait_time_sec)
        else:
            logging.error("Error fetching data")
            raise Exception(f"Error fetching data: {response.json()}")

    except Exception as e:
        error_msg = f"Error processing repository {repo_name}: {str(e)}"
        logging.error(error_msg)
        data["error"] = error_msg
        # producer.send(json.dumps(data).encode('utf-8'))
        return

    # Add "tdd" key to the data dictionary
    data["tdd"] = is_repo_tdd
    producer.send(json.dumps(data).encode('utf-8'))
    logging.info(f"The repository {repo_name} follows TDD: {is_repo_tdd}")



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
                process_message(msg, bearer_token, producer)
                consumer.acknowledge(msg)
            except Exception as e:
                logging.error(f"Failed to process message: {e}")
                consumer.negative_acknowledge(msg)
    finally:
        client.close()

if __name__ == "__main__":
    main()
