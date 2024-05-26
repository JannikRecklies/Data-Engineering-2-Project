import os
import requests
import json
import pulsar

# Constants for environment variables
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PULL_TOPIC = "PULSAR_PULL_TOPIC"
PULSAR_PUSH_TOPIC = "PULSAR_PUSH_TOPIC"

# Subscription name for Pulsar consumer
PULSAR_SUBSCRIPTION_NAME = "contents-fetcher-sub"

# List of test indicators to check for TDD
TEST_INDICATORS = [
    'test', 'tests', 'spec', '__tests__', 'pytest.ini', 'tox.ini', 
    'jest.config.js', 'karma.conf.js', 'nose.cfg', '.travis.yml', 
    'circle.yml', 'Jenkinsfile', '.github/workflows'
]

def fetch_contents(url, headers):
    """Fetch the contents of a GitHub repository directory from a given URL."""
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    return []

def is_test_file_or_dir(item):
    """Check if a given item is a test file or directory."""
    return any(indicator in item['name'].lower() for indicator in TEST_INDICATORS)

def check_directory(url, headers):
    """Recursively check a directory for test files or directories."""
    contents = fetch_contents(url, headers)
    for item in contents:
        if is_test_file_or_dir(item):
            return True
        if item['type'] == 'dir':
            if check_directory(item['url'], headers):
                return True
    return False

def check_tdd(contents_url, headers):
    """Check if a repository follows TDD based on its contents."""
    return check_directory(contents_url, headers)

def process_message(message, bearer_token):
    """Process a received message and determine if the repository follows TDD."""
    print(f"Received message: '{message.data()}'")
    data = json.loads(message.data())
    repo_name = data.get('full_name')
    contents_url = data.get('contents_url').replace('{+path}', '')

    if contents_url:
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {bearer_token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }
    else:
        print(f"Error: No contents_url available for {repo_name}")
        return

    if check_tdd(contents_url, headers):
        print(f"The repository {repo_name} follows TDD.")
    else:
        print(f"The repository {repo_name} does not follow TDD.")

def env_vars_set_correct():
    """Check if the required environment variables are set."""
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PULL_TOPIC, PULSAR_PUSH_TOPIC]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            print(f"Error: {var} not found in environment variables.")
        return False
    return True

def main():
    """Main function to run the Pulsar consumer."""
    if not env_vars_set_correct():
        print("Environment variables are missing or not set properly")
        return

    bearer_token = os.getenv(GITHUB_BEARER_TOKEN)
    pulsar_broker_url = os.getenv(PULSAR_BROKER_URL)
    pulsar_pull_topic = os.getenv(PULSAR_PULL_TOPIC)

    client = pulsar.Client(pulsar_broker_url)
    consumer = client.subscribe(
        topic=pulsar_pull_topic, 
        subscription_name=PULSAR_SUBSCRIPTION_NAME, 
        consumer_type=pulsar.ConsumerType.Shared
    )
    try:
        while True:
            msg = consumer.receive()
            try:
                process_message(msg, bearer_token)
                consumer.acknowledge(msg)
            except Exception as e:
                print(f"Failed to process message: {e}")
                consumer.negative_acknowledge(msg)
    finally:
        client.close()

if __name__ == "__main__":
    main()
