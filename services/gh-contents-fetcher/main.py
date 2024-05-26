import os    
import pulsar
import requests
import json


bearer_token = os.getenv("GITHUB_BEARER_TOKEN") 
pulsar_broker_url = os.getenv('PULSAR_BROKER_URL') # export PULSAR_BROKER_URL="pulsar://localhost:6650"
pulsar_pull_topic = os.getenv("PULSAR_PULL_TOPIC") # export PULSAR_PULL_TOPIC="gh_obj_topic" 
pulsar_push_topic = os.getenv("PULSAR_PUSH_TOPIC") # export PULSAR_PUSH_TOPIC="gh_language_topic"
pulsar_subscription_name = "contents-fetcher-sub"


def check_tdd(contents_url):
    test_indicators = ['test', 'tests', 'spec', '__tests__', 'pytest.ini', 'tox.ini', 'jest.config.js', 'karma.conf.js', 'nose.cfg', '.travis.yml', 'circle.yml', 'Jenkinsfile', '.github/workflows']
    
    def fetch_contents(url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return []

    def is_test_file_or_dir(item):
        return any(indicator in item['name'].lower() for indicator in test_indicators)

    def check_directory(url):
        contents = fetch_contents(url)
        for item in contents:
            if is_test_file_or_dir(item):
                return True
            if item['type'] == 'dir':
                if check_directory(item['url']):
                    return True
        return False

    return check_directory(contents_url)

def process_message(message):
    print("Received message: '%s'" % message.data())
    data = json.loads(message.data())
    repo_name = data.get('full_name')
    contents_url = data.get('contents_url').replace('{+path}', '')

    if check_tdd(contents_url):
        print(f"The repository {repo_name} follows TDD.")
    else:
        print(f"The repository {repo_name} does not follow TDD.")


def env_vars_set_correct():
    if not bearer_token:
        print("Error: GitHub bearer token not found in environment variable 'GITHUB_BEARER_TOKEN'")
        return False
    if not pulsar_broker_url:
        print("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return False
    if not pulsar_pull_topic:
        print("Error: Pulsar pull topic not found in environment variable 'PULSAR_PULL_TOPIC'")
        return False
    if not pulsar_push_topic:
        print("Error: Pulsar subscription not found in environment variable 'PULSAR_PUSH_TOPIC'")
        return False
    
    return True

def main():
    if not env_vars_set_correct():
        print("Env variables are missing and or not set properly")
        return
        
    client = pulsar.Client(pulsar_broker_url)
    consumer = client.subscribe(topic=pulsar_pull_topic, subscription_name=pulsar_subscription_name, consumer_type=pulsar.ConsumerType.Shared)
    try:
        while True:
            msg = consumer.receive()
            try:
                process_message(msg)
                consumer.acknowledge(msg)
            except Exception as e:
                print(f"Failed to process message: {e}")
                consumer.negative_acknowledge(msg)
    finally:
        client.close()

if __name__ == "__main__":
    main()