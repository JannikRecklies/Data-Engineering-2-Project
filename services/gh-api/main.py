import os
import requests
import json
import pulsar

# Environment variable names
GITHUB_BEARER_TOKEN = "GITHUB_BEARER_TOKEN"
PULSAR_BROKER_URL = "PULSAR_BROKER_URL"
PULSAR_PUSH_TOPIC = "PULSAR_PUSH_TOPIC"

# GitHub repositories API URL
GITHUB_REPOS_URL = "https://api.github.com/repositories?per_page=5"

def get_repositories_meta_data(token, pulsar_broker_url, pulsar_topic):
    # Create Pulsar client and producer
    client = pulsar.Client(pulsar_broker_url)
    producer = client.create_producer(pulsar_topic)
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.get(GITHUB_REPOS_URL, headers=headers)
    if response.status_code == 200:
        data = response.json()
        for repo in data:
            repo_info = {
                "full_name": repo["full_name"],
                "languages_url": repo["languages_url"],
                "commits_url": repo["commits_url"],
                "contents_url": repo["contents_url"]
            }
            producer.send(json.dumps(repo_info).encode('utf-8'))
    else:
        print(f"Request failed with status code: {response.status_code}")
        print("Response body:", response.text)
    
    # Close the Pulsar client
    client.close()

def env_vars_set_correct():
    required_env_vars = [GITHUB_BEARER_TOKEN, PULSAR_BROKER_URL, PULSAR_PUSH_TOPIC]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        for var in missing_vars:
            print(f"Error: {var} not found in environment variables.")
        return False
    return True

def main():
    if not env_vars_set_correct():
        print("Environment variables are missing or not set properly")
        return

    bearer_token = os.getenv(GITHUB_BEARER_TOKEN)
    pulsar_broker_url = os.getenv(PULSAR_BROKER_URL)
    pulsar_push_topic = os.getenv(PULSAR_PUSH_TOPIC)

    get_repositories_meta_data(bearer_token, pulsar_broker_url, pulsar_push_topic)

if __name__ == "__main__":
    main()
