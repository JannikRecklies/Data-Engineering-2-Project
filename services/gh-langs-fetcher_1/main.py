import requests, json, pulsar
import os
import sys
import logging
import time

# initial_obj = {
#     "full_name": "test"
#     "languages_url": "https:/github.api"
#     "commits_url": "https:/github.api"
#     "contents_url": ""
# }

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def handle_rate_limit(response):
    """Handle GitHub API rate limiting."""
    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
    wait_time_sec = max(reset_time - time.time(), 1)
    wait_time_min = wait_time_sec / 60
    logging.warning(f"Rate limit exceeded. Waiting for {wait_time_min:.2f} minutes.")
    time.sleep(wait_time_sec + 60) # extra safegaurd
    logging.info("waking from my slumber\n\n")

def GetRepoLanguages(msg, token, producer):
    gh_obj = json.loads(msg.data())
    logging.info("recieved info: {}".format(gh_obj))
    if gh_obj["languages_url"]:
        url = "{}".format(gh_obj["languages_url"])
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": "Bearer {}".format(token),
            "X-GitHub-Api-Version": "2022-11-28"
        }
    else:
        logging.error("Error: No language ep available for {}".format(gh_obj["full_name"]))
        return
    response = requests.get(url, headers=headers)
    gh_obj["languages"] = []
    if response.status_code == 200:
        data = response.json()
        for k in data:
            gh_obj["languages"].append(k)
            # this dies not take into account that some repositiories might be empty, 
            # so in the subsequent modules
            # we can, perhaps, first filter out the repos that are empty and then continue
    elif response.status_code == 403 and 'API rate limit exceeded' in response.text:
        handle_rate_limit(response)
    else:
        logging.error("Request failed with status code: {}".format(response.status_code))
        logging.error("Response body: {}".format(response.text))
        return
    logging.info("the new repo object along with languages:: {}\n\n\n".format(json.dumps(gh_obj)))
    producer.send(json.dumps(gh_obj).encode('utf-8'))

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
                GetRepoLanguages(msg, bearer_token, producer)
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