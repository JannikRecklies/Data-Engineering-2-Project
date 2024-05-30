import requests, json, pulsar
import os
import logging
import mysql.connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

init_query = """CREATE TABLE IF NOT EXISTS repo_commits (
    repository_name VARCHAR(50) PRIMARY KEY,
    commits_count INT
    );"""

# recvd_obj = {
#     "id": 10,
#     "name": "repo_name",
#     "languages": ["python", "ruby", ...],
#     "tdd": True,
#     "devops": True
# }


def persist_repo_commits(msg, db_conn):
    repos_obj = json.loads(msg.data())
    commits_count = repos_obj["commits_count"]
    repo_name = repos_obj["full_name"]
    query = """INSERT INTO repo_commits (repository_name, commits_count) VALUES ({}, {})""".format(repo_name, commits_count)
    try:
        with db_conn.cursor() as cursor:
            cursor.execute(query)
        db_conn.commit()
    except Exception as e:
        logging.error(f"Error: {e}")
        db_conn.rollback()


def main():
    pulsar_ep = os.getenv("PULSAR_EP")
    if not pulsar_ep:
        print("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return
    pulsar_subscription = os.getenv("PULSAR_SUBSCRIPTION")
    if not pulsar_subscription:
        print("Error: Pulsar subscription not found in environment variable 'PULSAR_SUBSCRIPTION'")
        return
    pulsar_pull_topic = os.getenv("PULSAR_PULL_TOPIC")
    if not pulsar_pull_topic:
        print("Error: Pulsar pull topic not found in environment variable 'PULSAR_PULL_TOPIC'")
        return
    db_host = os.getenv("DB_HOST")
    if not db_host:
        print("Error: DB HOST not found in environment variable 'DB_HOST'")
        return
    db_user = os.getenv("DB_USER")
    if not db_user:
        print("Error: DB USER not found in environment variable 'DB_USER'")
        return
    db_pass = os.getenv("DB_PASS")
    if not db_pass:
        print("Error: DB PASS not found in environment variable 'DB_PASS'")
        return
    db_name = os.getenv("DB_NAME")
    if not db_name:
        print("Error: DB NAME not found in environment variable 'DB_NAME'")
        return
    # export PULSAR_PULL_TOPIC="gh_language_aggregator_topic"
    # export PULSAR_SUBSCRIPTION="gh_lang_aggr_subscription_global"
    # export PULSAR_EP="pulsar://localhost:6650"
    # export DB_HOST="127.0.0.1:3306"
    # export DB_USER="user"
    # export DB_PASS="password"
    # export DB_NAME="languages"
    mysql_db_connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )
    try:
        with mysql_db_connection.cursor() as cursor:
            cursor.execute(init_query)
        mysql_db_connection.commit()
    except Exception as e:
        logging.error(f"Error: {e}")
        mysql_db_connection.rollback()

    client = pulsar.Client(pulsar_ep)
    consumer = client.subscribe(topic=pulsar_pull_topic, subscription_name=pulsar_subscription, consumer_type=pulsar.ConsumerType.Shared)

    logging.info("connected to db and pulsar")
    while True:
        msg = consumer.receive()
        logging.info("Received message: '%s'" % msg.data())
        persist_repo_commits(msg, mysql_db_connection)
        consumer.acknowledge(msg)

    client.close()
    mysql_db_connection.close()

if __name__ == "__main__":
    main()
