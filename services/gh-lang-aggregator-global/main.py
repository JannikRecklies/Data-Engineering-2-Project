import requests, json, pulsar
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer, PulsarConsumer
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'mysql_client')))
from mysql_client import MySQLClient

# recvd_obj = {
#     "test": 10,
#     "python": 100,...]
# }

# language table schema:
# col1=lang_name, col2=count

def CalcSum(my_consumer, my_db, table_name):
    langs_obj = json.loads(my_consumer.Consume())
    print("recieved info: {}".format(langs_obj))
    langs_count = {}
    for lang in langs_obj["languages"]:
        langs_count[lang] += 1
        query = "select * from {} where lang_name == {}".format(table_name, lang)
        lang_records = my_db.fetch_all(query)
        new_count = 0
        for l in lang_records:
            new_count = l[1] + 1
        update_query = "UPDATE {} \
        SET count = {} \
        WHERE lang_name = {};".format(table_name, new_count, lang)
        my_db.execute_query(update_query)
    print("total aggregated langauges: ", langs_count)
    # TODO: should there be an interval after which the producer should be called? somethig like rate limiting


def main():
    pulsar_ep = os.getenv("PULSAR_EP")
    if not pulsar_ep:
        print("Error: Pulsar endpoint not found in environment variable 'PULSAR_EP'")
        return
    pulsar_pull_topic = os.getenv("PULSAR_PULL_TOPIC")
    if not pulsar_pull_topic:
        print("Error: Pulsar pull topic not found in environment variable 'PULSAR__PULL_TOPIC'")
        return
    pulsar_subscription = os.getenv("PULSAR_SUBSCRIPTION")
    if not pulsar_subscription:
        print("Error: Pulsar subscription not found in environment variable 'PULSAR_SUBSCRIPTION'")
        return
    pulsar_push_topic = os.getenv("PULSAR_PUSH_TOPIC")
    if not pulsar_subscription:
        print("Error: Pulsar push topic not found in environment variable 'PULSAR_PUSH_TOPIC'")
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
    my_consumer = PulsarConsumer(pulsar_ep, pulsar_pull_topic, pulsar_subscription, pulsar.ConsumerType.Shared)
    mysql_db = MySQLClient(db_host, db_user, db_pass, db_name)
    CalcSum(my_consumer, mysql_db, "languages")

if __name__ == "__main__":
    main()