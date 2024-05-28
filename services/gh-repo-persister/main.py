import requests, json, pulsar
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PulsarClient')))
from client import PulsarProducer, PulsarConsumer
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'mysql_client')))
from mysql_client import MySQLClient

# recvd_obj = [{
#     "id": 10,
#     "name": "repo_name",
#     "languages": ["python", "ruby", ...],
#     "tdd": True,
#     "devops": True
# }, ...]

def create_insert_update_sql(data):
    sql = """
    INSERT INTO languages (language_name, popularity_count, tdd_count, tdd_and_devops_count)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        popularity_count = VALUES(popularity_count),
        tdd_count = VALUES(tdd_count),
        tdd_and_devops_count = VALUES(tdd_and_devops_count)
    """
    values = []
    for lang, counts in data.items():
        values.append((
            lang,
            counts['popularity_count'],
            counts['tdd_count'],
            counts['tdd_and_devops_count']
        ))
    return sql, values

def update_language_values(languages_table):
    query = "select * from languages"
    languages_record = my_db.fetch_all(query)
    for row in languages_record:
        language_name = row[0]
        language_popularity = row[1]
        language_tdd = row[2]
        language_t_and_d = row[3]
        languages_table[language_name] = languages_table[language_name]["popularity_count"] + language_popularity
        languages_table[language_name] = languages_table[language_name]["tdd_count"] + language_tdd
        languages_table[language_name] = languages_table[language_name]["tdd_and_devops_count"] + language_t_and_d
    query, values = create_insert_update_sql(languages_table)
    return query, values



# language_name | popularity_count | tdd_count | tdd_and_devops_count
# python        |     100     |     70    |      50

def CalcSum(my_consumer, my_db, table_name):
    repos_obj = json.loads(my_consumer.Consume())
    # print("recieved info: {}".format(langs_obj))
    languages_table = {}
    # langs_count = { "lang1": {"popularity_count": count_repos, "tdd_count": tdd_count, "tdd_and_devops_count": t_and_d_count}, {"lang2": {...}}}
    for repo in repos_obj:
        for language in repo["languages"]:
            if not languages_table[language]:
                languages_table[language] = {
                    "popularity_count": 0, "tdd_count": 0, "tdd_and_devops_count": 0
                }
            if repo["tdd"] and repo["devops"]:
                languages_table[language]["tdd_count"] += 1
                languages_table[language]["tdd_and_devops_count"] += 1
            elif repo["tdd"]:
                languages_table[language]["tdd_count"] += 1
            languages_table[language]["popularity_count"] += 1

    query, values = update_language_values(languages_table)
    my_db.execute_many_query(update_query)


        langs_count[lang] += 1
        
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