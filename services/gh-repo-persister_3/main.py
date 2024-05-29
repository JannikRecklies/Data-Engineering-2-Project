import requests, json, pulsar
import os
import logging
import mysql.connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)

init_query = """CREATE TABLE IF NOT EXISTS languages (
    language_name VARCHAR(50) PRIMARY KEY,
    popularity_count INT,
    tdd_count INT,
    tdd_and_devops_count INT
);"""

# recvd_obj = {
#     "id": 10,
#     "name": "repo_name",
#     "languages": ["python", "ruby", ...],
#     "tdd": True,
#     "devops": True
# }

def create_insert_update_sql(data):
    sql = """
    INSERT INTO languages (language_name, popularity_count, tdd_count, tdd_and_devops_count)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        popularity_count = VALUES(popularity_count),
        tdd_count = VALUES(tdd_count),
        tdd_and_devops_count = VALUES(tdd_and_devops_count)
    """
    values = [(lang, counts['popularity_count'], counts['tdd_count'], counts['tdd_and_devops_count'])
              for lang, counts in data.items()]
    return sql, values

def update_language_values(languages_table, mysql_db):
    query = "select * from languages"
    languages_record = ""
    with mysql_db.cursor() as cursor:
        cursor.execute(query)
        languages_record = cursor.fetchall()
    for row in languages_record:
        language_name = row[0]
        language_popularity = row[1]
        language_tdd = row[2]
        language_t_and_d = row[3]
        if language_name in languages_table:
            languages_table[language_name]["popularity_count"] += language_popularity
            languages_table[language_name]["tdd_count"] += language_tdd
            languages_table[language_name]["tdd_and_devops_count"] += language_t_and_d
            logging.info(f"Updated language {language_name}, with pop_count: {languages_table[language_name]['popularity_count']}, tdd_count: {languages_table[language_name]['tdd_count']}, t_and_d_count: {languages_table[language_name]['tdd_and_devops_count']}")
    query, values = create_insert_update_sql(languages_table)
    logging.info("values to be inserted are: {}\n---\n".format(values))
    return query, values



# language_name | popularity_count | tdd_count | tdd_and_devops_count
# python        |     100     |     70    |      50

def calculate_sum(msg, db_conn):
    repos_obj = json.loads(msg.data())
    languages_table = {}
    # langs_count = { "lang1": {"popularity_count": count_repos, "tdd_count": tdd_count, "tdd_and_devops_count": t_and_d_count}, {"lang2": {...}}}
    for language in repos_obj["languages"]:
        if language not in languages_table:
            languages_table[language] = {
                "popularity_count": 0, "tdd_count": 0, "tdd_and_devops_count": 0
            }
        if "tdd_files_found" in repos_obj and "devops_files_found" in repos_obj:
            if repos_obj["tdd_files_found"] and repos_obj["devops_files_found"]:
                languages_table[language]["tdd_count"] += 1
                languages_table[language]["tdd_and_devops_count"] += 1
            elif repos_obj["tdd_files_found"]:
                languages_table[language]["tdd_count"] += 1
        languages_table[language]["popularity_count"] += 1

    query, values = update_language_values(languages_table, db_conn)
    try:
        with db_conn.cursor() as cursor:
            cursor.executemany(query, values)
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
    # mysql_db = MySQLClient(db_host, db_user, db_pass, db_name)
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
        calculate_sum(msg, mysql_db_connection)
        consumer.acknowledge(msg)

    client.close()
    mysql_db_connection.close()

if __name__ == "__main__":
    main()
