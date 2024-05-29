import mysql.connector

class MySQLClient:
    def __init__(self, host, user, password, database):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        self.connection.commit()
    
    def execute_many_query(self, query, values):
        try:
            with self.connection.cursor() as cursor:
                self.cursor.executemany(query, values)
            self.connection.commit()
        except Exception as e:
            print(f"Error: {e}")
            self.connection.rollback()

    def fetch_all(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()
    
    def __del__(self):
        self.close()