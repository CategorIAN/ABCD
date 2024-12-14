import pandas as pd
import psycopg2
from datetime import datetime
from decouple import config

class FormManagement_DB:
    def __init__(self):
        pass

    def queried_df(self, cursor, query):
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [[str(x) for x in tuple(y)] for y in cursor.fetchall()]
        return pd.DataFrame(data=data, columns=columns)

    def request(self, form, timestamp = None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if timestamp is None else timestamp
        print(timestamp)
        def execute(cursor):
            go = 'y'
            while go == 'y' or go == 'Y':
                columns = ["Person"]
                values = []
                for column in columns:
                    value = input(f"{column}: ")
                    values.append(value)
                invite_stmt = (f"Insert INTO form_requests (Timestamp, Form, Person) VALUES "
                        f"{(timestamp, form, values[0])};".replace("''", "NULL"))
                cursor.execute(invite_stmt)
                go = input("Continue? [y/n]: ")
        return execute

    def submission(self, form):
        def execute(cursor):
            go = 'y'
            while go == 'y' or go == 'Y':
                columns = ["Person", "Timestamp"]
                values = []
                for column in columns:
                    value = input(f"{column}: ")
                    values.append(value)
                submission_stmt = (f"Insert INTO form_submissions (Form, Person, Timestamp) VALUES "
                        f"{(form,) + tuple(values)};".replace("''", "NULL"))
                cursor.execute(submission_stmt)
                go = input("Continue? [y/n]: ")
        return execute

    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = config("DB_USER"),
                                          password = config("DB_PASSWORD"),
                                          host = config("DB_HOST"),
                                          port = config("DB_PORT"),
                                          database = config("DB_DATABASE"))
            cursor = connection.cursor()
            for command in commands:
                command(cursor)
            connection.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")