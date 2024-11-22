import pandas as pd
import psycopg2

class FormManagement_DB:
    def __init__(self):
        pass

    def queried_df(self, cursor, query):
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [[str(x) for x in tuple(y)] for y in cursor.fetchall()]
        return pd.DataFrame(data=data, columns=columns)

    def request(self, timestamp, form):
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

    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "abcd.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
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