import pandas as pd
import psycopg2
from prettytable import PrettyTable
from tabulate import tabulate
from functools import reduce


class Event_DB:
    def __init__(self):
        self.nextHour = self.nextMap(["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM",
                                        "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM", "7:00 PM", "8:00 PM"])

    def queried_df(self, cursor, query):
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [[str(x) for x in tuple(y)] for y in cursor.fetchall()]
        return pd.DataFrame(data=data, columns=columns)

    def updateNext(self, cursor):
        def update_stmt(i):
            col, row = df.loc[i, ['columnid', 'next']]
            print(col, row)
            index = None if row == "None" else df[(df['columnid'] == col) & (df['rowid'] == row)].index[0]
            return f"UPDATE AVAILABILITY SET Next = '{index}' WHERE ID = '{i}';".replace("'None'", "NULL")
        query = """
            SELECT Availability.ID, Availability.ColumnID, Availability.RowID, Availability_Row.Next
            From Availability Inner Join Availability_Row ON Availability.RowID = Availability_Row.RowID;
            """
        df = self.queried_df(cursor, query).set_index('id')
        for i in df.index:
            cursor.execute(update_stmt(i))

    def nextMap(self, times):
        def go(map, current, remaining):
            if len(remaining) == 0:
                return map
            else:
                next = remaining[0]
                return go(map | {current: next}, next, remaining[1:])
        return go({}, times[0], times[1:]) if len(times) > 0 else {}

    def insertTimeSpanRows(self, duration):
        def f(cursor):
            def go(indices, current, remaining):
                if remaining == 0:
                    return indices
                else:
                    return None if current == "None" else go(indices + [current], df.at[current, 'next'], remaining - 1)
            def timespan(start):
                times = go([start], df.at[start, 'next'], duration - 1)
                if times is None:
                    return None
                else:
                    day, starthour = tuple(df.loc[start, ['columnname', 'rowname']])
                    lasthour = self.nextHour[df.at[times[-1], 'rowname']]
                    name = f"'{day} from {starthour} to {lasthour}'"
                    name_insert = f"INSERT INTO TimeSpan (Name) Values (\n{name}\n);"
                    values = ",\n".join([f"({time}, {name})" for time in times])
                    values_stmt = f"INSERT INTO Availability_TimeSpan (AvailabilityID, TimeSpan) VALUES \n{values}\n;"
                    return [name_insert, values_stmt]

            query = """
                SELECT Availability.ID, Availability.Next, Availability_Column.ColumnName, Availability_Row.RowName
                FROM AVAILABILITY INNER JOIN AVAILABILITY_COLUMN ON AVAILABILITY.ColumnID = Availability_Column.ColumnID
                INNER JOIN Availability_Row ON Availability.RowID = Availability_Row.RowID;
                """

            df = self.queried_df(cursor, query).set_index('id')
            insert_stmts = reduce(lambda l, i: l if timespan(i) is None else l + timespan(i), df.index, [])
            for stmt in insert_stmts:
                print(stmt)
                cursor.execute(stmt)
        return f






    def readSQL(self, commands):
        try:
            connection = psycopg2.connect(user="postgres",
                                          password="WeAreGroot",
                                          host="database-1.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port="5432",
                                          database="postgres")
            cursor = connection.cursor()
            for command in commands:
                df = self.queried_df(cursor, command)
                pretty_df = tabulate(df, headers='keys', tablefmt='pretty')
                print(pretty_df)
                print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)
            connection.commit()

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")


    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "database-1.cbeq26equftn.us-east-2.rds.amazonaws.com",
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