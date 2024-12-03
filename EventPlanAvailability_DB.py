import os
import pandas as pd
from functools import reduce
import psycopg2
import numpy as np
from itertools import product
from prettytable import PrettyTable

class EventPlanAvailability_DB:
    def __init__(self):
        self.name = "EventPlanAvailability_DB"
        self.keys = ["Name", "Event Plan"]
        self.df = self.createDF()
        form_types = ["Keys", "Text", "LinScale", "MultChoice"]
        sql_types = ["VARCHAR(160) PRIMARY KEY", "VARCHAR(160)", "INT", "VARCHAR(160)"]
        self.type_dict = dict(zip(form_types, sql_types))

    def removeDuplicates(self, df):
        def appendDF(df_keys, index):
            current_df, key_set, df_row = df_keys + (df.loc[[index], :],)
            keys = tuple(df_row.loc[index, self.keys])
            return (current_df, key_set) if keys in key_set else (pd.concat([df_row, current_df]), key_set | {keys})
        initial_df = pd.DataFrame(columns=df.columns)
        return reduce(appendDF, reversed(df.index), (initial_df, set()))[0].reset_index(drop=True)

    def save(self, df, file):
        df.to_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(file)]))

    def hashTime(self, s):
        f = lambda weights, s, sep: sum([w * int(t) for (w, t) in zip(weights, s.split(sep))])
        h = lambda l: 1000000 * f([100, 1, 10000], l[0], "/") + f([10000, 100, 1], l[1], ":")
        return h(s.split(" "))

    def trim_paren(self, v):
        return v.partition(" (")[0]

    def q_map(self, q):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
        col_mapping = dict(list(zip(form_columns["Question"], form_columns["Name"])))
        return col_mapping.get(self.trim_paren(q), self.trim_paren(q))

    def r_map(self, r):
        return r.partition(' to')[0]

    def prod_func(self, column):
        question, _, row = column.partition(" [")
        if len(row) == 0:
            return self.q_map(question)
        else:
            return "{} [{}]".format(self.q_map(question), self.r_map(row.strip("]")))

    def createDF(self):
        ren = lambda df: df.rename(self.prod_func, axis=1)
        f = lambda df: df.fillna("")
        rem = lambda df: self.removeDuplicates(df)
        trim = lambda df: df.applymap(self.trim_paren)
        df = trim(rem(f(ren(pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)]))))))
        self.save(df, "original")
        return df

    def createPersonEventplanTimespan(self, cursor):
        create_stmt = """
            CREATE VIEW Person_Eventplan_Timespan AS
            Select PersonID, eventplanid, week, timespan
            From Person_Eventplan_Availability
                JOIN event_plan
                on person_eventplan_availability.eventplanid = event_plan.name
                JOIN Availability_Timespan
                on Person_Eventplan_Availability.dayhour = Availability_Timespan.availabilityid
                JOIN Timespan_Duration on availability_timespan.timespan = timespan_duration.name
                WHERE timespan_duration.duration = event_plan.duration
                Group By  personid, eventplanid, week, timespan
                Having COUNT(Distinct Availability_Timespan.availabilityid) = (
                    SELECT COUNT(*)
                    FROM Availability_Timespan as Availability_Timespan_inner
                    WHERE Availability_Timespan_Inner.timespan = Availability_Timespan.timespan
                );
        """
        cursor.execute(create_stmt)

    def createPersonEventplanTimespanCount(self, cursor):
        create_stmt = """
            CREATE VIEW Person_Eventplan_Timespan_Count AS
            SELECT EVENTPLANID, WEEK, PERSON_EVENTPLAN_TIMESPAN.TIMESPAN, 
            COUNT(*) AS PRIMARY_COUNT, NUMBERAVAILABLE AS SECONDARY_COUNT
            FROM Person_Eventplan_Timespan
            JOIN event_plan
            on person_eventplan_timespan.eventplanid = event_plan.name
            JOIN timespan_gamecount
            on person_eventplan_timespan.timespan = timespan_gamecount.timespan 
            and event_plan.game = timespan_gamecount.gamesid
            GROUP BY EVENTPLANID, WEEK, PERSON_EVENTPLAN_TIMESPAN.TIMESPAN, numberavailable
            ORDER BY PRIMARY_COUNT DESC, SECONDARY_COUNT DESC
        """
        cursor.execute(create_stmt)

    def availabilityDF(self, cursor):
        query = """
                SELECT ID, COLUMNNAME, ROWNAME
                FROM AVAILABILITY JOIN AVAILABILITY_COLUMN ON AVAILABILITY.COLUMNID = AVAILABILITY_COLUMN.COLUMNID
                JOIN AVAILABILITY_ROW ON AVAILABILITY.ROWID = AVAILABILITY_ROW.ROWID
        """
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        data = [[str(x) for x in tuple(y)] for y in cursor.fetchall()]
        return pd.DataFrame(data, columns=columns)

    def insertSubmission(self, name, timestamp):
        def execute(cursor):
            insert_stmt = f"""
                INSERT INTO FORM_SUBMISSIONS(PERSON, FORM, TIMESTAMP) VALUES {(name, 'Event Plan Availability', timestamp)};
                """
            cursor.execute(insert_stmt)
        return execute

    def insertPersonEventplanAvailability(self, df, opts, name, event_plan):
        def execute(cursor):
            data = self.df[(self.df["Name"] == name) & (self.df["Event Plan"] == event_plan)].iloc[0]
            cols = lambda week, row: set(data[f"Week {week} [{row}]"].split(", "))
            f = lambda col, row: df[(df["columnname"] == col) & (df["rowname"] == row)].iloc[0]["id"]
            values = lambda week: ",\n".join([str((name, event_plan, week, f(col, row)))
                                             for col, row in opts if col in cols(week, row)])
            insert_stmt = lambda week: f"""
                            INSERT INTO PERSON_EVENTPLAN_AVAILABILITY (personid, eventplanid, week, dayhour) 
                            VALUES \n{values(week)}\n;
                            """
            commands = [insert_stmt(week) for week in [1, 2, 3, 4] if len(values(week)) > 0]
            for command in commands:
                print(command)
                cursor.execute(command)
        return execute

    def updatePersonEventplanAvailability(self, name, event_plan):
        def execute(cursor):
            delete_stmt = f"""
                            DELETE FROM PERSON_EVENTPLAN_AVAILABILITY
                            WHERE PERSONID = '{name}' AND EVENTPLANID = '{event_plan}';
                            """
            cursor.execute(delete_stmt)
            df = self.availabilityDF(cursor)
            opts = set(product(df["columnname"], df["rowname"]))
            self.insertPersonEventplanAvailability(df, opts, name, event_plan)(cursor)
        return execute

    def updateData(self, month, day, year):
        hashed_time = self.hashTime("/".join([str(x) for x in [month, day, year]]) + " 0:00")
        update_index = [i for i in self.df.index if self.hashTime(self.df.at[i, "Timestamp"]) >= hashed_time]
        def execute(cursor):
            for i in update_index:
                timestamp, name, event_plan = self.df.loc[i, ["Timestamp"] + self.keys]
                self.insertSubmission(name, timestamp)(cursor)
                self.updatePersonEventplanAvailability(name, event_plan)(cursor)
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