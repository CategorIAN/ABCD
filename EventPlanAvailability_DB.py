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
        print(df)
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
        df = rem(f(ren(pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)])))))
        self.save(df, "original")
        return df

    def insertSubmission(self, name, timestamp):
        def execute(cursor):
            insert_stmt = f"""
                INSERT INTO FORM_SUBMISSIONS(PERSON, FORM, TIMESTAMP) VALUES {(name, 'Event Plan Availability', timestamp)};
                """
            cursor.execute(insert_stmt)
        return execute

    def insertPersonEventplanAvailability(self, df, q_name, opts, name, event_plan):
        data = self.df[(self.df["Name"] == name) & (self.df["Event Plan"] == event_plan)].iloc[0]
        cols = lambda qid, row: set(data[f"{q_name(qid)} [{row}]"].split(", "))
        f = lambda qid, col, row: df[(df["ID"] == qid) & (df["ColumnName"] == col) & (df["RowName"] == row)].index[0]
        values = lambda qid: ",\n".join([str((data["Name"], self.trim_paren(data["Event Plan"]),
                                              int(q_name(qid).strip("Week ")), f(qid, col, row)))
                                         for col, row in opts(qid) if col in cols(qid, row)])
        insert_stmt = lambda qid: f"""
                        INSERT INTO PERSON_EVENTPLAN_AVAILABILITY (personid, eventplanid, week, dayhour) 
                        VALUES \n{values(qid)}\n;
                        """
        return [insert_stmt(qid) for qid in set(df.ID) if len(values(qid)) > 0]

    def updatePersonEventplanAvailability(self, name, event_plan):
        def execute(cursor):
            delete_stmt = f"""
                            DELETE FROM PERSON_EVENTPLAN_AVAILABILITY
                            WHERE PERSONID = '{name}' AND EVENTPLANID = '{event_plan}';
                            """
            cursor.execute(delete_stmt)
            questions = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Questions.csv"]))
            grid_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridColumn.csv"]))
            grid_rows = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "GridRow.csv"]))
            append_df = lambda left_df, right_df: left_df.merge(right_df, how="inner", left_on="ID", right_on="QID")
            df = append_df(append_df(questions, grid_columns), grid_rows)
            q_df = lambda qid: df[df["ID"] == qid].reindex()
            q_name = lambda qid: q_df(qid)["Name"].iat[0]
            opts = lambda qid: set(product(df[df["ID"] == qid]["ColumnName"], df[df["ID"] == qid]["RowName"]))
            commands = self.insertPersonEventplanAvailability(df, q_name, opts, name, event_plan)
            for command in commands:
                print(command)
                cursor.execute(command)
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