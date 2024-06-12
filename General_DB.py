import os
import pandas as pd
from functools import reduce
import psycopg2
import numpy as np

class General_DB:
    def __init__(self):
        self.name = "General_DB"
        self.keys = ["Email"]
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

    def trim_paren(self, v):
        return v.partition(" (")[0]

    def q_map(self, q):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
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

    def df_map(self, features, my_func):
        def f(df):
            transformColumn = lambda df_dict, column: df_dict | {column: [my_func(x) for x in df_dict[column]]}
            return pd.DataFrame(reduce(transformColumn, features, df.to_dict('series')))
        return f

    def save(self, df, file):
        df.to_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(file)]))

    def hashTime(self, s):
        f = lambda weights, s, sep: sum([w * int(t) for (w, t) in zip(weights, s.split(sep))])
        h = lambda l: 1000000 * f([100, 1, 10000], l[0], "/") + f([10000, 100, 1], l[1], ":")
        return h(s.split(" "))

    def createDF(self):
        ren = lambda df: df.rename(self.prod_func, axis=1)
        f = lambda df: df.fillna("")
        h = lambda df: self.df_map({"Timestamp"}, self.hashTime)(df)
        rem = lambda df: self.removeDuplicates(df)
        df = h(rem(f(ren(pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)]))))))
        self.save(df, "original")
        return df

    def transform(self, form_type, df):
        def f(id):
            if form_type == "MultChoice":
                sub_df = df[df["ID"] == id]
                transform_dict = dict(zip(sub_df["OldValue"], sub_df["NewValue"]))
                return lambda v: transform_dict.get(self.trim_paren(v), self.trim_paren(v))
            else:
                return lambda v: v
        return f

    def getTypeNames(self, form_type, with_sql_type = False):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
        type_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "{}.csv".format(form_type)]))
        df = pd.merge(form_columns, type_columns, how="inner", left_on="ID", right_on="ColumnID")
        sql_type = " " + self.type_dict[form_type] if with_sql_type else ""
        name = lambda id: "{}{}".format(df[df["ID"] == id]["Name"].iat[0], sql_type)
        transform_func = self.transform(form_type, df)
        return dict([(name(col_id), transform_func(col_id)) for col_id in set(df.ID)])

    def getDBDict(self, with_sql_type = False):
        return reduce(lambda d, t: d | self.getTypeNames(t, with_sql_type), self.type_dict.keys(), {})

    def createPersonDB(self):
        db_cols = ",\n".join(self.getDBDict(True).keys())
        return "CREATE TABLE person (\n{}\n);".format(db_cols)

    def insertPersonRows(self):
        db_dict = self.getDBDict()
        db_cols = list(db_dict.keys())
        df = self.df[db_cols]
        appendRow = lambda rows, i: rows + [str(tuple([db_dict[col](df.at[i, col]) for col in db_cols]))]
        rows = ",\n".join(reduce(appendRow, df.index, [])).replace("''", "NULL")
        return "INSERT INTO person ({}) VALUES\n{};".format(", ".join(db_cols), rows)

    def createCheckBoxDBs(self):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
        type_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "CheckBox.csv"]))
        df = pd.merge(form_columns, type_columns, how="inner", left_on="ID", right_on="ColumnID")
        name = lambda id: df[df["ID"] == id]["Name"].iat[0]
        db_cols = ",\n".join(["Name VARCHAR(160) PRIMARY KEY", "Other BOOLEAN"])
        stmt = lambda id: "CREATE TABLE {} (\n{}\n);".format(name(id), db_cols)
        return [stmt(col_id) for col_id in set(df.ID)]

    def insertCheckBoxRows(self):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
        type_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "CheckBox.csv"]))
        df = pd.merge(form_columns, type_columns, how="inner", left_on="ID", right_on="ColumnID")
        name = lambda id: df[df["ID"] == id]["Name"].iat[0]
        values = lambda id: ",\n".join([str((name, False)) for name in df[df["ID"] == id]["NewValue"]])
        stmt = lambda id: "INSERT INTO {} (Name, Other) VALUES \n{}\n;".format(name(id), values(id))
        return [stmt(col_id) for col_id in set(df.ID)]

    def executeSQL(self, commands):
        try:
            connection = psycopg2.connect(user = "postgres",
                                          password = "WeAreGroot",
                                          host = "database-1.cbeq26equftn.us-east-2.rds.amazonaws.com",
                                          port = "5432",
                                          database = "postgres")
            cursor = connection.cursor()
            for command in commands:
                cursor.execute(command)
                connection.commit()
                print(10 * "=" + "Executed" + 10 * "=" + "\n" + command)

        except psycopg2.Error as e:
            print(e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")






