import os
import pandas as pd
from functools import reduce

class General_DB:
    def __init__(self):
        self.name = "General_DB"

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
        rename = lambda df: df.rename(self.prod_func, axis=1)
        fillna = lambda df: df.fillna("")
        hashTime = lambda df: self.df_map({"Timestamp"}, self.hashTime)(df)
        df = hashTime(fillna(rename(pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)])))))
        self.save(df, "original")
        return df

    def getTypeNames(self, form_type, sql_type):
        form_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
        type_columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "{}.csv".format(form_type)]))
        names = set(pd.merge(form_columns, type_columns, how="inner", left_on="ID", right_on="ColumnID")["Name"])
        return ["{} {}".format(name, sql_type) for name in names]

    def createPersonDB(self):
        form_types = ["Keys", "Text", "LinScale", "MultChoice"]
        sql_types = ["VARCHAR(160) PRIMARY KEY", "VARCHAR(160)", "INT", "VARCHAR(160)"]
        create = ",\n".join(reduce(lambda l, t: l + self.getTypeNames(*t), list(zip(form_types, sql_types)), []))
        return "CREATE TABLE person (\n{}\n);".format(create)





