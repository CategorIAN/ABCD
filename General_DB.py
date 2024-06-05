import os
import pandas as pd
from functools import reduce

class General_DB:
    def __init__(self):
        self.name = "General_DB"

    def trim_paren(self, v):
        return v.partition(" (")[0]

    def q_map(self, q):
        columns = pd.read_csv("\\".join([os.getcwd(), self.name, "metadata", "Columns.csv"]))
        col_mapping = dict(list(zip(columns["Question"], columns["Name"])))
        return col_mapping.get(self.trim_paren(q), self.trim_paren(q))

    def r_map(self, r):
        return r.partition(' to')[0]

    def prod_func(self, column):
        question, _, row = column.partition(" [")
        if len(row) == 0:
            return self.q_map(question)
        else:
            return "{} [{}]".format(self.q_map(question), self.r_map(row.strip("]")))

    def createPersonDB(self):
        df = pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)]))
        df = df.rename(self.prod_func, axis=1)
        df = df.fillna("")
        df.to_csv("df.csv")




