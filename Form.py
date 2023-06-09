#==========Python Packages================================
import pandas as pd
import os
from functools import reduce
from itertools import product


class Form:
    def __init__(self, name, df, set_features):
        self.directory = os.getcwd() + '\\' + name + '\\'
        self.name = name
        self.time_func = lambda row: row.partition(' to')[0]
        df = df.rename(self.column_name_transform, axis=1)
        df = df.fillna("")
        df = self.df_map(set_features)(self.toSet, df)
        df = self.removeDuplicates(df)
        self.df = df

    def save(self, df, ext = None):
        file = self.name if ext is None else "{}".format(ext)
        df.to_csv(self.directory + "{}.csv".format(file))

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        df = df.drop(['Timestamp'], axis=1)
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        return df

    def toSet(self, x):
        def appendSet(s, y):
            return s|{y} if len(y) > 0 and y != "set()" else s
        return reduce(appendSet, [y.strip(" {}'") for y in x.split(",")], set())

    def toString(self, x):
        def appendString(s, y):
            return s + y + ", "
        return reduce(appendString, x, "").strip(", ")

    def df_map(self, features):
        def f(my_func, df):
            def transformColumn(df_dict, column):
                return df_dict | {column: [my_func(x) for x in df_dict[column]]}
            return pd.DataFrame(reduce(transformColumn, features, df.to_dict('series')))
        return f

    def column_name_transform(self, column):
        pass

    def makeActive(self, column, value):
        self.active = self.df[self.df[column] == value].reset_index(drop=True)

    def mult_choice(self, column, keys = None, values = None):
        self.df[column] = self.df[column].map(dict(list(zip(keys, values))))

    def checkbox(self, keys, column, options, file):
        x = pd.Series(options).map(lambda opt: (opt, self.active[column].map(lambda s: int(opt in s))))
        df = pd.DataFrame(dict([(key, self.active[key]) for key in keys] + list(x)))
        self.save(df, file)

    def other(self, keys, column, options, file):
        x = (column, self.active[column].map(lambda s: s.difference(set(options))))
        df = pd.DataFrame(dict([(key, self.active[key]) for key in keys] + [x]))
        df = self.df_map({column})(self.toString, df[df[column].map(lambda x: len(x) > 0)].reset_index(drop=True))
        self.save(df, "{}_Other".format(file))

    def checkbox_grid(self, keys, columns, rows, col_func, row_func, file):
        x = pd.Series(product(columns, rows)).map(lambda xy: (col_func(xy[0]), row_func(xy[1])))
        y = x.map(lambda xy: ("{} [{}]".format(xy[0], xy[1]), self.active[xy[1]].map(lambda s: int(xy[0] in s))))
        df = pd.DataFrame(dict([(key, self.active[key]) for key in keys] + list(y)))
        self.save(df, file)

    def long_ans(self, keys, column, file):
        df = self.active[keys + [column]]
        self.save(df[df[column].map(lambda x: len(x) > 0)].reset_index(drop=True), file)









