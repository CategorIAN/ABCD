#==========Python Packages================================
import pandas as pd
import numpy as np
import os
from functools import reduce
from itertools import product


class Form:
    def __init__(self, name, df, keys, set_features):
        self.directory = os.getcwd() + '\\' + name + '\\'
        self.name = name
        self.keys = keys
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
            return s | {y} if len(y) > 0 and y != "set()" else s

        return reduce(appendSet, [y.strip(" {}'") for y in x.split(",")], set())

    def toString(self, x):
        def appendString(s, y):
            return s + y + ", "
        return reduce(appendString, x, "").strip(", ")

    def elem_map(self, mapping = None):
        g = lambda y: y if mapping is None else lambda y: mapping[y]
        def f(x):
            return {g(y) for y in x}
        return f

    def df_map(self, features):
        def f(my_func, df):
            def transformColumn(df_dict, column):
                return df_dict | {column: [my_func(x) for x in df_dict[column]]}
            return pd.DataFrame(reduce(transformColumn, features, df.to_dict('series')))
        return f

    def column_name_transform(self, column):
        pass

    def filtered(self, column, options, target_index = None):
        target_index = 0 if target_index is None else target_index
        return self.df[self.df[column] == options[target_index]].reset_index(drop=True)

    def mult_choice(self, column, options, transformed = None, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        g = dict(list(zip(options, options))) if transformed is None else dict(list(zip(options, transformed)))
        x = (column, active[column].map(g))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + [x]))
        df = df.sort_values(by=[column]).reset_index(drop=True)
        self.save(df, file)

    def linear_scale(self, column, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        x = (column, active[column].map(lambda x: np.nan if x == "" else x))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + [x]))
        df = df.sort_values(by=[column]).reset_index(drop=True)
        self.save(df, file)

    def checkbox(self, column, options, transformed = None, other_opt = False, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        g = dict(list(zip(options, options))) if transformed is None else dict(list(zip(options, transformed)))
        x = pd.Series(options).map(lambda opt: (g[opt], active[column].map(lambda s: int(opt in s))))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + list(x)))
        self.save(df, file)
        if other_opt:
            self.other(column, options, active, file)


    def other(self, column, options, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        x = (column, active[column].map(lambda s: s.difference(set(options))))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + [x]))
        df = self.df_map({column})(self.toString, df[df[column].map(lambda x: len(x) > 0)].reset_index(drop=True))
        self.save(df, "{}_Other".format(file))

    def checkbox_grid(self, columns, rows, col_func, row_func, active = None, file = None):
        active = self.df if active is None else active
        x = pd.Series(product(columns, rows)).map(lambda xy: (col_func(xy[0]), row_func(xy[1])))
        y = x.map(lambda xy: ("{} [{}]".format(xy[0], xy[1]), active[xy[1]].map(lambda s: int(xy[0] in s))))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + list(y)))
        self.save(df, file)

    def long_ans(self, column, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        df = active[self.keys + [column]]
        self.save(df[df[column].map(lambda x: len(x) > 0)].reset_index(drop=True), file)









