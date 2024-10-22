#==========Python Packages================================
import pandas as pd
import numpy as np
import os
from functools import reduce
from itertools import product

class Form:
    def __init__(self, name, keys, active_pair, multchoice, linscale, textans, checkbox, checkboxgrid):
        self.name, self.keys, self.active_pair = name, keys, active_pair
        self.multchoice, self.linscale, self.textans = multchoice, linscale, textans
        self.checkbox, self.checkboxgrid = checkbox, checkboxgrid
        self.df = self.createDF(checkbox.keys())
        self.active_df = self.getActive_df(self.df)
        self.key_df()
        self.dfs = self.getFullDFDict()

    def save(self, df, file):
        df.to_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(file)]))

    def hashTime(self, s):
        f = lambda weights, s, sep: sum([w * int(t) for (w, t) in zip(weights, s.split(sep))])
        h = lambda l: 1000000 * f([100, 1, 10000], l[0], "/") + f([10000, 100, 1], l[1], ":")
        return h(s.split(" "))

    def removeDuplicates(self, df):
        def appendDF(df_keys, index):
            current_df, key_set, df_row = df_keys + (df.loc[[index], :],)
            keys = tuple(df_row.loc[index, self.keys])
            return (current_df, key_set) if keys in key_set else (pd.concat([df_row, current_df]), key_set | {keys})
        initial_df = pd.DataFrame(columns=df.columns)
        return reduce(appendDF, reversed(df.index), (initial_df, set()))[0].reset_index(drop=True)

    def toSet(self, x):
        appendSet = lambda s, y: s | {y} if len(y) > 0 and y != "set()" else s
        return reduce(appendSet, [self.trim_paren(y.strip(" {}'")) for y in x.split(",")], set())

    def toString(self, x):
        appendString = lambda s, y: s + y + ", "
        return reduce(appendString, x, "").strip(", ")

    def df_map(self, features):
        def f(my_func, df):
            transformColumn = lambda df_dict, column: df_dict | {column: [my_func(x) for x in df_dict[column]]}
            return pd.DataFrame(reduce(transformColumn, features, df.to_dict('series')))
        return f

    def q_map(self, q):
        pass

    def r_map(self, r):
        pass

    def prod_func(self, column):
        question, _, row = column.partition(" [")
        if len(row) == 0:
            return self.q_map(question)
        else:
            return "{} [{}]".format(self.q_map(question), self.r_map(row.strip("]")))

    def createDF(self, set_features):
        df = pd.read_csv("\\".join([os.getcwd(), "Raw Data", "{}.csv".format(self.name)]))
        df = df.rename(self.prod_func, axis=1)
        df = df.fillna("")
        df = self.df_map(set_features)(self.toSet, df)
        df = self.removeDuplicates(df)
        df = self.df_map({"Timestamp"})(self.hashTime, df)
        self.save(df, "original")
        return df

    def getActive_df(self, df):
        if self.active_pair is None:
            return df
        else:
            column, active_option = self.active_pair
            return df[df[column] == active_option]

    def key_df(self):
        try:
            df = pd.read_csv("\\".join([os.getcwd(), self.name, "Keys.csv"]), index_col=0)
            print("Read Keys")
        except FileNotFoundError:
            df = self.df.loc[:, self.keys].sort_values(by=self.keys)
            self.save(df, "Keys")
        return df

    def concatKeys(self, df):
        key_df = self.df.loc[df.index, self.keys]
        return pd.concat([key_df, df], axis=1)

    def lookFirst(self, toCSV_func):
        def f(col):
            try:
                df = pd.read_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(col)]), index_col=0)
                print("Read {}".format(col))
                return df
            except FileNotFoundError:
                df = toCSV_func(col)
                self.save(df, col)
                print("Created {}".format(col))
                return df
        return f

    def getSubDFDict(self, cols, df_func, checkCSV):
        our_func = self.lookFirst(df_func) if checkCSV else df_func
        return dict([(col, our_func(col)) for col in cols])

    def trim_paren(self, v):
        return v.partition(" (")[0]

    def createDict(self, keys, values = None):
        values = keys if values is None else values
        return dict(list(zip(keys, values)))

    def multchoice_dfs(self, source_df = None):
        full_df, active_df = (self.df, self.active_df) if source_df is None else (source_df, self.getActive_df(source_df))
        def f(my_dict):
            def getDF(col):
                my_df = full_df if self.active_pair is not None and col == self.active_pair[0] else active_df
                transform_dict = my_dict[col]
                print(col)
                print(my_df)
                print(my_df[col])
                new_values = my_df[col].map(lambda v: transform_dict.get(self.trim_paren(v), self.trim_paren(v)))
                df = self.concatKeys(pd.DataFrame({col: new_values}, index=my_df.index)).sort_values(by=[col])
                return df
            return self.getSubDFDict(my_dict.keys(), getDF, source_df is None)
        return f

    def linscale_dfs(self, source_df = None):
        active_df = self.active_df if source_df is None else self.getActive_df(source_df)
        def f(cols):
            def getDF(col):
                new_values = active_df[col].map(lambda x: np.nan if x == "" else x)
                df = self.concatKeys(pd.DataFrame({col: new_values}, index=active_df.index)).sort_values(by=[col])
                return df
            return self.getSubDFDict(cols, getDF, source_df is None)
        return f

    def textans_dfs(self, source_df = None):
        active_df = self.active_df if source_df is None else self.getActive_df(source_df)
        def f(cols):
            def getDF(col):
                new_values = active_df[col]
                df = self.concatKeys(pd.DataFrame({col: new_values}, index=active_df.index)).sort_values(by=[col])
                df = df[df[col].map(lambda x: len(x) > 0)]
                return df
            return self.getSubDFDict(cols, getDF, source_df is None)
        return f

    def checkbox_dfs(self, source_df = None):
        active_df = self.active_df if source_df is None else self.getActive_df(source_df)
        def f(my_dict):
            def getDF(col):
                g = my_dict[col]
                df_dict = dict([(g[opt], active_df[col].map(lambda s: int(opt in s))) for opt in g.keys()])
                df = pd.DataFrame(df_dict, index=active_df.index)
                other_values = active_df[col].map(lambda s: self.toString(s.difference(set(g.keys()))))
                other_df = pd.DataFrame({"Other": other_values}, index=active_df.index)
                df = self.concatKeys(pd.concat([df, other_df], axis=1))
                return df
            return self.getSubDFDict(my_dict.keys(), getDF, source_df is None)
        return f

    def checkboxgrid_dfs(self, source_df = None):
        active_df = self.active_df if source_df is None else self.getActive_df(source_df)
        def f(my_dict):
            def getDF(col):
                col_opts, row_opts = my_dict[col]
                x = pd.Series(product(col_opts, row_opts))
                df_dict = dict(list(x.map(lambda ab: ("{} [{}]".format(ab[0], ab[1]),
                                      active_df["{} [{}]".format(col, ab[1])].map(lambda s: int(ab[0] in s))))))
                df = self.concatKeys(pd.DataFrame(df_dict, index=active_df.index))
                return df
            return self.getSubDFDict(my_dict.keys(), getDF, source_df is None)
        return f

    def getFullDFDict(self, source_df = None):
        gs = [self.multchoice_dfs, self.linscale_dfs, self.textans_dfs, self.checkbox_dfs, self.checkboxgrid_dfs]
        data = [self.multchoice, self.linscale, self.textans, self.checkbox, self.checkboxgrid]
        return reduce(lambda d1, d2: d1 | d2, [g(source_df)(x) for g, x in zip(gs, data)], {})

    def mergeDFs(self, dfs_1, dfs_2, df_names = None, save = False):
        df_names = dfs_1.keys() if df_names is None else df_names
        def merge(key):
            df = self.removeDuplicates(pd.concat([dfs_1[key], dfs_2[key]], ignore_index=True))
            if save:
                self.save(df, key)
                print("Updated {}".format(key))
            return df
        return dict([(key, merge(key)) for key in df_names])

    def updateResults(self, month, day, year):
        timestamp = self.hashTime("/".join([str(x) for x in [month, day, year]]) + " 0:00")
        update_with = self.df.loc[lambda df: df["Timestamp"] >= timestamp]
        df_dict = self.getFullDFDict(update_with)
        return self.mergeDFs(self.dfs, df_dict, save = True)

    def nextMap(self, times):
        def go(map, current, remaining):
            if len(remaining) == 0:
                return map
            else:
                next = remaining[0]
                return go(map | {current: next}, next, remaining[1:])
        return go({}, times[0], times[1:]) if len(times) > 0 else {}

    def startTimeAvailability(self, day, df, duration, time_units):
        next_dict = self.nextMap(time_units)
        def cont_hours(hours, current, remaining):
            if remaining == 0:
                return hours
            else:
                next = next_dict[current]
                return cont_hours(hours + [next], next, remaining - 1)
        def f(hour):
            day_hours = ["{} [{}]".format(day, hr) for hr in cont_hours([hour], hour, duration - 1)]
            return df.index.map(lambda i: df.loc[i, day_hours].product())
        return f











