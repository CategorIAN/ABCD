#==========Python Packages================================
import pandas as pd
import numpy as np
import os
from functools import reduce
from itertools import product

class Form:
    def __init__(self, name, set_features, keys, active_pair, mult_choice, linscale, text, checkbox, checkboxgrid):
        self.df = self.createDF(name, set_features)
        self.name, self.keys, self.active_pair = name, keys, active_pair
        self.csvs = self.getCSVnames(mult_choice, linscale, text, checkbox, checkboxgrid)
        self.active_df = self.getActive_df()
        self.key_df()
        self.mult_choice_dfs(mult_choice)
        self.linear_scale_dfs(linscale)
        self.text_ans_dfs(text)
        self.checkbox_dfs(checkbox)
        self.checkbox_grid_dfs(checkboxgrid)

    def save(self, df, file):
        df.to_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(file)]))

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        df = df.drop(['Timestamp'], axis=1)
        df.reset_index(drop=True, inplace=True)
        return df

    def toSet(self, x):
        appendSet = lambda s, y: s | {y} if len(y) > 0 and y != "set()" else s
        return reduce(appendSet, [y.strip(" {}'") for y in x.split(",")], set())

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

    def prod_func(self):
        def f(column):
            question, _, row = column.partition(" [")
            if len(row) == 0:
                return self.q_map(question)
            else:
                return "{} [{}]".format(self.q_map(question), self.r_map(row.strip("]")))
        return f

    def createDF(self, name, set_features):
        df = pd.read_csv("\\".join([os.getcwd(), 'Raw Data', "{}.csv".format(name)]))
        df = df.rename(self.prod_func(), axis=1)
        df = df.fillna("")
        df = self.df_map(set_features)(self.toSet, df)
        df = self.removeDuplicates(df)
        return df

    def getActive_df(self):
        if self.active_pair is None:
            return self.df
        else:
            column, active_option = self.active_pair
            return self.df[self.df[column] == active_option].reset_index(drop=True)

    def key_df(self):
        try:
            df = pd.read_csv("\\".join([os.getcwd(), self.name, "Keys.csv"]), index_col=0)
            print("Read Keys")
        except FileNotFoundError:
            df = self.df.loc[:, self.keys].sort_values(by=self.keys).reset_index(drop=True)
            self.save(df, "Keys")
        return df

    def concatKeys(self, df, full = False):
        key_df = self.df.loc[:, self.keys] if full else self.active_df.loc[:, self.keys]
        return pd.concat([key_df, df], axis=1)

    def lookFirst(self, toCSV_func, other_opt = False):
        def f(col):
            try:
                df = pd.read_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(col)]), index_col=0)
                print("Read {}".format(col))
                return df
            except FileNotFoundError:
                df = toCSV_func(col)
                return df
        return f

    def mult_choice_dfs(self, my_dict):
        def toCSV(col):
            f = lambda pair: (self.df, True) if pair is not None and col == pair[0] else (self.active_df, False)
            source_df, full = f(self.active_pair)
            transform_dict = my_dict[col]
            new_values = source_df[col].map(lambda v: transform_dict.get(v, v))
            df = self.concatKeys(pd.DataFrame({col: new_values}), full).sort_values(by=[col]).reset_index(drop=True)
            self.save(df, col)
            return df
        return [self.lookFirst(toCSV)(col) for col in my_dict.keys()]

    def linear_scale_dfs(self, cols):
        def toCSV(col):
            new_values = self.active_df[col].map(lambda x: np.nan if x == "" else x)
            df = self.concatKeys(pd.DataFrame({col: new_values})).sort_values(by=[col]).reset_index(drop=True)
            self.save(df, col)
            return df
        return [self.lookFirst(toCSV)(col) for col in cols]

    def text_ans_dfs(self, cols):
        def toCSV(col):
            new_values = self.active_df[col]
            df = self.concatKeys(pd.DataFrame({col: new_values})).sort_values(by=[col])
            df = df[df[col].map(lambda x: len(x) > 0)].reset_index(drop=True)
            self.save(df, col)
            return df
        return [self.lookFirst(toCSV)(col) for col in cols]

    def other_df(self, col, options):
        new_values = self.active_df[col].map(lambda s: s.difference(set(options)))
        df = self.concatKeys(pd.DataFrame({col: new_values})).sort_values(by=[col])
        df = self.df_map({col})(self.toString, df[df[col].map(lambda x: len(x) > 0)].reset_index(drop=True))
        self.save(df, "{}_Other".format(col))
        return df

    def checkbox_dfs(self, my_dict):
        def toCSV(col):
            g, other_opt = my_dict[col]
            h = lambda name: name if g[name] is None else g[name]
            df = pd.DataFrame(dict([(h(opt), self.active_df[col].map(lambda s: int(opt in s))) for opt in g.keys()]))
            self.save(self.concatKeys(df), col)
            if other_opt:
                df_other = self.other_df(col, g.keys())
                return df, df_other
            else:
                return df
        [toCSV(col) for col in my_dict.keys()]

    def checkbox_grid_dfs(self, my_dict):
        def toCSV(col):
            col_opts, row_opts = my_dict[col]
            x = pd.Series(product(col_opts, row_opts))
            y = x.map(lambda ab: ("{} [{}]".format(ab[0], ab[1]),
                                  self.active_df["{} [{}]".format(col, ab[1])].map(lambda s: int(ab[0] in s))))
            df = self.concatKeys(pd.DataFrame(dict(list(y))))
            self.save(df, col)
        [toCSV(col) for col in my_dict.keys()]

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

    def getCSVnames(self, mult_choice, linscale, text, checkbox, checkboxgrid):
        names_main = list(mult_choice.keys()) + linscale + text + list(checkbox.keys()) + list(checkboxgrid.keys())
        names_other = ["{}_Other".format(key) for key in checkbox.keys() if checkbox[key][1]]
        return sorted(names_main + names_other)











