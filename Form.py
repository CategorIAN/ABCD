#==========Python Packages================================
import pandas as pd
import numpy as np
import os
from functools import reduce
from itertools import product

class Form:
    def __init__(self, name, q_map, r_map, set_features, keys,
                 active_pair, mult_choice_dict,
                 linscale_cols, text_cols, checkbox_cols, checkbox_optset, checkbox_newoptset, otherset,
                 checkboxgrid_dict):
        df = pd.read_csv("\\".join([os.getcwd(), 'Raw Data', "{}.csv".format(name)]))
        df = df.rename(self.prod_func(q_map, r_map), axis=1)
        df = df.fillna("")
        df = self.df_map(set_features)(self.toSet, df)
        df = self.removeDuplicates(df)
        self.df = df
        self.keys = keys
        self.name = name
        #==========================================================================================================
        self.key_df()
        #==========================================================================================================
        self.active_pair = active_pair
        self.active_df = self.getActive_df()
        '''
        for (col, options, transformed, other) in zip(checkbox_cols, checkbox_optset, checkbox_newoptset, otherset):
            self.checkbox(col, options, transformed, other, active)
        # ==========================================================================================================
        for (col, opts) in checkboxgrid_dict.items():
            col_opts, row_opts = opts
            self.checkbox_grid(col, col_opts, row_opts, active=active)
        # ==========================================================================================================
        '''
        # ==========================================================================================================
        self.mult_choice(mult_choice_dict)
        self.linear_scale(linscale_cols)
        self.text_ans(text_cols)

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
        def appendSet(s, y):
            return s | {y} if len(y) > 0 and y != "set()" else s
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

    def prod_func(self, q_func, r_func):
        def f(column):
            question, _, row = column.partition(" [")
            if len(row) == 0:
                return q_func(question)
            else:
                return "{} [{}]".format(q_func(question), r_func(row.strip("]")))
        return f

    def getActive_df(self):
        if self.active_pair is None:
            return self.df
        else:
            column, active_option = self.active_pair
            return self.df[self.df[column] == active_option].reset_index(drop=True)

    def concatKeys(self, df, full = False):
        key_df = self.df.loc[:, self.keys] if full else self.active_df.loc[:, self.keys]
        return pd.concat([key_df, df], axis=1)

    def filtered(self, column, options, target_index = None):
        target_index = 0 if target_index is None else target_index
        return self.df[self.df[column] == options[target_index]].reset_index(drop=True)

    def key_df(self):
        df = pd.DataFrame(dict([(key, self.df[key]) for key in self.keys]))
        df = df.sort_values(by=self.keys).reset_index(drop=True)
        self.save(df, "Keys")

    def mult_choice(self, dict):
        def toCSV(col):
            source_df, full = (self.df, True) if col == self.active_pair[0] else (self.active_df, False)
            transform_dict = dict[col]
            new_values = source_df[col].map(lambda v: transform_dict.get(v, v))
            df = self.concatKeys(pd.DataFrame({col: new_values}), full).sort_values(by=[col]).reset_index(drop=True)
            self.save(df, col)
        [toCSV(col) for col in dict.keys()]

    def linear_scale(self, cols):
        def toCSV(col):
            new_values = self.active_df[col].map(lambda x: np.nan if x == "" else x)
            df = self.concatKeys(pd.DataFrame({col: new_values})).sort_values(by=[col]).reset_index(drop=True)
            self.save(df, col)
        [toCSV(col) for col in cols]

    def text_ans(self, cols):
        def toCSV(col):
            new_values = self.active_df[col]
            df = self.concatKeys(pd.DataFrame({col: new_values})).sort_values(by=[col])
            df = df[df[col].map(lambda x: len(x) > 0)].reset_index(drop=True)
            self.save(df, col)
        [toCSV(col) for col in cols]

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

    def checkbox_grid(self, column, col_opts, row_opts, col_func = lambda c: c, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        x = pd.Series(product(col_opts, row_opts)).map(lambda ab: (col_func(ab[0]), ab[1]))
        y = x.map(lambda ab: ("{} [{}]".format(ab[0], ab[1]),
                              active["{} [{}]".format(column, ab[1])].map(lambda s: int(ab[0] in s))))
        df = pd.DataFrame(dict([(key, active[key]) for key in self.keys] + list(y)))
        self.save(df, file)










