#==========Python Packages================================
import pandas as pd
import numpy as np
import os
from functools import reduce
from itertools import product

class Form:
    def __init__(self, name, q_map, r_map, set_features, keys,
                 make_active, multchoice_cols, multchoice_optset, multchoice_newoptset,
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
        if make_active:
            self.mult_choice(multchoice_cols[0], multchoice_optset[0], multchoice_newoptset[0])
            active = self.filtered(multchoice_cols[0], multchoice_optset[0])
            mult_choice_tuples = zip(multchoice_cols[1:], multchoice_optset[1:], multchoice_newoptset[1:])
        else:
            active = None
            mult_choice_tuples = zip(multchoice_cols, multchoice_optset, multchoice_newoptset)
        for (col, options, transformed) in mult_choice_tuples:
            self.mult_choice(col, options, transformed, active)
        #==========================================================================================================
        for col in linscale_cols:
            self.linear_scale(col, active)
        #==========================================================================================================
        for col in text_cols:
            self.text_ans(col, active)
        # ==========================================================================================================
        for (col, options, transformed, other) in zip(checkbox_cols, checkbox_optset, checkbox_newoptset, otherset):
            self.checkbox(col, options, transformed, other, active)
        # ==========================================================================================================
        for (col, opts) in checkboxgrid_dict.items():
            col_opts, row_opts = opts
            self.checkbox_grid(col, col_opts, row_opts, active=active)

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

    def filtered(self, column, options, target_index = None):
        target_index = 0 if target_index is None else target_index
        return self.df[self.df[column] == options[target_index]].reset_index(drop=True)

    def key_df(self):
        df = pd.DataFrame(dict([(key, self.df[key]) for key in self.keys]))
        df = df.sort_values(by=self.keys).reset_index(drop=True)
        self.save(df, "Keys")

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

    def text_ans(self, column, active = None, file = None):
        active = self.df if active is None else active
        file = column if file is None else file
        df = active[self.keys + [column]]
        self.save(df[df[column].map(lambda x: len(x) > 0)].reset_index(drop=True), file)

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










