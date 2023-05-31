import os
import pandas as pd
from itertools import product

class Availability_Specific:
    def __init__(self):
        self.form_hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
             "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]
        self.hours = pd.Series(self.form_hours).map(lambda h: h.partition(' to')[0])
        self.days = ["Friday", "Saturday", "Sunday"]
        self.wk_hours = lambda wk: ["Weekend #{} [{}]".format(wk, h) for h in self.hours]

        df = pd.read_csv("\\".join([os.getcwd(), 'Raw Data', "GA_rawdata.csv"]))
        df = df.rename(self.column_name_transform, axis=1)
        df = df.fillna("")
        df = self.removeDuplicates(df)
        df = df.drop(['Timestamp'], axis=1)
        people = pd.read_csv("\\".join([os.getcwd(), 'ABCD', "ABCD_cleaned.csv"]), index_col=0)[['Email', 'Name']]
        df = people.merge(right = df, how = 'inner', on = 'Email')
        df = df.sort_values(by='Name')
        df.reset_index(drop=True, inplace=True)
        self.df = df
        self.df.to_csv("\\".join([os.getcwd(), 'Game Availability', 'GA_cleaned.csv']))

    def availability2(self, names, month, duration = None):
        index = self.df.loc[lambda df: df["Name"].isin(names)].index
        for wk in range(1, 5):
            wk_hours = self.wk_hours(wk)
            wk_df = self.df.loc[index, ["Email", "Name"] + wk_hours].rename(dict(list(zip(wk_hours, self.hours))), axis=1)
            x = pd.Series(product(self.days, self.hours))
            y = x.map(lambda dh: ("{} [{}]".format(dh[0], dh[1]), wk_df[dh[1]].map(lambda s: int(dh[0] in s))))
            df = pd.DataFrame(dict([(fix, wk_df[fix]) for fix in ['Email', 'Name']] + list(y)))
            df.to_csv("\\".join([os.getcwd(), "Game Availability", month, "Wk{}.csv".format(wk)]))

    def availability(self, name, month):
        i = self.df.loc[lambda df: df["Name"] == name].index[0]
        (game, min, max, get) = tuple(
            self.df.loc[i, ["Guest Game", "Min Players", "Max Players", "Guest Invite Number"]])
        for wk in range(1, 5):
            av = {}
            for day in ['Friday', 'Saturday', 'Sunday']:
                a = []
                for h in self.hours:
                    wk_h = "{} (Weekend #{})".format(h, wk)
                    b = int(day in self.df.at[i, wk_h])
                    a.append(b)
                av[day] = a
            pd.DataFrame(av, index=self.hours).to_csv(self.directory + name + '\\' + month + '\\'
                "{} On Weekend #{} (Min = {}, Max = {}, GuestGet = {}).csv".format(game, wk, min, max, get))

    def column_name_transform(self, column):
        if column == 'Timestamp':
            return 'Timestamp'
        elif column == "Email Address":
            return "Email"
        else:
            q_func = lambda q: q.partition(" (")[2].strip(" of the Month)")
            r_func = lambda row: row.partition(' to')[0]
            (q, _, r) = column.partition(" [")
            return "{} [{}]".format(q_func(q), r_func(r.strip("]")))

    def removeDuplicates(self, df):
        emails = set()
        for i in reversed(range(df.shape[0])):
            e = df.at[i, 'Email']
            if e in emails:
                df = df.drop(i)
            else:
                emails.add(e)
        return df



