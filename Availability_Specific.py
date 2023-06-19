import os
import pandas as pd
from itertools import product
from functools import reduce

class Availability_Specific:
    def __init__(self):
        self.form_hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
             "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]
        self.hours = pd.Series(self.form_hours).map(lambda h: h.partition(' to')[0])
        self.military_hours = lambda duration: ["11", "12", "13", "14", "15", "16", "17", "18"][:(9 - duration)]
        self.days = ["Friday", "Saturday", "Sunday"]
        self.wk_hours = lambda wk: ["Weekend #{} [{}]".format(wk, h) for h in self.hours]
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]

        df = pd.read_csv("\\".join([os.getcwd(), 'Raw Data', "GA.csv"]))
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

    def toMilitary(self, duration, day):
        return dict([("Name", "Name")] + list(zip(self.day_hours(duration, day), self.military_hours(duration))))

    def toStandard(self, duration, day):
        return dict([("Name", "Name")] + list(zip(self.military_hours(duration), self.day_hours(duration, day))))

    def gameAv(self, duration):
        dayAv = self.gameDayAv(duration)
        av = pd.read_csv("\\".join([os.getcwd(), 'ABCD', "ABCD_availability.csv"]), index_col=0)
        def appendDf(df, day):
            gameDayAv = dayAv(day).rename(self.toStandard(duration, day), axis=1)
            return df.merge(right = gameDayAv, how = 'inner', on = 'Name')
        gameAv_df = reduce(appendDf, self.days, av["Name"].to_frame())
        gameAv_df.to_csv("\\".join([os.getcwd(), "ABCD", "Availability", "Availability_{}hrs.csv".format(duration)]))
        return gameAv_df

    def startTimeAvailability(self, df, duration):
        def f(hour):
            hours = [str(h) for h in range(int(hour), int(hour) + duration)]
            return df.index.map(lambda i: df.loc[i, hours].product())
        return f

    def availability(self, names, month, duration):
        index = self.df.loc[lambda df: df["Name"].isin(names)].index
        for wk in range(1, 5):
            wk_hours = self.wk_hours(wk)
            wk_df = self.df.loc[index, ["Email", "Name"] + wk_hours].rename(dict(list(zip(wk_hours, self.hours))), axis=1)
            x = pd.Series(product(self.days, self.hours))
            y = x.map(lambda dh: ("{} [{}]".format(dh[0], dh[1]), wk_df[dh[1]].map(lambda s: int(dh[0] in s))))
            wk_df_grid = pd.DataFrame(dict([(fix, wk_df[fix]) for fix in ['Email', 'Name']] + list(y)))
            wk_df_grid.to_csv("\\".join([os.getcwd(), "Game Availability", month, "Wk{}.csv".format(wk)]))
            def appendDf(df, day):
                print("=====df=========")
                print(df)
                day_df = wk_df_grid.loc[:, ["Email"] + self.day_hours(1, day)].rename(self.toMilitary(1, day), axis=1)
                start_av = self.startTimeAvailability(day_df, duration)
                start_hours = self.military_hours(duration)
                gameDayAv_df = pd.DataFrame(dict([("Email", day_df["Email"])] + [(h, start_av(h)) for h in start_hours]))
                gameDayAv_df = gameDayAv_df.rename(self.toStandard(duration, day), axis=1)
                return df.merge(right=gameDayAv_df, how='inner', on='Email')
            print(wk_df_grid.loc[:, ["Email", "Name"]])
            gameAv_df = reduce(appendDf, self.days, wk_df_grid.loc[:, ["Email", "Name"]]).reset_index(drop=True)
            print("df:")
            print(gameAv_df)
            total = dict([("Name", "Total")] + [(col, gameAv_df[col].sum()) for col in gameAv_df.columns[2:]])
            gameAv_df = pd.concat([gameAv_df, pd.DataFrame(total, index=[gameAv_df.shape[0]])])
            gameAv_df.to_csv("\\".join([os.getcwd(), "Game Availability", month, "Wk{}_Availability.csv".format(wk)]))

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



