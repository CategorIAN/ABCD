import os
import pandas as pd
from functools import reduce


class Availability:
    def __init__(self, df):
        self.df = df
        self.days = ["Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.military_hours = lambda duration: ["11", "12", "13", "14", "15", "16", "17", "18"][:(9 - duration)]
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]

    def toMilitary(self, duration, day):
        return dict([("Name", "Name")] + list(zip(self.day_hours(duration, day), self.military_hours(duration))))

    def toStandard(self, duration, day):
        return dict([("Name", "Name")] + list(zip(self.military_hours(duration), self.day_hours(duration, day))))

    def dayAvailability(self):
        for day in ["Friday", "Saturday", "Sunday"]:
            day_df = self.df.loc[:, ["Name"] + self.day_hours(1, day)].rename(self.toMilitary(1, day), axis=1)
            day_df.to_csv("\\".join([os.getcwd(), "ABCD", "Availability", "{}.csv".format(day)]))

    def gameDayAv(self, duration):
        def f(day):
            day_df = pd.read_csv("\\".join([os.getcwd(), "ABCD", "Availability", "{}.csv".format(day)]), index_col=0)
            def person_av(hour):
                hours = [str(h) for h in range(int(hour), int(hour) + duration)]
                return day_df.index.map(lambda i: day_df.loc[i, hours].product())
            start_hours = self.military_hours(duration)
            gameDayAv_df = pd.DataFrame(dict([("Name", day_df["Name"])] + [(h, person_av(h)) for h in start_hours]))
            gameDayAv_df.to_csv("\\".join([os.getcwd(), "ABCD", "Availability", "{}_{}hrs.csv".format(day, duration)]))
            return gameDayAv_df
        return f

    def gameAv(self, duration):
        dayAv = self.gameDayAv(duration)
        def appendDf(df, day):
            gameDayAv = dayAv(day).rename(self.toStandard(duration, day), axis=1)
            return df.merge(right = gameDayAv, how = 'inner', on = 'Name')
        gameAv_df = reduce(appendDf, self.days, self.df["Name"].to_frame())
        gameAv_df.to_csv("\\".join([os.getcwd(), "ABCD", "Availability", "Availability_{}hrs.csv".format(duration)]))
        return gameAv_df


