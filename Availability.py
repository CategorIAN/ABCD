import os
import pandas as pd


class Availability:
    def __init__(self, df):
        self.df = df
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.military_hours = ["11", "12", "13", "14", "15", "16", "17", "18"]

    def dayAvailability(self):
        day_hours = lambda day: ["{} [{}]".format(day, hour) for hour in self.hours]
        toMilitary = dict([("Name", "Name")] + list(zip(self.hours, self.military_hours)))
        for day in ["Friday", "Saturday", "Sunday"]:
            day_df = self.df.loc[:, ["Name"] + day_hours(day)].rename(toMilitary, axis=1)
            day_df.to_csv("\\".join([os.getcwd(), "ABCD", "Availability", "{}.csv".format(day)]))

    def gameAvailability(self, day, duration):
        day_df = pd.read_csv("\\".join([os.getcwd(), "ABCD", "Availability", "{}.csv".format(day)]), index_col=0)
        start_hours = self.military_hours[:(9-duration)]
        def person_availability(hour):
            hours = [str(h) for h in range(int(hour), int(hour) + duration)]
            x = day_df.index.map(lambda i: day_df.loc[i, hours].product())
            return x
        return person_availability("17")

