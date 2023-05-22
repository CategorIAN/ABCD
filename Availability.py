import os


class Availability:
    def __init__(self, df):
        self.df = df
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]

    def dayAvailability(self):
        day_hours = lambda day: ["{} [{}]".format(day, hour) for hour in self.hours]
        for day in ["Friday", "Saturday", "Sunday"]:
            day_df = self.df.loc[:, ["Name"] + day_hours(day)].rename(self.militaryTime(day), axis=1)
            day_df.to_csv(os.getcwd() + '\\' + "ABCD" + '\\' + "Availability" + "\\" + "{}.csv".format(day))

    def militaryTime(self, day):
        def f(column):
            if column == "{} [11:00 AM]".format(day):
                return "11"
            if column == "{} [12:00 PM]".format(day):
                return "12"
            if column == "{} [1:00 PM]".format(day):
                return "13"
            if column == "{} [2:00 PM]".format(day):
                return "14"
            if column == "{} [3:00 PM]".format(day):
                return "15"
            if column == "{} [4:00 PM]".format(day):
                return "16"
            if column == "{} [5:00 PM]".format(day):
                return "17"
            if column == "{} [6:00 PM]".format(day):
                return "18"
        return f