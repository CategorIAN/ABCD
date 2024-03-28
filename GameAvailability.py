from Form import Form
import pandas as pd
import os
from functools import reduce

class GameAvailability (Form):
    def __init__(self):
        self.days = ["Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.military_hours = lambda duration: ["11", "12", "13", "14", "15", "16", "17", "18"][:(9 - duration)]
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]
        self.weeks = list(range(0+1, 4+1))


        name = "GameAvailability"
        col_mapping = {
            "Timestamp": "Timestamp",
            # ===========================
            "Email Address": "Email",
            # ===========================
        }
        grid_col_mapping = {
            "What times and dates are you available to play the game? (Weekend #1 of the Month)": (
                "Weekend #1", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to play the game? (Weekend #2 of the Month)": (
                "Weekend #2", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to play the game? (Weekend #3 of the Month)": (
                "Weekend #3", lambda row: row.partition(' to')[0]),
            "What times and dates are you available to play the game? (Weekend #4 of the Month)": (
                "Weekend #4", lambda row: row.partition(' to')[0])
        }
        set_features = set()
        #keys = ["Email", "Name"]
        keys = ["Email"]
        make_active = False
        multchoice_cols = []
        multchoice_optset = []
        multchoice_newoptset = []
        linscale_cols = []
        text_cols = []
        checkbox_cols = []
        checkbox_optset = []
        checkbox_newoptset = []
        otherset = []
        checkboxgrid_cols = ["Weekend #{}".format(i) for i in self.weeks]
        checkboxgrid_coloptset = [self.days for i in self.weeks]
        checkboxgrid_rowoptset = [self.hours for i in self.weeks]
        #mergeTuple = ("General", ["Email Address", "What is your name?"], "Email Address")
        mergeTuple = None
        super().__init__(name, col_mapping, grid_col_mapping, set_features, keys,
                         make_active, multchoice_cols, multchoice_optset, multchoice_newoptset,
                         linscale_cols, text_cols, checkbox_cols, checkbox_optset, checkbox_newoptset, otherset,
                         checkboxgrid_cols, checkboxgrid_coloptset, checkboxgrid_rowoptset, mergeTuple)

    def toMilitary(self, duration, day):
        return dict(list(zip(self.day_hours(duration, day), self.military_hours(duration))))

    def toStandard(self, duration, day):
        return dict(list(zip(self.military_hours(duration), self.day_hours(duration, day))))

    def startTimeAvailability(self, df, duration):
        def f(hour):
            hours = [str(h) for h in range(int(hour), int(hour) + duration)]
            return df.index.map(lambda i: df.loc[i, hours].product())
        return f

    def availability(self, month, duration):
        for wk in self.weeks:
            wk_df_grid = pd.read_csv("\\".join([os.getcwd(), self.name, "Weekend #{}.csv".format(wk)]), index_col=0)

            def appendDf(df, day):
                day_df = wk_df_grid.loc[:, self.keys + self.day_hours(1, day)].rename(self.toMilitary(1, day), axis=1)
                start_av = self.startTimeAvailability(day_df, duration)
                start_hours = self.military_hours(duration)
                gameDayAv_df = pd.DataFrame(
                    dict([(key, day_df[key]) for key in self.keys] + [(h, start_av(h)) for h in start_hours]))
                gameDayAv_df = gameDayAv_df.rename(self.toStandard(duration, day), axis=1)
                return df.merge(right=gameDayAv_df, how='inner', on=self.keys)

            gameAv_df = reduce(appendDf, self.days, wk_df_grid.loc[:, self.keys]).reset_index(drop=True)
            total = dict([("Name", "Total")] + [(col, gameAv_df[col].sum()) for col in gameAv_df.columns[3:]])
            gameAv_df = pd.concat([gameAv_df, pd.DataFrame(total, index=[gameAv_df.shape[0]])]).reset_index(drop=True)
            gameAv_df.to_csv(
                "\\".join([os.getcwd(), "GameAvailability", month, "Wk{}_Availability.csv".format(wk)]))