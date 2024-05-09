from Form import Form
import pandas as pd
import os
from functools import reduce

class GameAvailability (Form):
    def __init__(self):
        self.days = ["Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.weeks = list(range(1, 5))
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]
        self.day_hours_all = lambda duration: reduce(lambda l, day: l + self.day_hours(duration, day), self.days, [])

        name = "GameAvailability"
        set_features = set()
        keys = ["Email"]
        active_pair = None
        multchoice = {}
        linscale = []
        text = []
        checkbox = {}
        checkboxgrid = dict([("Weekend #{}".format(i), (self.days, self.hours)) for i in self.weeks])
        super().__init__(name, set_features, keys, active_pair, multchoice, linscale, text, checkbox, checkboxgrid)

    def availability(self, month, duration):
        for wk in self.weeks:
            wk_df_grid = pd.read_csv("\\".join([os.getcwd(), self.name, "Weekend #{}.csv".format(wk)]), index_col=0)

            def appendDf(df, day):
                day_df = wk_df_grid.loc[:, self.keys + self.day_hours(1, day)]
                start_av = self.startTimeAvailability(day, day_df, duration, self.hours)
                start_hours = self.hours[:(9 - duration)]
                gameDayAv_df = pd.DataFrame(
                    dict([(key, day_df[key]) for key in self.keys] +
                         [("{} [{}]".format(day, h), start_av(h)) for h in start_hours]))
                return df.merge(right=gameDayAv_df, how='inner', on=self.keys)

            gameAv_df = reduce(appendDf, self.days, wk_df_grid.loc[:, self.keys]).reset_index(drop=True)
            gameAv_df.to_csv("x.csv")
            total = dict([("Email", "Total")] + [(col, gameAv_df[col].sum()) for col in self.day_hours_all(duration)])
            gameAv_df = pd.concat([gameAv_df, pd.DataFrame(total, index=[gameAv_df.shape[0]])]).reset_index(drop=True)
            gameAv_df.to_csv(
                "\\".join([os.getcwd(), "GameAvailability", month, "Wk{}_Availability.csv".format(wk)]))

    def q_map(self, q):
        d1 = {"Email Address": "Email", "What is your name?": "Name"}
        game_question = "What times and dates are you available to play the game?"
        weekends = ["Weekend #{}".format(i) for i in self.weeks]
        d2 = dict([("{} ({} of the Month)".format(game_question, wk), wk) for wk in weekends])
        d = d1 | d2
        return d.get(q, q)

    def r_map(self, r):
        return r.partition(' to')[0]

