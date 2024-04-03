from Form import Form
import pandas as pd
import os
from functools import reduce

class GameAvailability (Form):
    def __init__(self):
        self.days = ["Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.weeks = list(range(1, 5))
        self.nextHour = self.nextMap(self.hours)
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]
        self.day_hours_all = lambda duration: reduce(lambda l, day: l + self.day_hours(duration, day), self.days, [])

        name = "GameAvailability"
        set_features = set()
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
        checkboxgrid_dict = dict([("Weekend #{}".format(i), (self.days, self.hours)) for i in self.weeks])
        super().__init__(name, self.q_map(), self.r_map(), set_features, keys,
                         make_active, multchoice_cols, multchoice_optset, multchoice_newoptset,
                         linscale_cols, text_cols, checkbox_cols, checkbox_optset, checkbox_newoptset, otherset,
                         checkboxgrid_dict)

    def nextMap(self, times):
        def go(map, current, remaining):
            if len(remaining) == 0:
                return map
            else:
                next = remaining[0]
                return go(map | {current: next}, next, remaining[1:])
        return go({}, times[0], times[1:]) if len(times) > 0 else {}

    def startTimeAvailability(self, day, df, duration):
        def cont_hours(hours, current, remaining):
            if remaining == 0:
                return hours
            else:
                next = self.nextHour[current]
                return cont_hours(hours + [next], next, remaining - 1)
        def f(hour):
            day_hours = ["{} [{}]".format(day, hr) for hr in cont_hours([hour], hour, duration - 1)]
            return df.index.map(lambda i: df.loc[i, day_hours].product())
        return f

    def availability(self, month, duration):
        for wk in self.weeks:
            wk_df_grid = pd.read_csv("\\".join([os.getcwd(), self.name, "Weekend #{}.csv".format(wk)]), index_col=0)

            def appendDf(df, day):
                day_df = wk_df_grid.loc[:, self.keys + self.day_hours(1, day)]
                start_av = self.startTimeAvailability(day, day_df, duration)
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

    def q_map(self):
        d1 = {"Email Address": "Email", "What is your name?": "Name"}
        game_question = "What times and dates are you available to play the game?"
        weekends = ["Weekend #{}".format(i) for i in self.weeks]
        d2 = dict([("{} ({} of the Month)".format(game_question, wk), wk) for wk in weekends])
        d = d1 | d2
        return lambda q: d.get(q, q)

    def r_map(self):
        return lambda r: r.partition(' to')[0]

