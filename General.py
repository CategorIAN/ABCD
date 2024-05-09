from Form import Form
import os
import pandas as pd
from functools import reduce

class General (Form):
    def __init__(self):
        self.days = ["Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM", "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM"]
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]
        self.game_duration = {"Codenames": 1, "Catan": 2}
        #=============================================================================================================
        name = "General"
        set_features = {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platforms'}
        keys = ["Email", "Name"]
        active_pair = ("Status", '"I would like to be active in your group." (You will be invited to game events.)')
        mult_choice = \
            {"Status":
                 {'"I would like to be active in your group." (You will be invited to game events.)': 'Active',
                  '"I am not available to participate in games for this season." (You will not be invited to game ' \
                  'events, but you will be asked to update this survey at least once a year.)': 'Not_Now',
                  '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                  'whenever you want.)': 'Please_Remove'}
             }
        linscale = ["Max_Hours"]
        text = ['Guest_Games', 'Guest_Food']
        checkbox = {
            "Games": (dict(list(zip(["Catan", "Chess", "Codenames", "War of the Ring"], 4 * [None]))), False),
            "Game_Types": (dict(list(zip(['Abstract', 'Customizable', 'Family', 'Party', 'Strategy', 'Thematic',
                                         'Wargames'], 7 * [None]))), False),
            "Meals": (dict(list(zip(['Chicken Cacciatore', 'Halibut in Lemon Wine Sauce', 'Hot Crab Dip',
            'Peanut Butter Hummus', 'Quinoa Lentil Berry Salad', 'Rosemary Pork and Mushrooms',
            'Spaghetti and Classic Marinara Sauce', 'Spinach and Artichoke Dip', 'Sweet Potato Casserole'],
                                   9 * [None]))), False),
            "Platforms": (dict(list(zip(['Google Groups [Group Email] (groups.google.com)', 'Evite (evite.com)',
             'Google Chat (chat.google.com)', 'Slack (slack.com)', 'Discord (discord.com)'], 5 * [None]))), False),
            "Allergies": (dict(list(zip(['Gluten', 'Dairy', 'Peanuts', 'Shellfish'], 4 * [None]))), True),
            "Commitment": (dict(list(zip(
                ["Yes, I would be willing to commit to playing a long game over multiple days.",
                 "Yes, I would be willing to participate in a tournament that lasts for multiple days."],
                ["Long Game", "Tournament"]))), False)
        }
        checkboxgrid = {"Availability": (self.days, self.hours)}
        super().__init__(name, set_features, keys, active_pair, mult_choice, linscale, text, checkbox, checkboxgrid)

    def gameDayAv(self, duration):
        av = pd.read_csv("\\".join([os.getcwd(), "General", "Availability.csv"]), index_col=0)
        def f(day):
            day_df = av.loc[:, self.keys + self.day_hours(1, day)]
            start_av = self.startTimeAvailability(day, day_df, duration, self.hours)
            start_hours = self.hours[:(9 - duration)]
            gameDayAv_df = self.concatKeys(
                pd.DataFrame(dict([("{} [{}]".format(day, h), start_av(h)) for h in start_hours])))
            gameDayAv_df.to_csv("\\".join([os.getcwd(), self.name,
                                           "Availability", "{}_{}hrs.csv".format(day, duration)]))
            return gameDayAv_df
        return f

    def gameAv(self, duration):
        dayAv = self.gameDayAv(duration)
        av = pd.read_csv("\\".join([os.getcwd(), self.name, "Availability.csv"]), index_col=0)
        appendDf = lambda df, day: df.merge(right = dayAv(day), how = "inner", on = self.keys)
        gameAv_df = reduce(appendDf, self.days, av[self.keys])
        gameAv_df.to_csv("\\".join([os.getcwd(), self.name, "Availability", "Availability_{}hrs.csv".format(duration)]))
        return gameAv_df

    def newbAv(self, game):
        game_df = pd.read_csv("\\".join([os.getcwd(), "General", "Games.csv"]), index_col=0)
        if game in self.game_duration:
            av_df = pd.read_csv("\\".join([os.getcwd(), "General", "Availability",
                                           "Availability_{}hrs.csv".format(self.game_duration[game])]), index_col=0)
            names = set(game_df[game_df[game] == 1]["Name"]).intersection(self.newbs)
        else:
            raise ValueError("Not Appropriate Game")
        df = av_df[av_df["Name"].isin(names)]
        total = dict([("Name", "Total")] + [(col, df[col].sum()) for col in av_df.columns[2:]])
        df = pd.concat([df, pd.DataFrame(total, index=[df.shape[0]])]).reset_index(drop=True)
        df.to_csv("\\".join([os.getcwd(), self.name, "Availability", "NewbAv_{}.csv".format(game)]))
        return df

    newbs = {
        "Alicia Clark",
        "Amber",
        "Andrew Cilker",
        "Bryce Morrow",
        "Caleb Baker",
        "Dan Ray",
        "Emily Warren",
        "Ethan Skelton",
        "Helen Paris",
        "Jared May",
        "Jazzy",
        "Jessie Troester",
        "Josh Bartling",
        "Leeza Burkland",
        "Lilly Ball",
        "Natasha Gesker",
        "Nolan Jenko",
        "Tadeo Zuniga",
        "Valerie Livingston"
    }

    def q_map(self, q):
        col_mapping = {
            "Timestamp": "Timestamp",
            # ===========================
            "Email Address": "Email",
            # ===========================
            "What is your name?": "Name",
            # ===========================
            "You are currently in my tabletop gaming group. What would you like your status to be? " \
            "(If you pick the second or third option, you may skip the rest of the questions in this survey.)": "Status",
            # ===========================
            "Every invite you receive for a game event brings you down the queue, making you less likely to be invited " \
            "to the next game event. Therefore, it is important I know what games you are interested in playing. " \
            "Which of my games are you interested in playing?": "Games",
            # ===========================
            "What types of games do you enjoy playing?": "Game_Types",
            # ===========================
            "What is the maximum number of hours you are willing to play a game in one sitting?": "Max_Hours",
            # ===========================
            "Would you be willing to a game commitment over multiple days?": "Commitment",
            # ===========================
            "Are there games that you own and know how to play that you would enjoy bringing the game for game events? " \
            "If so, which games would you enjoy bringing? (You would be responsible for bringing the game and explaining " \
            "the rules.)": "Guest_Games",
            # ===========================
            "Which of my signature meals would you be willing to eat at events?": 'Meals',
            # ===========================
            "What are your food allergies?": "Allergies",
            # ===========================
            "What food and/or drinks would you be willing to bring to a gaming event?": "Guest_Food",
            # ===========================
            "It is efficient to communicate using a group communication platform for invitations and coordination " \
            "of details for events. Which of these group communication platforms would you be willing to use?": "Platforms",
            "What times are you possibly available to play games?": "Availability"
        }
        return col_mapping.get(q, q)

    def r_map(self, r):
        return r.partition(' to')[0]

    def readPersonalFile(self, name):
        for csv_name in self.csvs:
            print((20 * "=") + csv_name + (20 * "="))
            df = pd.read_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(csv_name)]), index_col=0)
            name_df = df[df["Name"] == name].iloc[:, 2:].reset_index(drop=True)
            if len(name_df.index) > 0:
                for col in name_df.columns:
                    print((10 * "-") + col + (10 * "-"))
                    print(name_df[col][0])
                    input("Continue?")





