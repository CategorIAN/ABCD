from Form import Form
import os
import pandas as pd
from functools import reduce

class General (Form):
    def __init__(self):
        self.days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        self.hours = ["11:00 AM", "12:00 PM", "1:00 PM", "2:00 PM",
                      "3:00 PM", "4:00 PM", "5:00 PM", "6:00 PM", "7:00 PM"]
        self.day_hours = lambda duration, day: ["{} [{}]".format(day, hour) for hour in self.hours][:(9 - duration)]
        self.game_duration = {"Codenames": 1, "Catan": 2}
        #=============================================================================================================
        name = "General"
        keys = ["Email", "Name"]
        active_pair = ("Status", '"I would like to be active in your group." (You will be invited to game events.)')
        mult_choice = \
            {"Status":
                 {'"I would like to be active in your group."': 'Active',
                  '"I am not available to participate in games for this season."': 'Not_Now',
                  '"I would like to be taken off of this gaming list."': 'Please_Remove'},
             "Earliest_Invite": self.createDict(["1 Month", "3 Weeks", "2 Weeks", "1 Week", "3 Days", "24 Hours"]),
             "Latest_Invite": self.createDict(["1 Month", "3 Weeks", "2 Weeks", "1 Week", "3 Days", "24 Hours"])
             }
        linscale = ["Max_Hours"]
        text = ['Guest_Games', 'Guest_Food']
        checkbox = {
            "Location": self.createDict(["My Home", "Board Game Store", "Coffee Shop", "Public Library"]),
            "Games": self.createDict(["Catan", "Codenames", "The Lord of the Rings: The Card Game",
                                      "The Witcher: Old World", "War of the Ring", "Zombicide: Black Plague"]),
            "Game_Types": self.createDict(['Abstract','Customizable','Family','Party','Strategy','Thematic','Wargames']),
            "Meals": self.createDict(['Halibut in Lemon Wine Sauce', 'Hot Crab Dip', 'Peanut Butter Hummus',
                                      'Rosemary Pork and Mushrooms', 'Spaghetti and Classic Marinara Sauce',
                                      'Spinach Dip', 'Sweet Potato Casserole']),
            "Platforms": self.createDict(['Google Groups', 'Google Chat', 'Slack',
                                          'Discord', 'Facebook Groups', 'Meetup']),
            "Allergies": self.createDict(['Gluten', 'Dairy', 'Peanuts', 'Shellfish']),
            "Commitment": self.createDict(
                ['"I would be willing to commit to playing a long game over multiple days."',
                 '"I would be willing to participate in a tournament that lasts for multiple days."'],
                ["Long Game", "Tournament"])
        }
        checkboxgrid = {"Availability": (self.days, self.hours)}
        super().__init__(name, keys, active_pair, mult_choice, linscale, text, checkbox, checkboxgrid)

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
            "You are currently in my tabletop gaming group. What would you like your status to be?": "Status",
            # ===========================
            "Where would you be willing to play games?": "Location",
            # ===========================
            "What times are you possibly available to play games?": "Availability",
            # ===========================
            "Which of my games are you interested in playing?": "Games",
            # ===========================
            "What types of games do you enjoy playing?": "Game_Types",
            # ===========================
            "What is the maximum number of hours you are willing to play a game in one sitting?": "Max_Hours",
            # ===========================
            "Would you be willing to partake in a game commitment over multiple days?": "Commitment",
            # ===========================
            "Are there games that you own and know how to play that you would enjoy bringing for game events?"
            " If so, which games would you enjoy bringing?": "Guest_Games",
            # ===========================
            "Which of my meals would you be willing to eat at events?": 'Meals',
            # ===========================
            "What are your food allergies and restrictions?": "Allergies",
            # ===========================
            "What food and/or drinks would you be willing to bring to gaming events?": "Guest_Food",
            # ===========================
            "What is the earliest time you would like to receive invitations to game events?": "Earliest_Invite",
            # ===========================
            "What is the latest time you would like to receive invitations to game events?": "Latest_Invite",
            # ===========================
            "Which of these group communication platforms would you be willing to use?": "Platforms"
        }
        return col_mapping.get(self.trim_paren(q), self.trim_paren(q))

    def r_map(self, r):
        return r.partition(' to')[0]

    def readPersonalFile(self, name):
        for csv_name in self.dfs.keys():
            print((20 * "=") + csv_name + (20 * "="))
            df = pd.read_csv("\\".join([os.getcwd(), self.name, "{}.csv".format(csv_name)]), index_col=0)
            name_df = df[df["Name"] == name].iloc[:, 2:].reset_index(drop=True)
            if len(name_df.index) > 0:
                for col in name_df.columns:
                    print((10 * "-") + col + (10 * "-"))
                    print(name_df[col][0])
                    input("Continue?")





