#=====Python Packages===================
import pandas as pd
import os
from itertools import product
#======My Classes=======================
from Form import Form


class General (Form):
    def __init__(self, df):
        #==============================================================================================================
        self.hours = ["11:00 AM to 12:00 PM", "12:00 PM to 1:00 PM", "1:00 PM to 2:00 PM", "2:00 PM to 3:00 PM",
                      "3:00 PM to 4:00 PM", "4:00 PM to 5:00 PM", "5:00 PM to 6:00 PM", "6:00 PM to 7:00 PM"]
        self.days = ['Friday', 'Saturday', 'Sunday']
        keys = ['Email', 'Name']
        #=============================================================================================================
        set_features = {'Games', 'Game_Types', 'Meals', 'Allergies', 'Platforms'}
        super().__init__('ABCD', df, keys, set_features)

        #Mult Choice/Create Active======================================================================================
        status = {'"I would like to be active in your group." (You will be invited to game events.)': 'Active',
                '"I am not available to participate in games for this season." (You will not be invited to game ' \
                'events, but you will be asked to update this survey at least once a year.)': 'Not_Now',
                '"I would like to be taken off of this gaming list." (You can ask to be put back on this list ' \
                'whenever you want.)': 'Please_Remove'}
        mult_choice_cols = ["Status"]
        active = self.filtered("Status", "Active", status)
        for (col, mapping) in zip(mult_choice_cols, [status]):
            self.mult_choice(col, mapping)
        #Linear Scale===================================================================================================
        lin_scale_cols = ["Max_Hours"]
        for col in lin_scale_cols:
            self.linear_scale(col, active)
        #Checkbox Grid=====================================================
        self.checkbox_grid(self.days, self.hours, lambda c: c, self.time_func, active, "Availability")
        #Checkbox==========================================================
        checkbox_cols = ["Games", "Game_Types", "Meals", "Platforms", "Allergies", "Commitment"]
        opt_set = [
                    ['Catan', 'Chess', 'Codenames', 'War of the Ring'], #Games
                    ['Abstract', 'Customizable', 'Family', 'Party', 'Strategy', 'Thematic', 'Wargames'], #Game_Types
                    ['Chicken Cacciatore', 'Halibut in Lemon Wine Sauce', 'Hot Crab Dip', 'Peanut Butter Hummus',
                        'Quinoa Lentil Berry Salad', 'Rosemary Pork and Mushrooms','Spaghetti and Classic Marinara Sauce',
                        'Spinach and Artichoke Dip', 'Sweet Potato Casserole'], #Meals
                    ['Google Groups [Group Email] (groups.google.com)', 'Evite (evite.com)',
                        'Google Chat (chat.google.com)', 'Slack (slack.com)', 'Discord (discord.com)'], #Platforms
                    ['Gluten', 'Dairy', 'Peanuts', 'Shellfish'], #Allergies
                    ["Yes, I would be willing to commit to playing a long game over multiple days.",
                        "Yes, I would be willing to participate in a tournament that lasts for multiple days."], #Commitment
        ]
        new_opt_set = [None, None, None, None, None, ["Long Game", "Tournament"]]
        other_set = [False, False, False, False, True, False]
        for (col, options, transformed, other) in zip(checkbox_cols, opt_set, new_opt_set, other_set):
            self.checkbox(col, options, transformed, other, active)
        #Long Answer=======================================================
        for col in ['Guest_Games', 'Guest_Food']:
            self.long_ans(col, active)

    def column_name_transform(self, column):
        if column == "Timestamp":
            return "Timestamp"
        if column == "Email Address":
            return 'Email'
        if column == "What is your name?":
            return 'Name'
        if column == 'You are currently in my tabletop gaming group. What would you like your status to be? ' \
             '(If you pick the second or third option, you may skip the rest of the questions in this survey.)':
            return 'Status'
        if column == 'Every invite you receive for a game event brings you down the queue, making you less likely to be invited ' \
            'to the next game event. Therefore, it is important I know what games you are interested in playing. ' \
            'Which of my games are you interested in playing?':
            return 'Games'
        if column == 'What types of games do you enjoy playing?':
            return 'Game_Types'
        if column == 'What is the maximum number of hours you are willing to play a game in one sitting?':
            return 'Max_Hours'
        if column == 'Would you be willing to a game commitment over multiple days?':
            return 'Commitment'
        if column == 'Are there games that you own and know how to play that you would enjoy bringing the game for game events? ' \
          'If so, which games would you enjoy bringing? (You would be responsible for bringing the game and explaining ' \
          'the rules.)':
            return 'Guest_Games'
        if column == 'Which of my signature meals would you be willing to eat at events?':
            return 'Meals'
        if column == 'What are your food allergies?':
            return 'Allergies'
        if column == 'What food and/or drinks would you be willing to bring to a gaming event?':
            return 'Guest_Food'
        if column == 'It is efficient to communicate using a group communication platform for invitations and coordination ' \
               'of details for events. Which of these group communication platforms would you be willing to use?':
            return 'Platforms'
        else:
            question = "What times are you possibly available to play games?"
            return dict([("{} [{}]".format(question, row), self.time_func(row)) for row in self.hours])[column]





